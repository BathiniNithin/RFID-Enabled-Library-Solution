import asyncio
from contextlib import suppress
from fastapi import FastAPI, APIRouter, Query, HTTPException
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ASCENDING, DESCENDING, IndexModel
import os
import logging
from pathlib import Path
from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional, Any
import uuid
from datetime import datetime, timezone, date, timedelta


ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# MongoDB connection
mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

INDEXES: dict[str, list[IndexModel]] = {
    "books": [IndexModel([("id", ASCENDING)], name="id", unique=True), IndexModel([("isbn", ASCENDING)], name="isbn", unique=True)],
    "book_copies": [IndexModel([("id", ASCENDING)], name="id", unique=True), IndexModel([("accession_number", ASCENDING)], name="accession_number", unique=True)],
    "members": [IndexModel([("id", ASCENDING)], name="id", unique=True), IndexModel([("member_code", ASCENDING)], name="member_code", unique=True)],
    "rfid_tags": [IndexModel([("rfid_uid", ASCENDING)], name="rfid_uid", unique=True)],
    "loans": [IndexModel([("id", ASCENDING)], name="id", unique=True)],
}


async def ensure_indexes() -> None:
    for collection, models in INDEXES.items():
        for model in models:  # one at a time so a bad spec skips only itself
            try:
                await db[collection].create_indexes([model])
            except Exception as exc:  # never block boot on an index; the log line names what to fix
                logging.getLogger(__name__).error("ensure_indexes(%s.%s): %s", collection, model.document["name"], exc)

# Create the main app without a prefix
app = FastAPI()

# Create a router with the /api prefix
api_router = APIRouter(prefix="/api")


class BookCreate(BaseModel):
    title: str; isbn: str; author: str; publisher: str = ""; category: str = "General"; publication_year: int = 2024
class CopyCreate(BaseModel):
    book_id: str; accession_number: str; shelf_location: str; status: str = "Available"
class MemberCreate(BaseModel):
    member_code: str; full_name: str; email: str = ""; phone: str = ""; department: str = ""; membership_status: str = "Active"; outstanding_fine: float = 0
class TagCreate(BaseModel):
    rfid_uid: str; copy_id: str; tag_status: str = "Active"
class LoanCreate(BaseModel):
    member_id: str; copy_id: str; due_date: str
class InventoryCreate(BaseModel):
    session_name: str; shelf_location: str
class UpdatePayload(BaseModel):
    data: dict[str, Any]
class RenewPayload(BaseModel):
    due_date: Optional[str] = None
class ScanPayload(BaseModel):
    rfid_uid: str

COLLECTIONS = {"books", "book_copies", "members", "loans", "rfid_tags", "inventory_sessions", "inventory_scans", "audit_logs", "system_settings"}
def now(): return datetime.now(timezone.utc).isoformat()
async def write_audit(action: str, entity_type: str, entity_id: str):
    await db.audit_logs.insert_one({"id": str(uuid.uuid4()), "user_id": "staff", "action": action, "entity_type": entity_type, "entity_id": entity_id, "timestamp": now()})
async def safe_list(collection: str, limit: int = 500):
    return await db[collection].find({}, {"_id": 0}).sort("created_at", -1).limit(limit).to_list(limit)
async def refresh_overdues():
    today = date.today().isoformat()
    active = await db.loans.find({"status": "Active"}, {"_id": 0}).to_list(1000)
    for loan in active:
        if loan.get("due_date", "") < today:
            days = max(1, (date.today() - date.fromisoformat(loan["due_date"])).days)
            fine = round(days * 0.5, 2)
            await db.loans.update_one({"id": loan["id"]}, {"$set": {"status": "Overdue", "fine_amount": fine}})
            await db.members.update_one({"id": loan["member_id"]}, {"$set": {"outstanding_fine": fine}})

@api_router.get("/")
async def root():
    return {"message": "Bibliotex API online"}

@api_router.get("/dashboard")
async def dashboard():
    await refresh_overdues()
    books, copies, members, loans = [await db[c].count_documents({}) for c in ("books", "book_copies", "members", "loans")]
    active = await db.loans.count_documents({"status": {"$in": ["Active", "Overdue"]}}); overdue = await db.loans.count_documents({"status": "Overdue"})
    return {"books": books, "copies": copies, "members": members, "active_loans": active, "issued_copies": await db.book_copies.count_documents({"status": "On loan"}), "overdue": overdue, "available_copies": await db.book_copies.count_documents({"status": "Available"}), "recent_loans": await db.loans.find({}, {"_id": 0}).sort("issue_date", -1).limit(6).to_list(6)}

@api_router.get("/reports")
async def reports():
    await refresh_overdues()
    return {"issued": await db.loans.count_documents({"status": {"$in": ["Active", "Overdue"]}}), "overdue": await db.loans.count_documents({"status": "Overdue"}), "loan_history": await db.loans.find({}, {"_id": 0}).sort("issue_date", -1).limit(100).to_list(100), "inventory": await db.inventory_sessions.find({}, {"_id": 0}).sort("started_at", -1).limit(20).to_list(20)}

@api_router.get("/{collection}")
async def list_collection(collection: str, q: Optional[str] = Query(None), limit: int = Query(500, le=1000)):
    if collection not in COLLECTIONS: raise HTTPException(404, "Collection not found")
    if collection == "loans": await refresh_overdues()
    docs = await safe_list(collection, limit)
    if q:
        needle = q.lower(); docs = [d for d in docs if needle in " ".join(str(v) for v in d.values()).lower()]
    return docs

@api_router.post("/books")
async def create_book(item: BookCreate): return await create_entity("books", item.model_dump(), "Book")
@api_router.post("/book_copies")
async def create_copy(item: CopyCreate): return await create_entity("book_copies", item.model_dump(), "Copy")
@api_router.post("/members")
async def create_member(item: MemberCreate): return await create_entity("members", item.model_dump(), "Member")
@api_router.post("/rfid_tags")
async def create_tag(item: TagCreate): return await create_entity("rfid_tags", item.model_dump(), "RFID tag")
@api_router.put("/{collection}/{item_id}")
async def update_entity(collection: str, item_id: str, item: UpdatePayload):
    if collection not in {"books", "book_copies", "members", "rfid_tags"}: raise HTTPException(404, "Collection not found")
    allowed = set(item.data) - {"id", "created_at", "_id"}
    values = {k: v for k, v in item.data.items() if k in allowed}
    if not values: raise HTTPException(400, "No changes supplied")
    try: result = await db[collection].update_one({"id": item_id}, {"$set": values})
    except Exception as exc: raise HTTPException(400, "Could not save changes. Check unique fields.") from exc
    if not result.matched_count: raise HTTPException(404, "Record not found")
    await write_audit("Updated", collection, item_id)
    return await db[collection].find_one({"id": item_id}, {"_id": 0})

@api_router.get("/members/{member_id}/loans")
async def member_loans(member_id: str):
    await refresh_overdues(); return await db.loans.find({"member_id": member_id}, {"_id": 0}).sort("issue_date", -1).to_list(100)

@api_router.get("/rfid/scan/{rfid_uid}")
async def scan_rfid(rfid_uid: str):
    tag = await db.rfid_tags.find_one({"rfid_uid": rfid_uid}, {"_id": 0})
    if not tag: raise HTTPException(404, "Unknown RFID tag")
    copy = await db.book_copies.find_one({"id": tag["copy_id"]}, {"_id": 0}); book = await db.books.find_one({"id": copy.get("book_id")} if copy else {}, {"_id": 0})
    return {"tag": tag, "copy": copy, "book": book}
async def create_entity(collection: str, data: dict, label: str):
    data.update({"id": str(uuid.uuid4()), "created_at": now()})
    response = dict(data)
    try: await db[collection].insert_one(data)
    except Exception as exc: raise HTTPException(400, f"{label} could not be saved. Check unique fields.") from exc
    await write_audit("Created", label, response["id"]); return response

@api_router.post("/loans")
async def issue_loan(item: LoanCreate):
    copy = await db.book_copies.find_one({"id": item.copy_id}, {"_id": 0}); member = await db.members.find_one({"id": item.member_id}, {"_id": 0})
    if not copy or not member: raise HTTPException(404, "Member or copy not found")
    if member.get("membership_status") != "Active": raise HTTPException(400, "Blocked members cannot borrow books")
    if copy.get("status") != "Available": raise HTTPException(400, "This copy is not available")
    if await db.loans.count_documents({"member_id": item.member_id, "copy_id": item.copy_id, "status": {"$in": ["Active", "Overdue"]}}): raise HTTPException(400, "This member already has an active issue for this copy")
    loan = {"id": str(uuid.uuid4()), "member_id": item.member_id, "member_name": member["full_name"], "copy_id": item.copy_id, "accession_number": copy["accession_number"], "issue_date": now(), "due_date": item.due_date, "return_date": None, "status": "Active", "fine_amount": 0}
    response = dict(loan)
    await db.loans.insert_one(loan); await db.book_copies.update_one({"id": item.copy_id}, {"$set": {"status": "On loan"}}); await write_audit("Issued", "Loan", response["id"]); return response

@api_router.post("/loans/{loan_id}/return")
async def return_loan(loan_id: str):
    loan = await db.loans.find_one({"id": loan_id}, {"_id": 0})
    if not loan: raise HTTPException(404, "Loan not found")
    returned = now(); await db.loans.update_one({"id": loan_id}, {"$set": {"status": "Returned", "return_date": returned}}); await db.book_copies.update_one({"id": loan["copy_id"]}, {"$set": {"status": "Available"}}); await write_audit("Returned", "Loan", loan_id)
    return {**loan, "status": "Returned", "return_date": returned}

@api_router.post("/loans/{loan_id}/renew")
async def renew_loan(loan_id: str, item: RenewPayload):
    loan = await db.loans.find_one({"id": loan_id}, {"_id": 0})
    if not loan or loan.get("status") not in {"Active", "Overdue"}: raise HTTPException(400, "Only active loans can be renewed")
    due = item.due_date or (date.today() + timedelta(days=21)).isoformat()
    await db.loans.update_one({"id": loan_id}, {"$set": {"due_date": due, "status": "Active", "fine_amount": 0}}); await write_audit("Renewed", "Loan", loan_id)
    return {**loan, "due_date": due, "status": "Active", "fine_amount": 0}

@api_router.post("/inventory_sessions")
async def create_inventory(item: InventoryCreate):
    return await create_entity("inventory_sessions", {**item.model_dump(), "started_at": now(), "status": "In progress", "scanned_count": 0}, "Inventory session")

@api_router.post("/inventory_sessions/{session_id}/scan")
async def inventory_scan(session_id: str, item: ScanPayload):
    session = await db.inventory_sessions.find_one({"id": session_id}, {"_id": 0})
    if not session: raise HTTPException(404, "Inventory session not found")
    tag = await db.rfid_tags.find_one({"rfid_uid": item.rfid_uid}, {"_id": 0}); copy = await db.book_copies.find_one({"id": tag["copy_id"]}, {"_id": 0}) if tag else None
    status = "Found" if copy and copy.get("shelf_location") == session.get("shelf_location") else ("Misplaced" if copy else "Unknown")
    scan = {"id": str(uuid.uuid4()), "session_id": session_id, "copy_id": copy.get("id") if copy else None, "rfid_uid": item.rfid_uid, "scan_status": status, "scanned_at": now()}
    response = dict(scan)
    await db.inventory_scans.insert_one(scan); await db.inventory_sessions.update_one({"id": session_id}, {"$inc": {"scanned_count": 1}}); return response

@api_router.post("/seed")
async def seed():
    if await db.books.count_documents({}) > 0: return {"message": "Sample data already loaded"}
    categories = ["Technology", "Literature", "Science", "History", "Design", "Business", "Philosophy", "Arts", "Travel", "Education"]
    books = [{"id": str(uuid.uuid4()), "title": f"{t} {s}", "isbn": f"978-1-000-{i:04d}", "author": a, "publisher": "Northstar Press", "category": categories[i], "publication_year": 2018+i%7, "created_at": now()} for i,(t,s,a) in enumerate([("The Quiet","Archive","Mira Vale"),("Signals of","Tomorrow","Jon Bell"),("Field Notes on","Light","Anika Rao"),("A Map of","Memory","Elias Stone"),("Making Better","Systems","Iris Chen"),("The Long","Question","Owen Reed"),("Aerial","Gardens","Noah Price"),("Craft of","Attention","Lena Ortiz"),("Small","Horizons","Sam Calder"),("The Open","Library","Ruth Hale")])]
    members = [{"id": str(uuid.uuid4()), "member_code": f"MEM-{1001+i}", "full_name": n, "email": f"{n.lower().replace(' ',' .').replace(' .','')}@example.edu", "phone": "+1 555 010"+str(i+1), "department": d, "membership_status": "Active", "outstanding_fine": 0, "created_at": now()} for i,(n,d) in enumerate([("Ava Morgan","Computer Science"),("Theo Brooks","Architecture"),("Nia Patel","History"),("Lucas Kim","Engineering"),("Maya Stone","Design")])]
    copies=[]
    for i in range(15): copies.append({"id": str(uuid.uuid4()), "book_id": books[i%10]["id"], "accession_number": f"ACC-{2025+i:04d}", "shelf_location": f"{chr(65+i%5)}-{10+i%8}", "status": "Available", "created_at": now()})
    await db.books.insert_many(books); await db.members.insert_many(members); await db.book_copies.insert_many(copies); await write_audit("Seeded sample data", "System", "seed"); return {"message": "Sample data loaded"}

# Include the router in the main app
app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=os.environ.get('CORS_ORIGINS', '*').split(','),
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@app.on_event("startup")
async def start_index_build():
    app.state.index_task = asyncio.create_task(ensure_indexes())  # background: a big index build must not block boot

@app.on_event("shutdown")
async def shutdown_db_client():
    app.state.index_task.cancel()  # stop a build still running before its client closes
    with suppress(asyncio.CancelledError):
        await app.state.index_task
    client.close()