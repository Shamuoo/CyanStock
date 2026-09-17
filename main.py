import os
import io
import csv
import json
import re
from datetime import datetime
from contextlib import asynccontextmanager
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, Request, Form, HTTPException, UploadFile, File, Query
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
import aiosqlite

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "cyanstock.db")
CONFIG_PATH = os.path.join(DATA_DIR, "config.json")
IMAGES_DIR = os.path.join(DATA_DIR, "images")

templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

DEFAULT_CONFIG = {
    "store_name": "Tenterfield Firearms",
    "store_licence": "NSW-DEALER-40911082",
    "store_state": "NSW",
    "store_address": "126 Rouse St, Tenterfield NSW 2372",
    "store_phone": "(02) 6736 1234",
    "store_email": "sales@tenterfieldfirearms.com.au",
    "cyanlabel_url": "https://labels.cyannas.com",
    "default_label_profile": "72mm",
    "theme": "dark",
    "barcode_preference": "serial",
    "auto_enter_scanner": True,
    "features": {
        "special_orders": True,
        "suppliers": True,
        "customers": True,
        "laybys": True,
        "safe_drawers": True,
        "cyanlabel_preview": True
    }
}

def load_config() -> Dict[str, Any]:
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH, "w") as f:
            json.dump(DEFAULT_CONFIG, f, indent=2)
        return DEFAULT_CONFIG
    try:
        with open(CONFIG_PATH, "r") as f:
            cfg = json.load(f)
            for k, v in DEFAULT_CONFIG.items():
                if k not in cfg:
                    cfg[k] = v
            return cfg
    except Exception:
        return DEFAULT_CONFIG

def save_config(cfg: Dict[str, Any]):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    full_name TEXT NOT NULL,
    pin TEXT NOT NULL,
    role TEXT NOT NULL DEFAULT 'Employee',
    badge_code TEXT DEFAULT '',
    is_active INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS locations (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT NOT NULL DEFAULT 'Safe',
    max_capacity INTEGER DEFAULT 0,
    color TEXT DEFAULT '#0284c7',
    notes TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS customers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    business_name TEXT DEFAULT '',
    phone TEXT DEFAULT '',
    email TEXT DEFAULT '',
    licence_no TEXT DEFAULT '',
    address TEXT DEFAULT '',
    notes TEXT DEFAULT '',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS suppliers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_name TEXT NOT NULL UNIQUE,
    account_number TEXT DEFAULT '',
    rep_name TEXT DEFAULT '',
    rep_phone TEXT DEFAULT '',
    order_email TEXT DEFAULT '',
    payment_terms TEXT DEFAULT '30 Days Net',
    notes TEXT DEFAULT '',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS firearms (
    serial TEXT PRIMARY KEY,
    sku TEXT,
    rego_no TEXT DEFAULT '',
    book_no TEXT DEFAULT '',
    item_type TEXT DEFAULT 'Firearm',
    make TEXT NOT NULL,
    model TEXT NOT NULL,
    calibre TEXT NOT NULL,
    action TEXT DEFAULT '',
    barrel TEXT DEFAULT '',
    shot_capacity TEXT DEFAULT '',
    category TEXT NOT NULL DEFAULT 'Cat A/B',
    condition TEXT DEFAULT 'New',
    storage_type TEXT DEFAULT 'Sale',
    layby_step TEXT DEFAULT '',
    customer_id INTEGER,
    consignor_name TEXT DEFAULT '',
    consignor_phone TEXT DEFAULT '',
    price REAL DEFAULT 0.0,
    was_price REAL DEFAULT 0.0,
    on_sale INTEGER DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'In Store',
    current_location_id TEXT NOT NULL DEFAULT 'UNASSIGNED',
    notes TEXT DEFAULT '',
    printed_notes TEXT DEFAULT '',
    image_url TEXT DEFAULT '',
    last_scanned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (current_location_id) REFERENCES locations(id),
    FOREIGN KEY (customer_id) REFERENCES customers(id)
);

CREATE TABLE IF NOT EXISTS movement_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    serial TEXT NOT NULL,
    from_location_id TEXT,
    to_location_id TEXT,
    operator TEXT DEFAULT 'Duty Staff',
    notes TEXT,
    moved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

DEFAULT_LOCATIONS = [
    ("UNASSIGNED", "Unassigned Intake", "Intake", 0, "#64748b", "Awaiting safe placement"),
    ("SAFE-01", "Safe 1 (Spika)", "Safe", 30, "#0284c7", "Main customer storage & intake"),
    ("SAFE-02", "Safe 2 (Green)", "Safe", 25, "#10b981", "Deceased estates & longarms"),
    ("SAFE-03", "Safe 3 (Copper)", "Safe", 25, "#d97706", "Customer storage vault"),
    ("SHOP-02", "Shop #2 Safe", "Safe", 15, "#8b5cf6", "Cat H and antique pistols"),
    ("DISP-WALL-01", "Front Longarm Wall", "Display", 16, "#ec4899", "Counter sales rack"),
    ("BENCH-WORKSHOP", "Workshop Bench", "Workshop", 0, "#f97316", "Repairs & scope mounting")
]

def normalize_serial(s: str) -> str:
    return s.strip().upper() if s else ""

@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(IMAGES_DIR, exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(SCHEMA_SQL)
        for loc in DEFAULT_LOCATIONS:
            await db.execute("""
                INSERT OR IGNORE INTO locations (id, name, category, max_capacity, color, notes)
                VALUES (?, ?, ?, ?, ?, ?)
            """, loc)
        async with db.execute("SELECT COUNT(*) FROM users") as cur:
            count = (await cur.fetchone())[0]
            if count == 0:
                await db.execute("""
                    INSERT INTO users (username, full_name, pin, role, badge_code)
                    VALUES ('admin', 'Master Admin', '4091', 'Admin', 'STF:ADMIN-01')
                """)
                await db.execute("""
                    INSERT INTO users (username, full_name, pin, role, badge_code)
                    VALUES ('counter1', 'Duty Staff', '1234', 'Employee', 'STF:STAFF-01')
                """)
        await db.commit()
    yield

app = FastAPI(title="CyanStock", version="0.14.1", lifespan=lifespan)
app.mount("/static/images", StaticFiles(directory=IMAGES_DIR), name="images")

# Pydantic Models
class LocationModel(BaseModel):
    id: str
    name: str
    category: str = "Safe"
    max_capacity: int = 0
    color: str = "#0284c7"
    notes: Optional[str] = ""

class PinLoginPayload(BaseModel):
    pin: str

class UserCreateModel(BaseModel):
    username: str
    full_name: str
    pin: str
    role: str = "Employee"
    badge_code: Optional[str] = ""

class QuickMovePayload(BaseModel):
    serial: str
    target_location_id: str
    operator_name: Optional[str] = "Duty Staff"

class CustomerModel(BaseModel):
    id: Optional[int] = None
    name: str
    business_name: Optional[str] = ""
    phone: Optional[str] = ""
    email: Optional[str] = ""
    licence_no: Optional[str] = ""
    address: Optional[str] = ""
    notes: Optional[str] = ""

class FirearmModel(BaseModel):
    serial: str
    sku: Optional[str] = ""
    rego_no: Optional[str] = ""
    book_no: Optional[str] = ""
    item_type: Optional[str] = "Firearm"
    make: str
    model: str
    calibre: Optional[str] = ""
    action: Optional[str] = ""
    barrel: Optional[str] = ""
    shot_capacity: Optional[str] = ""
    category: Optional[str] = "Cat A/B"
    condition: Optional[str] = "New"
    storage_type: Optional[str] = "Sale"
    layby_step: Optional[str] = ""
    customer_id: Optional[int] = None
    consignor_name: Optional[str] = ""
    consignor_phone: Optional[str] = ""
    price: Optional[float] = 0.0
    was_price: Optional[float] = 0.0
    current_location_id: str = "UNASSIGNED"
    printed_notes: Optional[str] = ""
    notes: Optional[str] = ""

class SupplierModel(BaseModel):
    id: Optional[int] = None
    company_name: str
    account_number: Optional[str] = ""
    rep_name: Optional[str] = ""
    rep_phone: Optional[str] = ""
    order_email: Optional[str] = ""
    payment_terms: Optional[str] = "30 Days Net"
    notes: Optional[str] = ""

class SettingsUpdateModel(BaseModel):
    store_name: str
    store_licence: str
    theme: Optional[str] = "dark"
    barcode_preference: Optional[str] = "serial"
    auto_enter_scanner: Optional[bool] = True
    features: Optional[Dict[str, bool]] = None

# --- Global Search Route ---
@app.get("/api/search")
async def global_search(q: str = Query(...)):
    term = f"%{q.strip().upper()}%"
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        # Search Firearms
        async with db.execute("""
            SELECT f.serial, f.sku, f.make, f.model, f.calibre, f.rego_no, f.storage_type,
                   COALESCE(l.name, f.current_location_id) as location_name
            FROM firearms f
            LEFT JOIN locations l ON f.current_location_id = l.id
            WHERE UPPER(f.serial) LIKE ? OR UPPER(f.sku) LIKE ? OR UPPER(f.rego_no) LIKE ? 
               OR UPPER(f.book_no) LIKE ? OR UPPER(f.make) LIKE ? OR UPPER(f.model) LIKE ? 
               OR UPPER(f.consignor_name) LIKE ?
            LIMIT 8
        """, (term, term, term, term, term, term, term)) as cur_g:
            guns = [dict(r) for r in await cur_g.fetchall()]

        # Search Customers
        async with db.execute("""
            SELECT id, name, business_name, phone, licence_no 
            FROM customers 
            WHERE UPPER(name) LIKE ? OR UPPER(business_name) LIKE ? OR phone LIKE ? OR UPPER(licence_no) LIKE ?
            LIMIT 5
        """, (term, term, term, term)) as cur_c:
            customers = [dict(r) for r in await cur_c.fetchall()]

        # Search Safes
        async with db.execute("""
            SELECT id, name, category, color, max_capacity 
            FROM locations 
            WHERE UPPER(id) LIKE ? OR UPPER(name) LIKE ?
            LIMIT 4
        """, (term, term)) as cur_l:
            safes = [dict(r) for r in await cur_l.fetchall()]

    return {"firearms": guns, "customers": customers, "safes": safes}

# --- Authentication Routes ---
@app.post("/api/auth/pin-login")
async def pin_login(payload: PinLoginPayload):
    pin = payload.pin.strip()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT id, username, full_name, role, badge_code FROM users WHERE pin = ? AND is_active = 1", (pin,)) as cur:
            user = await cur.fetchone()
            if not user:
                raise HTTPException(status_code=401, detail="Invalid PIN code")
            return {"status": "success", "user": dict(user)}

@app.get("/api/users")
async def get_users():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT id, username, full_name, role, badge_code, is_active FROM users ORDER BY role ASC, full_name ASC") as cur:
            return [dict(r) for r in await cur.fetchall()]

@app.post("/api/users")
async def create_user(u: UserCreateModel):
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute("""
                INSERT INTO users (username, full_name, pin, role, badge_code)
                VALUES (?, ?, ?, ?, ?)
            """, (u.username.strip().lower(), u.full_name.strip(), u.pin.strip(), u.role, u.badge_code.strip().upper()))
            await db.commit()
            return {"status": "success"}
        except aiosqlite.IntegrityError:
            raise HTTPException(status_code=400, detail="Username already exists")

@app.delete("/api/users/{user_id}")
async def delete_user(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM users WHERE id = ?", (user_id,))
        await db.commit()
        return {"status": "success"}

# --- Safes & Locations CRUD ---
@app.get("/api/locations")
async def get_locations():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        sql = """
            SELECT l.*, COUNT(f.serial) as current_count 
            FROM locations l 
            LEFT JOIN firearms f ON f.current_location_id = l.id AND f.status = 'In Store' 
            GROUP BY l.id ORDER BY l.category, l.id
        """
        async with db.execute(sql) as cur:
            rows = [dict(r) for r in await cur.fetchall()]
            for r in rows:
                cap = r["max_capacity"]
                cur_cnt = r["current_count"]
                r["percent_full"] = round((cur_cnt / cap) * 100) if cap > 0 else 0
            return rows

@app.post("/api/locations")
async def save_location(loc: LocationModel):
    lid = loc.id.strip().upper()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO locations (id, name, category, max_capacity, color, notes)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                category = excluded.category,
                max_capacity = excluded.max_capacity,
                color = excluded.color,
                notes = excluded.notes
        """, (lid, loc.name.strip(), loc.category, loc.max_capacity, loc.color, loc.notes or ""))
        await db.commit()
    return {"status": "success", "id": lid}

@app.get("/api/locations/{loc_id}/firearms")
async def get_safe_contents(loc_id: str):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        sql = """
            SELECT f.*, COALESCE(c.name, f.consignor_name, '') as customer_name,
                   COALESCE(c.phone, f.consignor_phone, '') as customer_phone
            FROM firearms f
            LEFT JOIN customers c ON f.customer_id = c.id
            WHERE UPPER(f.current_location_id) = ? AND f.status = 'In Store'
            ORDER BY f.last_scanned_at DESC
        """
        async with db.execute(sql, (loc_id.strip().upper(),)) as cur:
            return [dict(r) for r in await cur.fetchall()]

# --- Customer Directory Routes ---
@app.get("/api/customers")
async def get_customers():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        sql = """
            SELECT c.*, 
                   COUNT(CASE WHEN f.status = 'In Store' THEN f.serial END) as active_guns,
                   COUNT(CASE WHEN f.storage_type = 'Customer Storage' AND f.status = 'In Store' THEN f.serial END) as storage_guns,
                   COUNT(CASE WHEN f.storage_type = 'Consignment Sale' AND f.status = 'In Store' THEN f.serial END) as consignment_guns
            FROM customers c
            LEFT JOIN firearms f ON f.customer_id = c.id
            GROUP BY c.id ORDER BY c.name ASC
        """
        async with db.execute(sql) as cur:
            return [dict(r) for r in await cur.fetchall()]

@app.get("/api/customers/{cid}")
async def get_customer_detail(cid: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM customers WHERE id = ?", (cid,)) as cur:
            cust = await cur.fetchone()
            if not cust:
                raise HTTPException(status_code=404, detail="Customer not found")
        async with db.execute("""
            SELECT f.*, COALESCE(l.name, f.current_location_id) as location_name
            FROM firearms f
            LEFT JOIN locations l ON f.current_location_id = l.id
            WHERE f.customer_id = ?
            ORDER BY f.status, f.last_scanned_at DESC
        """, (cid,)) as cur_g:
            guns = [dict(r) for r in await cur_g.fetchall()]
        data = dict(cust)
        data["firearms"] = guns
        return data

@app.post("/api/customers")
async def save_customer(c: CustomerModel):
    async with aiosqlite.connect(DB_PATH) as db:
        if c.id:
            await db.execute("""
                UPDATE customers SET name=?, business_name=?, phone=?, email=?, licence_no=?, address=?, notes=?
                WHERE id=?
            """, (c.name.strip(), c.business_name, c.phone, c.email, c.licence_no, c.address, c.notes, c.id))
            cid = c.id
        else:
            cur = await db.execute("""
                INSERT INTO customers (name, business_name, phone, email, licence_no, address, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (c.name.strip(), c.business_name, c.phone, c.email, c.licence_no, c.address, c.notes))
            cid = cur.lastrowid
        await db.commit()
    return {"status": "success", "id": cid}

# --- Supplier Directory Routes ---
@app.get("/api/suppliers")
async def get_suppliers():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM suppliers ORDER BY company_name ASC") as cur:
            return [dict(r) for r in await cur.fetchall()]

@app.post("/api/suppliers")
async def save_supplier(s: SupplierModel):
    async with aiosqlite.connect(DB_PATH) as db:
        if s.id:
            await db.execute("""
                UPDATE suppliers SET company_name=?, account_number=?, rep_name=?, rep_phone=?, order_email=?, payment_terms=?, notes=?
                WHERE id=?
            """, (s.company_name.strip(), s.account_number, s.rep_name, s.rep_phone, s.order_email, s.payment_terms, s.notes, s.id))
        else:
            await db.execute("""
                INSERT INTO suppliers (company_name, account_number, rep_name, rep_phone, order_email, payment_terms, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (s.company_name.strip(), s.account_number, s.rep_name, s.rep_phone, s.order_email, s.payment_terms, s.notes))
        await db.commit()
    return {"status": "success"}

@app.delete("/api/suppliers/{sid}")
async def delete_supplier(sid: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM suppliers WHERE id = ?", (sid,))
        await db.commit()
    return {"status": "success"}

# --- Firearms & Scan Operations ---
@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse(request=request, name="index.html", context={"settings": load_config()})

@app.get("/api/settings")
async def get_settings():
    return load_config()

@app.post("/api/settings")
async def update_settings(payload: SettingsUpdateModel):
    cfg = load_config()
    cfg.update(payload.model_dump())
    save_config(cfg)
    return {"status": "success", "settings": cfg}

@app.post("/api/scan")
async def handle_scan(barcode: str = Form(...)):
    raw = barcode.strip().upper()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        if raw.startswith("STF:"):
            async with db.execute("SELECT id, username, full_name, role FROM users WHERE UPPER(badge_code) = ? AND is_active = 1", (raw,)) as cur_b:
                staff = await cur_b.fetchone()
                if staff:
                    return {"scan_type": "staff_badge", "user": dict(staff)}

        loc_id = raw[4:] if raw.startswith("LOC:") else raw
        async with db.execute("SELECT * FROM locations WHERE UPPER(id) = ? OR UPPER(name) = ?", (loc_id, raw)) as cur:
            loc = await cur.fetchone()
            if loc:
                return {"scan_type": "location_locked", "location_id": loc["id"], "location_name": loc["name"], "color": loc["color"]}

        async with db.execute("""
            SELECT f.*, COALESCE(l.name, f.current_location_id) as location_name, l.color as location_color,
                   c.name as customer_db_name, c.phone as customer_db_phone
            FROM firearms f
            LEFT JOIN locations l ON f.current_location_id = l.id
            LEFT JOIN customers c ON f.customer_id = c.id
            WHERE UPPER(f.serial) = ? OR UPPER(f.sku) = ? OR UPPER(f.rego_no) = ? OR UPPER(f.book_no) = ?
        """, (raw, raw, raw, raw)) as cur:
            gun = await cur.fetchone()
            if gun:
                return {"scan_type": "firearm_query", "firearm": dict(gun)}

        return {"scan_type": "not_found", "scanned_value": raw}

@app.post("/api/scan/allocate")
async def allocate_scan(payload: QuickMovePayload):
    norm_s = normalize_serial(payload.serial)
    loc_id = payload.target_location_id.replace("LOC:", "").strip().upper()

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT serial, current_location_id FROM firearms WHERE UPPER(serial) = ?", (norm_s,)) as cur:
            gun = await cur.fetchone()
            if not gun:
                raise HTTPException(status_code=404, detail=f"Firearm {norm_s} not found.")

        async with db.execute("SELECT id, name FROM locations WHERE UPPER(id) = ?", (loc_id,)) as cur_l:
            loc = await cur_l.fetchone()
            if not loc:
                raise HTTPException(status_code=404, detail=f"Target safe '{loc_id}' is not registered.")

        await db.execute("""
            UPDATE firearms 
            SET current_location_id = ?, status = 'In Store', last_scanned_at = CURRENT_TIMESTAMP 
            WHERE UPPER(serial) = ?
        """, (loc["id"], norm_s))

        await db.execute("""
            INSERT INTO movement_logs (serial, from_location_id, to_location_id, operator, notes)
            VALUES (?, ?, ?, ?, 'Two-Scan Fast Reallocation')
        """, (norm_s, gun["current_location_id"], loc["id"], payload.operator_name or "Duty Staff"))

        await db.commit()
        return {"status": "success", "serial": norm_s, "moved_to": loc["name"], "location_id": loc["id"]}

@app.get("/api/firearms")
async def get_firearms(search: Optional[str] = None, location_id: Optional[str] = None, storage_type: Optional[str] = None):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        sql = """
            SELECT f.*, COALESCE(l.name, f.current_location_id) as location_name, l.color as location_color,
                   COALESCE(c.name, f.consignor_name, '') as customer_name,
                   COALESCE(c.phone, f.consignor_phone, '') as customer_phone
            FROM firearms f 
            LEFT JOIN locations l ON f.current_location_id = l.id 
            LEFT JOIN customers c ON f.customer_id = c.id
            WHERE 1=1
        """
        params = []
        if location_id:
            sql += " AND UPPER(f.current_location_id) = ?"
            params.append(location_id.strip().upper())
        if storage_type:
            sql += " AND f.storage_type = ?"
            params.append(storage_type)
        if search:
            sql += " AND (f.serial LIKE ? OR f.sku LIKE ? OR f.make LIKE ? OR f.model LIKE ? OR f.calibre LIKE ? OR f.rego_no LIKE ? OR c.name LIKE ?)"
            s = f"%{search.strip()}%"
            params.extend([s, s, s, s, s, s, s])
        sql += " ORDER BY f.last_scanned_at DESC"
        async with db.execute(sql, params) as cur:
            return [dict(r) for r in await cur.fetchall()]

@app.get("/api/firearms/{serial}")
async def get_firearm_detail(serial: str):
    norm_s = normalize_serial(serial)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT f.*, COALESCE(l.name, f.current_location_id) as location_name, l.color as location_color,
                   c.name as customer_db_name, c.phone as customer_db_phone, c.licence_no as customer_licence
            FROM firearms f
            LEFT JOIN locations l ON f.current_location_id = l.id
            LEFT JOIN customers c ON f.customer_id = c.id
            WHERE UPPER(f.serial) = ?
        """, (norm_s,)) as cur:
            row = await cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Firearm not found")
            return dict(row)

@app.post("/api/firearms")
async def save_firearm(gun: FirearmModel):
    norm_s = normalize_serial(gun.serial)
    loc = gun.current_location_id.strip().upper() if gun.current_location_id else "UNASSIGNED"
    final_sku = gun.sku.strip().upper() if gun.sku else norm_s

    owner_name = gun.consignor_name.strip() if gun.storage_type in ['Customer Storage', 'Consignment Sale', 'Layby'] else ""
    owner_phone = gun.consignor_phone.strip() if gun.storage_type in ['Customer Storage', 'Consignment Sale', 'Layby'] else ""

    async with aiosqlite.connect(DB_PATH) as db:
        cust_id = gun.customer_id
        if owner_name and not cust_id:
            async with db.execute("SELECT id FROM customers WHERE name = ?", (owner_name,)) as cur_c:
                row = await cur_c.fetchone()
                if row:
                    cust_id = row[0]
                else:
                    cur_ins = await db.execute("INSERT INTO customers (name, phone) VALUES (?, ?)", (owner_name, owner_phone))
                    cust_id = cur_ins.lastrowid

        await db.execute("""
            INSERT OR REPLACE INTO firearms (
                serial, sku, rego_no, book_no, item_type, make, model, calibre,
                action, barrel, shot_capacity, category, condition, storage_type,
                layby_step, customer_id, consignor_name, consignor_phone,
                price, was_price, status, current_location_id, notes, printed_notes, last_scanned_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'In Store', ?, ?, ?, CURRENT_TIMESTAMP)
        """, (
            norm_s, final_sku, gun.rego_no or "", gun.book_no or "", gun.item_type or "Firearm",
            gun.make.strip(), gun.model.strip(), gun.calibre.strip() or "N/A", gun.action or "",
            gun.barrel or "", gun.shot_capacity or "", gun.category or "Cat A/B", gun.condition or "New",
            gun.storage_type or "Sale", gun.layby_step or "", cust_id, owner_name, owner_phone,
            gun.price or 0.0, gun.was_price or 0.0, loc, gun.notes or "", gun.printed_notes or ""
        ))
        await db.commit()
    return {"status": "success", "serial": norm_s}

@app.delete("/api/firearms/{serial}")
async def delete_firearm(serial: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM firearms WHERE UPPER(serial) = ?", (normalize_serial(serial),))
        await db.commit()
    return {"status": "success"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8090, reload=True)