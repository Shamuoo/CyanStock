import os
import io
import csv
import json
import re
from datetime import datetime
from contextlib import asynccontextmanager
from typing import Optional, List, Dict, Any

try:
    import httpx
except ImportError:
    httpx = None

from fastapi import FastAPI, Request, Form, HTTPException, UploadFile, File, Query, Depends
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
    "features": {
        "special_orders": True,
        "laybys": True,
        "suppliers": True,
        "shipping": True,
        "cyanlabel_preview": True
    },
    "hike_pos_url": "",
    "hike_api_token": ""
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
            if "features" not in cfg:
                cfg["features"] = DEFAULT_CONFIG["features"]
            return cfg
    except Exception:
        return DEFAULT_CONFIG

def save_config(cfg: Dict[str, Any]):
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS locations (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT NOT NULL,
    max_capacity INTEGER DEFAULT 0,
    color TEXT DEFAULT '#0284c7',
    notes TEXT
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
    licence_categories TEXT DEFAULT '',
    store_credit REAL DEFAULT 0.0,
    hike_id TEXT DEFAULT '',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS suppliers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_name TEXT NOT NULL UNIQUE,
    account_number TEXT DEFAULT '',
    rep_name TEXT DEFAULT '',
    rep_phone TEXT DEFAULT '',
    rep_email TEXT DEFAULT '',
    order_email TEXT DEFAULT '',
    portal_url TEXT DEFAULT '',
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
    condition TEXT DEFAULT 'Storage',
    storage_type TEXT DEFAULT 'Customer Storage',
    layby_step TEXT DEFAULT '',
    repair_step TEXT DEFAULT '',
    customer_id INTEGER,
    consignor_name TEXT DEFAULT '',
    consignor_phone TEXT DEFAULT '',
    date_acquired TEXT DEFAULT '',
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

CREATE TABLE IF NOT EXISTS special_orders (
    order_id TEXT PRIMARY KEY,
    customer_id INTEGER,
    customer_name TEXT NOT NULL,
    customer_phone TEXT DEFAULT '',
    item_type TEXT DEFAULT 'Firearm',
    make TEXT NOT NULL,
    model TEXT NOT NULL,
    calibre TEXT DEFAULT '',
    supplier_name TEXT DEFAULT '',
    supplier_po_ref TEXT DEFAULT '',
    deposit_paid REAL DEFAULT 0.0,
    total_price REAL DEFAULT 0.0,
    order_status TEXT NOT NULL DEFAULT 'Staged / Ordered',
    serial_number TEXT DEFAULT '',
    notes TEXT DEFAULT '',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS movement_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    serial TEXT NOT NULL,
    from_location_id TEXT,
    to_location_id TEXT,
    operator TEXT DEFAULT 'Counter Staff',
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

async def ensure_columns(db: aiosqlite.Connection):
    col_migrations = [
        ("locations", "color", "TEXT DEFAULT '#0284c7'"),
        ("firearms", "barrel", "TEXT DEFAULT ''"),
        ("firearms", "shot_capacity", "TEXT DEFAULT ''"),
        ("firearms", "printed_notes", "TEXT DEFAULT ''"),
        ("firearms", "was_price", "REAL DEFAULT 0.0"),
        ("firearms", "on_sale", "INTEGER DEFAULT 0"),
        ("firearms", "rego_no", "TEXT DEFAULT ''"),
        ("firearms", "book_no", "TEXT DEFAULT ''")
    ]
    for tbl, col, ctype in col_migrations:
        async with db.execute(f"PRAGMA table_info({tbl})") as cur:
            cols = [r[1] for r in await cur.fetchall()]
            if col not in cols:
                try:
                    await db.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} {ctype}")
                    await db.commit()
                except Exception:
                    pass

@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(IMAGES_DIR, exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(SCHEMA_SQL)
        await ensure_columns(db)
        for loc in DEFAULT_LOCATIONS:
            await db.execute("""
                INSERT OR IGNORE INTO locations (id, name, category, max_capacity, color, notes)
                VALUES (?, ?, ?, ?, ?, ?)
            """, loc)
        await db.commit()
    yield

app = FastAPI(title="CyanStock", version="0.12.10", lifespan=lifespan)
app.mount("/static/images", StaticFiles(directory=IMAGES_DIR), name="images")

# Pydantic Schemas
class QuickMovePayload(BaseModel):
    serial: str
    target_location_id: str
    operator_name: Optional[str] = "Counter Staff"

class LinkBarcodePayload(BaseModel):
    target_serial: str
    scanned_barcode: str

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
    condition: Optional[str] = "Storage"
    storage_type: Optional[str] = "Customer Storage"
    layby_step: Optional[str] = ""
    customer_id: Optional[int] = None
    consignor_name: Optional[str] = ""
    consignor_phone: Optional[str] = ""
    price: Optional[float] = 0.0
    was_price: Optional[float] = 0.0
    on_sale: Optional[int] = 0
    status: Optional[str] = "In Store"
    current_location_id: str = "UNASSIGNED"
    notes: Optional[str] = ""
    printed_notes: Optional[str] = ""
    image_url: Optional[str] = ""

class SupplierModel(BaseModel):
    id: Optional[int] = None
    company_name: str
    account_number: Optional[str] = ""
    rep_name: Optional[str] = ""
    rep_phone: Optional[str] = ""
    rep_email: Optional[str] = ""
    order_email: Optional[str] = ""
    portal_url: Optional[str] = ""
    payment_terms: Optional[str] = "30 Days Net"
    notes: Optional[str] = ""

class SpecialOrderIn(BaseModel):
    order_id: str
    customer_name: str
    customer_phone: Optional[str] = ""
    make: str
    model: str
    calibre: Optional[str] = ""
    supplier_name: Optional[str] = ""
    supplier_po_ref: Optional[str] = ""
    deposit_paid: Optional[float] = 0.0
    total_price: Optional[float] = 0.0
    order_status: Optional[str] = "Staged / Ordered"
    serial_number: Optional[str] = ""
    notes: Optional[str] = ""

class SettingsUpdateModel(BaseModel):
    store_name: str
    store_licence: str
    store_state: Optional[str] = "NSW"
    store_address: Optional[str] = ""
    store_phone: Optional[str] = ""
    store_email: Optional[str] = ""
    theme: Optional[str] = "dark"
    barcode_preference: Optional[str] = "serial"
    features: Optional[Dict[str, bool]] = None

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

# Unified Scan Route
@app.post("/api/scan")
async def handle_scan(barcode: str = Form(...)):
    raw = barcode.strip().upper()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        # 1. Location Placard Check
        loc_id = raw[4:] if raw.startswith("LOC:") else raw
        async with db.execute("SELECT * FROM locations WHERE UPPER(id) = ? OR UPPER(name) = ?", (loc_id, raw)) as cur:
            loc = await cur.fetchone()
            if loc:
                return {"scan_type": "location_locked", "location_id": loc["id"], "location_name": loc["name"], "color": loc["color"]}

        # 2. Firearm Match Check (Serial, SKU, Rego, Book Reference)
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

# Link Scanned Barcode to an Existing Gun
@app.post("/api/firearms/link-barcode")
async def link_barcode(payload: LinkBarcodePayload):
    norm_s = normalize_serial(payload.target_serial)
    new_sku = payload.scanned_barcode.strip().upper()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE firearms SET sku = ? WHERE UPPER(serial) = ?", (new_sku, norm_s))
        await db.commit()
    return {"status": "success", "serial": norm_s, "sku": new_sku}

# Two-Scan Fast Relocation
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
            VALUES (?, ?, ?, ?, 'Two-Scan Reallocation')
        """, (norm_s, gun["current_location_id"], loc["id"], payload.operator_name or "Counter Staff"))

        await db.commit()
        return {"status": "success", "serial": norm_s, "moved_to": loc["name"], "location_id": loc["id"]}

# Firearms Directory
@app.get("/api/firearms")
async def get_firearms(search: Optional[str] = None, location_id: Optional[str] = None):
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
        if search:
            sql += " AND (f.serial LIKE ? OR f.sku LIKE ? OR f.make LIKE ? OR f.model LIKE ? OR f.calibre LIKE ? OR f.rego_no LIKE ? OR c.name LIKE ?)"
            s = f"%{search.strip()}%"
            params.extend([s, s, s, s, s, s, s])
        sql += " ORDER BY f.last_scanned_at DESC"
        async with db.execute(sql, params) as cur:
            return [dict(r) for r in await cur.fetchall()]

@app.post("/api/firearms")
async def save_firearm(gun: FirearmModel):
    norm_s = normalize_serial(gun.serial)
    loc = gun.current_location_id.strip().upper() if gun.current_location_id else "UNASSIGNED"
    cfg = load_config()
    final_sku = gun.sku.strip().upper() if gun.sku else (norm_s if cfg.get("barcode_preference") == "serial" else norm_s)

    async with aiosqlite.connect(DB_PATH) as db:
        cust_id = gun.customer_id
        if gun.consignor_name and not cust_id:
            async with db.execute("SELECT id FROM customers WHERE name = ?", (gun.consignor_name.strip(),)) as cur_c:
                row = await cur_c.fetchone()
                if row:
                    cust_id = row[0]
                else:
                    cur_ins = await db.execute("INSERT INTO customers (name, phone) VALUES (?, ?)", (gun.consignor_name.strip(), gun.consignor_phone.strip()))
                    cust_id = cur_ins.lastrowid

        await db.execute("""
            INSERT OR REPLACE INTO firearms (
                serial, sku, rego_no, book_no, item_type, make, model, calibre,
                action, barrel, shot_capacity, category, condition, storage_type,
                layby_step, customer_id, consignor_name, consignor_phone,
                price, was_price, on_sale, status, current_location_id, notes, printed_notes, image_url, last_scanned_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'In Store', ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (
            norm_s, final_sku, gun.rego_no or "", gun.book_no or "", gun.item_type or "Firearm",
            gun.make.strip(), gun.model.strip(), gun.calibre.strip() or "N/A", gun.action or "",
            gun.barrel or "", gun.shot_capacity or "", gun.category or "Cat A/B", gun.condition or "Storage",
            gun.storage_type or "Customer Storage", gun.layby_step or "", cust_id, gun.consignor_name or "",
            gun.consignor_phone or "", gun.price or 0.0, gun.was_price or 0.0, gun.on_sale or 0, loc,
            gun.notes or "", gun.printed_notes or "", gun.image_url or ""
        ))
        await db.commit()
    return {"status": "success", "serial": norm_s}

# Safes and Locations
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
            return [dict(r) for r in await cur.fetchall()]

@app.get("/api/locations/{loc_id}/firearms")
async def get_safe_contents(loc_id: str):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        sql = """
            SELECT f.*, COALESCE(c.name, f.consignor_name, 'Store Stock') as customer_name
            FROM firearms f
            LEFT JOIN customers c ON f.customer_id = c.id
            WHERE UPPER(f.current_location_id) = ? AND f.status = 'In Store'
            ORDER BY f.last_scanned_at DESC
        """
        async with db.execute(sql, (loc_id.strip().upper(),)) as cur:
            return [dict(r) for r in await cur.fetchall()]

# Supplier Directory
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
                UPDATE suppliers SET company_name=?, account_number=?, rep_name=?, rep_phone=?, order_email=?, portal_url=?, payment_terms=?, notes=?
                WHERE id=?
            """, (s.company_name.strip(), s.account_number, s.rep_name, s.rep_phone, s.order_email, s.portal_url, s.payment_terms, s.notes, s.id))
        else:
            await db.execute("""
                INSERT INTO suppliers (company_name, account_number, rep_name, rep_phone, order_email, portal_url, payment_terms, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (s.company_name.strip(), s.account_number, s.rep_name, s.rep_phone, s.order_email, s.portal_url, s.payment_terms, s.notes))
        await db.commit()
    return {"status": "success"}

@app.delete("/api/suppliers/{sid}")
async def delete_supplier(sid: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM suppliers WHERE id = ?", (sid,))
        await db.commit()
    return {"status": "success"}

# Special Orders
@app.get("/api/special-orders")
async def get_special_orders():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM special_orders ORDER BY created_at DESC") as cur:
            return [dict(r) for r in await cur.fetchall()]

@app.post("/api/special-orders")
async def save_special_order(o: SpecialOrderIn):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT OR REPLACE INTO special_orders (
                order_id, customer_name, customer_phone, make, model, calibre,
                supplier_name, supplier_po_ref, deposit_paid, total_price, order_status, serial_number, notes, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (o.order_id.strip().upper(), o.customer_name.strip(), o.customer_phone.strip(), o.make.strip(), o.model.strip(), o.calibre.strip(), o.supplier_name.strip(), o.supplier_po_ref.strip(), o.deposit_paid, o.total_price, o.order_status, o.serial_number.strip().upper(), o.notes.strip()))
        await db.commit()
    return {"status": "success"}

# Customers
@app.get("/api/customers")
async def get_customers():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT c.*, COUNT(f.serial) as active_guns 
            FROM customers c 
            LEFT JOIN firearms f ON f.customer_id = c.id AND f.status != 'Disposed/Sold' 
            GROUP BY c.id ORDER BY c.name ASC
        """) as cur:
            return [dict(r) for r in await cur.fetchall()]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8090, reload=True)