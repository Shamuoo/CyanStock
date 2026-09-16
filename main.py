import os
import io
import csv
import json
import re
import zipfile
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
HIKE_API_BASE = "https://api.hikeup.com/api/v1"

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
    category TEXT NOT NULL DEFAULT 'Cat A/B',
    condition TEXT DEFAULT 'Storage',
    storage_type TEXT DEFAULT 'Customer Storage',
    layby_step TEXT DEFAULT '',
    repair_step TEXT DEFAULT '',
    commission_rate REAL DEFAULT 0.0,
    consignment_paid INTEGER DEFAULT 0,
    customer_id INTEGER,
    consignor_name TEXT DEFAULT '',
    consignor_phone TEXT DEFAULT '',
    date_acquired TEXT DEFAULT '',
    price REAL DEFAULT 0.0,
    status TEXT NOT NULL DEFAULT 'In Store',
    current_location_id TEXT NOT NULL DEFAULT 'UNASSIGNED',
    notes TEXT DEFAULT '',
    repair_notes TEXT DEFAULT '',
    image_url TEXT DEFAULT '',
    import_batch TEXT DEFAULT '',
    last_scanned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (current_location_id) REFERENCES locations(id),
    FOREIGN KEY (customer_id) REFERENCES customers(id)
);

CREATE TABLE IF NOT EXISTS special_orders (
    order_id TEXT PRIMARY KEY,
    customer_id INTEGER,
    customer_name TEXT NOT NULL,
    customer_phone TEXT DEFAULT '',
    customer_email TEXT DEFAULT '',
    item_type TEXT DEFAULT 'Firearm',
    make TEXT NOT NULL,
    model TEXT NOT NULL,
    calibre TEXT DEFAULT '',
    supplier_name TEXT DEFAULT '',
    supplier_po_ref TEXT DEFAULT '',
    tracking_number TEXT DEFAULT '',
    deposit_paid REAL DEFAULT 0.0,
    total_price REAL DEFAULT 0.0,
    order_status TEXT NOT NULL DEFAULT 'Staged / Ordered',
    serial_number TEXT DEFAULT '',
    notes TEXT DEFAULT '',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customers(id)
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

CREATE TABLE IF NOT EXISTS hike_config (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    access_token TEXT,
    store_url TEXT,
    auto_sync_inventory INTEGER DEFAULT 0,
    last_synced_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transit_consignments (
    consignment_no TEXT PRIMARY KEY,
    carrier_type TEXT NOT NULL DEFAULT 'Third-Party Courier',
    carrier_name TEXT NOT NULL,
    waybill_tracking_ref TEXT DEFAULT '',
    consignee_dealer_name TEXT NOT NULL,
    consignee_licence_no TEXT NOT NULL,
    consignee_address TEXT NOT NULL,
    pickup_location TEXT DEFAULT 'SAFE-01',
    status TEXT NOT NULL DEFAULT 'Staged (Holding)',
    current_checkpoint TEXT DEFAULT 'Staged at Store Safe',
    estimated_delivery TEXT DEFAULT '',
    delivered_to_person TEXT DEFAULT '',
    delivered_at TIMESTAMP,
    notes TEXT DEFAULT '',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    dispatched_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS consignment_items (
    consignment_no TEXT NOT NULL,
    firearm_serial TEXT NOT NULL,
    PRIMARY KEY (consignment_no, firearm_serial),
    FOREIGN KEY (consignment_no) REFERENCES transit_consignments(consignment_no),
    FOREIGN KEY (firearm_serial) REFERENCES firearms(serial)
);
"""

DEFAULT_LOCATIONS = [
    ("UNASSIGNED", "Unassigned / Intake Bay", "Intake", 0, "Awaiting safe allocation"),
    ("SAFE-01", "Safe 1 (Spika)", "Safe", 30, "Main customer storage & intake"),
    ("SAFE-02", "Safe 2 (Green)", "Safe", 25, "Longarms, deceased estates & police"),
    ("SAFE-03", "Safe 3 (Copper)", "Safe", 25, "Police seizures & customer storage"),
    ("SHOP-02", "Shop #2 Safe", "Safe", 15, "Cat H & historical safe storage"),
    ("DISP-WALL-01", "Front Longarm Wall Rack", "Display", 16, "Counter display floor stock"),
    ("BENCH-WORKSHOP", "Intake & Workshop Bench", "Workshop", 0, "Repairs, scopes & intake"),
    ("STAGE-TRANSIT", "Interstate Staging Bay", "Transit-Staging", 0, "Crates staged for shipment")
]

def normalize_serial(s: str) -> str:
    return s.strip().upper() if s else ""

async def ensure_columns(db: aiosqlite.Connection):
    migrations = [
        ("customers", "licence_categories", "TEXT DEFAULT ''"),
        ("customers", "store_credit", "REAL DEFAULT 0.0"),
        ("customers", "hike_id", "TEXT DEFAULT ''"),
        ("firearms", "rego_no", "TEXT DEFAULT ''"),
        ("firearms", "book_no", "TEXT DEFAULT ''"),
        ("firearms", "storage_type", "TEXT DEFAULT 'Customer Storage'"),
        ("firearms", "commission_rate", "REAL DEFAULT 0.0"),
        ("firearms", "consignment_paid", "INTEGER DEFAULT 0"),
        ("firearms", "repair_step", "TEXT DEFAULT ''"),
        ("firearms", "repair_notes", "TEXT DEFAULT ''"),
        ("firearms", "import_batch", "TEXT DEFAULT ''")
    ]
    for tbl, col, ctype in migrations:
        async with db.execute(f"PRAGMA table_info({tbl})") as cur:
            existing = [row[1] for row in await cur.fetchall()]
            if col not in existing:
                try:
                    await db.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} {ctype}")
                    await db.commit()
                except Exception:
                    pass

@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(IMAGES_DIR, exist_ok=True)
    cfg = load_config()

    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(SCHEMA_SQL)
        await ensure_columns(db)
        for loc in DEFAULT_LOCATIONS:
            await db.execute("""
                INSERT OR IGNORE INTO locations (id, name, category, max_capacity, notes)
                VALUES (?, ?, ?, ?, ?)
            """, loc)
        if cfg.get("hike_api_token"):
            await db.execute("""
                INSERT INTO hike_config (id, access_token, store_url)
                VALUES (1, ?, ?)
                ON CONFLICT(id) DO UPDATE SET access_token = excluded.access_token, store_url = excluded.store_url
            """, (cfg["hike_api_token"], cfg.get("hike_pos_url", "")))
        await db.commit()
    yield

app = FastAPI(title="CyanStock", version="0.12.10", lifespan=lifespan)
app.mount("/static/images", StaticFiles(directory=IMAGES_DIR), name="images")

class QuickMovePayload(BaseModel):
    serial: str
    target_location_id: str
    operator_name: Optional[str] = "Counter Staff"

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
    category: Optional[str] = "Cat A/B"
    condition: Optional[str] = "Storage"
    storage_type: Optional[str] = "Customer Storage"
    customer_id: Optional[int] = None
    consignor_name: Optional[str] = ""
    consignor_phone: Optional[str] = ""
    price: Optional[float] = 0.0
    status: Optional[str] = "In Store"
    current_location_id: str = "UNASSIGNED"
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

class SupplierModel(BaseModel):
    id: Optional[int] = None
    company_name: str
    account_number: Optional[str] = ""
    rep_name: Optional[str] = ""
    rep_phone: Optional[str] = ""
    order_email: Optional[str] = ""
    payment_terms: Optional[str] = "30 Days Net"

class StoreSettingsIn(BaseModel):
    store_name: str
    store_licence: str
    store_state: Optional[str] = "NSW"
    store_address: Optional[str] = ""
    store_phone: Optional[str] = ""
    store_email: Optional[str] = ""
    cyanlabel_url: Optional[str] = "https://labels.cyannas.com"
    default_label_profile: Optional[str] = "72mm"
    hike_pos_url: Optional[str] = ""
    hike_api_token: Optional[str] = ""

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    return templates.TemplateResponse(request=request, name="index.html", context={"settings": load_config()})

@app.get("/api/settings")
async def api_get_settings():
    return load_config()

@app.post("/api/settings")
async def api_save_settings(settings: StoreSettingsIn):
    cfg = load_config()
    cfg.update(settings.model_dump())
    save_config(cfg)
    if settings.hike_api_token:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                INSERT INTO hike_config (id, access_token, store_url)
                VALUES (1, ?, ?)
                ON CONFLICT(id) DO UPDATE SET access_token = excluded.access_token, store_url = excluded.store_url
            """, (settings.hike_api_token, settings.hike_pos_url or ""))
            await db.commit()
    return {"status": "success", "settings": cfg}

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
            VALUES (?, ?, ?, ?, 'Two-Scan Fast Transfer')
        """, (norm_s, gun["current_location_id"], loc["id"], payload.operator_name or "Counter Staff"))

        await db.commit()
        return {"status": "success", "serial": norm_s, "moved_to": loc["name"]}

@app.post("/api/scan")
async def api_scan(barcode: str = Form(...)):
    raw = barcode.strip().upper()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        loc_id = raw[4:] if raw.startswith("LOC:") else raw
        async with db.execute("SELECT * FROM locations WHERE UPPER(id) = ? OR UPPER(name) = ?", (loc_id, raw)) as cur_l:
            loc = await cur_l.fetchone()
            if loc:
                return {"scan_type": "location_locked", "location_id": loc["id"], "location_name": loc["name"]}

        async with db.execute("""
            SELECT f.*, COALESCE(l.name, f.current_location_id) as location_name,
                   c.name as customer_db_name, c.phone as customer_db_phone, c.licence_no as customer_licence
            FROM firearms f 
            LEFT JOIN locations l ON f.current_location_id = l.id 
            LEFT JOIN customers c ON f.customer_id = c.id
            WHERE UPPER(f.serial) = ? OR UPPER(f.sku) = ? OR UPPER(f.rego_no) = ? OR UPPER(f.book_no) = ?
        """, (raw, raw, raw, raw)) as cur:
            gun = await cur.fetchone()
            if not gun:
                return {"scan_type": "not_found", "scanned_value": raw}
            return {"scan_type": "firearm_query", "firearm": dict(gun)}

@app.get("/api/firearms")
async def api_get_firearms(search: Optional[str] = None):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        sql = """
            SELECT f.*, COALESCE(l.name, f.current_location_id) as location_name,
                   COALESCE(c.name, f.consignor_name, '') as customer_name,
                   COALESCE(c.phone, f.consignor_phone, '') as customer_phone
            FROM firearms f 
            LEFT JOIN locations l ON f.current_location_id = l.id 
            LEFT JOIN customers c ON f.customer_id = c.id
            WHERE 1=1
        """
        params = []
        if search:
            sql += " AND (f.serial LIKE ? OR f.make LIKE ? OR f.model LIKE ? OR f.calibre LIKE ? OR f.rego_no LIKE ? OR f.book_no LIKE ? OR c.name LIKE ?)"
            s = f"%{search.strip()}%"
            params.extend([s, s, s, s, s, s, s])
        sql += " ORDER BY f.last_scanned_at DESC"
        async with db.execute(sql, params) as cur:
            return [dict(r) for r in await cur.fetchall()]

@app.get("/api/firearms/{serial}")
async def api_get_single_firearm(serial: str):
    norm_s = normalize_serial(serial)
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT f.*, COALESCE(l.name, f.current_location_id) as location_name,
                   c.name as customer_name, c.phone as customer_phone, c.licence_no as customer_licence, c.email as customer_email
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
async def api_save_firearm(item: FirearmModel):
    norm_s = normalize_serial(item.serial)
    loc = item.current_location_id.strip().upper() if item.current_location_id else "UNASSIGNED"
    async with aiosqlite.connect(DB_PATH) as db:
        cust_id = item.customer_id
        if item.consignor_name and not cust_id:
            async with db.execute("SELECT id FROM customers WHERE name = ?", (item.consignor_name.strip(),)) as cur_c:
                c_row = await cur_c.fetchone()
                if c_row:
                    cust_id = c_row[0]
                else:
                    c_ins = await db.execute("INSERT INTO customers (name, phone) VALUES (?, ?)", (item.consignor_name.strip(), item.consignor_phone.strip() if item.consignor_phone else ""))
                    cust_id = c_ins.lastrowid

        await db.execute("""
            INSERT OR REPLACE INTO firearms (
                serial, sku, rego_no, book_no, item_type, make, model, calibre,
                action, category, condition, storage_type, customer_id, consignor_name,
                consignor_phone, price, status, current_location_id, notes, last_scanned_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'In Store', ?, ?, CURRENT_TIMESTAMP)
        """, (
            norm_s, item.sku or norm_s, item.rego_no or "", item.book_no or "", item.item_type or "Firearm",
            item.make.strip(), item.model.strip(), item.calibre.strip() or "N/A", item.action or "",
            item.category or "Cat A/B", item.condition or "Storage", item.storage_type or "Customer Storage",
            cust_id, item.consignor_name or "", item.consignor_phone or "",
            item.price or 0.0, loc, item.notes or ""
        ))
        await db.commit()
    return {"status": "success", "serial": norm_s}

@app.delete("/api/firearms/{serial}")
async def api_delete_firearm(serial: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM firearms WHERE UPPER(serial) = ?", (normalize_serial(serial),))
        await db.commit()
    return {"status": "success"}

@app.get("/api/locations")
async def api_get_locations():
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

@app.get("/api/suppliers")
async def api_get_suppliers():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM suppliers ORDER BY company_name ASC") as cur:
            return [dict(r) for r in await cur.fetchall()]

@app.post("/api/suppliers")
async def api_save_supplier(sup: SupplierModel):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO suppliers (company_name, account_number, rep_name, rep_phone, order_email, payment_terms)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(company_name) DO UPDATE SET
                account_number = excluded.account_number,
                rep_name = excluded.rep_name,
                rep_phone = excluded.rep_phone,
                order_email = excluded.order_email,
                payment_terms = excluded.payment_terms
        """, (sup.company_name.strip(), sup.account_number, sup.rep_name, sup.rep_phone, sup.order_email, sup.payment_terms))
        await db.commit()
    return {"status": "success"}

@app.get("/api/customers")
async def api_get_customers():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        sql = """
            SELECT c.*, COUNT(f.serial) as active_guns 
            FROM customers c 
            LEFT JOIN firearms f ON f.customer_id = c.id AND f.status != 'Disposed/Sold' 
            GROUP BY c.id ORDER BY c.name ASC
        """
        async with db.execute(sql) as cur:
            return [dict(r) for r in await cur.fetchall()]

@app.get("/api/special-orders")
async def api_get_special_orders():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM special_orders ORDER BY created_at DESC") as cur:
            return [dict(r) for r in await cur.fetchall()]

@app.post("/api/special-orders")
async def api_save_special_order(order: SpecialOrderIn):
    oid = order.order_id.strip().upper()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT OR REPLACE INTO special_orders (
                order_id, customer_name, customer_phone, make, model, calibre,
                supplier_name, supplier_po_ref, deposit_paid, total_price, order_status, serial_number, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """, (
            oid, order.customer_name.strip(), order.customer_phone.strip(),
            order.make.strip(), order.model.strip(), order.calibre.strip(),
            order.supplier_name.strip(), order.supplier_po_ref.strip(),
            order.deposit_paid or 0.0, order.total_price or 0.0,
            order.order_status or "Staged / Ordered", order.serial_number.strip().upper()
        ))
        await db.commit()
    return {"status": "success", "order_id": oid}

@app.post("/api/special-orders/{order_id}/convert-to-stock")
async def api_convert_special_order(order_id: str, location_id: str = Form(...), serial: Optional[str] = Form(None)):
    oid = order_id.strip().upper()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM special_orders WHERE UPPER(order_id) = ?", (oid,)) as cur:
            so = await cur.fetchone()
            if not so:
                raise HTTPException(status_code=404, detail="Order not found")
        s = normalize_serial(serial or so["serial_number"])
        if not s:
            raise HTTPException(status_code=400, detail="Serial number is required")
        loc = location_id.strip().upper() if location_id else "UNASSIGNED"
        await db.execute("""
            INSERT OR REPLACE INTO firearms (serial, sku, make, model, calibre, price, status, current_location_id, notes, last_scanned_at)
            VALUES (?, ?, ?, ?, ?, ?, 'In Store', ?, ?, CURRENT_TIMESTAMP)
        """, (s, s, so["make"], so["model"], so["calibre"] or "N/A", so["total_price"], loc, f"Fulfilled from {oid}"))
        await db.execute("UPDATE special_orders SET order_status = 'Fulfilled / Converted', serial_number = ? WHERE UPPER(order_id) = ?", (s, oid))
        await db.commit()
    return {"status": "success", "serial": s, "location_id": loc}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8090, reload=True)