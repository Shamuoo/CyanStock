import os
import io
import csv
import re
import json
import shutil
import zipfile
import asyncio
from datetime import datetime
from contextlib import asynccontextmanager
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, Request, HTTPException, UploadFile, File, Query
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
import aiosqlite

APP_VERSION = "0.15.0"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR", os.path.join(BASE_DIR, "data"))
BACKUP_DIR = os.environ.get("BACKUP_DIR", os.path.join(BASE_DIR, "backups"))
MANIFEST_DIR = os.environ.get("MANIFEST_DIR", os.path.join(BASE_DIR, "manifests"))
DB_PATH = os.environ.get("DB_PATH", os.path.join(DATA_DIR, "cyanstock.db"))

for d in [DATA_DIR, BACKUP_DIR, MANIFEST_DIR, os.path.join(BASE_DIR, "templates")]:
    os.makedirs(d, exist_ok=True)


def normalize_code(val: Optional[str]) -> str:
    if not val:
        return ""
    return re.sub(r"[^A-Z0-9]", "", str(val).upper())


async def get_db():
    db = await aiosqlite.connect(DB_PATH)
    db.row_factory = aiosqlite.Row
    await db.execute("PRAGMA journal_mode=WAL;")
    await db.execute("PRAGMA busy_timeout=5000;")
    return db


async def safe_add_column(db, table: str, column_def: str):
    try:
        await db.execute(f"ALTER TABLE {table} ADD COLUMN {column_def}")
    except Exception:
        pass


async def create_backup_archive(prefix: str = "auto") -> str:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    zip_filename = f"cyanstock_{prefix}_{ts}.zip"
    zip_path = os.path.join(BACKUP_DIR, zip_filename)
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        if os.path.exists(DB_PATH):
            zf.write(DB_PATH, arcname="cyanstock.db")
        for root, _, files in os.walk(MANIFEST_DIR):
            for file in files:
                full_p = os.path.join(root, file)
                zf.write(full_p, arcname=os.path.join("manifests", file))
    # Retain last 30 backups
    backups = sorted(
        [os.path.join(BACKUP_DIR, f) for f in os.listdir(BACKUP_DIR) if f.endswith(".zip")],
        key=os.path.getmtime,
    )
    while len(backups) > 30:
        try:
            os.remove(backups.pop(0))
        except OSError:
            pass
    return zip_filename


async def nightly_backup_loop():
    while True:
        await asyncio.sleep(86400)  # 24 hours
        try:
            await create_backup_archive("nightly")
        except Exception as e:
            print(f"[Backup Warning] Nightly snapshot failed: {e}")


async def init_db():
    db = await get_db()
    try:
        # 1. Locations (Safes, Display Walls, Benches)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS locations (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                category TEXT NOT NULL DEFAULT 'Safe',
                max_capacity INTEGER DEFAULT 30,
                color TEXT DEFAULT '#0284c7',
                notes TEXT DEFAULT ''
            )
        """)
        await safe_add_column(db, "locations", "color TEXT DEFAULT '#0284c7'")

        # 2. Firearms & Regulated Inventory
        await db.execute("""
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
                barrel_length TEXT DEFAULT '',
                shot_capacity TEXT DEFAULT '',
                category TEXT NOT NULL DEFAULT 'Cat A',
                condition TEXT DEFAULT 'New',
                storage_type TEXT DEFAULT 'Sale (Floor Stock)',
                layby_stage TEXT DEFAULT '',
                consignor_name TEXT DEFAULT '',
                consignor_phone TEXT DEFAULT '',
                commission_pct REAL DEFAULT 15.0,
                commission_min_fee REAL DEFAULT 50.0,
                payout_status TEXT DEFAULT 'Unpaid',
                customer_id INTEGER,
                customer_name TEXT DEFAULT '',
                customer_phone TEXT DEFAULT '',
                customer_licence_no TEXT DEFAULT '',
                customer_licence_cats TEXT DEFAULT '',
                pta_number TEXT DEFAULT '',
                supplier_name TEXT DEFAULT '',
                supplier_licence_no TEXT DEFAULT '',
                date_acquired TEXT DEFAULT '',
                date_disposed TEXT DEFAULT '',
                disposed_to_name TEXT DEFAULT '',
                disposed_to_licence TEXT DEFAULT '',
                price REAL DEFAULT 0.0,
                was_price REAL DEFAULT 0.0,
                cost_price REAL DEFAULT 0.0,
                status TEXT NOT NULL DEFAULT 'In Store',
                current_location_id TEXT DEFAULT 'SAFE-01',
                printed_notes TEXT DEFAULT '',
                notes TEXT DEFAULT '',
                image_url TEXT DEFAULT '',
                last_scanned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        for col in [
            "item_type TEXT DEFAULT 'Firearm'",
            "barrel_length TEXT DEFAULT ''",
            "shot_capacity TEXT DEFAULT ''",
            "storage_type TEXT DEFAULT 'Sale (Floor Stock)'",
            "layby_stage TEXT DEFAULT ''",
            "commission_pct REAL DEFAULT 15.0",
            "commission_min_fee REAL DEFAULT 50.0",
            "payout_status TEXT DEFAULT 'Unpaid'",
            "customer_id INTEGER",
            "customer_name TEXT DEFAULT ''",
            "customer_phone TEXT DEFAULT ''",
            "customer_licence_no TEXT DEFAULT ''",
            "customer_licence_cats TEXT DEFAULT ''",
            "pta_number TEXT DEFAULT ''",
            "supplier_name TEXT DEFAULT ''",
            "supplier_licence_no TEXT DEFAULT ''",
            "date_acquired TEXT DEFAULT ''",
            "date_disposed TEXT DEFAULT ''",
            "disposed_to_name TEXT DEFAULT ''",
            "disposed_to_licence TEXT DEFAULT ''",
            "was_price REAL DEFAULT 0.0",
            "cost_price REAL DEFAULT 0.0",
            "printed_notes TEXT DEFAULT ''",
            "image_url TEXT DEFAULT ''"
        ]:
            await safe_add_column(db, "firearms", col)

        # 3. Customers Directory
        await db.execute("""
            CREATE TABLE IF NOT EXISTS customers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                phone TEXT DEFAULT '',
                email TEXT DEFAULT '',
                address TEXT DEFAULT '',
                licence_no TEXT DEFAULT '',
                licence_categories TEXT DEFAULT 'Cat A, Cat B',
                licence_expiry TEXT DEFAULT '',
                store_credit REAL DEFAULT 0.0,
                notes TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        for col in [
            "licence_categories TEXT DEFAULT 'Cat A, Cat B'",
            "licence_expiry TEXT DEFAULT ''",
            "store_credit REAL DEFAULT 0.0"
        ]:
            await safe_add_column(db, "customers", col)

        # 4. Special Orders & Indents
        await db.execute("""
            CREATE TABLE IF NOT EXISTS special_orders (
                order_id TEXT PRIMARY KEY,
                customer_id INTEGER,
                customer_name TEXT NOT NULL,
                customer_phone TEXT DEFAULT '',
                customer_licence_no TEXT DEFAULT '',
                item_type TEXT DEFAULT 'Firearm',
                make TEXT NOT NULL,
                model TEXT NOT NULL,
                calibre TEXT DEFAULT '',
                category TEXT DEFAULT 'Cat A',
                supplier_name TEXT DEFAULT 'NIOA',
                supplier_po_ref TEXT DEFAULT '',
                tracking_number TEXT DEFAULT '',
                carrier_name TEXT DEFAULT 'StarTrack Secure',
                estimated_arrival_date TEXT DEFAULT '',
                deposit_paid REAL DEFAULT 0.0,
                total_price REAL DEFAULT 0.0,
                order_status TEXT NOT NULL DEFAULT 'Staged / Ordered',
                pta_number TEXT DEFAULT '',
                serial_number TEXT DEFAULT '',
                notes TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 5. Suppliers Directory
        await db.execute("""
            CREATE TABLE IF NOT EXISTS suppliers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                account_no TEXT DEFAULT '',
                dealer_licence_no TEXT DEFAULT '',
                rep_name TEXT DEFAULT '',
                phone TEXT DEFAULT '',
                email TEXT DEFAULT '',
                notes TEXT DEFAULT ''
            )
        """)

        # 6. Regulated Consumables (Powder, Primers, Factory Ammo)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS consumables (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sku TEXT NOT NULL,
                brand TEXT NOT NULL,
                name TEXT NOT NULL,
                item_type TEXT NOT NULL DEFAULT 'Smokeless Powder',
                calibre_or_grade TEXT DEFAULT '',
                lot_number TEXT DEFAULT '',
                quantity INTEGER DEFAULT 0,
                unit_neq_kg REAL DEFAULT 0.5,
                price REAL DEFAULT 0.0,
                min_stock_alert INTEGER DEFAULT 5,
                location_id TEXT DEFAULT 'SAFE-POWDER',
                notes TEXT DEFAULT '',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 7. Gunsmithing & Workshop Work Orders
        await db.execute("""
            CREATE TABLE IF NOT EXISTS work_orders (
                job_id TEXT PRIMARY KEY,
                firearm_serial TEXT DEFAULT '',
                customer_name TEXT NOT NULL,
                customer_phone TEXT DEFAULT '',
                make_model TEXT NOT NULL,
                cal_cat TEXT DEFAULT '',
                service_type TEXT DEFAULT 'General Repair',
                fault_description TEXT DEFAULT '',
                parts_used TEXT DEFAULT '',
                labor_cost REAL DEFAULT 0.0,
                parts_cost REAL DEFAULT 0.0,
                status TEXT DEFAULT 'Awaiting Assessment',
                test_fired TEXT DEFAULT 'No',
                safe_location_id TEXT DEFAULT 'BENCH-WORKSHOP',
                operator_name TEXT DEFAULT 'Admin',
                notes TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 8. Interstate Shipments & QLD Transfers
        await db.execute("""
            CREATE TABLE IF NOT EXISTS shipments (
                shipment_no TEXT PRIMARY KEY,
                recipient_name TEXT NOT NULL,
                recipient_business TEXT DEFAULT '',
                recipient_phone TEXT DEFAULT '',
                recipient_licence TEXT DEFAULT '',
                destination_state TEXT DEFAULT 'QLD',
                destination_address TEXT NOT NULL,
                end_customer_name TEXT DEFAULT '',
                end_customer_phone TEXT DEFAULT '',
                end_customer_licence TEXT DEFAULT '',
                end_customer_pta TEXT DEFAULT '',
                carrier_name TEXT DEFAULT 'StarTrack Secure',
                tracking_number TEXT DEFAULT '',
                shipment_type TEXT DEFAULT 'Interstate Dealer Transfer (NSW -> QLD)',
                status TEXT DEFAULT 'Packed & Staged',
                signed_by TEXT DEFAULT '',
                delivered_at TEXT DEFAULT '',
                notes TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        for col in [
            "destination_state TEXT DEFAULT 'QLD'",
            "end_customer_name TEXT DEFAULT ''",
            "end_customer_phone TEXT DEFAULT ''",
            "end_customer_licence TEXT DEFAULT ''",
            "end_customer_pta TEXT DEFAULT ''",
            "signed_by TEXT DEFAULT ''",
            "delivered_at TEXT DEFAULT ''"
        ]:
            await safe_add_column(db, "shipments", col)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS shipment_items (
                shipment_no TEXT NOT NULL,
                firearm_serial TEXT NOT NULL,
                PRIMARY KEY (shipment_no, firearm_serial)
            )
        """)

        # 9. Audit & Chain-of-Custody Movement Logs
        await db.execute("""
            CREATE TABLE IF NOT EXISTS movement_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firearm_serial TEXT NOT NULL,
                from_location TEXT DEFAULT '',
                to_location TEXT DEFAULT '',
                action_type TEXT NOT NULL,
                operator_name TEXT DEFAULT 'Admin',
                details TEXT DEFAULT '',
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 10. Staff Accounts & Lanyard Wedge Badges
        await db.execute("""
            CREATE TABLE IF NOT EXISTS staff_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'Counter Staff',
                pin TEXT NOT NULL,
                badge_code TEXT DEFAULT ''
            )
        """)

        # 11. Store Settings
        await db.execute("""
            CREATE TABLE IF NOT EXISTS store_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)

        # Seed Default Safes if empty
        async with db.execute("SELECT COUNT(*) as c FROM locations") as cur:
            row = await cur.fetchone()
            if row["c"] == 0:
                default_locs = [
                    ("SAFE-01", "Safe 1 (Spika)", "Safe", 30, "#0284c7", "Main Cat A/B Longarms"),
                    ("SAFE-02", "Safe 2 (Green)", "Safe", 28, "#10b981", "Secondary Storage & Estates"),
                    ("SAFE-03", "Safe 3 (Copper)", "Safe", 25, "#d97706", "Long-term Xero Storage"),
                    ("SAFE-SHOP2", "Shop #2 Safe", "Safe", 20, "#8b5cf6", "Cat H / High Security"),
                    ("DISP-WALL-01", "Display Wall Rack 1", "Display", 40, "#0ea5e9", "Retail Floor Rack - Must sweep nightly"),
                    ("BENCH-WORKSHOP", "Gunsmith Workbench", "Workshop", 10, "#f43f5e", "Active Repairs & Mounts"),
                ]
                await db.executemany(
                    "INSERT INTO locations (id, name, category, max_capacity, color, notes) VALUES (?,?,?,?,?,?)",
                    default_locs,
                )

        # Seed Default Staff if empty
        async with db.execute("SELECT COUNT(*) as c FROM staff_accounts") as cur:
            if (await cur.fetchone())["c"] == 0:
                await db.executemany(
                    "INSERT INTO staff_accounts (name, role, pin, badge_code) VALUES (?,?,?,?)",
                    [
                        ("Master Admin", "Admin", "4091", "STF:ADMIN"),
                        ("Counter Operator", "Counter Staff", "1234", "STF:STAFF1"),
                    ],
                )

        # Seed Default Suppliers if empty
        async with db.execute("SELECT COUNT(*) as c FROM suppliers") as cur:
            if (await cur.fetchone())["c"] == 0:
                await db.executemany(
                    "INSERT INTO suppliers (name, account_no, dealer_licence_no, rep_name, phone, email, notes) VALUES (?,?,?,?,?,?,?)",
                    [
                        ("NIOA", "TF-NIOA-882", "QLD-DL-400192", "Northern NSW Rep", "07 3621 9999", "orders@nioa.com.au", "Primary firearms, optics & ammo wholesale"),
                        ("Outdoor Sporting Agencies (OSA)", "TF-OSA-410", "VIC-DL-291044", "Account Desk", "03 9796 5900", "sales@osaaustralia.com.au", "Tikka, Sako, Beretta, Steiner"),
                        ("Winchester Australia", "TF-WIN-109", "VIC-DL-109281", "Regional Manager", "03 5245 2700", "orders@winchesteraustralia.com.au", "Browning, Winchester, ADI Powders"),
                    ],
                )

        # Seed Default Settings
        default_settings = {
            "store_name": "Tenterfield Firearms",
            "dealer_licence": "NSW-DEALER-40911082",
            "store_address": "126 Rouse St, Tenterfield NSW 2372",
            "store_phone": "(02) 6736 1234",
            "max_neq_kg": "50.0",
            "barcode_source": "serial",
            "label_profile": "62mm",
            "auto_enter_scan": "true",
            "sticky_intake": "true",
            "hike_store_url": "https://tenterfieldfirearms.hikeup.com",
            "hike_webhook_secret": "CYAN-HIKE-SECRET-2026",
        }
        for k, v in default_settings.items():
            await db.execute("INSERT OR IGNORE INTO store_settings (key, value) VALUES (?, ?)", (k, v))

        # Seed Sample Inventory & Customers if empty
        async with db.execute("SELECT COUNT(*) as c FROM firearms") as cur:
            if (await cur.fetchone())["c"] == 0:
                await db.executemany(
                    "INSERT INTO customers (name, phone, email, address, licence_no, licence_categories, licence_expiry, store_credit, notes) VALUES (?,?,?,?,?,?,?,?,?)",
                    [
                        ("Tania Blewitt", "0411 222 333", "tania@example.com", "Tenterfield NSW 2372", "20918471", "Cat A, Cat B", "2028-11-15", 0.0, "Pays by the month on Xero Invoice automatically."),
                        ("Wendy Daley", "0422 333 444", "wendy@example.com", "Tenterfield NSW 2372", "20847192", "Cat A, Cat B", "2027-06-30", 45.0, "Pays by the month on Xero Invoice automatically."),
                        ("Ian Stewart Shannon", "0433 444 555", "ian@example.com", "Jennings NSW 2372", "20491822", "Cat A, Cat B, Cat H", "2029-03-20", 120.0, "Pays by the month on Xero Invoice automatically."),
                        ("Peter Joseph Graham (Estate)", "0408 928 771", "", "Tenterfield NSW 2372", "ESTATE-GRAHAM", "Cat A, Cat B", "2026-12-31", 0.0, "Deceased Estate - Son Steve Graham awaiting paperwork."),
                    ],
                )
                sample_guns = [
                    ("499618", "TF-34", "711812099", "BK-34", "Firearm", "Stirling", "Unknown", "12G", "Break Action", "28\"", "2-Shot", "Cat A", "Gun Storage", "Customer Storage", "", "Tania Blewitt", "0411 222 333", 15.0, 50.0, "Unpaid", 1, "Tania Blewitt", "0411 222 333", "20918471", "Cat A, Cat B", "", "Private Intake", "20918471", "2023-10-28", "", "", "", 0.0, 0.0, 0.0, "In Store", "SAFE-01", "Xero Monthly Storage", "Pays by the month on Xero Invoice automatically."),
                    ("A160633", "TF-35", "711812136", "BK-35", "Firearm", "H&R", "Model 1915", "410G", "Single Shot", "26\"", "1-Shot", "Cat A", "Gun Storage", "Customer Storage", "", "Tania Blewitt", "0411 222 333", 15.0, 50.0, "Unpaid", 1, "Tania Blewitt", "0411 222 333", "20918471", "Cat A, Cat B", "", "Private Intake", "20918471", "2023-10-28", "", "", "", 0.0, 0.0, 0.0, "In Store", "SAFE-01", "Xero Monthly Storage", "Pays by the month on Xero Invoice automatically."),
                    ("92622800", "TF-37", "721602431", "BK-37", "Firearm", "Marlin", "917", "17HMR", "Bolt Action", "22\"", "7-Shot", "Cat A", "Gun Storage", "Customer Storage", "", "Tania Blewitt", "0411 222 333", 15.0, 50.0, "Unpaid", 1, "Tania Blewitt", "0411 222 333", "20918471", "Cat A, Cat B", "", "Private Intake", "20918471", "2023-10-28", "", "", "", 0.0, 0.0, 0.0, "In Store", "SAFE-01", "Xero Monthly Storage", "Pays by the month on Xero Invoice automatically."),
                    ("11227", "TF-176", "726056943", "BK-176", "Firearm", "ATA Arms", "686", "12G", "Under/Over", "28\"", "2-Shot", "Cat A", "Gun Storage", "Customer Storage", "", "Wendy Daley", "0422 333 444", 15.0, 50.0, "Unpaid", 2, "Wendy Daley", "0422 333 444", "20847192", "Cat A, Cat B", "", "Private Intake", "20847192", "2024-11-09", "", "", "", 0.0, 0.0, 0.0, "In Store", "SAFE-01", "Xero Monthly Storage", "Phoned & emailed options 24/02/26."),
                    ("K323768", "TF-191", "738255499", "BK-191", "Firearm", "Rossi", "R92", "357MAG", "Lever Action", "20\"", "10-Shot", "Cat B", "Gun Storage", "Customer Storage", "", "Ian Stewart Shannon", "0433 444 555", 15.0, 50.0, "Unpaid", 3, "Ian Stewart Shannon", "0433 444 555", "20491822", "Cat A, Cat B, Cat H", "", "Private Intake", "20491822", "2024-12-17", "", "", "", 0.0, 0.0, 0.0, "In Store", "SAFE-03", "Xero Monthly Storage", "Pays by the month on Xero Invoice automatically."),
                    ("784-21691", "TF-194", "738255632", "BK-194", "Firearm", "Ruger", "M77 Hawkeye", "308 WIN", "Bolt Action", "22\"", "4-Shot", "Cat B", "Gun Storage", "Customer Storage", "", "Ian Stewart Shannon", "0433 444 555", 15.0, 50.0, "Unpaid", 3, "Ian Stewart Shannon", "0433 444 555", "20491822", "Cat A, Cat B, Cat H", "", "Private Intake", "20491822", "2024-12-17", "", "", "", 0.0, 0.0, 0.0, "In Store", "SAFE-03", "Xero Monthly Storage", "Pays by the month on Xero Invoice automatically."),
                    ("150-38020", "TF-197", "738255616", "BK-197", "Firearm", "Ruger", "Security Six", "357MAG", "Revolver", "6\"", "6-Shot", "Cat H", "Gun Storage", "Customer Storage", "", "Ian Stewart Shannon", "0433 444 555", 15.0, 50.0, "Unpaid", 3, "Ian Stewart Shannon", "0433 444 555", "20491822", "Cat A, Cat B, Cat H", "", "Private Intake", "20491822", "2024-12-17", "", "", "", 0.0, 0.0, 0.0, "In Store", "SAFE-SHOP2", "Cat H Storage", "Pays by the month on Xero Invoice automatically."),
                    ("19Y12345", "TF-LA102", "740019283", "BK-2026-081", "Firearm", "Lithgow Arms", "LA102 Crossover", "308 WIN", "Bolt Action", "22\"", "4-Shot", "Cat B", "New", "Layby", "Waiting for PTA", "", "", 15.0, 50.0, "Unpaid", 3, "Ian Stewart Shannon", "0433 444 555", "20491822", "Cat A, Cat B, Cat H", "PTA-994812", "OSA", "VIC-DL-291044", "2026-02-10", "", "", "", 1690.0, 1850.0, 1320.0, "In Store", "SAFE-01", "Cerakote Titanium • Threaded", "Deposit $400 paid."),
                    ("SCSA-08841", "TF-TAIPAN", "740099124", "BK-2026-095", "Firearm", "Southern Cross Small Arms", "Taipan X", "223 REM", "Pump / Straight Pull", "16.5\"", "10-Shot", "Cat B", "Consignment", "Consignment Sale", "", "Wendy Daley", "0422 333 444", 15.0, 75.0, "Unpaid", 2, "Wendy Daley", "0422 333 444", "20847192", "Cat A, Cat B", "", "Wendy Daley", "20847192", "2026-03-01", "", "", "", 1850.0, 2100.0, 0.0, "In Store", "DISP-WALL-01", "Includes ZeroTech 1-6x24 LPVO", "Consignment 15% split."),
                ]
                await db.executemany("""
                    INSERT INTO firearms (
                        serial, sku, rego_no, book_no, item_type, make, model, calibre, action,
                        barrel_length, shot_capacity, category, condition, storage_type, layby_stage,
                        consignor_name, consignor_phone, commission_pct, commission_min_fee, payout_status,
                        customer_id, customer_name, customer_phone, customer_licence_no, customer_licence_cats,
                        pta_number, supplier_name, supplier_licence_no, date_acquired, date_disposed,
                        disposed_to_name, disposed_to_licence, price, was_price, cost_price,
                        status, current_location_id, printed_notes, notes
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, sample_guns)

        # Seed Sample Consumables if empty
        async with db.execute("SELECT COUNT(*) as c FROM consumables") as cur:
            if (await cur.fetchone())["c"] == 0:
                await db.executemany("""
                    INSERT INTO consumables (sku, brand, name, item_type, calibre_or_grade, lot_number, quantity, unit_neq_kg, price, min_stock_alert, location_id, notes)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """, [
                    ("PWD-AR2208-1KG", "ADI World Class", "AR2208 Smokeless Rifle Powder (1kg)", "Smokeless Powder", "Varget / Medium", "LOT-2026-04A", 14, 1.0, 145.0, 5, "SAFE-02", "223 Rem / 308 Win primary propellant"),
                    ("PWD-AR2209-1KG", "ADI World Class", "AR2209 Smokeless Rifle Powder (1kg)", "Smokeless Powder", "H4350 Equivalent", "LOT-2026-02B", 8, 1.0, 145.0, 4, "SAFE-02", "243 Win / 6.5 Creedmoor / 270 Win"),
                    ("PRM-CCI200-1K", "CCI", "No. 200 Large Rifle Primers (Brick of 1000)", "Primers", "Large Rifle", "CCI-8841", 12, 0.04, 165.0, 5, "SAFE-02", "Standard Large Rifle"),
                    ("AMM-OSA-308-20", "Australian Outback", "308 Win 165gr Sierra GameKing (20rd Box)", "Factory Ammo", "308 WIN", "AO-308-99", 42, 0.06, 52.0, 10, "SAFE-01", "Adi brass / Sierra projectile"),
                ])

        # Seed Sample Special Order & Work Order if empty
        async with db.execute("SELECT COUNT(*) as c FROM special_orders") as cur:
            if (await cur.fetchone())["c"] == 0:
                await db.execute("""
                    INSERT INTO special_orders (
                        order_id, customer_id, customer_name, customer_phone, customer_licence_no,
                        item_type, make, model, calibre, category, supplier_name, supplier_po_ref,
                        tracking_number, carrier_name, estimated_arrival_date, deposit_paid, total_price,
                        order_status, pta_number, serial_number, notes
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    "SO-2026-0001", 3, "Ian Stewart Shannon", "0433 444 555", "20491822",
                    "Firearm", "Tikka", "T3x Super Varmint Cerakote", "22-250 REM", "Cat B",
                    "Outdoor Sporting Agencies (OSA)", "PO-OSA-88412", "ST-99481022", "StarTrack Secure",
                    "2026-11-05", 500.0, 2490.0, "Arrived in Store", "PTA-883920", "T3X-994821",
                    "Ready for 1-click conversion into active vault inventory."
                ))

        async with db.execute("SELECT COUNT(*) as c FROM work_orders") as cur:
            if (await cur.fetchone())["c"] == 0:
                await db.execute("""
                    INSERT INTO work_orders (
                        job_id, firearm_serial, customer_name, customer_phone, make_model, cal_cat,
                        service_type, fault_description, parts_used, labor_cost, parts_cost,
                        status, test_fired, safe_location_id, operator_name, notes
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    "REP-1001", "784-21691", "Ian Stewart Shannon", "0433 444 555", "Ruger M77 Hawkeye",
                    "308 WIN • Cat B", "Scope Mounting & Bore Sight", "Mount Leupold VX-Freedom 3-9x40, torque rings to 18 in-lbs, bore sight.",
                    "Leupold 1\" Rings + Loctite 242", 65.0, 110.0, "In Progress", "No", "BENCH-WORKSHOP", "Master Admin", "Customer supplying rifle from Safe 3."
                ))

        await db.commit()
    finally:
        await db.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    bg_task = asyncio.create_task(nightly_backup_loop())
    yield
    bg_task.cancel()


app = FastAPI(title="CyanStock Vault & Compliance Suite", version=APP_VERSION, lifespan=lifespan)
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))


# --- Pydantic Schemas ---
class FirearmPayload(BaseModel):
    serial: str
    sku: Optional[str] = ""
    rego_no: Optional[str] = ""
    book_no: Optional[str] = ""
    item_type: Optional[str] = "Firearm"
    make: str
    model: str
    calibre: str
    action: Optional[str] = ""
    barrel_length: Optional[str] = ""
    shot_capacity: Optional[str] = ""
    category: Optional[str] = "Cat A"
    condition: Optional[str] = "New"
    storage_type: Optional[str] = "Sale (Floor Stock)"
    layby_stage: Optional[str] = ""
    consignor_name: Optional[str] = ""
    consignor_phone: Optional[str] = ""
    commission_pct: Optional[float] = 15.0
    commission_min_fee: Optional[float] = 50.0
    payout_status: Optional[str] = "Unpaid"
    customer_id: Optional[int] = None
    customer_name: Optional[str] = ""
    customer_phone: Optional[str] = ""
    customer_licence_no: Optional[str] = ""
    customer_licence_cats: Optional[str] = ""
    pta_number: Optional[str] = ""
    supplier_name: Optional[str] = ""
    supplier_licence_no: Optional[str] = ""
    date_acquired: Optional[str] = ""
    price: Optional[float] = 0.0
    was_price: Optional[float] = 0.0
    cost_price: Optional[float] = 0.0
    status: Optional[str] = "In Store"
    current_location_id: Optional[str] = "SAFE-01"
    printed_notes: Optional[str] = ""
    notes: Optional[str] = ""
    operator_name: Optional[str] = "Admin"
    force_overwrite: Optional[bool] = False


class ReleaseGuardPayload(BaseModel):
    serial: str
    customer_name: str
    customer_licence_no: str
    customer_licence_cats: str
    pta_number: str
    disposal_date: Optional[str] = ""
    operator_name: Optional[str] = "Admin"


class ScanAllocatePayload(BaseModel):
    serial_or_code: str
    target_location_id: str
    operator_name: Optional[str] = "Admin"
    force_overcapacity: Optional[bool] = True


class BulkMovePayload(BaseModel):
    serials: List[str]
    target_location_id: str
    operator_name: Optional[str] = "Admin"


class SafeAuditPayload(BaseModel):
    location_id: str
    scanned_codes: List[str]


class LocationPayload(BaseModel):
    id: str
    name: str
    category: str = "Safe"
    max_capacity: int = 30
    color: str = "#0284c7"
    notes: Optional[str] = ""


class SpecialOrderPayload(BaseModel):
    order_id: Optional[str] = ""
    customer_name: str
    customer_phone: Optional[str] = ""
    customer_licence_no: Optional[str] = ""
    item_type: Optional[str] = "Firearm"
    make: str
    model: str
    calibre: Optional[str] = ""
    category: Optional[str] = "Cat A"
    supplier_name: Optional[str] = "NIOA"
    supplier_po_ref: Optional[str] = ""
    tracking_number: Optional[str] = ""
    carrier_name: Optional[str] = "StarTrack Secure"
    estimated_arrival_date: Optional[str] = ""
    deposit_paid: Optional[float] = 0.0
    total_price: Optional[float] = 0.0
    order_status: Optional[str] = "Staged / Ordered"
    pta_number: Optional[str] = ""
    serial_number: Optional[str] = ""
    notes: Optional[str] = ""


class ConvertSpecialOrderPayload(BaseModel):
    serial_number: str
    rego_no: Optional[str] = ""
    book_no: Optional[str] = ""
    target_location_id: str = "SAFE-01"
    storage_type: str = "Layby"
    operator_name: Optional[str] = "Admin"


class ConsumablePayload(BaseModel):
    id: Optional[int] = None
    sku: str
    brand: str
    name: str
    item_type: str = "Smokeless Powder"
    calibre_or_grade: Optional[str] = ""
    lot_number: Optional[str] = ""
    quantity: int = 0
    unit_neq_kg: float = 0.5
    price: float = 0.0
    min_stock_alert: int = 5
    location_id: Optional[str] = "SAFE-02"
    notes: Optional[str] = ""


class WorkOrderPayload(BaseModel):
    job_id: Optional[str] = ""
    firearm_serial: Optional[str] = ""
    customer_name: str
    customer_phone: Optional[str] = ""
    make_model: str
    cal_cat: Optional[str] = ""
    service_type: Optional[str] = "General Repair"
    fault_description: Optional[str] = ""
    parts_used: Optional[str] = ""
    labor_cost: Optional[float] = 0.0
    parts_cost: Optional[float] = 0.0
    status: Optional[str] = "Awaiting Assessment"
    test_fired: Optional[str] = "No"
    safe_location_id: Optional[str] = "BENCH-WORKSHOP"
    operator_name: Optional[str] = "Admin"
    notes: Optional[str] = ""


class ShipmentPayload(BaseModel):
    shipment_no: Optional[str] = ""
    recipient_name: str
    recipient_business: Optional[str] = ""
    recipient_phone: Optional[str] = ""
    recipient_licence: str
    destination_state: Optional[str] = "QLD"
    destination_address: str
    end_customer_name: Optional[str] = ""
    end_customer_phone: Optional[str] = ""
    end_customer_licence: Optional[str] = ""
    end_customer_pta: Optional[str] = ""
    carrier_name: Optional[str] = "StarTrack Secure"
    tracking_number: Optional[str] = ""
    shipment_type: Optional[str] = "Interstate Dealer Transfer (NSW -> QLD)"
    firearm_serials: List[str] = []
    notes: Optional[str] = ""


class CustomerPayload(BaseModel):
    id: Optional[int] = None
    name: str
    phone: Optional[str] = ""
    email: Optional[str] = ""
    address: Optional[str] = ""
    licence_no: Optional[str] = ""
    licence_categories: Optional[str] = "Cat A, Cat B"
    licence_expiry: Optional[str] = ""
    store_credit: Optional[float] = 0.0
    notes: Optional[str] = ""


class SupplierPayload(BaseModel):
    id: Optional[int] = None
    name: str
    account_no: Optional[str] = ""
    dealer_licence_no: Optional[str] = ""
    rep_name: Optional[str] = ""
    phone: Optional[str] = ""
    email: Optional[str] = ""
    notes: Optional[str] = ""


class StaffPayload(BaseModel):
    name: str
    role: str = "Counter Staff"
    pin: str
    badge_code: Optional[str] = ""


# --- Routes ---
@app.get("/", response_class=HTMLResponse)
async def serve_dashboard(request: Request):
    return templates.TemplateResponse("index.html", {"request": request, "version": APP_VERSION})


@app.get("/delivery/{shipment_no}", response_class=HTMLResponse)
async def serve_mobile_delivery(shipment_no: str):
    db = await get_db()
    try:
        async with db.execute("SELECT * FROM shipments WHERE shipment_no = ?", (shipment_no.upper(),)) as cur:
            sh = await cur.fetchone()
            if not sh:
                return HTMLResponse("<h2>Shipment Not Found</h2>", status_code=404)
            sh_dict = dict(sh)
        async with db.execute("""
            SELECT f.* FROM shipment_items si
            JOIN firearms f ON si.firearm_serial = f.serial
            WHERE si.shipment_no = ?
        """, (shipment_no.upper(),)) as cur:
            items = [dict(r) for r in await cur.fetchall()]
        rows_html = "".join([
            f"<tr><td style='padding:8px;border-bottom:1px solid #ddd;font-family:monospace;font-weight:bold;'>{i['serial']}</td>"
            f"<td style='padding:8px;border-bottom:1px solid #ddd;'>{i['make']} {i['model']} ({i['calibre']})</td>"
            f"<td style='padding:8px;border-bottom:1px solid #ddd;'>{i['rego_no'] or '-'}</td></tr>"
            for i in items
        ])
        html = f"""
        <!DOCTYPE html>
        <html>
        <head><meta name="viewport" content="width=device-width,initial-scale=1"><title>Delivery Sign-Off {sh_dict['shipment_no']}</title></head>
        <body style="font-family:system-ui,sans-serif;max-width:600px;margin:20px auto;padding:16px;background:#f8fafc;color:#0f172a;">
            <div style="background:#fff;border:2px solid #0f172a;border-radius:10px;padding:20px;">
                <div style="background:#0f172a;color:#fff;padding:10px 14px;border-radius:6px;font-weight:800;margin-bottom:14px;">
                    CYANTRANSIT • RECEIVING DEALER SIGN-OFF ({sh_dict['shipment_no']})
                </div>
                <p><strong>Receiving Dealer:</strong> {sh_dict['recipient_business'] or sh_dict['recipient_name']} (Lic: {sh_dict['recipient_licence']})</p>
                <p><strong>Destination:</strong> {sh_dict['destination_address']}</p>
                <p><strong>Status:</strong> <span style="font-weight:bold;color:#0284c7;">{sh_dict['status']}</span></p>
                <table style="width:100%;border-collapse:collapse;margin:16px 0;font-size:14px;">
                    <thead><tr style="background:#f1f5f9;text-align:left;"><th style="padding:8px;">Serial</th><th style="padding:8px;">Firearm</th><th style="padding:8px;">Rego</th></tr></thead>
                    <tbody>{rows_html}</tbody>
                </table>
                <form onsubmit="event.preventDefault(); fetch('/api/shipments/{sh_dict['shipment_no']}/deliver', {{method:'POST',headers:{{'Content-Type':'application/json'}},body:JSON.stringify({{signed_by:document.getElementById('sig').value}})}}).then(()=>location.reload());">
                    <label style="display:block;font-weight:700;margin-bottom:6px;">Receiving Dealer Name & Licence Confirmation:</label>
                    <input id="sig" required placeholder="e.g. J. Smith - Lic #50001234" value="{sh_dict['signed_by'] or ''}" style="width:100%;padding:10px;border:1px solid #94a3b8;border-radius:6px;margin-bottom:12px;box-sizing:border-box;">
                    <button type="submit" style="width:100%;padding:12px;background:#10b981;color:#fff;font-weight:800;border:none;border-radius:6px;cursor:pointer;">
                        ✓ CONFIRM SERIALS & SIGN OFF INTAKE
                    </button>
                </form>
            </div>
        </body>
        </html>
        """
        return HTMLResponse(html)
    finally:
        await db.close()


@app.get("/api/bootstrap")
async def api_bootstrap():
    db = await get_db()
    try:
        async with db.execute("SELECT * FROM store_settings") as cur:
            settings = {r["key"]: r["value"] for r in await cur.fetchall()}

        async with db.execute("SELECT * FROM locations ORDER BY id") as cur:
            locations = [dict(r) for r in await cur.fetchall()]

        async with db.execute("SELECT * FROM firearms ORDER BY last_scanned_at DESC") as cur:
            firearms = [dict(r) for r in await cur.fetchall()]

        # Calculate live safe counts & fill percentage + detect normalized duplicate serials
        loc_counts: Dict[str, int] = {}
        norm_serial_map: Dict[str, List[str]] = {}
        closing_sweep: List[Dict[str, Any]] = []

        for f in firearms:
            # Consignment commission calculation
            price = float(f.get("price") or 0.0)
            pct = float(f.get("commission_pct") or 15.0)
            min_fee = float(f.get("commission_min_fee") or 50.0)
            comm_cut = round(max(price * (pct / 100.0), min_fee), 2) if price > 0 else 0.0
            f["calculated_commission"] = comm_cut
            f["consignor_net_payout"] = round(max(price - comm_cut, 0.0), 2)

            if f["status"] == "In Store":
                lid = f.get("current_location_id") or ""
                loc_counts[lid] = loc_counts.get(lid, 0) + 1
                norm_s = normalize_code(f["serial"])
                if norm_s:
                    norm_serial_map.setdefault(norm_s, []).append(f["serial"])
                if lid.startswith("DISP-") or lid.startswith("BENCH-"):
                    closing_sweep.append(f)

        duplicate_serials = [
            {"normalized": k, "serials": v}
            for k, v in norm_serial_map.items()
            if len(v) > 1
        ]

        loc_lookup = {}
        for loc in locations:
            cnt = loc_counts.get(loc["id"], 0)
            cap = int(loc.get("max_capacity") or 0)
            loc["current_count"] = cnt
            loc["fill_pct"] = round((cnt / cap) * 100, 1) if cap > 0 else 0.0
            loc_lookup[loc["id"]] = loc["name"]

        for f in firearms:
            f["location_name"] = loc_lookup.get(f.get("current_location_id"), f.get("current_location_id") or "Unassigned")

        async with db.execute("SELECT * FROM customers ORDER BY name COLLATE NOCASE") as cur:
            customers = [dict(r) for r in await cur.fetchall()]

        # Auto customer total gun count
        cust_gun_counts: Dict[str, int] = {}
        for f in firearms:
            if f["status"] == "In Store":
                cname = (f.get("customer_name") or f.get("consignor_name") or "").strip().lower()
                if cname:
                    cust_gun_counts[cname] = cust_gun_counts.get(cname, 0) + 1
        for c in customers:
            c["active_gun_count"] = cust_gun_counts.get(c["name"].strip().lower(), 0)

        async with db.execute("SELECT * FROM special_orders ORDER BY created_at DESC") as cur:
            special_orders = [dict(r) for r in await cur.fetchall()]

        async with db.execute("SELECT * FROM suppliers ORDER BY name") as cur:
            suppliers = [dict(r) for r in await cur.fetchall()]

        async with db.execute("SELECT * FROM consumables ORDER BY item_type, brand, name") as cur:
            consumables = [dict(r) for r in await cur.fetchall()]

        total_neq_kg = round(
            sum(
                float(c.get("quantity") or 0) * float(c.get("unit_neq_kg") or 0.0)
                for c in consumables
                if c.get("item_type") in ("Smokeless Powder", "Black Powder", "Primers")
            ),
            2,
        )
        max_neq_kg = float(settings.get("max_neq_kg") or 50.0)

        async with db.execute("SELECT * FROM work_orders ORDER BY created_at DESC") as cur:
            work_orders = [dict(r) for r in await cur.fetchall()]

        async with db.execute("SELECT * FROM shipments ORDER BY created_at DESC") as cur:
            shipments = [dict(r) for r in await cur.fetchall()]
        for sh in shipments:
            async with db.execute("""
                SELECT f.* FROM shipment_items si
                JOIN firearms f ON si.firearm_serial = f.serial
                WHERE si.shipment_no = ?
            """, (sh["shipment_no"],)) as cur_i:
                sh["items"] = [dict(x) for x in await cur_i.fetchall()]

        async with db.execute("SELECT * FROM staff_accounts ORDER BY id") as cur:
            staff = [dict(r) for r in await cur.fetchall()]

        async with db.execute("SELECT * FROM movement_logs ORDER BY id DESC LIMIT 120") as cur:
            movement_logs = [dict(r) for r in await cur.fetchall()]

        backups = sorted(
            [f for f in os.listdir(BACKUP_DIR) if f.endswith(".zip")],
            reverse=True,
        )[:15]

        return {
            "version": APP_VERSION,
            "settings": settings,
            "locations": locations,
            "firearms": firearms,
            "customers": customers,
            "special_orders": special_orders,
            "suppliers": suppliers,
            "consumables": consumables,
            "neq_summary": {
                "total_neq_kg": total_neq_kg,
                "max_neq_kg": max_neq_kg,
                "pct": round((total_neq_kg / max_neq_kg) * 100, 1) if max_neq_kg > 0 else 0.0,
            },
            "work_orders": work_orders,
            "shipments": shipments,
            "staff": staff,
            "movement_logs": movement_logs,
            "duplicate_serials": duplicate_serials,
            "closing_sweep": closing_sweep,
            "backups": backups,
        }
    finally:
        await db.close()


@app.post("/api/firearms")
async def api_upsert_firearm(payload: FirearmPayload):
    serial_clean = payload.serial.strip().upper()
    if not serial_clean:
        raise HTTPException(status_code=400, detail="Serial number is required.")

    db = await get_db()
    try:
        # Duplicate Serial Collision Guard (checks normalized serials across active stock)
        norm_target = normalize_code(serial_clean)
        async with db.execute("SELECT serial, current_location_id, make, model, status FROM firearms") as cur:
            all_rows = await cur.fetchall()
            for r in all_rows:
                if normalize_code(r["serial"]) == norm_target and r["serial"].upper() != serial_clean and r["status"] == "In Store":
                    if not payload.force_overwrite:
                        raise HTTPException(
                            status_code=409,
                            detail=f"DUPLICATE SERIAL GUARD: Serial '{r['serial']}' ({r['make']} {r['model']}) is already active in {r['current_location_id']}!"
                        )

        # Condition-aware owner suppression for floor stock
        is_owner_applicable = payload.storage_type in ("Customer Storage", "Consignment Sale", "Layby", "Workshop / Repair") or payload.condition in ("Consignment", "Gun Storage")
        c_name = payload.consignor_name.strip() if is_owner_applicable else ""
        c_phone = payload.consignor_phone.strip() if is_owner_applicable else ""
        cust_name = (payload.customer_name.strip() or c_name) if is_owner_applicable else ""
        cust_phone = (payload.customer_phone.strip() or c_phone) if is_owner_applicable else ""

        sku_clean = (payload.sku or "").strip().upper() or f"TF-{serial_clean[-6:]}"
        acq_date = (payload.date_acquired or "").strip() or datetime.now().strftime("%Y-%m-%d")

        async with db.execute("SELECT serial, current_location_id FROM firearms WHERE serial = ?", (serial_clean,)) as cur:
            existing = await cur.fetchone()

        if existing and not payload.force_overwrite:
            raise HTTPException(
                status_code=409,
                detail=f"DUPLICATE SERIAL GUARD: Serial '{serial_clean}' already exists in location {existing['current_location_id']}. Confirm update or enable overwrite."
            )

        await db.execute("""
            INSERT INTO firearms (
                serial, sku, rego_no, book_no, item_type, make, model, calibre, action,
                barrel_length, shot_capacity, category, condition, storage_type, layby_stage,
                consignor_name, consignor_phone, commission_pct, commission_min_fee, payout_status,
                customer_id, customer_name, customer_phone, customer_licence_no, customer_licence_cats,
                pta_number, supplier_name, supplier_licence_no, date_acquired,
                price, was_price, cost_price, status, current_location_id, printed_notes, notes, last_scanned_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)
            ON CONFLICT(serial) DO UPDATE SET
                sku=excluded.sku, rego_no=excluded.rego_no, book_no=excluded.book_no,
                item_type=excluded.item_type, make=excluded.make, model=excluded.model,
                calibre=excluded.calibre, action=excluded.action, barrel_length=excluded.barrel_length,
                shot_capacity=excluded.shot_capacity, category=excluded.category, condition=excluded.condition,
                storage_type=excluded.storage_type, layby_stage=excluded.layby_stage,
                consignor_name=excluded.consignor_name, consignor_phone=excluded.consignor_phone,
                commission_pct=excluded.commission_pct, commission_min_fee=excluded.commission_min_fee,
                payout_status=excluded.payout_status, customer_id=excluded.customer_id,
                customer_name=excluded.customer_name, customer_phone=excluded.customer_phone,
                customer_licence_no=excluded.customer_licence_no, customer_licence_cats=excluded.customer_licence_cats,
                pta_number=excluded.pta_number, supplier_name=excluded.supplier_name,
                supplier_licence_no=excluded.supplier_licence_no, date_acquired=excluded.date_acquired,
                price=excluded.price, was_price=excluded.was_price, cost_price=excluded.cost_price,
                status=excluded.status, current_location_id=excluded.current_location_id,
                printed_notes=excluded.printed_notes, notes=excluded.notes, last_scanned_at=CURRENT_TIMESTAMP
        """, (
            serial_clean, sku_clean, payload.rego_no.strip(), payload.book_no.strip(),
            payload.item_type, payload.make.strip(), payload.model.strip(), payload.calibre.strip(),
            payload.action.strip(), payload.barrel_length.strip(), payload.shot_capacity.strip(),
            payload.category, payload.condition, payload.storage_type, payload.layby_stage,
            c_name, c_phone, payload.commission_pct, payload.commission_min_fee, payload.payout_status,
            payload.customer_id, cust_name, cust_phone, payload.customer_licence_no.strip(),
            payload.customer_licence_cats.strip(), payload.pta_number.strip().upper(),
            payload.supplier_name.strip(), payload.supplier_licence_no.strip(), acq_date,
            payload.price or 0.0, payload.was_price or 0.0, payload.cost_price or 0.0,
            payload.status, payload.current_location_id, payload.printed_notes.strip(), payload.notes.strip()
        ))

        action_lbl = "Updated Dossier" if existing else "Intake Recorded"
        await db.execute("""
            INSERT INTO movement_logs (firearm_serial, from_location, to_location, action_type, operator_name, details)
            VALUES (?,?,?,?,?,?)
        """, (
            serial_clean,
            existing["current_location_id"] if existing else "INTAKE",
            payload.current_location_id,
            action_lbl,
            payload.operator_name or "Admin",
            f"{payload.make} {payload.model} ({payload.storage_type})"
        ))
        await db.commit()
        return {"status": "success", "serial": serial_clean}
    finally:
        await db.close()


@app.post("/api/firearms/release")
async def api_release_firearm_with_guard(payload: ReleaseGuardPayload):
    """PTA & Licence Category Validation Guardrail prior to disposing/releasing a firearm."""
    db = await get_db()
    try:
        async with db.execute("SELECT * FROM firearms WHERE serial = ?", (payload.serial.strip().upper(),)) as cur:
            gun = await cur.fetchone()
            if not gun:
                raise HTTPException(status_code=404, detail="Firearm not found.")
            gun_dict = dict(gun)

        gun_cat = (gun_dict.get("category") or "Cat A").strip().upper()
        # Validate PTA & Licence Category if item is a regulated Firearm
        if gun_dict.get("item_type", "Firearm") == "Firearm" and gun_cat.startswith("CAT"):
            if not payload.pta_number or len(payload.pta_number.strip()) < 4:
                raise HTTPException(
                    status_code=400,
                    detail="COMPLIANCE BLOCK: A valid NSW Permit to Acquire (PTA) number is required before releasing this firearm."
                )
            if not payload.customer_licence_no or len(payload.customer_licence_no.strip()) < 5:
                raise HTTPException(
                    status_code=400,
                    detail="COMPLIANCE BLOCK: Customer Firearms Licence Number is required."
                )
            lic_cats_upper = (payload.customer_licence_cats or "").upper()
            if gun_cat not in lic_cats_upper:
                raise HTTPException(
                    status_code=400,
                    detail=f"LICENCE CATEGORY MISMATCH: Firearm requires '{gun_dict['category']}', but customer licence is only endorsed for '{payload.customer_licence_cats}'."
                )

        disp_date = payload.disposal_date.strip() or datetime.now().strftime("%Y-%m-%d")
        await db.execute("""
            UPDATE firearms
            SET status = 'Disposed/Sold',
                current_location_id = 'DISPOSED',
                customer_name = ?,
                customer_licence_no = ?,
                customer_licence_cats = ?,
                pta_number = ?,
                date_disposed = ?,
                disposed_to_name = ?,
                disposed_to_licence = ?,
                layby_stage = 'Collected / Finalized',
                last_scanned_at = CURRENT_TIMESTAMP
            WHERE serial = ?
        """, (
            payload.customer_name.strip(),
            payload.customer_licence_no.strip(),
            payload.customer_licence_cats.strip(),
            payload.pta_number.strip().upper(),
            disp_date,
            payload.customer_name.strip(),
            payload.customer_licence_no.strip(),
            gun_dict["serial"],
        ))

        await db.execute("""
            INSERT INTO movement_logs (firearm_serial, from_location, to_location, action_type, operator_name, details)
            VALUES (?,?,?,?,?,?)
        """, (
            gun_dict["serial"],
            gun_dict.get("current_location_id") or "",
            "DISPOSED",
            "PTA Validated Disposal",
            payload.operator_name or "Admin",
            f"Released to {payload.customer_name} (Lic: {payload.customer_licence_no}, PTA: {payload.pta_number.strip().upper()})"
        ))
        await db.commit()
        return {"status": "success", "serial": gun_dict["serial"]}
    finally:
        await db.close()


@app.delete("/api/firearms/{serial}")
async def api_delete_firearm(serial: str, operator: str = Query("Admin")):
    db = await get_db()
    try:
        await db.execute("DELETE FROM firearms WHERE serial = ?", (serial.strip().upper(),))
        await db.execute("""
            INSERT INTO movement_logs (firearm_serial, from_location, to_location, action_type, operator_name, details)
            VALUES (?,?,?,?,?,?)
        """, (serial.strip().upper(), "VAULT", "DELETED", "Record Deleted", operator, "Removed from active database"))
        await db.commit()
        return {"status": "success"}
    finally:
        await db.close()


@app.get("/api/lookup/{code}")
async def api_lookup_code(code: str):
    clean = normalize_code(code)
    db = await get_db()
    try:
        async with db.execute("SELECT * FROM firearms") as cur:
            rows = [dict(r) for r in await cur.fetchall()]
            for r in rows:
                if clean in (
                    normalize_code(r.get("serial")),
                    normalize_code(r.get("sku")),
                    normalize_code(r.get("rego_no")),
                    normalize_code(r.get("book_no")),
                ):
                    return {"matched": True, "firearm": r}
        return {"matched": False, "code": code}
    finally:
        await db.close()


@app.post("/api/scan/allocate")
async def api_scan_allocate(payload: ScanAllocatePayload):
    target_loc = payload.target_location_id.strip().upper().replace("LOC:", "")
    clean = normalize_code(payload.serial_or_code)
    db = await get_db()
    try:
        async with db.execute("SELECT * FROM locations WHERE UPPER(id) = ?", (target_loc,)) as cur:
            loc = await cur.fetchone()
            if not loc:
                raise HTTPException(status_code=404, detail=f"Safe location '{target_loc}' not found.")

        matched_gun = None
        async with db.execute("SELECT * FROM firearms") as cur:
            for r in await cur.fetchall():
                if clean and clean in (
                    normalize_code(r["serial"]),
                    normalize_code(r["sku"]),
                    normalize_code(r["rego_no"]),
                    normalize_code(r["book_no"]),
                ):
                    matched_gun = dict(r)
                    break

        if not matched_gun:
            raise HTTPException(status_code=404, detail=f"Unrecognized firearm barcode/serial: {payload.serial_or_code}")

        old_loc = matched_gun.get("current_location_id") or "UNASSIGNED"
        await db.execute("""
            UPDATE firearms
            SET current_location_id = ?, status = 'In Store', last_scanned_at = CURRENT_TIMESTAMP
            WHERE serial = ?
        """, (loc["id"], matched_gun["serial"]))

        await db.execute("""
            INSERT INTO movement_logs (firearm_serial, from_location, to_location, action_type, operator_name, details)
            VALUES (?,?,?,?,?,?)
        """, (
            matched_gun["serial"], old_loc, loc["id"], "Two-Scan Transfer",
            payload.operator_name or "Admin", f"{matched_gun['make']} {matched_gun['model']}"
        ))
        await db.commit()
        return {"status": "success", "serial": matched_gun["serial"], "make": matched_gun["make"], "model": matched_gun["model"], "from": old_loc, "to": loc["id"], "to_name": loc["name"]}
    finally:
        await db.close()


@app.post("/api/scan/bulk-move")
async def api_bulk_move(payload: BulkMovePayload):
    target_loc = payload.target_location_id.strip().upper().replace("LOC:", "")
    db = await get_db()
    try:
        moved = 0
        for s in payload.serials:
            async with db.execute("SELECT serial, current_location_id, make, model FROM firearms WHERE serial = ?", (s.strip().upper(),)) as cur:
                g = await cur.fetchone()
                if g:
                    await db.execute("UPDATE firearms SET current_location_id = ?, last_scanned_at = CURRENT_TIMESTAMP WHERE serial = ?", (target_loc, g["serial"]))
                    await db.execute("""
                        INSERT INTO movement_logs (firearm_serial, from_location, to_location, action_type, operator_name, details)
                        VALUES (?,?,?,?,?,?)
                    """, (g["serial"], g["current_location_id"], target_loc, "Bulk Safe Move", payload.operator_name or "Admin", f"{g['make']} {g['model']}"))
                    moved += 1
        await db.commit()
        return {"status": "success", "moved_count": moved, "target": target_loc}
    finally:
        await db.close()


@app.post("/api/scan/audit")
async def api_safe_stocktake_audit(payload: SafeAuditPayload):
    loc_id = payload.location_id.strip().upper().replace("LOC:", "")
    scanned_norms = {normalize_code(c) for c in payload.scanned_codes if normalize_code(c)}
    db = await get_db()
    try:
        async with db.execute("SELECT * FROM firearms WHERE status = 'In Store'") as cur:
            all_guns = [dict(r) for r in await cur.fetchall()]

        expected = [g for g in all_guns if (g.get("current_location_id") or "").upper() == loc_id]
        verified = []
        missing = []
        matched_norms = set()

        for g in expected:
            g_codes = {normalize_code(g.get("serial")), normalize_code(g.get("sku")), normalize_code(g.get("rego_no"))} - {""}
            hit = g_codes.intersection(scanned_norms)
            if hit:
                verified.append(g)
                matched_norms.update(hit)
            else:
                missing.append(g)

        misplaced = []
        for g in all_guns:
            if (g.get("current_location_id") or "").upper() != loc_id:
                g_codes = {normalize_code(g.get("serial")), normalize_code(g.get("sku")), normalize_code(g.get("rego_no"))} - {""}
                if g_codes.intersection(scanned_norms):
                    misplaced.append(g)

        return {"location_id": loc_id, "verified": verified, "missing": missing, "misplaced": misplaced}
    finally:
        await db.close()


@app.get("/api/bound-book/export")
async def api_bound_book_export(month: Optional[str] = Query(None)):
    """Exports NSW Police Firearms Registry Electronic Bound Book (Acquisition & Disposal) CSV."""
    db = await get_db()
    try:
        async with db.execute("SELECT * FROM firearms ORDER BY date_acquired DESC, last_scanned_at DESC") as cur:
            rows = [dict(r) for r in await cur.fetchall()]

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "Dealer Book Ref", "NSW Rego No", "Serial Number", "Make", "Model", "Calibre",
            "Action", "Category", "Acquisition Date", "Acquired From / Supplier",
            "Supplier/Transferor Licence", "Current Status / Safe", "Disposal Date",
            "Disposed To (Customer/Dealer)", "Acquirer Licence No", "NSW PTA Number"
        ])
        for r in rows:
            if month and not (str(r.get("date_acquired") or "").startswith(month) or str(r.get("date_disposed") or "").startswith(month)):
                continue
            writer.writerow([
                r.get("book_no") or "",
                r.get("rego_no") or "",
                r.get("serial") or "",
                r.get("make") or "",
                r.get("model") or "",
                r.get("calibre") or "",
                r.get("action") or "",
                r.get("category") or "",
                r.get("date_acquired") or "",
                r.get("supplier_name") or r.get("consignor_name") or r.get("customer_name") or "Dealer Intake",
                r.get("supplier_licence_no") or r.get("customer_licence_no") or "",
                f"{r.get('status')} ({r.get('current_location_id')})",
                r.get("date_disposed") or "",
                r.get("disposed_to_name") or "",
                r.get("disposed_to_licence") or "",
                r.get("pta_number") or "",
            ])
        output.seek(0)
        fname = f"NSW_Bound_Book_{month or 'ALL'}_{datetime.now().strftime('%Y%m%d')}.csv"
        return StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={fname}"},
        )
    finally:
        await db.close()


@app.post("/api/consignments/{serial}/settle")
async def api_settle_consignment(serial: str, status: str = Query("Paid & Settled")):
    db = await get_db()
    try:
        await db.execute("UPDATE firearms SET payout_status = ? WHERE serial = ?", (status, serial.strip().upper()))
        await db.commit()
        return {"status": "success"}
    finally:
        await db.close()


@app.post("/api/special-orders")
async def api_upsert_special_order(payload: SpecialOrderPayload):
    oid = (payload.order_id or "").strip().upper() or f"SO-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    db = await get_db()
    try:
        await db.execute("""
            INSERT INTO special_orders (
                order_id, customer_name, customer_phone, customer_licence_no, item_type,
                make, model, calibre, category, supplier_name, supplier_po_ref,
                tracking_number, carrier_name, estimated_arrival_date, deposit_paid,
                total_price, order_status, pta_number, serial_number, notes
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(order_id) DO UPDATE SET
                customer_name=excluded.customer_name, customer_phone=excluded.customer_phone,
                customer_licence_no=excluded.customer_licence_no, item_type=excluded.item_type,
                make=excluded.make, model=excluded.model, calibre=excluded.calibre,
                category=excluded.category, supplier_name=excluded.supplier_name,
                supplier_po_ref=excluded.supplier_po_ref, tracking_number=excluded.tracking_number,
                carrier_name=excluded.carrier_name, estimated_arrival_date=excluded.estimated_arrival_date,
                deposit_paid=excluded.deposit_paid, total_price=excluded.total_price,
                order_status=excluded.order_status, pta_number=excluded.pta_number,
                serial_number=excluded.serial_number, notes=excluded.notes
        """, (
            oid, payload.customer_name.strip(), payload.customer_phone.strip(),
            payload.customer_licence_no.strip(), payload.item_type, payload.make.strip(),
            payload.model.strip(), payload.calibre.strip(), payload.category,
            payload.supplier_name.strip(), payload.supplier_po_ref.strip(),
            payload.tracking_number.strip(), payload.carrier_name.strip(),
            payload.estimated_arrival_date.strip(), payload.deposit_paid or 0.0,
            payload.total_price or 0.0, payload.order_status, payload.pta_number.strip().upper(),
            payload.serial_number.strip().upper(), payload.notes.strip()
        ))
        await db.commit()
        return {"status": "success", "order_id": oid}
    finally:
        await db.close()


@app.post("/api/special-orders/{order_id}/convert")
async def api_convert_special_order(order_id: str, payload: ConvertSpecialOrderPayload):
    db = await get_db()
    try:
        async with db.execute("SELECT * FROM special_orders WHERE order_id = ?", (order_id.upper(),)) as cur:
            so = await cur.fetchone()
            if not so:
                raise HTTPException(status_code=404, detail="Special order not found.")
            so_d = dict(so)

        serial_clean = (payload.serial_number or so_d.get("serial_number") or "").strip().upper()
        if not serial_clean:
            raise HTTPException(status_code=400, detail="Enter a serial number before converting to active stock.")

        await db.execute("""
            INSERT INTO firearms (
                serial, sku, rego_no, book_no, item_type, make, model, calibre, category,
                condition, storage_type, layby_stage, customer_name, customer_phone,
                customer_licence_no, pta_number, supplier_name, date_acquired, price,
                status, current_location_id, notes
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(serial) DO UPDATE SET
                current_location_id=excluded.current_location_id,
                storage_type=excluded.storage_type,
                customer_name=excluded.customer_name,
                pta_number=excluded.pta_number
        """, (
            serial_clean, f"TF-{serial_clean[-6:]}", payload.rego_no.strip(), payload.book_no.strip(),
            so_d["item_type"], so_d["make"], so_d["model"], so_d["calibre"], so_d["category"],
            "New", payload.storage_type, "Ready for Collection" if so_d.get("pta_number") else "Waiting for PTA",
            so_d["customer_name"], so_d["customer_phone"], so_d["customer_licence_no"],
            so_d["pta_number"], so_d["supplier_name"], datetime.now().strftime("%Y-%m-%d"),
            so_d["total_price"], "In Store", payload.target_location_id,
            f"Converted from Special Order {order_id} (Deposit Paid: ${so_d['deposit_paid']:.2f}). {so_d['notes']}"
        ))

        await db.execute("UPDATE special_orders SET order_status = 'Fulfilled / Converted', serial_number = ? WHERE order_id = ?", (serial_clean, order_id.upper()))
        await db.execute("""
            INSERT INTO movement_logs (firearm_serial, from_location, to_location, action_type, operator_name, details)
            VALUES (?,?,?,?,?,?)
        """, (serial_clean, f"INDENT:{order_id}", payload.target_location_id, "Special Order Converted", payload.operator_name or "Admin", f"{so_d['make']} {so_d['model']} for {so_d['customer_name']}"))
        await db.commit()
        return {"status": "success", "serial": serial_clean}
    finally:
        await db.close()


@app.post("/api/consumables")
async def api_upsert_consumable(payload: ConsumablePayload):
    db = await get_db()
    try:
        if payload.id:
            await db.execute("""
                UPDATE consumables SET sku=?, brand=?, name=?, item_type=?, calibre_or_grade=?,
                lot_number=?, quantity=?, unit_neq_kg=?, price=?, min_stock_alert=?, location_id=?, notes=?, updated_at=CURRENT_TIMESTAMP
                WHERE id=?
            """, (
                payload.sku.strip().upper(), payload.brand.strip(), payload.name.strip(),
                payload.item_type, payload.calibre_or_grade.strip(), payload.lot_number.strip(),
                payload.quantity, payload.unit_neq_kg, payload.price, payload.min_stock_alert,
                payload.location_id, payload.notes.strip(), payload.id
            ))
        else:
            await db.execute("""
                INSERT INTO consumables (sku, brand, name, item_type, calibre_or_grade, lot_number, quantity, unit_neq_kg, price, min_stock_alert, location_id, notes)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                payload.sku.strip().upper(), payload.brand.strip(), payload.name.strip(),
                payload.item_type, payload.calibre_or_grade.strip(), payload.lot_number.strip(),
                payload.quantity, payload.unit_neq_kg, payload.price, payload.min_stock_alert,
                payload.location_id, payload.notes.strip()
            ))
        await db.commit()
        return {"status": "success"}
    finally:
        await db.close()


@app.post("/api/consumables/{cid}/adjust")
async def api_adjust_consumable_qty(cid: int, delta: int = Query(1)):
    db = await get_db()
    try:
        await db.execute("UPDATE consumables SET quantity = MAX(0, quantity + ?), updated_at=CURRENT_TIMESTAMP WHERE id = ?", (delta, cid))
        await db.commit()
        return {"status": "success"}
    finally:
        await db.close()


@app.post("/api/work-orders")
async def api_upsert_work_order(payload: WorkOrderPayload):
    jid = (payload.job_id or "").strip().upper() or f"REP-{datetime.now().strftime('%m%d%H%M')}"
    db = await get_db()
    try:
        await db.execute("""
            INSERT INTO work_orders (
                job_id, firearm_serial, customer_name, customer_phone, make_model, cal_cat,
                service_type, fault_description, parts_used, labor_cost, parts_cost,
                status, test_fired, safe_location_id, operator_name, notes
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(job_id) DO UPDATE SET
                firearm_serial=excluded.firearm_serial, customer_name=excluded.customer_name,
                customer_phone=excluded.customer_phone, make_model=excluded.make_model,
                cal_cat=excluded.cal_cat, service_type=excluded.service_type,
                fault_description=excluded.fault_description, parts_used=excluded.parts_used,
                labor_cost=excluded.labor_cost, parts_cost=excluded.parts_cost,
                status=excluded.status, test_fired=excluded.test_fired,
                safe_location_id=excluded.safe_location_id, operator_name=excluded.operator_name,
                notes=excluded.notes
        """, (
            jid, payload.firearm_serial.strip().upper(), payload.customer_name.strip(),
            payload.customer_phone.strip(), payload.make_model.strip(), payload.cal_cat.strip(),
            payload.service_type, payload.fault_description.strip(), payload.parts_used.strip(),
            payload.labor_cost or 0.0, payload.parts_cost or 0.0, payload.status,
            payload.test_fired, payload.safe_location_id, payload.operator_name or "Admin",
            payload.notes.strip()
        ))
        await db.commit()
        return {"status": "success", "job_id": jid}
    finally:
        await db.close()


@app.post("/api/shipments")
async def api_create_shipment(payload: ShipmentPayload):
    sh_no = (payload.shipment_no or "").strip().upper() or f"TRN-{datetime.now().strftime('%Y%m%d-%H%M')}"
    trk = (payload.tracking_number or "").strip().upper() or f"ST-{datetime.now().strftime('%m%d%H%M%S')}"
    db = await get_db()
    try:
        await db.execute("""
            INSERT INTO shipments (
                shipment_no, recipient_name, recipient_business, recipient_phone, recipient_licence,
                destination_state, destination_address, end_customer_name, end_customer_phone,
                end_customer_licence, end_customer_pta, carrier_name, tracking_number,
                shipment_type, status, notes
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,'Packed & Staged',?)
            ON CONFLICT(shipment_no) DO UPDATE SET
                recipient_name=excluded.recipient_name, recipient_business=excluded.recipient_business,
                recipient_licence=excluded.recipient_licence, destination_address=excluded.destination_address,
                end_customer_name=excluded.end_customer_name, end_customer_pta=excluded.end_customer_pta,
                tracking_number=excluded.tracking_number, notes=excluded.notes
        """, (
            sh_no, payload.recipient_name.strip(), payload.recipient_business.strip(),
            payload.recipient_phone.strip(), payload.recipient_licence.strip(),
            payload.destination_state, payload.destination_address.strip(),
            payload.end_customer_name.strip(), payload.end_customer_phone.strip(),
            payload.end_customer_licence.strip(), payload.end_customer_pta.strip().upper(),
            payload.carrier_name, trk, payload.shipment_type, payload.notes.strip()
        ))
        for s in payload.firearm_serials:
            s_clean = s.strip().upper()
            if s_clean:
                await db.execute("INSERT OR IGNORE INTO shipment_items (shipment_no, firearm_serial) VALUES (?, ?)", (sh_no, s_clean))
                await db.execute("UPDATE firearms SET status = 'Staged for Transit' WHERE serial = ?", (s_clean,))
        await db.commit()
        return {"status": "success", "shipment_no": sh_no}
    finally:
        await db.close()


@app.post("/api/shipments/{shipment_no}/deliver")
async def api_deliver_shipment(shipment_no: str, body: Dict[str, Any]):
    signed_by = (body.get("signed_by") or "Receiving Dealer").strip()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    db = await get_db()
    try:
        async with db.execute("SELECT * FROM shipments WHERE shipment_no = ?", (shipment_no.upper(),)) as cur:
            sh = await cur.fetchone()
            if not sh:
                raise HTTPException(status_code=404, detail="Shipment not found")
            sh_d = dict(sh)

        await db.execute("UPDATE shipments SET status = 'Delivered / Reconciled', signed_by = ?, delivered_at = ? WHERE shipment_no = ?", (signed_by, now_str, shipment_no.upper()))
        async with db.execute("SELECT firearm_serial FROM shipment_items WHERE shipment_no = ?", (shipment_no.upper(),)) as cur:
            for r in await cur.fetchall():
                await db.execute("""
                    UPDATE firearms SET status = 'Disposed/Sold', current_location_id = 'DISPOSED',
                    date_disposed = ?, disposed_to_name = ?, disposed_to_licence = ?
                    WHERE serial = ?
                """, (datetime.now().strftime("%Y-%m-%d"), sh_d["recipient_business"] or sh_d["recipient_name"], sh_d["recipient_licence"], r["firearm_serial"]))
        await db.commit()
        return {"status": "success"}
    finally:
        await db.close()


@app.post("/api/locations")
async def api_upsert_location(payload: LocationPayload):
    lid = payload.id.strip().upper().replace("LOC:", "")
    db = await get_db()
    try:
        await db.execute("""
            INSERT INTO locations (id, name, category, max_capacity, color, notes)
            VALUES (?,?,?,?,?,?)
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name, category=excluded.category, max_capacity=excluded.max_capacity,
                color=excluded.color, notes=excluded.notes
        """, (lid, payload.name.strip(), payload.category, payload.max_capacity, payload.color, payload.notes or ""))
        await db.commit()
        return {"status": "success", "id": lid}
    finally:
        await db.close()


@app.post("/api/customers")
async def api_upsert_customer(payload: CustomerPayload):
    db = await get_db()
    try:
        if payload.id:
            await db.execute("""
                UPDATE customers SET name=?, phone=?, email=?, address=?, licence_no=?,
                licence_categories=?, licence_expiry=?, store_credit=?, notes=? WHERE id=?
            """, (
                payload.name.strip(), payload.phone.strip(), payload.email.strip(),
                payload.address.strip(), payload.licence_no.strip(), payload.licence_categories.strip(),
                payload.licence_expiry.strip(), payload.store_credit or 0.0, payload.notes.strip(), payload.id
            ))
        else:
            await db.execute("""
                INSERT INTO customers (name, phone, email, address, licence_no, licence_categories, licence_expiry, store_credit, notes)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (
                payload.name.strip(), payload.phone.strip(), payload.email.strip(),
                payload.address.strip(), payload.licence_no.strip(), payload.licence_categories.strip(),
                payload.licence_expiry.strip(), payload.store_credit or 0.0, payload.notes.strip()
            ))
        await db.commit()
        return {"status": "success"}
    finally:
        await db.close()


@app.post("/api/suppliers")
async def api_upsert_supplier(payload: SupplierPayload):
    db = await get_db()
    try:
        if payload.id:
            await db.execute("""
                UPDATE suppliers SET name=?, account_no=?, dealer_licence_no=?, rep_name=?, phone=?, email=?, notes=? WHERE id=?
            """, (payload.name.strip(), payload.account_no.strip(), payload.dealer_licence_no.strip(), payload.rep_name.strip(), payload.phone.strip(), payload.email.strip(), payload.notes.strip(), payload.id))
        else:
            await db.execute("""
                INSERT INTO suppliers (name, account_no, dealer_licence_no, rep_name, phone, email, notes)
                VALUES (?,?,?,?,?,?,?)
            """, (payload.name.strip(), payload.account_no.strip(), payload.dealer_licence_no.strip(), payload.rep_name.strip(), payload.phone.strip(), payload.email.strip(), payload.notes.strip()))
        await db.commit()
        return {"status": "success"}
    finally:
        await db.close()


@app.post("/api/hike/webhook/sale")
async def api_hike_sale_webhook(payload: Dict[str, Any]):
    """Hike POS sale.completed webhook handler: automatically marks sold firearms Disposed/Sold."""
    items = payload.get("line_items") or payload.get("items") or []
    cust_name = payload.get("customer_name") or "Hike POS Customer"
    cust_lic = payload.get("customer_licence") or "HIKE-POS-VERIFIED"
    disposed_serials = []

    db = await get_db()
    try:
        for item in items:
            code = normalize_code(item.get("sku") or item.get("serial") or item.get("barcode") or "")
            if not code:
                continue
            async with db.execute("SELECT serial, current_location_id FROM firearms WHERE status = 'In Store'") as cur:
                for r in await cur.fetchall():
                    if code in (normalize_code(r["serial"]), normalize_code(data_sku := r["serial"])):
                        pass
            async with db.execute("SELECT serial, sku, current_location_id FROM firearms WHERE status = 'In Store'") as cur:
                for r in await cur.fetchall():
                    if code in (normalize_code(r["serial"]), normalize_code(r["sku"])):
                        await db.execute("""
                            UPDATE firearms SET status = 'Disposed/Sold', current_location_id = 'DISPOSED',
                            date_disposed = ?, disposed_to_name = ?, disposed_to_licence = ?
                            WHERE serial = ?
                        """, (datetime.now().strftime("%Y-%m-%d"), cust_name, cust_lic, r["serial"]))
                        await db.execute("""
                            INSERT INTO movement_logs (firearm_serial, from_location, to_location, action_type, operator_name, details)
                            VALUES (?,?,?,?,?,?)
                        """, (r["serial"], r["current_location_id"], "DISPOSED", "Hike POS Sale Webhook", "Hike POS", f"Auto-disposed to {cust_name}"))
                        disposed_serials.append(r["serial"])
        await db.commit()
        return {"status": "success", "disposed_serials": disposed_serials}
    finally:
        await db.close()


@app.post("/api/settings")
async def api_save_settings(payload: Dict[str, str]):
    db = await get_db()
    try:
        for k, v in payload.items():
            await db.execute("INSERT INTO store_settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(k), str(v)))
        await db.commit()
        return {"status": "success"}
    finally:
        await db.close()


@app.post("/api/staff")
async def api_add_staff(payload: StaffPayload):
    db = await get_db()
    try:
        await db.execute(
            "INSERT INTO staff_accounts (name, role, pin, badge_code) VALUES (?,?,?,?)",
            (payload.name.strip(), payload.role, payload.pin.strip(), (payload.badge_code or f"STF:{payload.name.upper()[:6]}").strip().upper())
        )
        await db.commit()
        return {"status": "success"}
    finally:
        await db.close()


@app.post("/api/backups/create")
async def api_manual_backup():
    fname = await create_backup_archive("manual")
    return {"status": "success", "filename": fname}


@app.get("/api/backups/download/{filename}")
async def api_download_backup(filename: str):
    safe_name = os.path.basename(filename)
    full_p = os.path.join(BACKUP_DIR, safe_name)
    if not os.path.exists(full_p):
        raise HTTPException(status_code=404, detail="Backup file not found")
    return StreamingResponse(
        open(full_p, "rb"),
        media_type="application/zip",
        headers={"Content-Disposition": f"attachment; filename={safe_name}"},
    )
