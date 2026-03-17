#!/usr/bin/env python3
"""
generate_and_load.py
====================
Generates a synthetic BSC-style medical device fulfillment dataset and loads it
into Snowflake under DEMO_BSC.

Data volumes (configurable via CLI flags):
  CUSTOMER_DIM  : 500
  FACILITY_DIM  : 1,200  (Zipf-distributed orders — a few facilities very active)
  PRODUCT_DIM   : 350
  CONTACT_DIM   : 800
  ORDER_FACT    : 100,000
  ORDER_ITEM_FACT: ~180,000

Fuzzy realism:
  - Facility names include common variants: "St." / "Saint", "Hosp" / "Hospital",
    abbreviation typos, trailing Inc/LLC, etc.
  - purchase_order_id uses customer-specific prefix patterns.
  - tracking_number is null until status ≥ SHIPPED.
  - Zipf-like order distribution per facility.

Usage:
  python generate_and_load.py [--orders N] [--dry-run]

Environment variables (or .env):
  SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
  SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
"""

import argparse
import os
import random
import re
import unicodedata
import uuid
from datetime import date, datetime, timedelta, timezone
from typing import List, Dict, Tuple

import snowflake.connector
from dotenv import load_dotenv
from faker import Faker
from tqdm import tqdm

load_dotenv()

fake = Faker()
Faker.seed(42)
random.seed(42)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

N_CUSTOMERS = 500
N_FACILITIES = 1_200
N_PRODUCTS = 350
N_CONTACTS = 800
N_ORDERS = 100_000
BATCH_SIZE = 5_000

STATUSES = [
    "CREATED", "ALLOCATED", "PICKED", "SHIPPED",
    "DELIVERED", "BACKORDERED", "CANCELLED", "ON_HOLD",
]
STATUS_WEIGHTS = [5, 8, 6, 20, 50, 4, 3, 4]

CARRIERS = ["UPS", "FedEx", "DHL", "USPS", "OnTrac", None]
CARRIER_WEIGHTS = [35, 30, 10, 5, 5, 15]

REGIONS = ["Northeast", "Southeast", "Midwest", "Southwest", "West", "Northwest", "Canada", "International"]
ACCOUNT_TYPES = ["HOSPITAL", "IDN", "CLINIC", "GPO", "DISTRIBUTOR"]
PRODUCT_FAMILIES = ["CRM", "EP", "Neuromod", "Endoscopy", "Urology", "Peripheral"]

# BSC-realistic hospital / facility name templates
FACILITY_TEMPLATES = [
    "{city} Medical Center",
    "{city} General Hospital",
    "St. {saint} Hospital",
    "Saint {saint} Medical Center",
    "{city} Regional Medical Center",
    "{city} Community Hospital",
    "{system} Health - {city}",
    "University of {state} Medical Center",
    "{name} Memorial Hospital",
    "{name} Medical Group",
    "{city} Heart & Vascular Center",
    "{name} Orthopedic Institute",
    "{city} Cancer Care Center",
    "Cleveland Clinic - {city}",
    "Mayo Clinic - {city}",
    "{name} Health System",
    "{city} VA Medical Center",
    "Mercy {city} Hospital",
    "Advocate {name} Medical Center",
    "HCA {name} Hospital",
]

SAINTS = ["Mary", "Joseph", "Luke", "Francis", "Elizabeth", "Anthony", "Michael",
          "Catherine", "Vincent", "Agnes", "Patrick", "Thomas", "James", "Paul"]
HEALTH_SYSTEMS = ["Ascension", "Trinity", "Bon Secours", "CommonSpirit", "Sutter",
                  "Banner", "Tenet", "Prime", "Steward", "Acuity"]
NAMES = ["Henderson", "Johnson", "Smith", "Williams", "Anderson", "Taylor",
         "Martinez", "Davis", "Wilson", "Moore", "Jackson", "Thompson"]


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------

def normalize(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^\w\s]", " ", text.lower())
    return re.sub(r"\s+", " ", text).strip()


def make_alt_name(name: str) -> str:
    """Generate a common abbreviation / typo variant of a facility name."""
    alt = name
    alt = alt.replace("Saint ", "St. ").replace("St. ", "Saint ")
    alt = alt.replace("Hospital", "Hosp").replace("Medical Center", "Med Ctr")
    alt = alt.replace("Center", "Ctr").replace("University", "Univ")
    alt = alt.replace("Memorial", "Mem").replace("Regional", "Reg")
    return alt if alt != name else name + " Inc."


# ---------------------------------------------------------------------------
# Data generators
# ---------------------------------------------------------------------------

def gen_customers() -> List[Dict]:
    customers = []
    for i in range(N_CUSTOMERS):
        name = fake.company()
        customers.append({
            "customer_account_id": f"ACC-{i+1:05d}",
            "customer_name": name,
            "customer_name_norm": normalize(name),
            "account_type": random.choice(ACCOUNT_TYPES),
            "territory": fake.state_abbr(),
            "sales_region": random.choice(REGIONS),
        })
    return customers


def gen_facilities(customers: List[Dict]) -> List[Dict]:
    facilities = []
    for i in range(N_FACILITIES):
        city = fake.city()
        state = fake.state()
        saint = random.choice(SAINTS)
        system = random.choice(HEALTH_SYSTEMS)
        person_name = random.choice(NAMES)
        template = random.choice(FACILITY_TEMPLATES)
        name = template.format(
            city=city, state=state, saint=saint,
            system=system, name=person_name,
        )
        customer = random.choice(customers)
        facilities.append({
            "facility_id": f"FAC-{i+1:06d}",
            "customer_account_id": customer["customer_account_id"],
            "facility_name": name,
            "facility_name_norm": normalize(name),
            "facility_name_alt": make_alt_name(name),
            "address_line1": fake.street_address(),
            "city": city,
            "state": state,
            "zip": fake.zipcode(),
            "country": "US",
        })
    return facilities


def gen_products() -> List[Dict]:
    products = []
    for i in range(N_PRODUCTS):
        family = random.choice(PRODUCT_FAMILIES)
        products.append({
            "product_id": f"PRD-{i+1:05d}",
            "product_name": f"{family} {fake.bs().title()[:40]}",
            "product_family": family,
            "product_line": f"{family}-{random.randint(1000, 9999)}",
            "unit_cost_usd": round(random.uniform(50, 25000), 2),
        })
    return products


def gen_contacts(customers: List[Dict], facilities: List[Dict]) -> List[Dict]:
    contacts = []
    for i in range(N_CONTACTS):
        name = fake.name()
        customer = random.choice(customers)
        facility = random.choice([f for f in facilities
                                  if f["customer_account_id"] == customer["customer_account_id"]]
                                 or facilities)
        contacts.append({
            "contact_id": f"CON-{i+1:05d}",
            "customer_account_id": customer["customer_account_id"],
            "facility_id": facility["facility_id"],
            "contact_name": name,
            "contact_name_norm": normalize(name),
            "contact_role": random.choice(["Purchasing", "Clinical", "Materials Mgmt", "OR Coord"]),
            "email": fake.email(),
        })
    return contacts


def _zipf_facility_index(n_facilities: int) -> int:
    """Sample from a Zipf-like distribution so a few facilities dominate."""
    # Use geometric with clipping
    idx = int(abs(random.gauss(0, n_facilities * 0.1)))
    return min(idx, n_facilities - 1)


def gen_orders(
    customers: List[Dict],
    facilities: List[Dict],
    contacts: List[Dict],
    n: int,
) -> List[Dict]:
    # Pre-sort facilities to enable Zipf sampling by index
    sorted_facilities = sorted(facilities, key=lambda f: f["facility_id"])
    contact_lookup: Dict[str, List[str]] = {}
    for c in contacts:
        contact_lookup.setdefault(c["facility_id"], []).append(c["contact_id"])

    orders = []
    base_date = datetime(2024, 1, 1, tzinfo=timezone.utc)

    for i in tqdm(range(n), desc="Generating orders"):
        fac = sorted_facilities[_zipf_facility_index(len(sorted_facilities))]
        customer = next(c for c in customers if c["customer_account_id"] == fac["customer_account_id"])
        contact_ids = contact_lookup.get(fac["facility_id"], [None])
        contact_id = random.choice(contact_ids)

        order_created_ts = base_date + timedelta(
            days=random.randint(0, 810),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )
        requested_ship_date = (order_created_ts + timedelta(days=random.randint(1, 14))).date()
        promised_delivery_date = requested_ship_date + timedelta(days=random.randint(1, 5))

        status = random.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0]
        status_ts = order_created_ts + timedelta(hours=random.randint(1, 72))

        actual_ship_ts = None
        actual_delivery_date = None
        carrier = None
        tracking_number = None

        if status in ("SHIPPED", "DELIVERED"):
            actual_ship_ts = order_created_ts + timedelta(days=random.randint(1, 7))
            carrier = random.choices(CARRIERS, weights=CARRIER_WEIGHTS, k=1)[0]
            if carrier:
                tracking_number = fake.bothify(text="1Z###########", letters="ABCDEF0123456789")
        if status == "DELIVERED":
            actual_delivery_date = (actual_ship_ts + timedelta(days=random.randint(1, 5))).date() \
                if actual_ship_ts else promised_delivery_date

        order_id = f"SO-{order_created_ts.year}-{i+1:06d}"
        po_prefix = customer["customer_account_id"].replace("ACC-", "PO-")
        purchase_order_id = f"{po_prefix}-{random.randint(100000, 999999)}"

        total = round(random.uniform(500, 150000), 2)

        orders.append({
            "order_id": order_id,
            "purchase_order_id": purchase_order_id,
            "customer_account_id": customer["customer_account_id"],
            "facility_id": fac["facility_id"],
            "contact_id": contact_id,
            "order_created_ts": order_created_ts,
            "requested_ship_date": requested_ship_date,
            "promised_delivery_date": promised_delivery_date,
            "actual_ship_ts": actual_ship_ts,
            "actual_delivery_date": actual_delivery_date,
            "status": status,
            "status_last_updated_ts": status_ts,
            "priority_flag": random.random() < 0.08,
            "carrier": carrier,
            "tracking_number": tracking_number,
            "sales_region": customer["sales_region"],
            "total_amount_usd": total,
            "currency": "USD",
        })

    return orders


def gen_order_items(orders: List[Dict], products: List[Dict]) -> List[Dict]:
    items = []
    for order in tqdm(orders, desc="Generating order items"):
        n_items = random.randint(1, 5)
        for j in range(n_items):
            product = random.choice(products)
            qty = random.randint(1, 10)
            unit_price = round(product["unit_cost_usd"] * random.uniform(0.9, 1.3), 2)
            items.append({
                "order_item_id": f"{order['order_id']}-{j+1:02d}",
                "order_id": order["order_id"],
                "product_id": product["product_id"],
                "quantity": qty,
                "unit_price_usd": unit_price,
                "line_total_usd": round(qty * unit_price, 2),
            })
    return items


# ---------------------------------------------------------------------------
# Snowflake loader
# ---------------------------------------------------------------------------

def get_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "DEMO_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "DEMO_DB"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "DEMO_BSC"),
    )


def load_table(conn, table: str, rows: List[Dict], columns: List[str], dry_run: bool):
    if dry_run:
        print(f"[DRY RUN] Would load {len(rows):,} rows into {table}")
        return

    db = os.environ.get("SNOWFLAKE_DATABASE", "DEMO_DB")
    schema = os.environ.get("SNOWFLAKE_SCHEMA", "DEMO_BSC")
    qualified_table = f"{db}.{schema}.{table}"

    cur = conn.cursor()
    placeholders = ", ".join(["%s"] * len(columns))
    col_list = ", ".join(columns)
    sql = f"INSERT INTO {qualified_table} ({col_list}) VALUES ({placeholders})"

    for start in tqdm(range(0, len(rows), BATCH_SIZE), desc=f"Loading {table}"):
        batch = rows[start: start + BATCH_SIZE]
        cur.executemany(sql, [[row[c] for c in columns] for row in batch])

    cur.close()
    print(f"  Loaded {len(rows):,} rows → {table}")


def run_sql_file(conn, path: str):
    with open(path) as f:
        statements = [s.strip() for s in f.read().split(";") if s.strip()]
    cur = conn.cursor()
    for stmt in statements:
        if stmt:
            cur.execute(stmt)
    cur.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate + load BSC synthetic dataset")
    parser.add_argument("--orders", type=int, default=N_ORDERS, help="Number of orders to generate")
    parser.add_argument("--dry-run", action="store_true", help="Generate data but do not load")
    parser.add_argument("--skip-ddl", action="store_true", help="Skip DDL execution")
    args = parser.parse_args()

    print("=== BSC Order Status Assistant — Synthetic Data Generator ===\n")

    print("Generating data…")
    customers = gen_customers()
    print(f"  {len(customers):,} customers")
    facilities = gen_facilities(customers)
    print(f"  {len(facilities):,} facilities")
    products = gen_products()
    print(f"  {len(products):,} products")
    contacts = gen_contacts(customers, facilities)
    print(f"  {len(contacts):,} contacts")
    orders = gen_orders(customers, facilities, contacts, args.orders)
    print(f"  {len(orders):,} orders")
    items = gen_order_items(orders, products)
    print(f"  {len(items):,} order items")

    if args.dry_run:
        print("\n[DRY RUN] Data generated successfully. No Snowflake writes performed.")
        return

    print("\nConnecting to Snowflake…")
    conn = get_conn()

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sql_dir = os.path.join(base_dir, "sql")

    if not args.skip_ddl:
        print("Running DDL…")
        for fname in ["create_schema.sql", "create_tables.sql", "create_views.sql", "trace_log_tables.sql"]:
            fpath = os.path.join(sql_dir, fname)
            if os.path.exists(fpath):
                print(f"  {fname}")
                run_sql_file(conn, fpath)

    print("\nLoading dimension tables…")
    load_table(conn, "CUSTOMER_DIM",
               customers,
               ["customer_account_id", "customer_name", "customer_name_norm",
                "account_type", "territory", "sales_region"],
               dry_run=False)

    load_table(conn, "FACILITY_DIM",
               facilities,
               ["facility_id", "customer_account_id", "facility_name",
                "facility_name_norm", "facility_name_alt",
                "address_line1", "city", "state", "zip", "country"],
               dry_run=False)

    load_table(conn, "PRODUCT_DIM",
               products,
               ["product_id", "product_name", "product_family", "product_line", "unit_cost_usd"],
               dry_run=False)

    load_table(conn, "CONTACT_DIM",
               contacts,
               ["contact_id", "customer_account_id", "facility_id",
                "contact_name", "contact_name_norm", "contact_role", "email"],
               dry_run=False)

    print("\nLoading ORDER_FACT…")
    load_table(conn, "ORDER_FACT",
               orders,
               ["order_id", "purchase_order_id", "customer_account_id", "facility_id",
                "contact_id", "order_created_ts", "requested_ship_date",
                "promised_delivery_date", "actual_ship_ts", "actual_delivery_date",
                "status", "status_last_updated_ts", "priority_flag",
                "carrier", "tracking_number", "sales_region",
                "total_amount_usd", "currency"],
               dry_run=False)

    print("\nLoading ORDER_ITEM_FACT…")
    load_table(conn, "ORDER_ITEM_FACT",
               items,
               ["order_item_id", "order_id", "product_id", "quantity",
                "unit_price_usd", "line_total_usd"],
               dry_run=False)

    conn.close()
    print("\n✅ Dataset loaded successfully.")


if __name__ == "__main__":
    main()
