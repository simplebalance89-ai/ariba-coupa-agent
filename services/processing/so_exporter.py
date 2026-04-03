"""
so_exporter.py -- Pull Sales Orders from P21 SQL and export to CSV.

Usage:
    python so_exporter.py                          # defaults: 90 days, all tables
    python so_exporter.py --days 365               # last year of SOs
    python so_exporter.py --no-customers --no-items # SOs only, skip reference data
    python so_exporter.py --output ./my_exports    # custom output dir

Outputs:
    so_headers_YYYYMMDD_HHMMSS.csv   — oe_hdr (SO headers)
    so_lines_YYYYMMDD_HHMMSS.csv     — oe_line (SO line items)
    customers_YYYYMMDD_HHMMSS.csv    — customer master
    ship_tos_YYYYMMDD_HHMMSS.csv     — ship-to addresses
    items_YYYYMMDD_HHMMSS.csv        — inv_mast (item master)
"""

import argparse
import csv
import logging
import os
import sys
from datetime import datetime, timedelta
from io import StringIO

try:
    import pyodbc
except ImportError:
    pyodbc = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def get_p21_conn(server: str, database: str, uid: str = "", pwd: str = "",
                 driver: str = "{ODBC Driver 17 for SQL Server}"):
    conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};"
    if uid:
        conn_str += f"UID={uid};PWD={pwd};"
    else:
        conn_str += "Trusted_Connection=yes;"
    conn_str += "Encrypt=yes;TrustServerCertificate=yes;"
    return pyodbc.connect(conn_str, timeout=60)


def _fetch(conn, sql: str, params: tuple = ()) -> list[dict]:
    cur = conn.cursor()
    cur.execute(sql, params)
    cols = [d[0] for d in cur.description]
    rows = []
    for row in cur.fetchall():
        rows.append({
            c: (v.isoformat() if isinstance(v, datetime) else v)
            for c, v in zip(cols, row)
        })
    return rows


def _write_csv(rows: list[dict], path: str):
    if not rows:
        logger.warning(f"No rows to write for {path}")
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)
    logger.info(f"Wrote {len(rows)} rows -> {path}")


# ---------------------------------------------------------------------------
# SQL Queries
# ---------------------------------------------------------------------------

SO_HEADERS = """
SELECT
    h.order_no,
    h.company_no,
    h.location_id,
    h.customer_id,
    h.ship2_name,
    h.ship2_add1,
    h.ship2_add2,
    h.ship2_city,
    h.ship2_state,
    h.ship2_zip,
    h.ship2_country,
    h.order_date,
    h.date_created,
    h.date_last_modified,
    h.requested_date,
    h.promise_date,
    h.cancel_date,
    h.completed,
    h.approved,
    h.po_no,
    h.terms_id,
    h.carrier_id,
    h.ship_via_desc,
    h.fob,
    h.freight_terms,
    h.salesperson_id,
    h.taken_by,
    h.order_type,
    h.order_status,
    h.total_amount,
    h.tax_amount,
    h.freight_amount,
    h.discount_amount,
    h.currency_id,
    h.branch_id,
    h.division_id,
    h.department_id,
    h.contact_id,
    h.source_type,
    h.job_no,
    h.project_id,
    h.comments,
    h.ship_complete_flag,
    h.backorder_flag,
    h.hold_flag,
    h.hold_reason,
    h.web_order_no,
    h.external_po_no
FROM oe_hdr h
WHERE h.date_last_modified >= ?
ORDER BY h.date_last_modified DESC
"""

SO_LINES = """
SELECT
    l.order_no,
    l.line_no,
    l.item_id,
    l.item_desc,
    l.extended_desc,
    l.unit_of_measure,
    l.unit_size,
    l.unit_quantity,
    l.qty_ordered,
    l.qty_shipped,
    l.qty_canceled,
    l.qty_backordered,
    l.qty_allocated,
    l.unit_price,
    l.extended_price,
    l.unit_cost,
    l.extended_cost,
    l.date_due,
    l.required_date,
    l.promise_date,
    l.date_shipped,
    l.date_created,
    l.date_last_modified,
    l.line_status,
    l.line_type,
    l.complete,
    l.cancel_flag,
    l.source_type,
    l.customer_part_number,
    l.mfg_part_no,
    l.vendor_id,
    l.supplier_id,
    l.inv_mast_uid,
    l.location_id,
    l.warehouse_id,
    l.gl_revenue_account,
    l.gl_cogs_account,
    l.ship_via,
    l.carrier_id,
    l.tracking_no,
    l.pick_ticket_no,
    l.invoice_no,
    l.job_no,
    l.project_id,
    l.tax_code,
    l.discount_pct,
    l.commission_pct,
    l.notes
FROM oe_line l
WHERE l.date_last_modified >= ?
ORDER BY l.order_no, l.line_no
"""

CUSTOMERS = """
SELECT
    c.customer_id,
    c.customer_name,
    c.address_line_1,
    c.address_line_2,
    c.city,
    c.state,
    c.zip,
    c.country,
    c.phone_number,
    c.fax_number,
    c.email_address,
    c.web_address,
    c.terms_id,
    c.salesperson_id,
    c.credit_limit,
    c.credit_status,
    c.tax_code,
    c.currency_id,
    c.branch_id,
    c.company_no,
    c.class_id1 AS customer_class,
    c.date_created,
    c.date_last_modified
FROM customer c
WHERE c.delete_flag = 'N'
ORDER BY c.customer_name
"""

SHIP_TOS = """
SELECT
    s.customer_id,
    s.ship_to_id,
    s.ship_to_name,
    s.ship_to_add1,
    s.ship_to_add2,
    s.ship_to_city,
    s.ship_to_state,
    s.ship_to_zip,
    s.ship_to_country,
    s.ship_to_phone,
    s.contact_name,
    s.date_last_modified
FROM ship_to s
WHERE s.delete_flag = 'N'
ORDER BY s.customer_id, s.ship_to_id
"""

ITEMS = """
SELECT
    i.item_id,
    i.item_desc,
    i.extended_desc,
    i.supplier_id AS default_vendor_id,
    i.class_id1 AS product_group,
    i.class_id2 AS product_line,
    i.class_id3 AS product_category,
    i.default_selling_unit,
    i.default_purchasing_unit,
    i.weight,
    i.item_type,
    i.date_created,
    i.date_last_modified
FROM inv_mast i
WHERE i.delete_flag = 'N'
ORDER BY i.item_id
"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def export(server: str, database: str, uid: str = "", pwd: str = "",
           days_back: int = 90, output_dir: str = "./so_exports",
           include_customers: bool = True, include_ship_tos: bool = True,
           include_items: bool = True):
    """Connect to P21, pull SO data, write CSVs."""

    os.makedirs(output_dir, exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    since = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%d")

    logger.info(f"Connecting to {server}/{database}")
    conn = get_p21_conn(server, database, uid, pwd)

    try:
        # SO headers + lines (date-filtered)
        logger.info(f"Pulling SO headers since {since}")
        headers = _fetch(conn, SO_HEADERS, (since,))
        _write_csv(headers, os.path.join(output_dir, f"so_headers_{ts}.csv"))

        logger.info(f"Pulling SO lines since {since}")
        lines = _fetch(conn, SO_LINES, (since,))
        _write_csv(lines, os.path.join(output_dir, f"so_lines_{ts}.csv"))

        # Reference data (full pull, no date filter)
        if include_customers:
            logger.info("Pulling customers")
            customers = _fetch(conn, CUSTOMERS)
            _write_csv(customers, os.path.join(output_dir, f"customers_{ts}.csv"))

        if include_ship_tos:
            logger.info("Pulling ship-to addresses")
            ship_tos = _fetch(conn, SHIP_TOS)
            _write_csv(ship_tos, os.path.join(output_dir, f"ship_tos_{ts}.csv"))

        if include_items:
            logger.info("Pulling item master")
            items = _fetch(conn, ITEMS)
            _write_csv(items, os.path.join(output_dir, f"items_{ts}.csv"))
    finally:
        conn.close()

    logger.info(f"Done. CSVs in {output_dir}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Export P21 Sales Orders to CSV")
    p.add_argument("--server", required=True, help="P21 SQL server (e.g. P21SERVER\\INSTANCE)")
    p.add_argument("--database", default="P21", help="P21 database name")
    p.add_argument("--uid", default="", help="SQL login (blank = Windows auth)")
    p.add_argument("--pwd", default="", help="SQL password")
    p.add_argument("--days", type=int, default=90, help="Days back for SO data")
    p.add_argument("--output", default="./so_exports", help="Output directory")
    p.add_argument("--no-customers", action="store_true")
    p.add_argument("--no-ship-tos", action="store_true")
    p.add_argument("--no-items", action="store_true")
    args = p.parse_args()

    export(
        server=args.server,
        database=args.database,
        uid=args.uid,
        pwd=args.pwd,
        days_back=args.days,
        output_dir=args.output,
        include_customers=not args.no_customers,
        include_ship_tos=not args.no_ship_tos,
        include_items=not args.no_items,
    )
