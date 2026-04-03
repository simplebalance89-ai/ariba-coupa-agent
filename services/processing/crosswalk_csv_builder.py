"""
crosswalk_csv_builder.py -- Generate crosswalk CSVs from P21 SO/Customer/Line exports.

Input:  Raw P21 CSV exports (SO Headers, SO Lines, Customers)
Output: Crosswalk CSVs ready for the matching engine

Usage:
    python crosswalk_csv_builder.py \
        --headers "path/to/so_headers.csv" \
        --lines "path/to/so_lines.csv" \
        --customers "path/to/customers.csv" \
        --output ./crosswalks
"""

import argparse
import csv
import logging
import os
from collections import defaultdict
from datetime import datetime

from services.processing.address_normalizer import normalize_name, normalize_address, normalize_zip

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def _read_csv(path: str) -> list[dict]:
    """Read CSV with BOM handling."""
    with open(path, "r", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def _write_csv(rows: list[dict], path: str):
    if not rows:
        logger.warning(f"No rows for {path}")
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)
    logger.info(f"Wrote {len(rows)} rows -> {path}")


# ---------------------------------------------------------------------------
# 1. Customer crosswalk — group SO headers by customer + ship-to
# ---------------------------------------------------------------------------

def build_customer_crosswalk(headers: list[dict]) -> list[dict]:
    """
    Build customer crosswalk from SO headers.
    Key: normalized(ship2_name) + zip5 → customer_id
    """
    # Group by (customer_id, ship2_name_norm, ship2_zip5)
    groups = defaultdict(lambda: {
        "count": 0, "last_order": "", "ship2_add1": "", "ship2_city": "",
        "ship2_state": "", "customer_name": "",
    })

    for h in headers:
        cid = h.get("customer_id", "").strip()
        name = h.get("ship2_name", "").strip()
        zip_code = h.get("ship2_zip", "").strip()
        if not cid or not name:
            continue

        key = (cid, normalize_name(name), normalize_zip(zip_code))
        g = groups[key]
        g["count"] += 1
        g["customer_name"] = name
        g["ship2_add1"] = h.get("ship2_add1", "").strip() or g["ship2_add1"]
        g["ship2_city"] = h.get("ship2_city", "").strip() or g["ship2_city"]
        g["ship2_state"] = h.get("ship2_state", "").strip() or g["ship2_state"]
        od = h.get("order_date", "")
        if od > g["last_order"]:
            g["last_order"] = od

    rows = []
    for (cid, name_norm, zip5), g in groups.items():
        rows.append({
            "source_system": "P21",
            "source_customer_id": "",
            "source_customer_name": "",
            "ship2_name": g["customer_name"],
            "ship2_name_normalized": name_norm,
            "ship2_add1": g["ship2_add1"],
            "ship2_add1_normalized": normalize_address(g["ship2_add1"]),
            "ship2_city": g["ship2_city"],
            "ship2_state": g["ship2_state"],
            "ship2_zip": zip5,
            "p21_customer_id": cid,
            "p21_customer_name": g["customer_name"],
            "match_score": "1.0",
            "match_method": "seed",
            "is_active": "1",
            "last_seen": g["last_order"],
            "seen_count": str(g["count"]),
        })

    rows.sort(key=lambda r: int(r["seen_count"]), reverse=True)
    logger.info(f"Customer crosswalk: {len(rows)} unique (customer, ship-to, zip) combos")
    return rows


# ---------------------------------------------------------------------------
# 2. Customer-item crosswalk — oe_line customer_part_number → inv_mast_uid
# ---------------------------------------------------------------------------

def build_customer_item_crosswalk(headers: list[dict], lines: list[dict]) -> list[dict]:
    """
    Build customer-item crosswalk from SO lines.
    Key: customer_id + customer_part_number → inv_mast_uid / item_id
    """
    # Map order_no → customer_id from headers
    order_customer = {}
    for h in headers:
        ono = h.get("order_no", "").strip()
        cid = h.get("customer_id", "").strip()
        if ono and cid:
            order_customer[ono] = cid

    # Group lines by (customer_id, customer_part_number, inv_mast_uid)
    groups = defaultdict(lambda: {
        "count": 0, "last_seen": "", "prices": [],
        "extended_desc": "", "product_group_id": "", "uom": "",
    })

    for l in lines:
        ono = l.get("order_no", "").strip()
        cid = order_customer.get(ono, "")
        cpn = l.get("customer_part_number", "").strip()
        uid = l.get("inv_mast_uid", "").strip()
        if not cid or not cpn or not uid:
            continue

        key = (cid, cpn, uid)
        g = groups[key]
        g["count"] += 1
        g["extended_desc"] = l.get("extended_desc", "").strip() or g["extended_desc"]
        g["product_group_id"] = l.get("product_group_id", "").strip() or g["product_group_id"]
        g["uom"] = l.get("unit_of_measure", "").strip() or g["uom"]

        try:
            price = float(l.get("unit_price", 0) or 0)
            if price > 0:
                g["prices"].append(price)
        except (ValueError, TypeError):
            pass

        dt = l.get("date_created", "")
        if dt > g["last_seen"]:
            g["last_seen"] = dt

    rows = []
    for (cid, cpn, uid), g in groups.items():
        prices = g["prices"]
        rows.append({
            "p21_customer_id": cid,
            "customer_part_number": cpn,
            "p21_inv_mast_uid": uid,
            "p21_item_desc": g["extended_desc"],
            "unit_of_measure": g["uom"],
            "product_group_id": g["product_group_id"],
            "unit_price_last": f"{prices[-1]:.4f}" if prices else "",
            "unit_price_avg": f"{sum(prices)/len(prices):.4f}" if prices else "",
            "unit_price_min": f"{min(prices):.4f}" if prices else "",
            "unit_price_max": f"{max(prices):.4f}" if prices else "",
            "last_seen": g["last_seen"],
            "seen_count": str(g["count"]),
        })

    rows.sort(key=lambda r: int(r["seen_count"]), reverse=True)
    logger.info(f"Customer-item crosswalk: {len(rows)} unique (customer, part, item) combos")
    return rows


# ---------------------------------------------------------------------------
# 3. Customer PO history — oe_hdr po_no per customer
# ---------------------------------------------------------------------------

def build_po_history(headers: list[dict]) -> list[dict]:
    """Build PO history for duplicate detection and customer confirmation."""
    rows = []
    for h in headers:
        po = h.get("po_no", "").strip()
        cid = h.get("customer_id", "").strip()
        if not po or not cid:
            continue
        rows.append({
            "p21_customer_id": cid,
            "customer_po_no": po,
            "p21_order_no": h.get("order_no", "").strip(),
            "order_date": h.get("order_date", "").strip(),
            "completed": h.get("completed", "").strip(),
            "approved": h.get("approved", "").strip(),
            "ship2_name": h.get("ship2_name", "").strip(),
        })

    logger.info(f"PO history: {len(rows)} PO-to-SO linkages")
    return rows


# ---------------------------------------------------------------------------
# 4. Item master index — for fallback matching
# ---------------------------------------------------------------------------

def build_item_master(lines: list[dict]) -> list[dict]:
    """
    Build item master index from SO lines.
    Dedup by inv_mast_uid, keep most-seen description and product group.
    """
    items = {}
    for l in lines:
        uid = l.get("inv_mast_uid", "").strip()
        if not uid or uid in items:
            continue
        items[uid] = {
            "p21_inv_mast_uid": uid,
            "p21_item_desc": l.get("extended_desc", "").strip() or l.get("customer_part_number", "").strip(),
            "p21_item_desc_normalized": normalize_name(
                l.get("extended_desc", "") or l.get("customer_part_number", "")
            ),
            "default_selling_unit": l.get("unit_of_measure", "").strip(),
            "product_group": l.get("product_group_id", "").strip(),
            "default_supplier_id": l.get("supplier_id", "").strip(),
        }

    rows = list(items.values())
    logger.info(f"Item master index: {len(rows)} unique items")
    return rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def build_all(headers_path: str, lines_path: str, customers_path: str, output_dir: str):
    """Build all crosswalk CSVs from P21 exports."""
    os.makedirs(output_dir, exist_ok=True)

    logger.info("Loading P21 exports...")
    headers = _read_csv(headers_path)
    lines = _read_csv(lines_path)
    customers = _read_csv(customers_path)
    logger.info(f"Loaded: {len(headers)} headers, {len(lines)} lines, {len(customers)} customers")

    # Build crosswalks
    cust_xw = build_customer_crosswalk(headers)
    _write_csv(cust_xw, os.path.join(output_dir, "customer_crosswalk.csv"))

    cust_item_xw = build_customer_item_crosswalk(headers, lines)
    _write_csv(cust_item_xw, os.path.join(output_dir, "customer_item_crosswalk.csv"))

    po_hist = build_po_history(headers)
    _write_csv(po_hist, os.path.join(output_dir, "customer_po_history.csv"))

    item_idx = build_item_master(lines)
    _write_csv(item_idx, os.path.join(output_dir, "item_master_index.csv"))

    # Copy customers as-is (already has address data joined)
    _write_csv(customers, os.path.join(output_dir, "customers_p21.csv"))

    logger.info(f"All crosswalk CSVs written to {output_dir}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Build crosswalk CSVs from P21 exports")
    p.add_argument("--headers", required=True, help="Path to SO headers CSV")
    p.add_argument("--lines", required=True, help="Path to SO lines CSV")
    p.add_argument("--customers", required=True, help="Path to customers CSV")
    p.add_argument("--output", default="./crosswalks", help="Output directory")
    args = p.parse_args()

    build_all(args.headers, args.lines, args.customers, args.output)
