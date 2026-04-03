"""
crosswalk_learner.py -- Learning loop: feed approved matches back into crosswalk CSVs.

On each approval (auto or human), this module:
1. UPSERTs customer mapping into customer_crosswalk.csv
2. UPSERTs item mappings into customer_item_crosswalk.csv
3. Logs PO linkage into customer_po_history.csv
"""

import csv
import logging
import os
from datetime import datetime

from services.processing.address_normalizer import normalize_name, normalize_zip, normalize_address

logger = logging.getLogger(__name__)

CROSSWALK_DIR = os.environ.get("CROSSWALK_DIR", "./crosswalks")


def _read_csv(filename: str) -> list[dict]:
    path = os.path.join(CROSSWALK_DIR, filename)
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def _write_csv(filename: str, rows: list[dict]):
    if not rows:
        return
    path = os.path.join(CROSSWALK_DIR, filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)


def learn_from_approval(
    # Customer match
    p21_customer_id: str,
    p21_customer_name: str,
    source_system: str = "",
    source_customer_id: str = "",
    # Ship-to
    ship2_name: str = "",
    ship2_add1: str = "",
    ship2_city: str = "",
    ship2_state: str = "",
    ship2_zip: str = "",
    ship_to_id: str = "",
    # PO
    po_no: str = "",
    p21_order_no: str = "",
    # Lines: list of dicts with supplier_part_id, item_id_p21, inv_mast_uid, uom, price
    lines: list = None,
    # Corrections from human review
    corrections: dict = None,
    crosswalk_dir: str = None,
):
    """Feed an approved PO match back into the crosswalk CSVs."""
    global CROSSWALK_DIR
    if crosswalk_dir:
        CROSSWALK_DIR = crosswalk_dir

    now = datetime.utcnow().isoformat()

    # Apply corrections if provided
    if corrections:
        p21_customer_id = corrections.get("customer_id", p21_customer_id)
        p21_customer_name = corrections.get("customer_name", p21_customer_name)

    # 1. Customer crosswalk UPSERT
    _upsert_customer(
        p21_customer_id, p21_customer_name,
        source_system, source_customer_id,
        ship2_name, ship2_add1, ship2_city, ship2_state, ship2_zip,
        now,
    )

    # 2. Item crosswalk UPSERTs
    if lines:
        _upsert_items(p21_customer_id, lines, corrections, now)

    # 3. PO history append
    if po_no:
        _append_po_history(p21_customer_id, po_no, p21_order_no, ship2_name, now)

    logger.info(
        f"Learning recorded: customer={p21_customer_id} po={po_no} "
        f"lines={len(lines or [])} source={source_system}"
    )


def _upsert_customer(
    cid, cname, source_sys, source_cid,
    ship2_name, ship2_add1, ship2_city, ship2_state, ship2_zip,
    now,
):
    rows = _read_csv("customer_crosswalk.csv")
    name_norm = normalize_name(ship2_name)
    zip5 = normalize_zip(ship2_zip)

    # Check for existing entry to update
    found = False
    for row in rows:
        # Match on normalized name + zip
        if row.get("ship2_name_normalized") == name_norm and row.get("ship2_zip") == zip5:
            row["p21_customer_id"] = cid
            row["p21_customer_name"] = cname
            row["last_seen"] = now
            row["seen_count"] = str(int(row.get("seen_count", 0)) + 1)
            if source_sys and source_cid:
                row["source_system"] = source_sys
                row["source_customer_id"] = source_cid
            if row.get("match_method") == "seed":
                row["match_method"] = "auto"
            found = True
            break

    if not found:
        rows.append({
            "source_system": source_sys or "manual",
            "source_customer_id": source_cid or "",
            "source_customer_name": "",
            "ship2_name": ship2_name,
            "ship2_name_normalized": name_norm,
            "ship2_add1": ship2_add1,
            "ship2_add1_normalized": normalize_address(ship2_add1),
            "ship2_city": ship2_city,
            "ship2_state": ship2_state,
            "ship2_zip": zip5,
            "p21_customer_id": cid,
            "p21_customer_name": cname,
            "match_score": "1.0",
            "match_method": "manual" if not source_sys else "auto",
            "is_active": "1",
            "last_seen": now,
            "seen_count": "1",
        })

    _write_csv("customer_crosswalk.csv", rows)


def _upsert_items(cid, lines, corrections, now):
    rows = _read_csv("customer_item_crosswalk.csv")

    # Build index for fast lookup
    idx = {}
    for i, row in enumerate(rows):
        key = (row.get("p21_customer_id", ""), row.get("customer_part_number", ""))
        idx[key] = i

    line_corrections = (corrections or {}).get("lines", {})

    for line in lines:
        part = line.get("supplier_part_id", "").strip()
        uid = line.get("inv_mast_uid", "").strip()
        if not part or not uid:
            continue

        # Apply line-level correction if provided
        line_no = str(line.get("line_no", ""))
        if line_no in line_corrections:
            uid = line_corrections[line_no].get("inv_mast_uid", uid)

        key = (cid, part)
        if key in idx:
            row = rows[idx[key]]
            row["p21_inv_mast_uid"] = uid
            row["last_seen"] = now
            row["seen_count"] = str(int(row.get("seen_count", 0)) + 1)
            try:
                price = float(line.get("unit_price", 0) or 0)
                if price > 0:
                    row["unit_price_last"] = f"{price:.4f}"
            except (ValueError, TypeError):
                pass
        else:
            price = 0
            try:
                price = float(line.get("unit_price", 0) or 0)
            except (ValueError, TypeError):
                pass
            new_row = {
                "p21_customer_id": cid,
                "customer_part_number": part,
                "p21_inv_mast_uid": uid,
                "p21_item_desc": line.get("item_description", ""),
                "unit_of_measure": line.get("unit_of_measure", "EA"),
                "product_group_id": line.get("product_group", ""),
                "unit_price_last": f"{price:.4f}" if price else "",
                "unit_price_avg": f"{price:.4f}" if price else "",
                "unit_price_min": f"{price:.4f}" if price else "",
                "unit_price_max": f"{price:.4f}" if price else "",
                "last_seen": now,
                "seen_count": "1",
            }
            rows.append(new_row)
            idx[key] = len(rows) - 1

    _write_csv("customer_item_crosswalk.csv", rows)


def _append_po_history(cid, po_no, order_no, ship2_name, now):
    rows = _read_csv("customer_po_history.csv")

    # Check if already exists
    for row in rows:
        if row.get("customer_po_no") == po_no and row.get("p21_customer_id") == cid:
            if order_no:
                row["p21_order_no"] = order_no
            return

    rows.append({
        "p21_customer_id": cid,
        "customer_po_no": po_no,
        "p21_order_no": order_no or "",
        "order_date": now[:10],
        "completed": "N",
        "approved": "Y",
        "ship2_name": ship2_name,
    })

    _write_csv("customer_po_history.csv", rows)
