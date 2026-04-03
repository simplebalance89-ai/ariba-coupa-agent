"""
cism_batch.py -- Accumulate approved POs into a single CISM batch for P21 import.

Flow: Approved POs → batch header CSV + batch lines CSV → Azure Blob → Local → P21 CISM

The batch grows as POs are approved. When ready, export/download the batch,
upload to Azure Blob, sync to local machine, and P21's CISM job imports it.
"""

import csv
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)

BATCH_DIR = os.environ.get("CISM_BATCH_DIR", "/app/data/cism_batch")
BATCH_HEADER_FILE = "batch_orderquoteheader.csv"
BATCH_LINES_FILE = "batch_orderquoteline.csv"

HEADER_COLUMNS = [
    "Import Set No", "Customer ID", "Customer Name", "Company ID",
    "Sales Location ID", "Customer PO Number", "Contact ID", "Contact Name",
    "Taker", "Job Name", "Order Date", "Requested Date", "Quote", "Approved",
    "Ship To ID", "Ship To Name", "Ship To Address 1", "Ship To Address 2",
    "Ship To City", "Ship To State", "Ship To Zip Code", "Ship To Country",
    "Packing Basis", "Delivery Instructions", "Terms", "Carrier ID", "Will Call",
]

LINE_COLUMNS = [
    "Import Set Number", "Line No", "Item ID", "Unit Quantity",
    "Unit of Measure", "Unit Price", "Extended Description",
    "Source Location ID", "Ship Location ID", "Product Group ID",
    "Required Date", "Disposition", "Manual Price Override", "Capture Usage",
    "Item Description",
]


def _ensure_dir():
    os.makedirs(BATCH_DIR, exist_ok=True)


def _header_path():
    return os.path.join(BATCH_DIR, BATCH_HEADER_FILE)


def _lines_path():
    return os.path.join(BATCH_DIR, BATCH_LINES_FILE)


def add_to_batch(po_data: dict):
    """Add an approved PO to the running CISM batch."""
    _ensure_dir()

    cism = po_data.get("cism", {})
    header = po_data.get("header", {})
    lines = po_data.get("lines", [])
    cust = po_data.get("customer_match", {})
    import_set = cism.get("import_set_no", "")

    if not import_set:
        logger.warning(f"No import_set_no for PO {po_data.get('po_no')}, skipping batch")
        return

    # Get customer defaults (contact_id, address_id, terms, carrier)
    defaults = po_data.get("customer_defaults", {})

    # Append header row
    h_path = _header_path()
    write_header_row = not os.path.exists(h_path) or os.path.getsize(h_path) == 0
    with open(h_path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=HEADER_COLUMNS)
        if write_header_row:
            w.writeheader()
        w.writerow({
            "Import Set No": import_set,
            "Customer ID": cust.get("p21_id", ""),
            "Customer Name": cust.get("name", "")[:50],
            "Company ID": "1",
            "Sales Location ID": "10",
            "Customer PO Number": po_data.get("po_no", "")[:50],
            "Contact ID": defaults.get("default_contact_id", ""),
            "Contact Name": (header.get("buyer") or cust.get("name", ""))[:50],
            "Taker": header.get("taker", "") or "SYSTEM",
            "Job Name": "",
            "Order Date": _fmt_date(header.get("order_date", "")),
            "Requested Date": _fmt_date(header.get("order_date", "")),
            "Quote": "",
            "Approved": "Y",
            "Ship To ID": defaults.get("default_address_id", ""),
            "Ship To Name": header.get("ship2_name", "")[:50],
            "Ship To Address 1": header.get("ship2_add1", "")[:50],
            "Ship To Address 2": "",
            "Ship To City": header.get("ship2_city", "")[:50],
            "Ship To State": header.get("ship2_state", "")[:50],
            "Ship To Zip Code": header.get("ship2_zip", "")[:10],
            "Ship To Country": header.get("ship2_country", "US")[:50],
            "Packing Basis": "Partial/Order",
            "Delivery Instructions": (header.get("comments") or "")[:255],
            "Terms": defaults.get("default_terms", ""),
            "Carrier ID": defaults.get("default_carrier_id", ""),
            "Will Call": "N",
        })

    # Append line rows
    l_path = _lines_path()
    write_line_header = not os.path.exists(l_path) or os.path.getsize(l_path) == 0
    with open(l_path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=LINE_COLUMNS)
        if write_line_header:
            w.writeheader()
        for i, line in enumerate(lines, 1):
            w.writerow({
                "Import Set Number": import_set,
                "Line No": str(line.get("line_no", i)),
                "Item ID": (line.get("item_id_p21") or line.get("supplier_part_id", ""))[:40],
                "Unit Quantity": str(line.get("qty_ordered", 0)),
                "Unit of Measure": (line.get("unit_of_measure") or "EA")[:8],
                "Unit Price": f"{float(line.get('unit_price', 0) or 0):.4f}",
                "Extended Description": (line.get("item_description") or "")[:255],
                "Source Location ID": "10",
                "Ship Location ID": "10",
                "Product Group ID": (line.get("product_group") or "")[:8],
                "Required Date": "",
                "Disposition": "B",
                "Manual Price Override": "Y" if line.get("unit_price") else "N",
                "Capture Usage": "N",
                "Item Description": (line.get("item_description") or "")[:40],
            })

    logger.info(f"Added to CISM batch: PO {po_data.get('po_no')} ({import_set}), {len(lines)} lines")


def get_batch_status() -> dict:
    """Get current batch contents and stats."""
    _ensure_dir()
    h_path = _header_path()
    l_path = _lines_path()

    headers = []
    lines = []

    if os.path.exists(h_path) and os.path.getsize(h_path) > 0:
        with open(h_path, encoding="utf-8") as f:
            headers = list(csv.DictReader(f))

    if os.path.exists(l_path) and os.path.getsize(l_path) > 0:
        with open(l_path, encoding="utf-8") as f:
            lines = list(csv.DictReader(f))

    return {
        "header_count": len(headers),
        "line_count": len(lines),
        "headers": headers,
        "lines": lines,
        "header_file": h_path,
        "lines_file": l_path,
        "header_size": os.path.getsize(h_path) if os.path.exists(h_path) else 0,
        "lines_size": os.path.getsize(l_path) if os.path.exists(l_path) else 0,
    }


def clear_batch():
    """Clear the batch after upload to Azure."""
    h_path = _header_path()
    l_path = _lines_path()
    # Archive before clearing
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    archive_dir = os.path.join(BATCH_DIR, "archive")
    os.makedirs(archive_dir, exist_ok=True)
    if os.path.exists(h_path):
        os.rename(h_path, os.path.join(archive_dir, f"batch_header_{ts}.csv"))
    if os.path.exists(l_path):
        os.rename(l_path, os.path.join(archive_dir, f"batch_lines_{ts}.csv"))
    logger.info(f"Batch cleared and archived ({ts})")
    return {"status": "cleared", "archive": ts}


def _fmt_date(d):
    if not d:
        return ""
    try:
        dt = datetime.fromisoformat(str(d).replace("Z", "").split("T")[0])
        return dt.strftime("%m/%d/%Y")
    except:
        return str(d)[:10]
