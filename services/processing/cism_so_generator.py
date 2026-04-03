"""
cism_so_generator.py -- Generate CISM batch import files for P21 Sales Order creation.

Outputs CSV files matching P21's Order/Quote Import schema:
  - orderquoteheader import (75 columns)
  - orderquoteline import (40 columns)

These CSVs are picked up by P21's CISM scheduled import job.
"""

import csv
import hashlib
import logging
import os
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# Default output directory
CISM_OUTPUT_DIR = os.environ.get("CISM_SO_OUTPUT_DIR", "./cism_so_output")

# P21 defaults
DEFAULT_COMPANY_ID = os.environ.get("P21_COMPANY_ID", "1")
DEFAULT_LOCATION_ID = os.environ.get("P21_LOCATION_ID", "10")
DEFAULT_TAKER = os.environ.get("P21_DEFAULT_TAKER", "SYSTEM")
DEFAULT_PACKING_BASIS = "Partial/Order"
DEFAULT_DISPOSITION = "B"  # Backorder


def generate_import_set_no() -> str:
    """Generate unique 8-char import set number."""
    ts = datetime.utcnow().strftime("%y%m%d%H%M%S%f")
    h = hashlib.md5(ts.encode()).hexdigest()[:4].upper()
    return f"A{h}{ts[-3:]}"[:8]


def generate_cism_so(
    # Customer match results
    p21_customer_id: str,
    p21_customer_name: str,
    p21_ship_to_id: str = "",
    # Incoming PO data
    po_no: str = "",
    order_date: str = "",
    requested_date: str = "",
    ship2_name: str = "",
    ship2_add1: str = "",
    ship2_add2: str = "",
    ship2_city: str = "",
    ship2_state: str = "",
    ship2_zip: str = "",
    ship2_country: str = "US",
    ship2_email: str = "",
    ship2_phone: str = "",
    # Contact / taker
    contact_id: str = "",
    contact_name: str = "",
    taker: str = "",
    # Terms / shipping
    terms: str = "",
    carrier_id: str = "",
    carrier_name: str = "",
    delivery_instructions: str = "",
    # Flags
    approved: str = "Y",
    # Classification
    class_1: str = "",
    class_2: str = "",
    source_id: str = "",
    # Lines: list of dicts with item match results + PO line data
    lines: list = None,
    # Output
    output_dir: str = None,
) -> dict:
    """
    Generate P21 CISM Order/Quote Import files (header CSV + line CSV).

    Returns dict with paths and import_set_no.
    """
    out_dir = output_dir or CISM_OUTPUT_DIR
    os.makedirs(out_dir, exist_ok=True)

    import_set_no = generate_import_set_no()
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    if not order_date:
        order_date = datetime.utcnow().strftime("%m/%d/%Y")
    if not requested_date:
        requested_date = order_date

    # ------------------------------------------------------------------
    # Header CSV
    # ------------------------------------------------------------------

    header_row = {
        "Import Set No": import_set_no,
        "Customer ID": p21_customer_id,
        "Customer Name": p21_customer_name[:50],
        "Company ID": DEFAULT_COMPANY_ID,
        "Sales Location ID": DEFAULT_LOCATION_ID,
        "Customer PO Number": po_no[:50] if po_no else "",
        "Contact ID": contact_id[:16] if contact_id else "",
        "Contact Name": contact_name[:50] if contact_name else p21_customer_name[:50],
        "Taker": (taker or DEFAULT_TAKER)[:30],
        "Job Name": "",
        "Order Date": _format_date(order_date),
        "Requested Date": _format_date(requested_date),
        "Quote": "",
        "Approved": approved,
        "Ship To ID": p21_ship_to_id[:9] if p21_ship_to_id else "",
        "Ship To Name": ship2_name[:50] if ship2_name else p21_customer_name[:50],
        "Ship To Address 1": ship2_add1[:50] if ship2_add1 else "",
        "Ship To Address 2": ship2_add2[:50] if ship2_add2 else "",
        "Ship To City": ship2_city[:50] if ship2_city else "",
        "Ship To State": ship2_state[:50] if ship2_state else "",
        "Ship To Zip Code": ship2_zip[:10] if ship2_zip else "",
        "Ship To Country": ship2_country[:50] if ship2_country else "US",
        "Source Location ID": DEFAULT_LOCATION_ID,
        "Carrier ID": carrier_id[:9] if carrier_id else "",
        "Carrier Name": carrier_name[:50] if carrier_name else "",
        "Route": "",
        "Packing Basis": DEFAULT_PACKING_BASIS,
        "Delivery Instructions": delivery_instructions[:255] if delivery_instructions else "",
        "Terms": terms[:2] if terms else "",
        "Terms Desc": "",
        "Will Call": "N",
        "Class 1": class_1[:8] if class_1 else "",
        "Class 2": class_2[:8] if class_2 else "",
        "Class 3": "",
        "Class 4": "",
        "Class 5": "",
        "RMA_Flag": "",
        "Freight Code": "",
        "Third Party Billing Flag Desc": "",
        "Capture Usage Default": "",
        "Allocate": "",
        "Contract Number": "",
        "Invoice Batch Number": "",
        "Ship To Email Address": ship2_email[:255] if ship2_email else "",
        "Set Invoice Exchange Rate Source Desc": "",
        "Ship To Phone": ship2_phone[:20] if ship2_phone else "",
        "Currency ID": "",
        "Apply Builder Allowance Flag": "",
        "Quote Expiration Date": "",
        "Promise Date": "",
        "Import As Quote": "",
        "Quote Number": "",
        "Web Reference Number": "",
        "Create Invoice": "",
        "Strategic Pricing Library ID": "",
        "Merchandise Credit": "",
        "Order Type Priority": "",
        "UPS Code": "",
        "Supplier Order No": source_id[:255] if source_id else "",
        "Supplier Release No": "",
        "Placed By Name": "",
        "Req Payment Upon Release": "",
        "Freight Out": "",
        "Ship To Address": "",
        "Quote Type": "",
        "Homeowner": "",
        "Installer": "",
        "Building": "",
        "Architect": "",
        "Designer": "",
        "Pricing Source": "",
        "Ship to Latitude": "",
        "Ship To Longitude": "",
        "Exemption No": "",
        "Order Number": "",
    }

    header_path = os.path.join(out_dir, f"ENP_SO_HDR_{ts}_{import_set_no}.csv")
    _write_row(header_path, header_row)

    # ------------------------------------------------------------------
    # Line CSV
    # ------------------------------------------------------------------

    line_rows = []
    for i, line in enumerate(lines or [], start=1):
        line_row = {
            "Import Set Number": import_set_no,
            "Line No": str(i),
            "Item ID": line.get("item_id", "")[:40],
            "Unit Quantity": str(line.get("qty_ordered", 0)),
            "Unit of Measure": (line.get("unit_of_measure", "") or "EA")[:8],
            "Unit Price": _format_price(line.get("unit_price", 0)),
            "Extended Description": line.get("item_description", "")[:255],
            "Source Location ID": DEFAULT_LOCATION_ID,
            "Ship Location ID": DEFAULT_LOCATION_ID,
            "Product Group ID": line.get("product_group", "")[:8],
            "Supplier ID": line.get("supplier_id", "")[:9],
            "Supplier Name": "",
            "Required Date": _format_date(line.get("required_date", "") or requested_date),
            "Expedite Date": "",
            "Will Call": "N",
            "Tax Item": "Y",
            "OK to Interchange": "N",
            "Pricing Unit": (line.get("unit_of_measure", "") or "EA")[:8],
            "Commission Cost": "",
            "Other Cost": "",
            "PO Cost": "",
            "Disposition": line.get("disposition", DEFAULT_DISPOSITION)[:1],
            "Scheduled": "N",
            "Manual Price Override": "Y" if line.get("unit_price") else "N",
            "Commission Cost Edited": "N",
            "Other Cost Edited": "N",
            "Capture Usage": "N",
            "Tag and Hold Class ID": "",
            "Contract Bin ID": "",
            "Contract No.": "",
            "Allocation Qty": "",
            "Promise Date": "",
            "Revision Level": "",
            "Resolve Item Contract": "N",
            "Sample": "N",
            "Quote Line No.": "",
            "Quote Complete": "",
            "Item Description": line.get("item_description", "")[:40],
            "Invoice No.": "",
            "Line No": str(i),
        }
        line_rows.append(line_row)

    lines_path = os.path.join(out_dir, f"ENP_SO_LIN_{ts}_{import_set_no}.csv")
    if line_rows:
        _write_rows(lines_path, line_rows)

    logger.info(
        f"CISM SO generated: {import_set_no} | customer={p21_customer_id} | "
        f"po={po_no} | lines={len(line_rows)} | {header_path}"
    )

    return {
        "import_set_no": import_set_no,
        "header_path": header_path,
        "lines_path": lines_path,
        "customer_id": p21_customer_id,
        "po_no": po_no,
        "line_count": len(line_rows),
    }


def _format_date(date_str: str) -> str:
    """Convert various date formats to MM/DD/YYYY for P21."""
    if not date_str:
        return ""
    # Already in MM/DD/YYYY
    if len(date_str) == 10 and date_str[2] == "/":
        return date_str
    # ISO format YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "").split("T")[0])
        return dt.strftime("%m/%d/%Y")
    except (ValueError, AttributeError):
        return date_str[:10]


def _format_price(price) -> str:
    """Format price for CISM."""
    try:
        return f"{float(price):.4f}"
    except (ValueError, TypeError):
        return "0.0000"


def _write_row(path: str, row: dict):
    """Write single-row CSV."""
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=row.keys())
        w.writeheader()
        w.writerow(row)


def _write_rows(path: str, rows: list[dict]):
    """Write multi-row CSV."""
    if not rows:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)
