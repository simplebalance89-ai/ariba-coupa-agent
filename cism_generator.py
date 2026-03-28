"""
cism_generator.py — Generate P21 CISM flat files from parsed PO data.
Outputs fixed-width ASCII files that P21's CISM scheduled import picks up.

This replaces direct SQL INSERT as the transport layer into P21.
Same POHeader/POLineItem models, different output.

Drop location: configurable (default: ./cism_output/)
Production: \\P21Server\CISM\Import\Incoming\
Archive: \\P21Server\CISM\Import\Processed\

File format: Fixed-width, 130 chars per record, CRLF line endings.
Record types: H (header), L (line), T (trailer)
"""

import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

from models import POHeader, POLineItem


# ── Configuration ────────────────────────────────────────────────────────────

CISM_OUTPUT_DIR = os.environ.get("CISM_OUTPUT_DIR", "./cism_output")
CISM_ARCHIVE_DIR = os.environ.get("CISM_ARCHIVE_DIR", "./cism_archive")
RECORD_WIDTH = 130


# ── Field Formatters ─────────────────────────────────────────────────────────

def _str(value: str, width: int) -> str:
    """Left-aligned, space-padded string field."""
    return str(value or "")[:width].ljust(width)


def _num(value: float, width: int, decimals: int = 4) -> str:
    """Zero-padded numeric field with implied decimals."""
    multiplier = 10 ** decimals
    int_val = int(round(value * multiplier))
    if int_val < 0:
        int_val = 0
    return str(int_val).zfill(width)[:width]


def _date(value: str) -> str:
    """Date field as YYYYMMDD. Handles various input formats."""
    if not value:
        return "        "  # 8 spaces
    # Strip time component if present
    date_str = str(value)[:10]
    # Remove dashes
    clean = date_str.replace("-", "").replace("/", "")
    if len(clean) == 8 and clean.isdigit():
        return clean
    return "        "


# ── Record Builders ──────────────────────────────────────────────────────────

def build_header_record(header: POHeader) -> str:
    """
    H record — PO header.

    Layout (130 chars):
    Pos 1     : Record Type (1) = 'H'
    Pos 2-21  : PO Number (20)
    Pos 22-41 : Supplier ID (20)
    Pos 42-49 : Order Date (8) YYYYMMDD
    Pos 50-69 : Ship To ID / Location (20)
    Pos 70-89 : Buyer ID (20)
    Pos 90-119: Quote Reference (30)
    Pos 120-130: Filler (11)
    """
    record = (
        "H"
        + _str(header.po_no, 20)
        + _str(str(header.supplier_id or ""), 20)
        + _date(header.order_date)
        + _str(header.ship2_name or str(header.location_id), 20)
        + _str(header.requested_by or header.buyer or "SYSTEM", 20)
        + _str(header.external_po_no or header.contract_no or "", 30)
        + _str("", 11)  # filler
    )
    return record[:RECORD_WIDTH].ljust(RECORD_WIDTH)


def build_line_record(header: POHeader, line: POLineItem) -> str:
    """
    L record — PO line item.

    Layout (130 chars):
    Pos 1     : Record Type (1) = 'L'
    Pos 2-21  : PO Number (20)
    Pos 22-26 : Line Number (5) zero-padded
    Pos 27-56 : Item ID (30) — mapped via crosswalk
    Pos 57-68 : Qty Ordered (12) implied 4 decimals
    Pos 69-80 : Unit Price (12) implied 4 decimals
    Pos 81-90 : UOM (10)
    Pos 91-98 : Request Date (8) YYYYMMDD
    Pos 99-130: Filler (32)
    """
    # Item ID: use supplier_part_id (will be crosswalked to P21 item_id externally)
    item_id = line.supplier_part_id or line.mfg_part_no or line.item_description[:30]

    record = (
        "L"
        + _str(header.po_no, 20)
        + str(line.line_no).zfill(5)[:5]
        + _str(item_id, 30)
        + _num(line.qty_ordered, 12, 4)
        + _num(line.unit_price, 12, 4)
        + _str(line.unit_of_measure or "EA", 10)
        + _date(line.date_due or line.required_date or header.order_date)
        + _str("", 32)  # filler
    )
    return record[:RECORD_WIDTH].ljust(RECORD_WIDTH)


def build_trailer_record(header: POHeader, lines: list[POLineItem]) -> str:
    """
    T record — PO trailer / summary.

    Layout (130 chars):
    Pos 1     : Record Type (1) = 'T'
    Pos 2-21  : PO Number (20)
    Pos 22-26 : Line Count (5) zero-padded
    Pos 27-41 : Total Amount (15) implied 2 decimals
    Pos 42-130: Filler (89)
    """
    total = sum(l.qty_ordered * l.unit_price for l in lines)

    record = (
        "T"
        + _str(header.po_no, 20)
        + str(len(lines)).zfill(5)[:5]
        + _num(total, 15, 2)
        + _str("", 89)  # filler
    )
    return record[:RECORD_WIDTH].ljust(RECORD_WIDTH)


# ── File Generator ───────────────────────────────────────────────────────────

def generate_cism_file(
    header: POHeader,
    lines: list[POLineItem],
    output_dir: Optional[str] = None,
) -> str:
    """
    Generate a CISM flat file for one PO.

    Returns the file path of the generated file.
    File is written atomically (temp → rename) to prevent partial reads.
    """
    out_dir = Path(output_dir or CISM_OUTPUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)

    # File naming: ENP_PO_YYYYMMDD_HHMMSS_GUID.txt
    now = datetime.now()
    guid = uuid.uuid4().hex[:8]
    filename = f"ENP_PO_{now.strftime('%Y%m%d_%H%M%S')}_{guid}.txt"
    filepath = out_dir / filename
    temp_path = out_dir / f".tmp_{filename}"

    # Build records
    records = []
    records.append(build_header_record(header))
    for line in lines:
        records.append(build_line_record(header, line))
    records.append(build_trailer_record(header, lines))

    # Write atomically: temp file → rename
    content = "\r\n".join(records) + "\r\n"
    with open(temp_path, "w", encoding="utf-8", newline="") as f:
        f.write(content)
    temp_path.rename(filepath)

    return str(filepath)


def generate_cism_batch(
    pos: list[tuple[POHeader, list[POLineItem]]],
    output_dir: Optional[str] = None,
) -> list[str]:
    """Generate CISM files for multiple POs. One file per PO."""
    paths = []
    for header, lines in pos:
        path = generate_cism_file(header, lines, output_dir)
        paths.append(path)
    return paths


# ── Validation ───────────────────────────────────────────────────────────────

def validate_cism_file(filepath: str) -> dict:
    """
    Read back a CISM file and validate structure.
    Returns validation result with any errors found.
    """
    errors = []
    h_count = 0
    l_count = 0
    t_count = 0
    po_no = None

    with open(filepath, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            line = line.rstrip("\r\n")

            if len(line) < 1:
                continue

            rec_type = line[0]
            if rec_type == "H":
                h_count += 1
                po_no = line[1:21].strip()
                if not po_no:
                    errors.append(f"Line {i}: H record missing PO number")
            elif rec_type == "L":
                l_count += 1
                line_po = line[1:21].strip()
                if line_po != po_no:
                    errors.append(f"Line {i}: L record PO '{line_po}' doesn't match H PO '{po_no}'")
            elif rec_type == "T":
                t_count += 1
                expected_lines = int(line[21:26].strip() or 0)
                if expected_lines != l_count:
                    errors.append(f"Line {i}: T line count {expected_lines} doesn't match actual {l_count}")
            else:
                errors.append(f"Line {i}: Unknown record type '{rec_type}'")

    if h_count != 1:
        errors.append(f"Expected 1 H record, found {h_count}")
    if t_count != 1:
        errors.append(f"Expected 1 T record, found {t_count}")

    return {
        "file": filepath,
        "valid": len(errors) == 0,
        "po_no": po_no,
        "header_records": h_count,
        "line_records": l_count,
        "trailer_records": t_count,
        "errors": errors,
    }


# ── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    """Quick test: generate a sample CISM file."""
    sample_header = POHeader(
        po_no="TEST-PO-001",
        order_date="2026-03-26",
        supplier_id=120368,
        ship2_name="ENPRO INDUSTRIES",
        location_id=10,
        buyer="SYSTEM",
        external_po_no="QTE-2026-001",
    )

    sample_lines = [
        POLineItem(
            line_no=1,
            supplier_part_id="FILT-MAST-001",
            qty_ordered=10,
            unit_price=125.00,
            unit_of_measure="EA",
            date_due="2026-04-15",
        ),
        POLineItem(
            line_no=2,
            supplier_part_id="FILT-MAST-002",
            qty_ordered=5,
            unit_price=89.90,
            unit_of_measure="EA",
            date_due="2026-04-15",
        ),
    ]

    path = generate_cism_file(sample_header, sample_lines, "./cism_test")
    print(f"Generated: {path}")

    result = validate_cism_file(path)
    print(f"Valid: {result['valid']}")
    print(f"PO: {result['po_no']}")
    print(f"Lines: {result['line_records']}")
    if result['errors']:
        for e in result['errors']:
            print(f"  ERROR: {e}")

    # Print file contents
    print("\n--- File Contents ---")
    with open(path) as f:
        print(f.read())
