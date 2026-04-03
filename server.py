"""
server.py — Ariba/Coupa PO Automation Agent: FastAPI main application.
Forked from Vega MRO agent, extended with crosswalk + confidence + review portal.

Routes:
  GET  /                        → Dashboard / Review Portal
  GET  /health                  → Health check

  POST /api/v1/intake/cxml      → Receive cXML (returns cXML Response)
  POST /api/v1/intake/upload    → Upload cXML or PDF file
  POST /api/v1/intake/parse     → Parse only (preview mode)

  GET  /api/v1/review/queue     → Review queue (green/yellow/red)
  GET  /api/v1/review/po/{id}   → PO detail
  POST /api/v1/review/po/{id}/approve  → Approve → CISM → blob
  POST /api/v1/review/po/{id}/reject   → Reject with reason
  POST /api/v1/review/crosswalk/vendor → Add vendor mapping
  POST /api/v1/review/crosswalk/item   → Add item mapping

  GET  /api/v1/stats            → Dashboard stats
"""

import hashlib
import logging
import os
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Request, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from config import get_settings
from models import (
    POPayload, POImportResult, POStatus, SourceSystem, HealthResponse,
    VEGA_SOURCE_TYPE,
)
import po_parser
from services.intake.email_classifier import classify_email
from services.processing.crosswalk_engine import (
    crosswalk_vendor, crosswalk_item,
    save_vendor_mapping, save_item_mapping,
)
from services.processing.confidence_scorer import score_payload
from services.processing.duplicate_detector import (
    generate_intake_id, is_duplicate, log_intake,
)
from cism_generator import generate_cism_file
from services.processing.blob_uploader import upload_approved_cism, upload_rejected_cism
from services.processing.quote_exporter import export_quotes_to_blob
from services.processing.so_exporter import export as export_so_data
from services.processing.customer_crosswalk_engine import CustomerCrosswalkEngine
from services.processing.confidence_scorer import score_customer_po
from services.processing.cism_so_generator import generate_cism_so
from services.processing.crosswalk_learner import learn_from_approval
from services.processing import local_store

settings = get_settings()

logging.basicConfig(
    level=logging.DEBUG if settings.debug else logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Ariba/Coupa PO Automation Agent",
    version=settings.app_version,
    docs_url="/docs",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="static"), name="static")


# ── CSV PO Parser ────────────────────────────────────────────────────────────

def _parse_csv_po(content: str):
    """Parse a CSV PO file into header + lines. Expects columns like po_no, ship2_name, item_id, qty, price."""
    import csv as csvmod
    from io import StringIO
    from models import POHeader, POLineItem

    reader = csvmod.DictReader(StringIO(content))
    rows = list(reader)
    if not rows:
        raise ValueError("Empty CSV file")

    first = rows[0]
    header = POHeader(
        po_no=first.get("po_no", first.get("po_number", first.get("PO Number", ""))),
        order_date=first.get("order_date", first.get("Order Date", "")),
        ship2_name=first.get("ship2_name", first.get("Ship To Name", first.get("ship_to_name", ""))),
        ship2_add1=first.get("ship2_add1", first.get("Ship To Address", "")),
        ship2_city=first.get("ship2_city", first.get("Ship To City", "")),
        ship2_state=first.get("ship2_state", first.get("Ship To State", "")),
        ship2_zip=first.get("ship2_zip", first.get("Ship To Zip", "")),
        ship2_country=first.get("ship2_country", "US"),
        supplier_name=first.get("supplier_name", first.get("Supplier", first.get("vendor_name", ""))),
        buyer=first.get("buyer", first.get("Buyer", "")),
        buyer_email=first.get("buyer_email", ""),
        comments=first.get("comments", first.get("delivery_instructions", "")),
    )

    lines = []
    for i, row in enumerate(rows, 1):
        lines.append(POLineItem(
            line_no=int(row.get("line_no", i)),
            supplier_part_id=row.get("item_id", row.get("supplier_part_id", row.get("Part Number", row.get("customer_part_number", "")))),
            item_description=row.get("description", row.get("item_description", row.get("Description", ""))),
            qty_ordered=float(row.get("qty", row.get("qty_ordered", row.get("Quantity", 0))) or 0),
            unit_price=float(row.get("unit_price", row.get("price", row.get("Unit Price", 0))) or 0),
            unit_of_measure=row.get("uom", row.get("unit_of_measure", row.get("UOM", "EA"))),
        ))

    return header, lines, content


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return HealthResponse(
        status="healthy",
        version=settings.app_version,
        environment=settings.environment,
        services={"staging_db": "configured" if settings.staging_sql_server else "not configured"},
    )


# ── Dashboard ─────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    with open("static/index.html") as f:
        return f.read()


@app.get("/review", response_class=HTMLResponse)
async def review_portal():
    """PO Review Portal UI."""
    with open("portal/review.html") as f:
        return f.read()


# ── Intake: cXML ──────────────────────────────────────────────────────────────

@app.post("/api/v1/intake/cxml")
async def intake_cxml(request: Request):
    """Receive cXML OrderRequest from Ariba/Coupa. Returns cXML Response."""
    body = await request.body()
    content = body.decode("utf-8")

    try:
        header, lines, raw = po_parser.parse_cxml(content)
    except Exception as e:
        logger.error(f"cXML parse error: {e}")
        return HTMLResponse(
            content=po_parser.generate_cxml_response("error", 400, str(e)),
            media_type="text/xml",
            status_code=200,  # Ariba expects 200 even on errors
        )

    # Classify source from cXML content
    source = "ariba"
    if "coupa" in content.lower():
        source = "coupa"

    payload = await _process_po_to_so(header, lines, raw, source, "cxml")

    return HTMLResponse(
        content=po_parser.generate_cxml_response(header.po_no, 200, "OK"),
        media_type="text/xml",
    )


# ── Intake: File Upload ──────────────────────────────────────────────────────

@app.post("/api/v1/intake/upload")
async def intake_upload(
    file: UploadFile = File(...),
    source: str = Form("direct"),
):
    """Upload a cXML or PDF file for processing."""
    content = await file.read()
    filename = file.filename or ""

    if filename.lower().endswith(".xml"):
        header, lines, raw = po_parser.parse_cxml(content.decode("utf-8"))
        fmt = "cxml"
    elif filename.lower().endswith(".csv"):
        header, lines, raw = _parse_csv_po(content.decode("utf-8-sig"))
        fmt = "csv"
    elif filename.lower().endswith(".pdf"):
        import tempfile
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp.write(content)
            tmp_path = tmp.name
        header, lines, raw = po_parser.parse_pdf(
            tmp_path, settings.doc_intel_endpoint, settings.doc_intel_key
        )
        os.unlink(tmp_path)
        fmt = "pdf"
    else:
        raise HTTPException(400, "Unsupported file type. Upload .xml, .csv, or .pdf")

    payload = await _process_po_to_so(header, lines, raw, source, fmt)
    return payload


# ── Processing Pipeline ──────────────────────────────────────────────────────

async def _process_po(header, lines, raw_content, source, fmt) -> dict:
    """Full processing pipeline: crosswalk → score → CISM → log."""

    # Generate intake ID for dedup
    intake_id = generate_intake_id(
        header.po_no,
        header.supplier_name or str(header.supplier_id or ""),
        source,
    )

    # Duplicate check
    if is_duplicate(intake_id, header.po_no, source):
        logger.info(f"Duplicate PO: {header.po_no} from {source}")
        return {"status": "duplicate", "po_no": header.po_no}

    # Vendor crosswalk
    vendor_p21, vendor_score = crosswalk_vendor(
        str(header.supplier_id or header.supplier_name or ""),
        header.supplier_name,
        source,
    )
    header.vendor_id_raw = str(header.supplier_id or header.supplier_name or "")
    header.vendor_id_p21 = vendor_p21
    header.vendor_match_score = vendor_score
    if vendor_p21:
        header.supplier_id = int(vendor_p21) if vendor_p21.isdigit() else None

    # Item crosswalk for each line
    for line in lines:
        item_p21, item_score = crosswalk_item(
            line.supplier_part_id,
            source,
            vendor_p21,
        )
        line.item_id_p21 = item_p21
        line.crosswalk_match_score = item_score

    # Build payload
    payload = POPayload(
        intake_id=intake_id,
        source=SourceSystem(source.upper()) if source.upper() in SourceSystem.__members__ else SourceSystem.DIRECT,
        format=fmt,
        received_at=datetime.utcnow().isoformat(),
        header=header,
        lines=lines,
        raw_content=raw_content[:8000] if isinstance(raw_content, str) else "",
    )

    # Confidence scoring
    payload = score_payload(payload)

    # Generate CISM file
    cism_path = generate_cism_file(header, lines, settings.cism_output_dir)
    payload.cism_blob_path = cism_path

    # Log to staging DB
    log_intake(payload)

    logger.info(
        f"Processed PO {header.po_no} | source={source} | "
        f"confidence={payload.overall_confidence} | "
        f"vendor={vendor_p21}({vendor_score:.2f}) | "
        f"lines={len(lines)} | cism={cism_path}"
    )

    return {
        "status": "processed",
        "po_no": header.po_no,
        "intake_id": intake_id,
        "confidence": payload.overall_confidence,
        "review_required": payload.review_required,
        "vendor_match": {"p21_id": vendor_p21, "score": vendor_score},
        "lines": len(lines),
        "cism_path": cism_path,
    }


# ── Customer Crosswalk Engine (lazy init) ────────────────────────────────────

_customer_engine = None

def _get_customer_engine():
    global _customer_engine
    if _customer_engine is None:
        _customer_engine = CustomerCrosswalkEngine(settings.crosswalk_dir)
    return _customer_engine


# ── SO Processing Pipeline (customer-focused) ───────────────────────────────

async def _process_po_to_so(header, lines, raw_content, source, fmt) -> dict:
    """
    Customer-focused pipeline: match incoming PO to P21 customer/items,
    score confidence, generate CISM SO import files.
    """
    engine = _get_customer_engine()

    # Generate intake ID for dedup
    intake_id = generate_intake_id(
        header.po_no,
        header.ship2_name or header.supplier_name or "",
        source,
    )

    if is_duplicate(intake_id, header.po_no, source):
        logger.info(f"Duplicate PO: {header.po_no} from {source}")
        return {"status": "duplicate", "po_no": header.po_no}

    # 1. Customer matching
    cust_match = engine.match_customer(
        ship2_name=header.ship2_name,
        ship2_add1=header.ship2_add1,
        ship2_city=header.ship2_city,
        ship2_state=header.ship2_state,
        ship2_zip=header.ship2_zip,
        buyer_email=header.buyer_email,
        source_system=source,
        po_no=header.po_no,
    )

    header.customer_id_p21 = cust_match.p21_customer_id
    header.customer_name_p21 = cust_match.p21_customer_name
    header.customer_match_score = cust_match.match_score
    header.customer_match_method = cust_match.match_method

    # Get customer detail for defaults
    cust_detail = engine.get_customer_detail(cust_match.p21_customer_id) if cust_match.p21_customer_id else {}

    # 2. Item matching per line
    item_scores = []
    cism_lines = []
    for line in lines:
        item_match = engine.match_item(
            supplier_part_id=line.supplier_part_id,
            item_description=line.item_description,
            unit_price=line.unit_price,
            uom=line.unit_of_measure,
            p21_customer_id=cust_match.p21_customer_id,
            source_system=source,
        )
        line.item_id_p21 = item_match.p21_inv_mast_uid or None
        line.crosswalk_match_score = item_match.match_score
        item_scores.append(item_match.match_score)

        cism_lines.append({
            "item_id": item_match.p21_inv_mast_uid or line.supplier_part_id,
            "qty_ordered": line.qty_ordered,
            "unit_of_measure": item_match.unit_of_measure or line.unit_of_measure or "EA",
            "unit_price": line.unit_price,
            "item_description": line.item_description or item_match.p21_item_desc,
            "product_group": item_match.product_group,
            "required_date": line.required_date or line.date_due,
            "supplier_part_id": line.supplier_part_id,
            "inv_mast_uid": item_match.p21_inv_mast_uid,
            "line_no": line.line_no,
        })

    # 3. Duplicate PO check
    dup = engine.check_duplicate_po(header.po_no, cust_match.p21_customer_id)

    # 4. Confidence scoring (4-dimension)
    ship_to_score = cust_match.match_score * 0.95 if cust_match.p21_customer_id else 0.0
    conf = score_customer_po(
        customer_score=cust_match.match_score,
        shipto_score=ship_to_score,
        item_scores=item_scores,
        is_duplicate=dup.is_duplicate,
    )

    # 5. Generate CISM SO files if customer matched
    cism_result = None
    if cust_match.p21_customer_id and conf.overall != "red":
        cism_result = generate_cism_so(
            p21_customer_id=cust_match.p21_customer_id,
            p21_customer_name=cust_match.p21_customer_name,
            po_no=header.po_no,
            order_date=header.order_date,
            requested_date=header.date_due if hasattr(header, "date_due") else "",
            ship2_name=header.ship2_name,
            ship2_add1=header.ship2_add1,
            ship2_add2=header.ship2_add2,
            ship2_city=header.ship2_city,
            ship2_state=header.ship2_state,
            ship2_zip=header.ship2_zip,
            ship2_country=header.ship2_country,
            contact_name=header.buyer or cust_match.p21_customer_name,
            taker=settings.cism_output_dir,  # TODO: configurable taker
            terms=cust_detail.get("terms_id", ""),
            delivery_instructions=header.comments or header.po_desc,
            approved="Y" if conf.overall == "green" else "N",
            class_1=cust_detail.get("class_1id", ""),
            source_id=source,
            lines=cism_lines,
            output_dir=settings.cism_so_output_dir,
        )

    # 6. Auto-learn from green matches
    if conf.overall == "green" and cust_match.p21_customer_id:
        learn_from_approval(
            p21_customer_id=cust_match.p21_customer_id,
            p21_customer_name=cust_match.p21_customer_name,
            source_system=source,
            ship2_name=header.ship2_name,
            ship2_add1=header.ship2_add1,
            ship2_city=header.ship2_city,
            ship2_state=header.ship2_state,
            ship2_zip=header.ship2_zip,
            po_no=header.po_no,
            lines=[{
                "supplier_part_id": cl["supplier_part_id"],
                "inv_mast_uid": cl["inv_mast_uid"],
                "unit_price": cl["unit_price"],
                "unit_of_measure": cl["unit_of_measure"],
                "item_description": cl["item_description"],
                "line_no": cl["line_no"],
            } for cl in cism_lines if cl.get("inv_mast_uid")],
            crosswalk_dir=settings.crosswalk_dir,
        )

    # Build payload for staging
    payload = POPayload(
        intake_id=intake_id,
        source=SourceSystem(source.upper()) if source.upper() in SourceSystem.__members__ else SourceSystem.DIRECT,
        format=fmt,
        received_at=datetime.utcnow().isoformat(),
        header=header,
        lines=lines,
        raw_content=raw_content[:8000] if isinstance(raw_content, str) else "",
        overall_confidence=conf.overall,
        review_required=conf.review_required,
        cism_blob_path=cism_result["header_path"] if cism_result else None,
    )

    try:
        log_intake(payload)
    except Exception:
        pass  # SQL not available on Render

    # Save to local file store (always works)
    result_data = {
        "status": "processed",
        "po_no": header.po_no,
        "intake_id": intake_id,
        "source": source,
        "confidence": conf.overall,
        "review_required": conf.review_required,
        "review_status": "pending" if conf.review_required else "approved",
        "reason": conf.reason,
        "customer_match": {
            "p21_id": cust_match.p21_customer_id,
            "name": cust_match.p21_customer_name,
            "score": cust_match.match_score,
            "method": cust_match.match_method,
            "candidates": cust_match.candidates,
        },
        "duplicate": {
            "is_duplicate": dup.is_duplicate,
            "existing_order": dup.existing_order_no,
        },
        "lines_count": len(lines),
        "item_scores": [round(s, 2) for s in item_scores],
        "cism": cism_result,
        "received_at": datetime.utcnow().isoformat(),
        "header": {
            "po_no": header.po_no,
            "ship2_name": header.ship2_name,
            "ship2_add1": header.ship2_add1,
            "ship2_city": header.ship2_city,
            "ship2_state": header.ship2_state,
            "ship2_zip": header.ship2_zip,
            "ship2_country": header.ship2_country,
            "buyer": header.buyer,
            "buyer_email": header.buyer_email,
            "comments": header.comments,
            "order_date": header.order_date,
            "supplier_name": header.supplier_name,
            "customer_id_p21": cust_match.p21_customer_id,
            "customer_name_p21": cust_match.p21_customer_name,
            "customer_match_score": cust_match.match_score,
            "customer_match_method": cust_match.match_method,
        },
        "lines": [{
            "line_no": cl["line_no"],
            "supplier_part_id": cl["supplier_part_id"],
            "item_description": cl["item_description"],
            "qty_ordered": cl["qty_ordered"],
            "unit_price": cl["unit_price"],
            "unit_of_measure": cl["unit_of_measure"],
            "item_id_p21": cl["inv_mast_uid"],
            "product_group": cl.get("product_group", ""),
            "crosswalk_match_score": item_scores[i] if i < len(item_scores) else 0,
        } for i, cl in enumerate(cism_lines)],
    }
    local_store.save_po(intake_id, result_data)

    logger.info(
        f"Processed PO→SO {header.po_no} | source={source} | "
        f"customer={cust_match.p21_customer_id}({cust_match.match_score:.2f},{cust_match.match_method}) | "
        f"confidence={conf.overall} | lines={len(lines)} | "
        f"duplicate={dup.is_duplicate}"
    )

    return {
        "status": "processed",
        "po_no": header.po_no,
        "intake_id": intake_id,
        "confidence": conf.overall,
        "review_required": conf.review_required,
        "reason": conf.reason,
        "customer_match": {
            "p21_id": cust_match.p21_customer_id,
            "name": cust_match.p21_customer_name,
            "score": cust_match.match_score,
            "method": cust_match.match_method,
            "candidates": cust_match.candidates,
        },
        "duplicate": {
            "is_duplicate": dup.is_duplicate,
            "existing_order": dup.existing_order_no,
        },
        "lines": len(lines),
        "item_scores": [round(s, 2) for s in item_scores],
        "cism": cism_result,
    }


# ── Review Portal API (local file store — no SQL needed) ─────────────────────

@app.get("/api/v1/review/queue")
async def review_queue(confidence: Optional[str] = None):
    """Get POs for review from local store."""
    all_pos = local_store.list_pos(status="pending", confidence=confidence)
    return [{
        "intake_id": po.get("intake_id"),
        "po_number": po.get("po_no"),
        "source": po.get("source"),
        "confidence": po.get("confidence"),
        "status": po.get("review_status"),
        "received": po.get("received_at"),
        "reason": po.get("reason"),
        "supplier": po.get("header", {}).get("supplier_name", ""),
        "ship_to": po.get("header", {}).get("ship2_name", ""),
        "customer_p21": po.get("customer_match", {}).get("name", ""),
        "customer_id_p21": po.get("customer_match", {}).get("p21_id", ""),
        "customer_score": po.get("customer_match", {}).get("score", 0),
        "lines_count": po.get("lines_count", 0),
        "item_scores": po.get("item_scores", []),
        "header": po.get("header", {}),
        "lines": po.get("lines", []),
    } for po in all_pos]


@app.get("/api/v1/review/all")
async def review_all():
    """Get all processed POs (any status)."""
    all_pos = local_store.list_pos()
    return [{
        "intake_id": po.get("intake_id"),
        "po_number": po.get("po_no"),
        "source": po.get("source"),
        "confidence": po.get("confidence"),
        "review_status": po.get("review_status"),
        "received": po.get("received_at"),
        "customer_p21": po.get("customer_match", {}).get("name", ""),
    } for po in all_pos]


class ApproveRequest(BaseModel):
    reviewer: str = "system"
    notes: str = ""

@app.post("/api/v1/review/po/{intake_id}/approve")
async def approve_po(intake_id: str, req: ApproveRequest):
    """Approve a PO — triggers learning loop and CISM generation."""
    po = local_store.get_po(intake_id)
    if not po:
        raise HTTPException(404, f"PO {intake_id} not found")
    if po.get("review_status") != "pending":
        raise HTTPException(400, f"PO {intake_id} already {po.get('review_status')}")

    local_store.update_po(intake_id, {
        "review_status": "approved",
        "reviewed_by": req.reviewer,
        "reviewer_notes": req.notes,
        "reviewed_at": datetime.utcnow().isoformat(),
    })

    # Learning loop
    cust = po.get("customer_match", {})
    hdr = po.get("header", {})
    if cust.get("p21_id"):
        try:
            learn_from_approval(
                p21_customer_id=cust["p21_id"],
                p21_customer_name=cust.get("name", ""),
                source_system=po.get("source", ""),
                ship2_name=hdr.get("ship2_name", ""),
                ship2_add1=hdr.get("ship2_add1", ""),
                ship2_city=hdr.get("ship2_city", ""),
                ship2_state=hdr.get("ship2_state", ""),
                ship2_zip=hdr.get("ship2_zip", ""),
                po_no=po.get("po_no", ""),
                lines=[{
                    "supplier_part_id": l.get("supplier_part_id", ""),
                    "inv_mast_uid": l.get("item_id_p21", ""),
                    "unit_price": l.get("unit_price", 0),
                    "unit_of_measure": l.get("unit_of_measure", "EA"),
                    "item_description": l.get("item_description", ""),
                    "line_no": l.get("line_no", 0),
                } for l in po.get("lines", []) if l.get("item_id_p21")],
                crosswalk_dir=settings.crosswalk_dir,
            )
        except Exception as e:
            logger.error(f"Learning loop error: {e}")

    return {"status": "approved", "intake_id": intake_id}


class RejectRequest(BaseModel):
    reviewer: str = "system"
    reason: str = ""

@app.post("/api/v1/review/po/{intake_id}/reject")
async def reject_po(intake_id: str, req: RejectRequest):
    """Reject a PO with reason."""
    po = local_store.get_po(intake_id)
    if not po:
        raise HTTPException(404, f"PO {intake_id} not found")

    local_store.update_po(intake_id, {
        "review_status": "rejected",
        "reviewed_by": req.reviewer,
        "reject_reason": req.reason,
        "reviewed_at": datetime.utcnow().isoformat(),
    })
    return {"status": "rejected", "intake_id": intake_id}


@app.get("/api/v1/review/po/{intake_id}")
async def get_po_detail(intake_id: str):
    """Get full PO detail."""
    po = local_store.get_po(intake_id)
    if not po:
        raise HTTPException(404, f"PO {intake_id} not found")
    return po


class EditPORequest(BaseModel):
    customer_id_p21: Optional[str] = None
    customer_name_p21: Optional[str] = None
    ship2_name: Optional[str] = None
    ship2_add1: Optional[str] = None
    ship2_city: Optional[str] = None
    ship2_state: Optional[str] = None
    ship2_zip: Optional[str] = None
    lines: Optional[list] = None  # [{line_no, item_id_p21, qty_ordered, unit_price, ...}]
    notes: Optional[str] = None

@app.post("/api/v1/review/po/{intake_id}/edit")
async def edit_po(intake_id: str, req: EditPORequest):
    """Edit a PO — update customer mapping, line items, ship-to, etc."""
    po = local_store.get_po(intake_id)
    if not po:
        raise HTTPException(404, f"PO {intake_id} not found")

    updates = {}
    header = po.get("header", {})
    cust_match = po.get("customer_match", {})

    if req.customer_id_p21 is not None:
        header["customer_id_p21"] = req.customer_id_p21
        header["customer_name_p21"] = req.customer_name_p21 or ""
        header["customer_match_method"] = "manual_edit"
        header["customer_match_score"] = 1.0
        cust_match["p21_id"] = req.customer_id_p21
        cust_match["name"] = req.customer_name_p21 or ""
        cust_match["score"] = 1.0
        cust_match["method"] = "manual_edit"
        updates["customer_match"] = cust_match

    if req.ship2_name is not None: header["ship2_name"] = req.ship2_name
    if req.ship2_add1 is not None: header["ship2_add1"] = req.ship2_add1
    if req.ship2_city is not None: header["ship2_city"] = req.ship2_city
    if req.ship2_state is not None: header["ship2_state"] = req.ship2_state
    if req.ship2_zip is not None: header["ship2_zip"] = req.ship2_zip
    updates["header"] = header

    if req.lines is not None:
        existing_lines = po.get("lines", [])
        for edit_line in req.lines:
            ln = edit_line.get("line_no")
            for el in existing_lines:
                if el.get("line_no") == ln:
                    if "item_id_p21" in edit_line: el["item_id_p21"] = edit_line["item_id_p21"]
                    if "qty_ordered" in edit_line: el["qty_ordered"] = edit_line["qty_ordered"]
                    if "unit_price" in edit_line: el["unit_price"] = edit_line["unit_price"]
                    if "unit_of_measure" in edit_line: el["unit_of_measure"] = edit_line["unit_of_measure"]
                    if "item_description" in edit_line: el["item_description"] = edit_line["item_description"]
                    el["crosswalk_match_score"] = 1.0  # manual = 100%
                    break
        updates["lines"] = existing_lines
        # Recalculate item scores
        updates["item_scores"] = [l.get("crosswalk_match_score", 0) for l in existing_lines]

    if req.notes is not None:
        updates["edit_notes"] = req.notes

    updates["edited_at"] = datetime.utcnow().isoformat()
    local_store.update_po(intake_id, updates)
    return {"status": "edited", "intake_id": intake_id}


# ── CISM Schema Reference ───────────────────────────────────────────────────

CISM_HEADER_SCHEMA = [
    {"field": "Import Set No", "type": "Alphanumeric", "required": True, "max_len": 8, "desc": "Unique batch ID linking header to lines"},
    {"field": "Customer ID", "type": "Decimal", "required": True, "max_len": 19, "desc": "P21 customer_id from crosswalk match"},
    {"field": "Customer Name", "type": "Alphanumeric", "required": True, "max_len": 50, "desc": "P21 customer name"},
    {"field": "Company ID", "type": "Alphanumeric", "required": True, "max_len": 8, "desc": "P21 company (default '1')"},
    {"field": "Sales Location ID", "type": "Decimal", "required": True, "max_len": 9, "desc": "P21 location (default 10)"},
    {"field": "Customer PO Number", "type": "Alphanumeric", "required": False, "max_len": 50, "desc": "Customer's PO# from Ariba/Coupa"},
    {"field": "Contact ID", "type": "Alphanumeric", "required": True, "max_len": 16, "desc": "P21 contact ID"},
    {"field": "Contact Name", "type": "Alphanumeric", "required": True, "max_len": 50, "desc": "Buyer/contact name from PO"},
    {"field": "Taker", "type": "Alphanumeric", "required": True, "max_len": 30, "desc": "Order taker / inside sales rep"},
    {"field": "Job Name", "type": "Alphanumeric", "required": False, "max_len": 40, "desc": "Job/project name"},
    {"field": "Order Date", "type": "DateTime", "required": False, "max_len": 8, "desc": "PO date (MM/DD/YYYY)"},
    {"field": "Requested Date", "type": "DateTime", "required": False, "max_len": 8, "desc": "Requested delivery date"},
    {"field": "Quote", "type": "Alphanumeric", "required": False, "max_len": 1, "desc": "Quote flag"},
    {"field": "Approved", "type": "Alphanumeric", "required": False, "max_len": 1, "desc": "Y=approved, N=pending"},
    {"field": "Ship To ID", "type": "Numeric", "required": True, "max_len": 9, "desc": "P21 ship-to address ID"},
    {"field": "Ship To Name", "type": "Alphanumeric", "required": True, "max_len": 50, "desc": "Ship-to company name"},
    {"field": "Ship To Address 1", "type": "Alphanumeric", "required": False, "max_len": 50, "desc": "Street address line 1"},
    {"field": "Ship To Address 2", "type": "Alphanumeric", "required": False, "max_len": 50, "desc": "Street address line 2"},
    {"field": "Ship To City", "type": "Alphanumeric", "required": False, "max_len": 50, "desc": "City"},
    {"field": "Ship To State", "type": "Alphanumeric", "required": False, "max_len": 50, "desc": "State"},
    {"field": "Ship To Zip Code", "type": "Alphanumeric", "required": False, "max_len": 10, "desc": "Zip code"},
    {"field": "Ship To Country", "type": "Alphanumeric", "required": False, "max_len": 50, "desc": "Country code"},
    {"field": "Packing Basis", "type": "Alphanumeric", "required": True, "max_len": 16, "desc": "Partial/Order, Item Complete, etc."},
    {"field": "Delivery Instructions", "type": "Alphanumeric", "required": False, "max_len": 255, "desc": "Shipping/delivery notes"},
    {"field": "Terms", "type": "Alphanumeric", "required": False, "max_len": 2, "desc": "Payment terms ID"},
    {"field": "Carrier ID", "type": "Numeric", "required": False, "max_len": 9, "desc": "P21 carrier ID"},
    {"field": "Will Call", "type": "Alphanumeric", "required": False, "max_len": 1, "desc": "Y/N will call pickup"},
    {"field": "Ship To Email Address", "type": "Alphanumeric", "required": False, "max_len": 255, "desc": "Email for ship-to contact"},
    {"field": "Promise Date", "type": "DateTime", "required": False, "max_len": 8, "desc": "Promised delivery date"},
    {"field": "Supplier Order No", "type": "Alphanumeric", "required": False, "max_len": 255, "desc": "Source system reference"},
]

CISM_LINE_SCHEMA = [
    {"field": "Import Set Number", "type": "Alphanumeric", "required": True, "max_len": 8, "desc": "Must match header Import Set No"},
    {"field": "Line No", "type": "Numeric", "required": True, "max_len": 9, "desc": "Sequential line number"},
    {"field": "Item ID", "type": "Alphanumeric", "required": True, "max_len": 40, "desc": "P21 item ID from crosswalk"},
    {"field": "Unit Quantity", "type": "Numeric", "required": True, "max_len": 10, "desc": "Quantity ordered"},
    {"field": "Unit of Measure", "type": "Alphanumeric", "required": True, "max_len": 8, "desc": "UOM (EA, CS, FT, etc.)"},
    {"field": "Unit Price", "type": "Decimal", "required": False, "max_len": "19,4", "desc": "Price per unit"},
    {"field": "Extended Description", "type": "Alphanumeric", "required": False, "max_len": 255, "desc": "Item description from PO"},
    {"field": "Source Location ID", "type": "Numeric", "required": False, "max_len": 9, "desc": "Sourcing warehouse"},
    {"field": "Ship Location ID", "type": "Numeric", "required": False, "max_len": 9, "desc": "Shipping warehouse"},
    {"field": "Product Group ID", "type": "Alphanumeric", "required": False, "max_len": 8, "desc": "P21 product group"},
    {"field": "Supplier ID", "type": "Numeric", "required": False, "max_len": 9, "desc": "Vendor/supplier ID"},
    {"field": "Required Date", "type": "DateTime", "required": False, "max_len": 8, "desc": "Line-level required date"},
    {"field": "Disposition", "type": "Alphanumeric", "required": False, "max_len": 1, "desc": "B=Backorder, D=Direct, S=Special, H=Hold"},
    {"field": "Manual Price Override", "type": "Alphanumeric", "required": False, "max_len": 1, "desc": "Y=use provided price"},
    {"field": "Capture Usage", "type": "Alphanumeric", "required": True, "max_len": 1, "desc": "Y/N capture usage tracking"},
    {"field": "Item Description", "type": "Alphanumeric", "required": False, "max_len": 40, "desc": "Short item description"},
]

@app.get("/api/v1/cism/schema")
async def cism_schema():
    """Return the P21 CISM Order/Quote Import schema."""
    return {"header": CISM_HEADER_SCHEMA, "line": CISM_LINE_SCHEMA}


# ── Quote Crosswalk (Dynamics) ──────────────────────────────────────────────

QUOTE_DATA_DIR = "./quote_data"

@app.post("/api/v1/crosswalk/upload-quotes")
async def upload_quote_data(file: UploadFile = File(...)):
    """Upload Dynamics quote JSON or CSV for crosswalk reference."""
    os.makedirs(QUOTE_DATA_DIR, exist_ok=True)
    content = await file.read()
    filename = file.filename or "quotes.json"
    dest = os.path.join(QUOTE_DATA_DIR, filename)
    with open(dest, "wb") as f:
        f.write(content)
    return {"status": "uploaded", "file": filename, "size": len(content)}


@app.get("/api/v1/crosswalk/quotes")
async def list_quotes(limit: int = 100):
    """List Dynamics quote data if available."""
    import json as jsonmod
    results = []
    if not os.path.exists(QUOTE_DATA_DIR):
        return results
    for fname in os.listdir(QUOTE_DATA_DIR):
        path = os.path.join(QUOTE_DATA_DIR, fname)
        try:
            if fname.endswith(".json"):
                with open(path) as f:
                    data = jsonmod.load(f)
                if isinstance(data, list):
                    for item in data[:limit]:
                        results.append(item)
                elif isinstance(data, dict) and "value" in data:
                    for item in data["value"][:limit]:
                        results.append(item)
            elif fname.endswith(".csv"):
                import csv as csvmod
                with open(path, encoding="utf-8-sig") as f:
                    for i, row in enumerate(csvmod.DictReader(f)):
                        if i >= limit: break
                        results.append(row)
        except Exception as e:
            logger.error(f"Error reading quote file {fname}: {e}")
    return results


class VendorMappingRequest(BaseModel):
    source: str
    source_vendor_id: str
    source_vendor_name: str
    p21_vendor_id: str
    p21_vendor_name: str

@app.post("/api/v1/review/crosswalk/vendor")
async def add_vendor_mapping(req: VendorMappingRequest):
    """Add or update a vendor crosswalk mapping from review portal."""
    save_vendor_mapping(
        req.source, req.source_vendor_id, req.source_vendor_name,
        req.p21_vendor_id, req.p21_vendor_name, "manual",
    )
    return {"status": "saved", "mapping": req.dict()}


class ItemMappingRequest(BaseModel):
    source: str
    source_item_id: str
    source_item_desc: str
    p21_item_id: str
    p21_item_desc: str
    p21_vendor_id: Optional[str] = None

@app.post("/api/v1/review/crosswalk/item")
async def add_item_mapping(req: ItemMappingRequest):
    """Add or update an item crosswalk mapping from review portal."""
    save_item_mapping(
        req.source, req.source_item_id, req.source_item_desc,
        req.p21_item_id, req.p21_item_desc, req.p21_vendor_id, "manual",
    )
    return {"status": "saved", "mapping": req.dict()}


# ── Customer Item Lookup (for Edit PO dropdowns) ────────────────────────────

@app.get("/api/v1/lookup/customer-items/{customer_id}")
async def lookup_customer_items(customer_id: str, q: str = "", limit: int = 50):
    """
    Get items this customer has ordered before, sorted by frequency.
    Used to populate the Edit PO part number dropdown.
    Optional q param filters by part# or description.
    """
    engine = _get_customer_engine()
    items = engine.customer_items.get(customer_id, [])

    if q:
        q_upper = q.upper()
        items = [i for i in items if
                 q_upper in (i.get("customer_part_number", "").upper()) or
                 q_upper in (i.get("p21_item_desc", "").upper())]

    # Sort by seen_count descending
    items.sort(key=lambda x: int(x.get("seen_count", 0)), reverse=True)

    return [{
        "customer_part_number": i.get("customer_part_number"),
        "p21_inv_mast_uid": i.get("p21_inv_mast_uid"),
        "p21_item_desc": i.get("p21_item_desc"),
        "unit_of_measure": i.get("unit_of_measure"),
        "unit_price_avg": i.get("unit_price_avg"),
        "unit_price_last": i.get("unit_price_last"),
        "seen_count": i.get("seen_count"),
    } for i in items[:limit]]


@app.get("/api/v1/lookup/customers")
async def lookup_customers(q: str = "", limit: int = 20):
    """Search customers by name for Edit PO customer dropdown."""
    engine = _get_customer_engine()
    if not q:
        return engine.customer_xw[:limit]

    q_upper = q.upper()
    matches = [r for r in engine.customer_xw if
               q_upper in (r.get("p21_customer_name", "").upper()) or
               q_upper in (r.get("p21_customer_id", ""))]
    return [{
        "p21_customer_id": r.get("p21_customer_id"),
        "p21_customer_name": r.get("p21_customer_name"),
        "ship2_name": r.get("ship2_name"),
        "ship2_zip": r.get("ship2_zip"),
    } for r in matches[:limit]]


# ── Quote Export ──────────────────────────────────────────────────────────────

class QuoteExportRequest(BaseModel):
    days_back: int = 365

@app.post("/api/v1/quotes/export")
async def export_quotes(req: QuoteExportRequest):
    """Trigger Dynamics quote export to blob storage."""
    try:
        result = export_quotes_to_blob(req.days_back)
        return {
            "status": "success",
            "timestamp": result.get("timestamp"),
            "quotes_count": result.get("quotes_count"),
            "lines_count": result.get("lines_count"),
            "uploads": result.get("uploads", {}),
        }
    except Exception as e:
        logger.error(f"Quote export failed: {e}")
        raise HTTPException(500, f"Export failed: {str(e)}")


@app.get("/api/v1/quotes/status")
async def quotes_status():
    """Get status of latest quote export."""
    try:
        from services.processing.blob_uploader import get_uploader
        uploader = get_uploader()
        
        if not uploader.is_configured():
            return {"status": "not_configured", "blob": None}
        
        # Try to get latest quote file metadata
        try:
            blob_client = uploader.client.get_blob_client(
                container=uploader.BLOB_CONTAINER_NAME,
                blob="crosswalk/quotes/quotes_dynamics_latest.csv"
            )
            props = blob_client.get_blob_properties()
            return {
                "status": "available",
                "last_modified": props.last_modified.isoformat() if props.last_modified else None,
                "size_bytes": props.size,
                "blob_path": "crosswalk/quotes/quotes_dynamics_latest.csv"
            }
        except Exception:
            return {"status": "no_data", "blob": None}
    except Exception as e:
        return {"status": "error", "error": str(e)}


# ── Crosswalk Build ─────────────────────────────────────────────────────────

P21_DATA_DIR = "./p21_data"

@app.post("/api/v1/crosswalk/upload")
async def upload_p21_csv(
    file: UploadFile = File(...),
    file_type: str = Form(...),  # "headers", "lines", or "customers"
):
    """Upload a P21 CSV export. file_type must be 'headers', 'lines', or 'customers'."""
    if file_type not in ("headers", "lines", "customers"):
        raise HTTPException(400, "file_type must be 'headers', 'lines', or 'customers'")

    os.makedirs(P21_DATA_DIR, exist_ok=True)
    dest = os.path.join(P21_DATA_DIR, f"p21_{file_type}.csv")
    content = await file.read()
    with open(dest, "wb") as f:
        f.write(content)

    size = len(content)
    logger.info(f"Uploaded P21 {file_type} CSV: {file.filename} ({size} bytes) -> {dest}")
    return {"status": "uploaded", "file_type": file_type, "size": size, "path": dest}


_build_status = {"state": "idle", "result": None}

@app.post("/api/v1/crosswalk/build")
async def build_crosswalks(background_tasks=None):
    """Build crosswalk CSVs from uploaded P21 exports. Runs in background."""
    import threading

    headers_path = os.path.join(P21_DATA_DIR, "p21_headers.csv")
    lines_path = os.path.join(P21_DATA_DIR, "p21_lines.csv")
    customers_path = os.path.join(P21_DATA_DIR, "p21_customers.csv")

    missing = []
    if not os.path.exists(headers_path): missing.append("headers")
    if not os.path.exists(lines_path): missing.append("lines")
    if not os.path.exists(customers_path): missing.append("customers")
    if missing:
        raise HTTPException(400, f"Missing P21 uploads: {', '.join(missing)}. Upload via POST /api/v1/crosswalk/upload first.")

    if _build_status["state"] == "building":
        return {"status": "already_building"}

    def _do_build():
        global _customer_engine
        _build_status["state"] = "building"
        _build_status["result"] = None
        try:
            from services.processing.crosswalk_csv_builder import build_all
            build_all(
                headers_path=headers_path,
                lines_path=lines_path,
                customers_path=customers_path,
                output_dir=settings.crosswalk_dir,
            )
            _customer_engine = None

            import glob as g
            csv_files = g.glob(os.path.join(settings.crosswalk_dir, "*.csv"))
            counts = {}
            for f in csv_files:
                name = os.path.basename(f)
                with open(f) as fh:
                    counts[name] = sum(1 for _ in fh) - 1
            _build_status["result"] = {"status": "success", "files": counts}
            _build_status["state"] = "done"
            logger.info(f"Crosswalk build complete: {counts}")
        except Exception as e:
            _build_status["result"] = {"status": "error", "error": str(e)}
            _build_status["state"] = "error"
            logger.error(f"Crosswalk build failed: {e}")

    threading.Thread(target=_do_build, daemon=True).start()
    return {"status": "building", "message": "Build started in background. Poll GET /api/v1/crosswalk/build/status"}


@app.get("/api/v1/crosswalk/build/status")
async def build_status():
    """Check crosswalk build progress."""
    return {"state": _build_status["state"], "result": _build_status["result"]}


@app.get("/api/v1/crosswalk/customers")
async def list_customer_crosswalk(limit: int = 100):
    """List customer crosswalk entries."""
    engine = _get_customer_engine()
    rows = engine.customer_xw[:limit]
    return [{
        "p21_customer_id": r.get("p21_customer_id"),
        "p21_customer_name": r.get("p21_customer_name"),
        "ship2_name": r.get("ship2_name"),
        "ship2_zip": r.get("ship2_zip"),
        "source_system": r.get("source_system"),
        "match_method": r.get("match_method"),
        "seen_count": r.get("seen_count"),
    } for r in rows]


# ── P21 Sales Order Pull ────────────────────────────────────────────────────

class SOExportRequest(BaseModel):
    days_back: int = 90
    include_customers: bool = True
    include_ship_tos: bool = True
    include_items: bool = True

@app.post("/api/v1/so/export")
async def export_sales_orders(req: SOExportRequest):
    """Pull Sales Orders from P21 and export to blob storage."""
    try:
        result = export_so_data(
            days_back=req.days_back,
            include_customers=req.include_customers,
            include_ship_tos=req.include_ship_tos,
            include_items=req.include_items,
        )
        return {
            "status": "success",
            "timestamp": result.get("timestamp"),
            "so_headers_count": result.get("so_headers_count"),
            "so_lines_count": result.get("so_lines_count"),
            "customers_count": result.get("customers_count"),
            "ship_tos_count": result.get("ship_tos_count"),
            "items_count": result.get("items_count"),
            "uploads": result.get("uploads", {}),
        }
    except Exception as e:
        logger.error(f"SO export failed: {e}")
        raise HTTPException(500, f"SO export failed: {str(e)}")


@app.get("/api/v1/so/status")
async def so_export_status():
    """Get status of latest SO export in blob storage."""
    try:
        from services.processing.blob_uploader import get_uploader
        uploader = get_uploader()

        if not uploader.is_configured():
            return {"status": "not_configured", "blob": None}

        files = {}
        for name in ["so_headers", "so_lines", "customers", "ship_tos", "items"]:
            blob_name = f"crosswalk/p21/{name}_latest.csv"
            try:
                blob_client = uploader.client.get_blob_client(
                    container=uploader.BLOB_CONTAINER_NAME, blob=blob_name,
                )
                props = blob_client.get_blob_properties()
                files[name] = {
                    "last_modified": props.last_modified.isoformat() if props.last_modified else None,
                    "size_bytes": props.size,
                }
            except Exception:
                files[name] = None

        has_data = any(v is not None for v in files.values())
        return {"status": "available" if has_data else "no_data", "files": files}
    except Exception as e:
        return {"status": "error", "error": str(e)}


# ── Stats (local file store) ─────────────────────────────────────────────────

@app.get("/api/v1/stats")
async def stats():
    """Dashboard statistics from local store + crosswalk CSVs."""
    s = local_store.get_stats()

    # Crosswalk counts from CSV files
    engine = _get_customer_engine()
    cust_count = len(engine.customer_xw)
    item_count = sum(len(v) for v in engine.customer_items.values())

    return {
        "total": s["total"],
        "green": s["green"],
        "yellow": s["yellow"],
        "red": s["red"],
        "approved": s["approved"],
        "rejected": s["rejected"],
        "pending": s["pending"],
        "crosswalk": {
            "customers": cust_count,
            "items": item_count,
        }
    }


# ── Crosswalk APIs (CSV-based) ──────────────────────────────────────────────

@app.get("/api/v1/crosswalk/customer-items")
async def list_customer_items(customer_id: str = "", limit: int = 100):
    """List customer-item crosswalk entries."""
    engine = _get_customer_engine()
    if customer_id:
        rows = engine.customer_items.get(customer_id, [])[:limit]
    else:
        rows = []
        for cid_rows in engine.customer_items.values():
            rows.extend(cid_rows)
            if len(rows) >= limit:
                break
        rows = rows[:limit]
    return [{
        "p21_customer_id": r.get("p21_customer_id"),
        "customer_part_number": r.get("customer_part_number"),
        "p21_inv_mast_uid": r.get("p21_inv_mast_uid"),
        "p21_item_desc": r.get("p21_item_desc"),
        "unit_of_measure": r.get("unit_of_measure"),
        "product_group_id": r.get("product_group_id"),
        "unit_price_avg": r.get("unit_price_avg"),
        "seen_count": r.get("seen_count"),
    } for r in rows]


@app.get("/api/v1/crosswalk/po-history")
async def list_po_history(customer_id: str = "", limit: int = 100):
    """List PO-to-SO history."""
    engine = _get_customer_engine()
    if customer_id:
        rows = engine.po_history.get("", [])  # indexed by po_no, not customer
        rows = [r for r in rows if r.get("p21_customer_id") == customer_id][:limit]
    else:
        rows = []
        for po_rows in engine.po_history.values():
            rows.extend(po_rows)
            if len(rows) >= limit:
                break
        rows = rows[:limit]
    return [{
        "p21_customer_id": r.get("p21_customer_id"),
        "customer_po_no": r.get("customer_po_no"),
        "p21_order_no": r.get("p21_order_no"),
        "order_date": r.get("order_date"),
        "completed": r.get("completed"),
        "ship2_name": r.get("ship2_name"),
    } for r in rows]


@app.get("/api/v1/crosswalk/items")
async def list_item_master(limit: int = 100):
    """List item master index."""
    engine = _get_customer_engine()
    rows = list(engine.item_master.values())[:limit]
    return [{
        "p21_inv_mast_uid": r.get("p21_inv_mast_uid"),
        "p21_item_desc": r.get("p21_item_desc"),
        "default_selling_unit": r.get("default_selling_unit"),
        "product_group": r.get("product_group"),
        "default_supplier_id": r.get("default_supplier_id"),
    } for r in rows]
