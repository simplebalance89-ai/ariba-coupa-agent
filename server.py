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
from services.processing.so_exporter import export_so_data
from services.processing.customer_crosswalk_engine import CustomerCrosswalkEngine
from services.processing.confidence_scorer import score_customer_po
from services.processing.cism_so_generator import generate_cism_so
from services.processing.crosswalk_learner import learn_from_approval

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
        raise HTTPException(400, "Unsupported file type. Upload .xml or .pdf")

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

    log_intake(payload)

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


# ── Review Portal API ────────────────────────────────────────────────────────

@app.get("/api/v1/review/queue")
async def review_queue(confidence: Optional[str] = None):
    """Get pending POs for review, optionally filtered by confidence."""
    try:
        from services.processing.crosswalk_engine import get_staging_conn
        import json
        conn = get_staging_conn()
        cur = conn.cursor()

        query = """
            SELECT id, intake_id, po_number, source_system, vendor_id_raw,
                   vendor_id_p21, overall_confidence, review_status, received_at,
                   raw_payload
            FROM dbo.po_staging_log
            WHERE review_status = 'pending'
        """
        params = []
        if confidence:
            query += " AND overall_confidence = ?"
            params.append(confidence)
        query += " ORDER BY received_at DESC"

        cur.execute(query, params)
        rows = cur.fetchall()
        conn.close()

        results = []
        for r in rows:
            # Parse raw_payload to get full PO details
            payload_data = {}
            try:
                if r.raw_payload:
                    payload_data = json.loads(r.raw_payload)
            except:
                pass
            
            header = payload_data.get('header', {})
            lines = payload_data.get('lines', [])
            
            # Calculate total from lines
            total = sum(line.get('extended_price', 0) or 0 for line in lines)
            
            results.append({
                "id": r.id,
                "intake_id": r.intake_id,
                "po_number": r.po_number,
                "source": r.source_system,
                "vendor_raw": r.vendor_id_raw,
                "vendor_p21": r.vendor_id_p21,
                "confidence": r.overall_confidence,
                "status": r.review_status,
                "received": str(r.received_at) if r.received_at else None,
                # Additional fields from payload
                "supplier": header.get('supplier_name', ''),
                "ship_to": header.get('ship2_name', ''),
                "total": total,
                "lines_count": len(lines),
                "raw_payload": payload_data,
            })
        return results
    except Exception as e:
        logger.error(f"Review queue error: {e}")
        return []


class ApproveRequest(BaseModel):
    reviewer: str = "system"
    notes: str = ""

@app.post("/api/v1/review/po/{intake_id}/approve")
async def approve_po(intake_id: str, req: ApproveRequest):
    """Approve a PO — marks as approved and uploads CISM to blob storage."""
    try:
        from services.processing.crosswalk_engine import get_staging_conn
        
        # Get PO details first
        conn = get_staging_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT po_number, cism_blob_path 
            FROM dbo.po_staging_log 
            WHERE intake_id = ?
        """, (intake_id,))
        row = cur.fetchone()
        
        if not row:
            conn.close()
            raise HTTPException(404, f"PO {intake_id} not found")
        
        po_number = row.po_number
        cism_path = row.cism_blob_path
        
        # Update status
        cur.execute("""
            UPDATE dbo.po_staging_log
            SET review_status = 'approved', reviewed_by = ?, reviewed_at = GETUTCDATE(),
                reviewer_notes = ?, p21_import_status = 'pending_blob_upload'
            WHERE intake_id = ? AND review_status = 'pending'
        """, (req.reviewer, req.notes, intake_id))
        conn.commit()
        affected = cur.rowcount
        conn.close()

        if affected == 0:
            raise HTTPException(400, f"PO {intake_id} already reviewed")

        # Upload CISM to blob storage if file exists
        blob_result = {"success": False, "error": "No CISM file"}
        if cism_path and os.path.exists(cism_path):
            blob_result = upload_approved_cism(cism_path, po_number, intake_id)
            
            # Update blob status in DB
            if blob_result["success"]:
                conn = get_staging_conn()
                cur = conn.cursor()
                cur.execute("""
                    UPDATE dbo.po_staging_log
                    SET p21_import_status = 'uploaded_to_blob',
                        blob_url = ?
                    WHERE intake_id = ?
                """, (blob_result.get("blob_url"), intake_id))
                conn.commit()
                conn.close()

        return {
            "status": "approved", 
            "intake_id": intake_id,
            "blob_upload": blob_result,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


class RejectRequest(BaseModel):
    reviewer: str = "system"
    reason: str = ""

@app.post("/api/v1/review/po/{intake_id}/reject")
async def reject_po(intake_id: str, req: RejectRequest):
    """Reject a PO with reason."""
    try:
        from services.processing.crosswalk_engine import get_staging_conn
        conn = get_staging_conn()
        cur = conn.cursor()
        cur.execute("""
            UPDATE dbo.po_staging_log
            SET review_status = 'rejected', reviewed_by = ?, reviewed_at = GETUTCDATE(),
                reviewer_notes = ?
            WHERE intake_id = ? AND review_status = 'pending'
        """, (req.reviewer, req.reason, intake_id))
        conn.commit()
        conn.close()
        return {"status": "rejected", "intake_id": intake_id}
    except Exception as e:
        raise HTTPException(500, str(e))


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

class CrosswalkBuildRequest(BaseModel):
    headers_csv: str = ""
    lines_csv: str = ""
    customers_csv: str = ""

@app.post("/api/v1/crosswalk/build")
async def build_crosswalks(req: CrosswalkBuildRequest):
    """Build crosswalk CSVs from P21 SO exports."""
    try:
        from services.processing.crosswalk_csv_builder import build_all
        build_all(
            headers_path=req.headers_csv,
            lines_path=req.lines_csv,
            customers_path=req.customers_csv,
            output_dir=settings.crosswalk_dir,
        )
        # Reload the engine
        global _customer_engine
        _customer_engine = None
        return {"status": "success", "crosswalk_dir": settings.crosswalk_dir}
    except Exception as e:
        raise HTTPException(500, str(e))


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


# ── Stats ─────────────────────────────────────────────────────────────────────

@app.get("/api/v1/stats")
async def stats():
    """Dashboard statistics."""
    try:
        from services.processing.crosswalk_engine import get_staging_conn
        conn = get_staging_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN overall_confidence = 'green' THEN 1 ELSE 0 END) as green,
                SUM(CASE WHEN overall_confidence = 'yellow' THEN 1 ELSE 0 END) as yellow,
                SUM(CASE WHEN overall_confidence = 'red' THEN 1 ELSE 0 END) as red,
                SUM(CASE WHEN review_status = 'approved' THEN 1 ELSE 0 END) as approved,
                SUM(CASE WHEN review_status = 'rejected' THEN 1 ELSE 0 END) as rejected,
                SUM(CASE WHEN review_status = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN p21_import_status = 'imported' THEN 1 ELSE 0 END) as imported
            FROM dbo.po_staging_log
        """)
        row = cur.fetchone()
        
        # Get crosswalk stats
        cur.execute("SELECT COUNT(*) as vendor_count FROM dbo.vendor_crosswalk WHERE is_active = 1")
        vendor_row = cur.fetchone()
        
        cur.execute("SELECT COUNT(*) as item_count FROM dbo.item_crosswalk WHERE is_active = 1")
        item_row = cur.fetchone()
        
        conn.close()
        return {
            "total": row.total,
            "green": row.green,
            "yellow": row.yellow,
            "red": row.red,
            "approved": row.approved,
            "rejected": row.rejected,
            "pending": row.pending,
            "imported": row.imported,
            "crosswalk": {
                "vendors": vendor_row.vendor_count if vendor_row else 0,
                "items": item_row.item_count if item_row else 0,
            }
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/v1/crosswalk/vendors")
async def list_vendor_mappings(limit: int = 100):
    """List vendor crosswalk mappings."""
    try:
        from services.processing.crosswalk_engine import get_staging_conn
        conn = get_staging_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT source_system, source_vendor_id, source_vendor_name, 
                   p21_vendor_id, p21_vendor_name, match_score, seen_count
            FROM dbo.vendor_crosswalk
            WHERE is_active = 1
            ORDER BY seen_count DESC, match_score DESC
        """ + (f" TOP {limit}" if limit else ""))
        rows = cur.fetchall()
        conn.close()
        return [{
            "source_system": r.source_system,
            "source_vendor_id": r.source_vendor_id,
            "source_vendor_name": r.source_vendor_name,
            "p21_vendor_id": r.p21_vendor_id,
            "p21_vendor_name": r.p21_vendor_name,
            "match_score": r.match_score,
            "seen_count": r.seen_count,
        } for r in rows]
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/v1/crosswalk/items")
async def list_item_mappings(limit: int = 100):
    """List item crosswalk mappings."""
    try:
        from services.processing.crosswalk_engine import get_staging_conn
        conn = get_staging_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT source_system, source_item_id, source_item_name,
                   p21_item_id, p21_item_name, vendor_id, match_score
            FROM dbo.item_crosswalk
            WHERE is_active = 1
            ORDER BY match_score DESC
        """ + (f" TOP {limit}" if limit else ""))
        rows = cur.fetchall()
        conn.close()
        return [{
            "source_system": r.source_system,
            "source_item_id": r.source_item_id,
            "source_item_name": r.source_item_name,
            "p21_item_id": r.p21_item_id,
            "p21_item_name": r.p21_item_name,
            "vendor_id": r.vendor_id,
            "match_score": r.match_score,
        } for r in rows]
    except Exception as e:
        return {"error": str(e)}
