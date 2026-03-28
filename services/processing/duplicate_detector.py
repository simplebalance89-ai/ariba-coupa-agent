"""
duplicate_detector.py — Detect duplicate POs before processing.
Uses intake_id (SHA-256 hash) and PO number + source combo.
"""

import hashlib
import logging
import pyodbc
from services.processing.crosswalk_engine import get_staging_conn

logger = logging.getLogger(__name__)


def generate_intake_id(po_number: str, vendor_id: str, source: str) -> str:
    """Generate dedup key: SHA-256 of PO# + vendor + source, truncated to 16 hex chars."""
    key = f"{po_number}-{vendor_id}-{source}"
    return hashlib.sha256(key.encode()).hexdigest()[:16].upper()


def is_duplicate(intake_id: str, po_number: str, source: str) -> bool:
    """Check staging DB for same intake_id or same PO# from same source in last 30 days."""
    try:
        conn = get_staging_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(1)
            FROM dbo.po_staging_log
            WHERE (intake_id = ? OR (po_number = ? AND source_system = ?))
              AND created_at > DATEADD(DAY, -30, GETUTCDATE())
        """, (intake_id, po_number, source))
        count = cur.fetchone()[0]
        conn.close()
        return count > 0
    except Exception as e:
        logger.warning(f"Duplicate check failed (continuing): {e}")
        return False


def log_intake(payload) -> None:
    """Log a processed PO to staging for audit + dedup."""
    try:
        conn = get_staging_conn()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO dbo.po_staging_log
                (intake_id, po_number, source_system, vendor_id_raw, vendor_id_p21,
                 overall_confidence, review_required, received_at, cism_blob_path)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            payload.intake_id,
            payload.header.po_no,
            payload.source.value,
            payload.header.vendor_id_raw,
            payload.header.vendor_id_p21,
            payload.overall_confidence,
            int(payload.review_required),
            payload.received_at,
            payload.cism_blob_path,
        ))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to log intake: {e}")
