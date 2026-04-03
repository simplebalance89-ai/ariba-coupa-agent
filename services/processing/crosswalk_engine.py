"""
crosswalk_engine.py — Vendor and item crosswalk lookups.
Exact match first, SOUNDEX fuzzy fallback.
Reads from Azure SQL staging DB (dbo.vendor_crosswalk, dbo.item_crosswalk).
"""

import logging
from typing import Optional
from config import get_settings

try:
    import pyodbc
except ImportError:
    pyodbc = None

logger = logging.getLogger(__name__)


def get_staging_conn():
    """Get connection to Azure SQL staging database."""
    if pyodbc is None:
        raise RuntimeError("pyodbc not installed — Azure SQL not available in this environment")
    s = get_settings()
    return pyodbc.connect(
        f"DRIVER={s.staging_sql_driver};"
        f"SERVER={s.staging_sql_server};"
        f"DATABASE={s.staging_sql_database};"
        f"Authentication=ActiveDirectoryMsi;"
        f"Encrypt=yes;",
        timeout=30,
    )


def crosswalk_vendor(
    vendor_id_raw: str,
    vendor_name: str,
    source: str,
) -> tuple[Optional[str], float]:
    """
    Look up P21 vendor ID from source vendor identifier.
    Returns (p21_vendor_id, match_score). Score: 1.0=exact, <1.0=fuzzy, 0.0=no match.
    """
    conn = get_staging_conn()
    cur = conn.cursor()

    # 1. Exact match on vendor ID
    cur.execute("""
        SELECT p21_vendor_id, match_score
        FROM dbo.vendor_crosswalk
        WHERE source_system = ? AND source_vendor_id = ? AND is_active = 1
        ORDER BY match_score DESC, seen_count DESC
    """, (source, vendor_id_raw))
    row = cur.fetchone()
    if row:
        # Update last_seen and seen_count
        cur.execute("""
            UPDATE dbo.vendor_crosswalk
            SET last_seen = GETUTCDATE(), seen_count = seen_count + 1
            WHERE source_system = ? AND source_vendor_id = ?
        """, (source, vendor_id_raw))
        conn.commit()
        conn.close()
        return row.p21_vendor_id, row.match_score

    # 2. Exact match on vendor name
    if vendor_name:
        cur.execute("""
            SELECT p21_vendor_id, match_score
            FROM dbo.vendor_crosswalk
            WHERE source_system = ? AND source_vendor_name = ? AND is_active = 1
            ORDER BY match_score DESC
        """, (source, vendor_name))
        row = cur.fetchone()
        if row:
            conn.close()
            return row.p21_vendor_id, row.match_score * 0.9

    # 3. SOUNDEX fuzzy fallback on vendor name
    if vendor_name:
        cur.execute("""
            SELECT p21_vendor_id, match_score, source_vendor_name
            FROM dbo.vendor_crosswalk
            WHERE source_system = ? AND SOUNDEX(source_vendor_name) = SOUNDEX(?) AND is_active = 1
            ORDER BY match_score DESC
        """, (source, vendor_name))
        row = cur.fetchone()
        if row:
            logger.info(f"Fuzzy vendor match: '{vendor_name}' -> '{row.source_vendor_name}' -> P21:{row.p21_vendor_id}")
            conn.close()
            return row.p21_vendor_id, row.match_score * 0.7

    conn.close()
    return None, 0.0


def crosswalk_item(
    item_id_raw: str,
    source: str,
    vendor_id_p21: Optional[str] = None,
) -> tuple[Optional[str], float]:
    """
    Look up P21 item ID from source item identifier.
    Prioritizes vendor-specific mappings over generic ones.
    """
    conn = get_staging_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT p21_item_id, match_score
        FROM dbo.item_crosswalk
        WHERE source_system = ? AND source_item_id = ? AND is_active = 1
          AND (p21_vendor_id = ? OR p21_vendor_id IS NULL)
        ORDER BY
            CASE WHEN p21_vendor_id = ? THEN 0 ELSE 1 END,
            match_score DESC
    """, (source, item_id_raw, vendor_id_p21, vendor_id_p21))

    row = cur.fetchone()
    if row:
        # Update last_seen
        cur.execute("""
            UPDATE dbo.item_crosswalk
            SET last_seen = GETUTCDATE(), seen_count = seen_count + 1
            WHERE source_system = ? AND source_item_id = ?
              AND (p21_vendor_id = ? OR p21_vendor_id IS NULL)
        """, (source, item_id_raw, vendor_id_p21))
        conn.commit()
        conn.close()
        return row.p21_item_id, row.match_score

    conn.close()
    return None, 0.0


def save_vendor_mapping(
    source: str,
    source_vendor_id: str,
    source_vendor_name: str,
    p21_vendor_id: str,
    p21_vendor_name: str,
    match_method: str = "manual",
) -> None:
    """Save a new vendor crosswalk mapping (from OMS portal approval)."""
    conn = get_staging_conn()
    cur = conn.cursor()
    cur.execute("""
        MERGE dbo.vendor_crosswalk AS tgt
        USING (SELECT ? AS ss, ? AS svi) AS src
            ON tgt.source_system = src.ss AND tgt.source_vendor_id = src.svi
        WHEN MATCHED THEN
            UPDATE SET p21_vendor_id = ?, p21_vendor_name = ?,
                       match_score = 1.0, match_method = ?,
                       last_seen = GETUTCDATE(), seen_count = seen_count + 1
        WHEN NOT MATCHED THEN
            INSERT (source_system, source_vendor_id, source_vendor_name,
                    p21_vendor_id, p21_vendor_name, match_score, match_method)
            VALUES (?, ?, ?, ?, ?, 1.0, ?);
    """, (
        source, source_vendor_id,
        p21_vendor_id, p21_vendor_name, match_method,
        source, source_vendor_id, source_vendor_name,
        p21_vendor_id, p21_vendor_name, match_method,
    ))
    conn.commit()
    conn.close()


def save_item_mapping(
    source: str,
    source_item_id: str,
    source_item_desc: str,
    p21_item_id: str,
    p21_item_desc: str,
    p21_vendor_id: Optional[str] = None,
    match_method: str = "manual",
) -> None:
    """Save a new item crosswalk mapping (from OMS portal approval)."""
    conn = get_staging_conn()
    cur = conn.cursor()
    cur.execute("""
        MERGE dbo.item_crosswalk AS tgt
        USING (SELECT ? AS ss, ? AS sii, ? AS pvi) AS src
            ON tgt.source_system = src.ss AND tgt.source_item_id = src.sii
               AND ISNULL(tgt.p21_vendor_id, '') = ISNULL(src.pvi, '')
        WHEN MATCHED THEN
            UPDATE SET p21_item_id = ?, p21_item_desc = ?,
                       match_score = 1.0, match_method = ?,
                       last_seen = GETUTCDATE(), seen_count = seen_count + 1
        WHEN NOT MATCHED THEN
            INSERT (source_system, source_item_id, source_item_desc,
                    p21_item_id, p21_item_desc, p21_vendor_id, match_score, match_method)
            VALUES (?, ?, ?, ?, ?, ?, 1.0, ?);
    """, (
        source, source_item_id, p21_vendor_id,
        p21_item_id, p21_item_desc, match_method,
        source, source_item_id, source_item_desc,
        p21_item_id, p21_item_desc, p21_vendor_id, match_method,
    ))
    conn.commit()
    conn.close()
