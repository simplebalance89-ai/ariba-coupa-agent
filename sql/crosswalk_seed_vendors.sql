-- ============================================================
-- P21 Vendor Crosswalk Seed
-- Run on: P21 SQL Server (Prophet 21 database)
-- Export as CSV, then bulk import to Azure SQL dbo.vendor_crosswalk
-- EnPro Industries | 2026-03-26
-- ============================================================

-- All vendors with PO activity in last 3 years
-- Seeds the crosswalk with P21 vendor IDs as both source and target
-- (baseline — Ariba/Coupa-specific mappings added via OMS portal)

SELECT DISTINCT
    'P21'                               AS source_system,
    CAST(v.vendor_id AS VARCHAR(100))   AS source_vendor_id,
    v.vendor_name                       AS source_vendor_name,
    CAST(v.vendor_id AS VARCHAR(50))    AS p21_vendor_id,
    v.vendor_name                       AS p21_vendor_name,
    1.0                                 AS match_score,
    'exact'                             AS match_method,
    1                                   AS is_active,
    GETUTCDATE()                        AS created_at,
    MAX(h.po_date)                      AS last_seen,
    COUNT(DISTINCT h.po_no)             AS seen_count
FROM dbo.po_hdr h
    INNER JOIN dbo.vendor v ON h.vendor_id = v.vendor_id
WHERE h.po_date >= DATEADD(YEAR, -3, GETDATE())
  AND h.delete_flag = 'N'
  AND v.delete_flag = 'N'
GROUP BY v.vendor_id, v.vendor_name
ORDER BY seen_count DESC;

-- ============================================================
-- If Ariba vendor codes exist in P21 (check user_def fields):
-- ============================================================
/*
SELECT DISTINCT
    'ariba'                             AS source_system,
    v.user_def_1                        AS source_vendor_id,
    v.vendor_name                       AS source_vendor_name,
    CAST(v.vendor_id AS VARCHAR(50))    AS p21_vendor_id,
    v.vendor_name                       AS p21_vendor_name,
    0.95                                AS match_score,
    'mapped'                            AS match_method,
    1                                   AS is_active,
    GETUTCDATE()                        AS created_at,
    GETUTCDATE()                        AS last_seen,
    1                                   AS seen_count
FROM dbo.vendor v
WHERE v.user_def_1 IS NOT NULL
  AND v.user_def_1 <> ''
  AND v.delete_flag = 'N';
*/
