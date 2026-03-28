-- ============================================================
-- P21 Item Crosswalk Seed
-- Run on: P21 SQL Server (Prophet 21 database)
-- Export as CSV, then bulk import to Azure SQL dbo.item_crosswalk
-- EnPro Industries | 2026-03-26
-- ============================================================

-- All items ordered in last 3 years with supplier part numbers
-- Only items seen at least twice (reduces noise from one-off orders)

SELECT
    'P21'                               AS source_system,
    pl.supplier_part_num                AS source_item_id,
    ISNULL(pl.supplier_part_desc, pl.item_desc)  AS source_item_desc,
    CAST(pl.item_id AS VARCHAR(100))    AS p21_item_id,
    i.item_desc                         AS p21_item_desc,
    CAST(h.vendor_id AS VARCHAR(50))    AS p21_vendor_id,
    pl.unit_of_measure                  AS uom_source,
    pl.unit_of_measure                  AS uom_p21,
    1.0                                 AS match_score,
    'exact'                             AS match_method,
    1                                   AS is_active,
    GETUTCDATE()                        AS created_at,
    MAX(h.po_date)                      AS last_seen,
    COUNT(*)                            AS seen_count
FROM dbo.po_line pl
    INNER JOIN dbo.po_hdr h   ON pl.po_no = h.po_no
    INNER JOIN dbo.inv_mast i ON pl.item_id = i.item_id
WHERE h.po_date >= DATEADD(YEAR, -3, GETDATE())
  AND h.delete_flag = 'N'
  AND pl.delete_flag = 'N'
  AND pl.supplier_part_num IS NOT NULL
  AND pl.supplier_part_num <> ''
GROUP BY
    pl.supplier_part_num,
    pl.supplier_part_desc,
    pl.item_desc,
    pl.item_id,
    i.item_desc,
    h.vendor_id,
    pl.unit_of_measure
HAVING COUNT(*) >= 2
ORDER BY seen_count DESC;


-- ============================================================
-- Price bounds per vendor/item (for anomaly detection in scoring)
-- ============================================================

SELECT
    CAST(h.vendor_id AS VARCHAR(50))    AS p21_vendor_id,
    CAST(pl.item_id AS VARCHAR(100))    AS p21_item_id,
    i.item_desc,
    COUNT(*)                            AS order_count,
    MIN(pl.unit_price)                  AS min_price,
    MAX(pl.unit_price)                  AS max_price,
    AVG(pl.unit_price)                  AS avg_price,
    STDEV(pl.unit_price)                AS price_stdev,
    MAX(h.po_date)                      AS last_ordered
FROM dbo.po_line pl
    INNER JOIN dbo.po_hdr h   ON pl.po_no = h.po_no
    INNER JOIN dbo.inv_mast i ON pl.item_id = i.item_id
WHERE h.po_date >= DATEADD(YEAR, -2, GETDATE())
  AND h.delete_flag = 'N'
  AND pl.delete_flag = 'N'
  AND pl.unit_price > 0
GROUP BY h.vendor_id, pl.item_id, i.item_desc
HAVING COUNT(*) >= 3
ORDER BY order_count DESC;


-- ============================================================
-- Item cross-reference table (if exists in P21)
-- ============================================================
/*
SELECT DISTINCT
    'P21'                               AS source_system,
    x.vendor_item_no                    AS source_item_id,
    x.vendor_item_desc                  AS source_item_desc,
    CAST(x.item_id AS VARCHAR(100))     AS p21_item_id,
    i.item_desc                         AS p21_item_desc,
    CAST(x.vendor_id AS VARCHAR(50))    AS p21_vendor_id,
    NULL                                AS uom_source,
    NULL                                AS uom_p21,
    1.0                                 AS match_score,
    'xref'                              AS match_method,
    1                                   AS is_active,
    GETUTCDATE()                        AS created_at,
    GETUTCDATE()                        AS last_seen,
    1                                   AS seen_count
FROM dbo.itemxref x
    INNER JOIN dbo.inv_mast i ON x.item_id = i.item_id
WHERE x.delete_flag = 'N';
*/


-- ============================================================
-- Full vendor list with contact info (for manual mapping sessions)
-- ============================================================

SELECT
    v.vendor_id,
    v.vendor_name,
    v.contact_name,
    v.phone_number,
    v.email_address,
    COUNT(DISTINCT h.po_no)             AS po_count_3yr,
    MAX(h.po_date)                      AS last_po_date
FROM dbo.vendor v
    LEFT JOIN dbo.po_hdr h ON v.vendor_id = h.vendor_id
        AND h.po_date >= DATEADD(YEAR, -3, GETDATE())
        AND h.delete_flag = 'N'
WHERE v.delete_flag = 'N'
GROUP BY v.vendor_id, v.vendor_name, v.contact_name, v.phone_number, v.email_address
ORDER BY po_count_3yr DESC;
