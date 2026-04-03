-- ============================================================
-- Azure SQL Database: po_staging
-- P21 Sales Order Crosswalk Tables
-- EnPro Industries | 2026-04-03
-- ============================================================

-- ── PO-to-SO CROSSWALK ─────────────────────────────────────
-- Links incoming Ariba/Coupa POs to P21 Sales Orders

CREATE TABLE dbo.po_so_crosswalk (
    id                  INT IDENTITY PRIMARY KEY,
    source_po_number    VARCHAR(100) NOT NULL,    -- Ariba/Coupa PO#
    source_system       VARCHAR(20)  NOT NULL,    -- ARIBA, COUPA, etc.
    p21_order_no        VARCHAR(50)  NOT NULL,    -- P21 oe_hdr.order_no
    p21_customer_id     VARCHAR(50),
    match_method        VARCHAR(20)  NOT NULL DEFAULT 'manual', -- manual, auto_po_match, auto_item_match
    match_confidence    DECIMAL(5,4) NOT NULL DEFAULT 1.0,
    status              VARCHAR(20)  NOT NULL DEFAULT 'active', -- active, superseded, canceled
    created_at          DATETIME2    NOT NULL DEFAULT GETUTCDATE(),
    created_by          VARCHAR(100),
    notes               VARCHAR(1000),
    CONSTRAINT UQ_po_so_xwalk UNIQUE (source_po_number, source_system, p21_order_no)
);
CREATE INDEX IX_po_so_by_po ON dbo.po_so_crosswalk(source_po_number, source_system);
CREATE INDEX IX_po_so_by_so ON dbo.po_so_crosswalk(p21_order_no);

-- ── CUSTOMER CROSSWALK ─────────────────────────────────────
-- Maps Ariba/Coupa buyer/org IDs to P21 customer_id

CREATE TABLE dbo.customer_crosswalk (
    id                      INT IDENTITY PRIMARY KEY,
    source_system           VARCHAR(20)  NOT NULL,
    source_customer_id      VARCHAR(200) NOT NULL,   -- Ariba org ID, Coupa buyer ID
    source_customer_name    VARCHAR(255),
    p21_customer_id         VARCHAR(50)  NOT NULL,
    p21_customer_name       VARCHAR(255),
    match_score             DECIMAL(5,4) NOT NULL DEFAULT 1.0,
    match_method            VARCHAR(20)  NOT NULL DEFAULT 'manual',
    is_active               BIT          NOT NULL DEFAULT 1,
    created_at              DATETIME2    NOT NULL DEFAULT GETUTCDATE(),
    last_seen               DATETIME2    NOT NULL DEFAULT GETUTCDATE(),
    seen_count              INT          NOT NULL DEFAULT 1,
    CONSTRAINT UQ_cust_xwalk UNIQUE (source_system, source_customer_id)
);
CREATE INDEX IX_cust_xwalk_lookup ON dbo.customer_crosswalk(source_system, source_customer_id);
CREATE INDEX IX_cust_xwalk_name ON dbo.customer_crosswalk(source_system, source_customer_name);

-- ── SO EXPORT LOG ──────────────────────────────────────────
-- Tracks each SO pull from P21

CREATE TABLE dbo.so_export_log (
    id                  INT IDENTITY PRIMARY KEY,
    export_timestamp    DATETIME2    NOT NULL DEFAULT GETUTCDATE(),
    days_back           INT          NOT NULL,
    so_headers_count    INT          NOT NULL DEFAULT 0,
    so_lines_count      INT          NOT NULL DEFAULT 0,
    customers_count     INT          NOT NULL DEFAULT 0,
    items_count         INT          NOT NULL DEFAULT 0,
    blob_paths          NVARCHAR(MAX),   -- JSON list of blob paths
    status              VARCHAR(20)  NOT NULL DEFAULT 'success',
    error_message       VARCHAR(1000)
);

-- ── VIEW: SO with PO linkage ───────────────────────────────

CREATE VIEW dbo.vw_po_so_linked AS
SELECT
    sl.intake_id,
    sl.po_number AS source_po,
    sl.source_system,
    sl.overall_confidence,
    sl.review_status,
    xw.p21_order_no,
    xw.p21_customer_id,
    xw.match_confidence,
    xw.status AS link_status
FROM dbo.po_staging_log sl
LEFT JOIN dbo.po_so_crosswalk xw
    ON sl.po_number = xw.source_po_number
   AND sl.source_system = xw.source_system
   AND xw.status = 'active';
GO
