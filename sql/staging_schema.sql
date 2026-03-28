-- ============================================================
-- Azure SQL Database: po_staging
-- Ariba/Coupa PO Automation — Staging + Crosswalk Tables
-- EnPro Industries | 2026-03-26
-- ============================================================

-- ── VENDOR CROSSWALK ────────────────────────────────────────

CREATE TABLE dbo.vendor_crosswalk (
    id                  INT IDENTITY PRIMARY KEY,
    source_system       VARCHAR(20)  NOT NULL,   -- ariba, coupa, vega, direct, P21
    source_vendor_id    VARCHAR(100) NOT NULL,
    source_vendor_name  VARCHAR(255),
    p21_vendor_id       VARCHAR(50)  NOT NULL,
    p21_vendor_name     VARCHAR(255),
    match_score         DECIMAL(5,4) NOT NULL DEFAULT 1.0,
    match_method        VARCHAR(20)  NOT NULL DEFAULT 'exact', -- exact, fuzzy, soundex, manual
    is_active           BIT          NOT NULL DEFAULT 1,
    created_at          DATETIME2    NOT NULL DEFAULT GETUTCDATE(),
    last_seen           DATETIME2    NOT NULL DEFAULT GETUTCDATE(),
    seen_count          INT          NOT NULL DEFAULT 1,
    CONSTRAINT UQ_vendor_xwalk UNIQUE (source_system, source_vendor_id)
);
CREATE INDEX IX_vendor_xwalk_lookup ON dbo.vendor_crosswalk(source_system, source_vendor_id);
CREATE INDEX IX_vendor_xwalk_name ON dbo.vendor_crosswalk(source_system, source_vendor_name);

-- ── ITEM CROSSWALK ──────────────────────────────────────────

CREATE TABLE dbo.item_crosswalk (
    id                  INT IDENTITY PRIMARY KEY,
    source_system       VARCHAR(20)  NOT NULL,
    source_item_id      VARCHAR(200) NOT NULL,
    source_item_desc    VARCHAR(500),
    p21_item_id         VARCHAR(100) NOT NULL,
    p21_item_desc       VARCHAR(500),
    p21_vendor_id       VARCHAR(50),             -- NULL = applies to all vendors
    uom_source          VARCHAR(20),
    uom_p21             VARCHAR(20),
    match_score         DECIMAL(5,4) NOT NULL DEFAULT 1.0,
    match_method        VARCHAR(20)  NOT NULL DEFAULT 'exact',
    is_active           BIT          NOT NULL DEFAULT 1,
    created_at          DATETIME2    NOT NULL DEFAULT GETUTCDATE(),
    last_seen           DATETIME2    NOT NULL DEFAULT GETUTCDATE(),
    seen_count          INT          NOT NULL DEFAULT 1,
    CONSTRAINT UQ_item_xwalk UNIQUE (source_system, source_item_id, ISNULL(p21_vendor_id, ''))
);
CREATE INDEX IX_item_xwalk_lookup ON dbo.item_crosswalk(source_system, source_item_id, p21_vendor_id);

-- ── PO STAGING LOG ──────────────────────────────────────────

CREATE TABLE dbo.po_staging_log (
    id                  INT IDENTITY PRIMARY KEY,
    intake_id           VARCHAR(32)  NOT NULL UNIQUE,
    po_number           VARCHAR(100),
    source_system       VARCHAR(20),
    vendor_id_raw       VARCHAR(100),
    vendor_id_p21       VARCHAR(50),
    overall_confidence  VARCHAR(10),  -- green, yellow, red
    review_required     BIT          NOT NULL DEFAULT 0,
    review_status       VARCHAR(20)  NOT NULL DEFAULT 'pending', -- pending, approved, rejected
    reviewed_by         VARCHAR(100),
    reviewed_at         DATETIME2,
    reviewer_notes      VARCHAR(1000),
    received_at         DATETIME2,
    cism_blob_path      VARCHAR(500),
    p21_import_status   VARCHAR(20)  NOT NULL DEFAULT 'pending', -- pending, imported, failed
    p21_imported_at     DATETIME2,
    raw_payload         NVARCHAR(MAX),
    created_at          DATETIME2    NOT NULL DEFAULT GETUTCDATE()
);
CREATE INDEX IX_staging_review ON dbo.po_staging_log(review_required, review_status);
CREATE INDEX IX_staging_confidence ON dbo.po_staging_log(overall_confidence);
CREATE INDEX IX_staging_po ON dbo.po_staging_log(po_number, source_system);

-- ── REVIEW QUEUE VIEW (for OMS Portal) ──────────────────────

CREATE VIEW dbo.vw_review_queue AS
SELECT
    sl.id,
    sl.intake_id,
    sl.po_number,
    sl.source_system,
    sl.vendor_id_raw,
    sl.vendor_id_p21,
    sl.overall_confidence,
    sl.review_status,
    sl.received_at,
    sl.cism_blob_path,
    sl.reviewer_notes,
    vc.p21_vendor_name,
    vc.source_vendor_name
FROM dbo.po_staging_log sl
LEFT JOIN dbo.vendor_crosswalk vc
    ON sl.vendor_id_p21 = vc.p21_vendor_id AND vc.is_active = 1
WHERE sl.review_required = 1
  AND sl.review_status = 'pending'
  AND sl.p21_import_status = 'pending';
GO

-- ── UOM CROSSWALK ───────────────────────────────────────────

CREATE TABLE dbo.uom_crosswalk (
    id              INT IDENTITY PRIMARY KEY,
    p21_uom         VARCHAR(20) NOT NULL,
    ariba_uom       VARCHAR(20),    -- UN/CEFACT code
    coupa_uom       VARCHAR(20),
    description     VARCHAR(100),
    CONSTRAINT UQ_uom UNIQUE (p21_uom)
);

-- Seed common UOM mappings
INSERT INTO dbo.uom_crosswalk (p21_uom, ariba_uom, coupa_uom, description) VALUES
('EA', 'C62', 'EA', 'Each'),
('CS', 'CS', 'CS', 'Case'),
('BX', 'BX', 'BX', 'Box'),
('FT', 'FOT', 'FT', 'Foot'),
('LB', 'LBR', 'LB', 'Pound'),
('KG', 'KGM', 'KG', 'Kilogram'),
('GL', 'GLL', 'GL', 'Gallon'),
('PR', 'PR', 'PR', 'Pair'),
('RL', 'RL', 'RL', 'Roll'),
('PK', 'PK', 'PK', 'Pack'),
('DZ', 'DZN', 'DZ', 'Dozen'),
('ST', 'SET', 'ST', 'Set');
GO
