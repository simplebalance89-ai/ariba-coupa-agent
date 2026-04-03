# EnPro PO Agent — Gap Analysis

## What's Built and Working

### Core Pipeline
- [x] Email classification (Ariba/Coupa/Direct regex detection)
- [x] cXML parser (Ariba/Coupa OrderRequest format)
- [x] PDF parser (Azure Document Intelligence)
- [x] CSV parser (direct column mapping)
- [x] Customer crosswalk engine (ship-to name + zip matching, fuzzy fallback)
- [x] Item crosswalk engine (customer_part_number → inv_mast_uid)
- [x] 4-dimension confidence scoring (customer/ship-to/items/dedup)
- [x] CISM SO generator (P21 Order/Quote Import schema)
- [x] CISM batch accumulator (single batch file for multiple approved POs)
- [x] Learning loop (approved matches feed back into crosswalks)
- [x] Local file-based PO store (works without SQL on Render)
- [x] Persistent disk storage (survives deploys)

### Crosswalk Data
- [x] Customer crosswalk: 4,880 customer + ship-to combos
- [x] Customer-item crosswalk: 22,675 part number mappings
- [x] PO history: 23,452 PO-to-SO linkages
- [x] Item master: 11,911 unique P21 items
- [x] Customer master: 7,833 customers with addresses
- [x] Dynamics quotes: 2,065 active quotes
- [x] Quote-to-PO linkage: 910 cases linking quotes → POs → orders
- [x] Salespeople: 52 reps with customer assignments

### Dashboard (Render sandbox)
- [x] Review Queue with green/yellow/red filters and reason display
- [x] PO detail panel with customer match, line items, match labels
- [x] Edit PO with customer item dropdown (from SO history)
- [x] Approve → adds to CISM batch + triggers learning loop
- [x] Reject with reason
- [x] CISM Batch tab showing accumulated batch with visual pipeline flow
- [x] Processed tab with all POs history (clickable rows)
- [x] Crosswalk tab (5 tables + quotes + PO linkage)
- [x] CISM Schema tab (header + line column definitions)
- [x] Pipeline tab (7-step flow explanation)
- [x] Match scoring guide (EXACT/MATCHED/FUZZY/GUESS/UNMAPPED)
- [x] Inline "Add PO to Review" in queue sidebar

---

## Gaps — Not Built Yet

### 1. Email Inbox Integration (HIGH PRIORITY)
- [ ] Microsoft Graph API polling is coded but NOT connected
- [ ] Need Azure AD credentials (tenant_id, client_id, client_secret)
- [ ] Need to configure orders@enproinc.com mailbox access
- [ ] Email → attachment download → parser → queue flow is coded but untested
- [ ] Auto-move processed emails to "Processed-PO" folder

### 2. Ariba/Coupa Direct Integration (HIGH PRIORITY)
- [ ] Current approach: single emails to inbox — fragile, one-at-a-time, easy to miss
- [ ] **Ariba Network options:**
  - [ ] cXML direct POST to our endpoint (Ariba sends OrderRequest to a webhook URL)
  - [ ] Ariba scheduled CSV/Excel report export (batch of POs on a schedule)
  - [ ] Ariba Network SFTP drop (PO files delivered to SFTP folder)
  - [ ] Ariba APIs (OrderRequest API, PO status API)
- [ ] **Coupa options:**
  - [ ] Coupa CSP (Coupa Supplier Portal) API — pull POs via REST
  - [ ] Coupa scheduled report export (CSV/Excel batch on schedule)
  - [ ] Coupa cXML integration (similar to Ariba webhook)
  - [ ] Coupa SFTP integration
- [ ] **Better approach:** scheduled batch pull (hourly/daily) from Ariba/Coupa portals
  - Ariba: configure a scheduled report of new POs → CSV → SFTP or email
  - Coupa: configure PO export → CSV → SFTP or API pull
  - Agent processes the batch CSV (already supports CSV intake)
  - More reliable than individual emails, catches everything, audit trail
- [ ] **Hybrid:** keep email inbox as fallback for non-Ariba/Coupa POs (direct customers)
- [ ] Need Ariba Network credentials (ANID, shared secret) for cXML integration
- [ ] Need Coupa API key for CSP API access

### 3. Azure Blob Integration (HIGH PRIORITY)
- [ ] CISM batch files sit on Render disk — NOT pushed to Azure Blob
- [ ] Need: Azure Blob connection string + container name
- [ ] Need: automated push of batch_orderquoteheader.csv + batch_orderquoteline.csv to blob
- [ ] Need: blob → local machine sync (Azure Storage Explorer or azcopy)
- [ ] Need: local machine CISM folder path (\\P21Server\CISM\Import\Incoming\)

### 3. P21 CISM Import Verification
- [ ] Batch CSV format matches the schema but NOT tested against actual P21 CISM import
- [ ] Column order/naming may need adjustment based on P21 version
- [ ] Contact ID field is blank — needs default or lookup
- [ ] Ship To ID field is blank — needs address_id from P21
- [ ] Terms field needs P21 terms_id (currently blank)
- [ ] Carrier ID needs P21 carrier lookup

### 4. Internal ID System / Order Tracking
- [ ] No internal SO number assignment before P21 creates the order
- [ ] No tracking of: PO received → SO created in P21 → SO number assigned
- [ ] Need feedback loop: after CISM import, get the P21 order_no back
- [ ] Quote-to-SO linkage: quote number should be attached to the SO

### 5. Edit PO — Incomplete Features
- [ ] Customer search dropdown (currently manual ID entry)
- [ ] Ship-to address ID lookup (P21 address_id needed for CISM)
- [ ] Line item add/remove (currently can only edit existing lines)
- [ ] Re-run through full crosswalk after edit (currently just re-scores)
- [ ] Audit trail of edits

### 6. Crosswalk Improvements
- [ ] Item master shows NULL descriptions for many items — needs inv_mast table pull with item_id + item_desc
- [ ] Supplier names show IDs, not names — needs vendor table join
- [ ] Customer-item crosswalk doesn't show the P21 item_id (part number), only inv_mast_uid
- [ ] UOM crosswalk not connected (Ariba UN/CEFACT codes → P21 UOM mapping)
- [ ] No price anomaly detection (historical price bounds exist but not used in scoring)

### 7. Dynamics CRM Integration
- [ ] Quote data is static CSV upload — not live API pull
- [ ] Need Dynamics 365 OAuth token management
- [ ] Need real-time quote lookup when processing POs
- [ ] Need to create/update Dynamics cases when POs are processed
- [ ] Quote line items (lvp_quotelines) came back empty — needs investigation

### 8. Production Deployment (Azure)
- [ ] Render is sandbox only — production goes to Azure
- [ ] Need Azure App Service or Container Instance setup
- [ ] Need Azure SQL database for staging/crosswalk (replacing local files)
- [ ] Need managed identity for Azure Blob + SQL access
- [ ] Need deployment pipeline (GitHub Actions → Azure)
- [ ] Need environment variable management (Key Vault)

### 9. Security / Auth
- [ ] Dashboard has NO authentication
- [ ] No user management (Brittany, other reviewers)
- [ ] No audit log of who approved/rejected what
- [ ] API endpoints are fully open
- [ ] Need Azure AD / SSO integration for production

### 10. Monitoring / Alerting
- [ ] No monitoring of email polling failures
- [ ] No alerting when POs go red (unmatched)
- [ ] No metrics dashboard (processing time, match rates, approval rates)
- [ ] No logging aggregation (currently just stdout)
- [ ] No health check for crosswalk staleness

### 11. Data Refresh
- [ ] Crosswalk data is static — uploaded once, not refreshed
- [ ] Need scheduled re-pull from P21 SQL (daily/weekly)
- [ ] Need crosswalk merge logic (P21 baseline + learned entries)
- [ ] Score decay for stale crosswalk entries not implemented
- [ ] No mechanism to detect P21 schema changes

### 12. Edge Cases Not Handled
- [ ] Multi-company POs (company_id != 1)
- [ ] International orders (non-US addresses, currency conversion)
- [ ] Blanket POs / release orders
- [ ] PO amendments (changes to existing POs)
- [ ] PO cancellations
- [ ] Split shipments
- [ ] Back-to-back POs from same customer (batch dedup)
- [ ] PDF POs with non-standard layouts (Document Intelligence may fail)

---

## Recommended Next Steps (Priority Order)

1. **Test with real Ariba/Coupa PDFs** — validate parser + crosswalk matching
2. **Fix CISM batch fields** — Contact ID, Ship To ID, Terms, Carrier from P21 data
3. **Azure Blob push** — automate batch upload to blob storage
4. **Test CISM import** — run batch through actual P21 CISM job
5. **Wire email inbox** — connect Graph API to orders@enproinc.com
6. **Production Azure deploy** — move from Render sandbox
7. **Add authentication** — Azure AD for dashboard access
8. **Dynamics API** — live quote lookup instead of static CSV
9. **Scheduled crosswalk refresh** — daily P21 data pull
10. **Monitoring + alerting** — track match rates, failures, SLAs
