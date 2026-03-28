# P21 MCP Server

Model Context Protocol server for direct P21 ODBC insertion.

## Purpose

Bypass the CISM file import entirely. Insert POs directly into P21 via ODBC:
- `po_hdr` — Purchase order header
- `po_line` — Purchase order lines

## Why MCP?

- Standardized protocol for AI tool integration
- Claude Desktop, Cursor, other AI tools can call it
- Easier than custom REST API for this use case

## Prerequisites

- ODBC driver for P21 (P21 ODBC driver or SQL Server driver)
- P21 database connection string
- Write permissions on `po_hdr` and `po_line`

## Configuration

Edit `config.json`:
```json
{
  "odbc_connection_string": "DRIVER={P21 ODBC Driver};SERVER=...;DATABASE=...;UID=...;PWD=...",
  "test_mode": true
}
```

## Tools Provided

| Tool | Purpose |
|------|---------|
| `insert_po_header` | Insert into `po_hdr` table |
| `insert_po_line` | Insert into `po_line` table |
| `validate_vendor` | Check if vendor exists in P21 |
| `validate_item` | Check if item exists in P21 |
| `get_po_status` | Check PO import status |

## Run

```bash
python mcp_server.py
```

## Test

```bash
python test_mcp.py
```
