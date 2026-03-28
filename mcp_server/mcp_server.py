"""
P21 MCP Server — Direct ODBC insertion into P21.
Bypasses CISM file import entirely.
"""

import json
import logging
import pyodbc
from datetime import datetime
from typing import Optional, Dict, Any, List

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load config
with open("config.json") as f:
    CONFIG = json.load(f)


class P21Database:
    """P21 ODBC connection manager."""
    
    def __init__(self):
        self.conn_string = CONFIG["odbc_connection_string"]
        self.test_mode = CONFIG.get("test_mode", True)
        self._connection = None
    
    def connect(self):
        """Establish ODBC connection to P21."""
        try:
            self._connection = pyodbc.connect(self.conn_string)
            logger.info("Connected to P21 via ODBC")
            return self._connection
        except Exception as e:
            logger.error(f"ODBC connection failed: {e}")
            raise
    
    def close(self):
        if self._connection:
            self._connection.close()
            self._connection = None
    
    def execute(self, sql: str, params: tuple = None) -> List[Dict]:
        """Execute SQL and return results as dicts."""
        if not self._connection:
            self.connect()
        
        cursor = self._connection.cursor()
        try:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            
            if cursor.description:
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                return [dict(zip(columns, row)) for row in rows]
            else:
                self._connection.commit()
                return [{"affected": cursor.rowcount}]
        finally:
            cursor.close()


# Global DB instance
db = P21Database()


# ── MCP Tool Functions ───────────────────────────────────────────────────────

def insert_po_header(
    po_no: str,
    vendor_id: str,
    order_date: str,
    ship_to_id: str = "",
    terms_id: str = "",
    fob: str = "",
    buyer: str = "",
    po_type: str = "D",
    company_no: int = None,
    location_id: int = None
) -> Dict[str, Any]:
    """
    Insert a purchase order header into P21 po_hdr table.
    
    Args:
        po_no: Purchase order number (unique)
        vendor_id: P21 vendor ID
        order_date: Order date (YYYY-MM-DD)
        ship_to_id: Ship-to location ID
        terms_id: Payment terms ID
        fob: FOB terms
        buyer: Buyer name/code
        po_type: PO type (default 'D' for direct)
        company_no: Company number (default from config)
        location_id: Location ID (default from config)
    """
    if CONFIG["test_mode"]:
        logger.info(f"[TEST MODE] Would insert PO header: {po_no}")
        return {"status": "test_mode", "po_no": po_no, "vendor_id": vendor_id}
    
    company = company_no or CONFIG.get("p21_company_no", 1)
    location = location_id or CONFIG.get("p21_location_id", 10)
    
    sql = """
        INSERT INTO po_hdr (
            po_no, company_no, location_id, vendor_id, order_date,
            ship_to_id, terms_id, fob, buyer, po_type,
            approved, date_created, date_last_modified
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'Y', GETDATE(), GETDATE())
    """
    
    try:
        result = db.execute(sql, (
            po_no, company, location, vendor_id, order_date,
            ship_to_id, terms_id, fob, buyer, po_type
        ))
        logger.info(f"Inserted PO header: {po_no}")
        return {
            "status": "success",
            "po_no": po_no,
            "vendor_id": vendor_id,
            "rows_affected": result[0].get("affected", 0)
        }
    except Exception as e:
        logger.error(f"Failed to insert PO header {po_no}: {e}")
        return {"status": "error", "error": str(e)}


def insert_po_line(
    po_no: str,
    line_no: int,
    item_id: str,
    qty_ordered: float,
    unit_price: float,
    description: str = "",
    required_date: str = None,
    gl_account: str = ""
) -> Dict[str, Any]:
    """
    Insert a purchase order line into P21 po_line table.
    
    Args:
        po_no: Purchase order number (must exist in po_hdr)
        line_no: Line number (1, 2, 3...)
        item_id: P21 item ID
        qty_ordered: Quantity ordered
        unit_price: Unit price
        description: Line description
        required_date: Required delivery date (YYYY-MM-DD)
        gl_account: GL account code
    """
    if CONFIG["test_mode"]:
        logger.info(f"[TEST MODE] Would insert PO line: {po_no} line {line_no}")
        return {"status": "test_mode", "po_no": po_no, "line_no": line_no}
    
    sql = """
        INSERT INTO po_line (
            po_no, line_no, item_id, qty_ordered, unit_price,
            description, required_date, gl_account,
            date_created, date_last_modified
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), GETDATE())
    """
    
    try:
        result = db.execute(sql, (
            po_no, line_no, item_id, qty_ordered, unit_price,
            description, required_date, gl_account
        ))
        logger.info(f"Inserted PO line: {po_no} line {line_no}")
        return {
            "status": "success",
            "po_no": po_no,
            "line_no": line_no,
            "rows_affected": result[0].get("affected", 0)
        }
    except Exception as e:
        logger.error(f"Failed to insert PO line {po_no}/{line_no}: {e}")
        return {"status": "error", "error": str(e)}


def validate_vendor(vendor_id: str) -> Dict[str, Any]:
    """Check if vendor exists in P21."""
    sql = "SELECT vendor_id, vendor_name FROM ap_hdr WHERE vendor_id = ?"
    try:
        result = db.execute(sql, (vendor_id,))
        if result:
            return {"exists": True, "vendor": result[0]}
        return {"exists": False, "vendor_id": vendor_id}
    except Exception as e:
        return {"exists": False, "error": str(e)}


def validate_item(item_id: str) -> Dict[str, Any]:
    """Check if item exists in P21."""
    sql = "SELECT item_id, item_desc FROM inv_mast WHERE item_id = ?"
    try:
        result = db.execute(sql, (item_id,))
        if result:
            return {"exists": True, "item": result[0]}
        return {"exists": False, "item_id": item_id}
    except Exception as e:
        return {"exists": False, "error": str(e)}


def get_po_status(po_no: str) -> Dict[str, Any]:
    """Check if PO exists and get its status."""
    sql = """
        SELECT h.po_no, h.vendor_id, h.order_date, h.approved,
               COUNT(l.line_no) as line_count
        FROM po_hdr h
        LEFT JOIN po_line l ON h.po_no = l.po_no
        WHERE h.po_no = ?
        GROUP BY h.po_no, h.vendor_id, h.order_date, h.approved
    """
    try:
        result = db.execute(sql, (po_no,))
        if result:
            return {"exists": True, "po": result[0]}
        return {"exists": False, "po_no": po_no}
    except Exception as e:
        return {"exists": False, "error": str(e)}


# ── MCP Server Protocol ───────────────────────────────────────────────────────

def handle_request(request: Dict) -> Dict:
    """Handle incoming MCP request."""
    tool = request.get("tool")
    params = request.get("params", {})
    
    tools = {
        "insert_po_header": insert_po_header,
        "insert_po_line": insert_po_line,
        "validate_vendor": validate_vendor,
        "validate_item": validate_item,
        "get_po_status": get_po_status,
    }
    
    if tool not in tools:
        return {"error": f"Unknown tool: {tool}"}
    
    try:
        result = tools[tool](**params)
        return {"tool": tool, "result": result}
    except Exception as e:
        return {"tool": tool, "error": str(e)}


def main():
    """Main MCP server loop (stdin/stdout)."""
    logger.info("P21 MCP Server starting...")
    logger.info(f"Test mode: {CONFIG['test_mode']}")
    
    # Test connection
    try:
        db.connect()
        db.close()
        logger.info("P21 connection test: OK")
    except Exception as e:
        logger.error(f"P21 connection test failed: {e}")
        if not CONFIG["test_mode"]:
            raise
    
    # MCP protocol loop
    logger.info("MCP server ready (stdin/stdout)")
    
    while True:
        try:
            line = input()
            if not line:
                continue
            
            request = json.loads(line)
            response = handle_request(request)
            print(json.dumps(response), flush=True)
            
        except EOFError:
            break
        except json.JSONDecodeError as e:
            print(json.dumps({"error": f"Invalid JSON: {e}"}), flush=True)
        except Exception as e:
            print(json.dumps({"error": str(e)}), flush=True)
    
    logger.info("MCP server stopped")


if __name__ == "__main__":
    main()
