"""
Test P21 MCP Server tools without needing real ODBC connection.
Set test_mode: true in config.json for safe testing.
"""

import json
import sys

# Import the tool functions
from mcp_server import (
    insert_po_header,
    insert_po_line,
    validate_vendor,
    validate_item,
    get_po_status
)

def test_insert_po_header():
    """Test PO header insertion."""
    print("\n🧪 Test: insert_po_header")
    result = insert_po_header(
        po_no="TEST-PO-001",
        vendor_id="120368",
        order_date="2026-03-27",
        ship_to_id="ENPRO-HQ",
        terms_id="NET30",
        buyer="Brittany"
    )
    print(f"   Result: {json.dumps(result, indent=2)}")
    assert result["status"] in ["success", "test_mode"]
    return True

def test_insert_po_line():
    """Test PO line insertion."""
    print("\n🧪 Test: insert_po_line")
    result = insert_po_line(
        po_no="TEST-PO-001",
        line_no=1,
        item_id="FILT-001",
        qty_ordered=10,
        unit_price=125.00,
        description="Filter Element 10um"
    )
    print(f"   Result: {json.dumps(result, indent=2)}")
    assert result["status"] in ["success", "test_mode"]
    return True

def test_validate_vendor():
    """Test vendor validation."""
    print("\n🧪 Test: validate_vendor")
    result = validate_vendor(vendor_id="120368")
    print(f"   Result: {json.dumps(result, indent=2)}")
    # May return exists: false if no DB, but should not error
    assert "exists" in result
    return True

def test_validate_item():
    """Test item validation."""
    print("\n🧪 Test: validate_item")
    result = validate_item(item_id="FILT-001")
    print(f"   Result: {json.dumps(result, indent=2)}")
    assert "exists" in result
    return True

def test_get_po_status():
    """Test PO status check."""
    print("\n🧪 Test: get_po_status")
    result = get_po_status(po_no="TEST-PO-001")
    print(f"   Result: {json.dumps(result, indent=2)}")
    assert "exists" in result
    return True

def main():
    print("=" * 60)
    print("P21 MCP Server — Tool Tests")
    print("=" * 60)
    print("\nConfig: Check config.json for test_mode setting")
    print("If test_mode: true → safe testing, no real DB writes")
    print("If test_mode: false → requires working ODBC connection")
    
    tests = [
        ("PO Header", test_insert_po_header),
        ("PO Line", test_insert_po_line),
        ("Validate Vendor", test_validate_vendor),
        ("Validate Item", test_validate_item),
        ("PO Status", test_get_po_status),
    ]
    
    passed = 0
    failed = 0
    
    for name, test_func in tests:
        try:
            if test_func():
                passed += 1
                print(f"   ✅ {name} passed")
        except Exception as e:
            failed += 1
            print(f"   ❌ {name} failed: {e}")
    
    print("\n" + "=" * 60)
    print(f"Results: {passed}/{len(tests)} passed")
    if failed == 0:
        print("🎉 All tests passed!")
        return 0
    else:
        print(f"⚠️  {failed} test(s) failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())
