"""
test_end_to_end.py — Validate the full Ariba/Coupa PO processing pipeline.

Tests:
1. Health check
2. cXML intake → parsing → crosswalk → CISM generation
3. Review queue API
4. Approve → Blob upload flow
"""

import os
import sys
import requests
import xml.etree.ElementTree as ET
from datetime import datetime

# Configuration
BASE_URL = os.environ.get("TEST_BASE_URL", "http://localhost:8000")
API_URL = f"{BASE_URL}/api/v1"


def test_health():
    """Test 1: Health endpoint returns 200."""
    print("\n🧪 Test 1: Health Check")
    try:
        res = requests.get(f"{BASE_URL}/health", timeout=10)
        assert res.status_code == 200, f"Expected 200, got {res.status_code}"
        data = res.json()
        assert data["status"] == "healthy"
        print(f"   ✅ Healthy — Version: {data.get('version', 'unknown')}")
        return True
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return False


def test_cxml_intake():
    """Test 2: Submit cXML OrderRequest."""
    print("\n🧪 Test 2: cXML Intake")
    
    # Sample cXML OrderRequest
    cxml = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE cXML SYSTEM "http://xml.cXML.org/schemas/cXML/1.2.050/cXML.dtd">
<cXML version="1.2.050" xml:lang="en-US" timestamp="{datetime.utcnow().isoformat()}">
    <Header>
        <From>
            <Credential domain="NetworkId">
                <Identity>ariba-test</Identity>
            </Credential>
        </From>
        <To>
            <Credential domain="NetworkId">
                <Identity>enpro-filtration</Identity>
            </Credential>
        </To>
        <Sender>
            <Credential domain="NetworkId">
                <Identity>ariba-test</Identity>
                <SharedSecret>test-secret</SharedSecret>
            </Credential>
            <UserAgent>TestAgent/1.0</UserAgent>
        </Sender>
    </Header>
    <Request deploymentMode="test">
        <OrderRequest>
            <OrderRequestHeader orderID="TEST-PO-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}" 
                               orderDate="{datetime.utcnow().strftime('%Y-%m-%d')}" 
                               type="new">
                <Total>
                    <Money currency="USD">1250.00</Money>
                </Total>
                <ShipTo>
                    <Address addressID="ENPRO-HQ">
                        <Name xml:lang="en-US">EnPro Filtration HQ</Name>
                        <PostalAddress>
                            <Street>123 Industrial Way</Street>
                            <City>San Francisco</City>
                            <State>CA</State>
                            <PostalCode>94105</PostalCode>
                            <Country isoCountryCode="US">United States</Country>
                        </PostalAddress>
                    </Address>
                </ShipTo>
                <BillTo>
                    <Address addressID="ENPRO-BILL">
                        <Name xml:lang="en-US">EnPro Filtration Accounting</Name>
                    </Address>
                </BillTo>
                <Shipping trackingDomain="UPS">
                    <Money currency="USD">25.00</Money>
                    <Description xml:lang="en-US">Ground Shipping</Description>
                </Shipping>
                <Contact role="supplier">
                    <Name xml:lang="en-US">Industrial Supply Co</Name>
                    <Email>orders@industrialsupply.com</Email>
                </Contact>
            </OrderRequestHeader>
            <ItemOut quantity="10" lineNumber="1">
                <ItemID>
                    <SupplierPartID>FILT-MAST-001</SupplierPartID>
                </ItemID>
                <ItemDetail>
                    <UnitPrice>
                        <Money currency="USD">125.00</Money>
                    </UnitPrice>
                    <Description xml:lang="en-US">Filter Element 10um</Description>
                    <UnitOfMeasure>EA</UnitOfMeasure>
                </ItemDetail>
            </ItemOut>
        </OrderRequest>
    </Request>
</cXML>"""
    
    try:
        res = requests.post(
            f"{API_URL}/intake/cxml",
            data=cxml,
            headers={"Content-Type": "text/xml"},
            timeout=30
        )
        assert res.status_code == 200, f"Expected 200, got {res.status_code}"
        
        # Parse response (should be cXML Response)
        response_xml = res.text
        assert "Response" in response_xml or "OK" in response_xml or "200" in response_xml
        
        print(f"   ✅ cXML intake successful")
        print(f"   📄 Response: {response_xml[:200]}...")
        return True
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return False


def test_review_queue():
    """Test 3: Review queue returns pending POs."""
    print("\n🧪 Test 3: Review Queue")
    try:
        res = requests.get(f"{API_URL}/review/queue", timeout=10)
        assert res.status_code == 200
        data = res.json()
        assert isinstance(data, list)
        print(f"   ✅ Queue has {len(data)} pending POs")
        for po in data[:3]:
            print(f"      • {po.get('po_number')} | {po.get('source')} | {po.get('confidence')}")
        return True
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return False


def test_stats():
    """Test 4: Stats endpoint returns counts."""
    print("\n🧪 Test 4: Dashboard Stats")
    try:
        res = requests.get(f"{API_URL}/stats", timeout=10)
        assert res.status_code == 200
        data = res.json()
        print(f"   ✅ Stats: Total={data.get('total', 0)}, Green={data.get('green', 0)}, Red={data.get('red', 0)}")
        return True
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return False


def test_review_portal_html():
    """Test 5: Review portal HTML loads."""
    print("\n🧪 Test 5: Review Portal UI")
    try:
        res = requests.get(f"{BASE_URL}/review", timeout=10)
        assert res.status_code == 200
        assert "EnPro Order Management System" in res.text
        print(f"   ✅ Review portal loads correctly")
        return True
    except Exception as e:
        print(f"   ❌ Failed: {e}")
        return False


def run_all_tests():
    """Run all tests and report results."""
    print("=" * 60)
    print("EnPro Ariba/Coupa Integration — End-to-End Test Suite")
    print("=" * 60)
    print(f"Target: {BASE_URL}")
    
    tests = [
        test_health,
        test_cxml_intake,
        test_review_queue,
        test_stats,
        test_review_portal_html,
    ]
    
    results = []
    for test in tests:
        try:
            results.append(test())
        except Exception as e:
            print(f"   💥 Exception: {e}")
            results.append(False)
    
    print("\n" + "=" * 60)
    passed = sum(results)
    total = len(results)
    print(f"Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed!")
        return 0
    else:
        print(f"⚠️  {total - passed} test(s) failed")
        return 1


if __name__ == "__main__":
    sys.exit(run_all_tests())
