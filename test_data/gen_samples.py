"""Generate sample PO XMLs from real P21 customer/item data for testing."""

import os

SAMPLES = [
    # 4: Nucor Steel — GREEN (top customer, 493 orders, known parts)
    {"po": "NS-2026-04-001", "ship_name": "Nucor Steel", "addr": "2100 Roanoke Ave", "city": "Berkeley", "state": "SC", "zip": "29440", "phone": "843-761-8000", "buyer": "Jim Torres", "email": "jtorres@nucor.com", "source": "ariba",
     "lines": [("PH0260", "Pall Filter Element HP", 12, 185.50), ("9000238", "Pall Seal Kit Standard", 24, 96.52)]},

    # 5: Steel Dynamics — GREEN (176 orders)
    {"po": "SDI-PO-88712", "ship_name": "Steel Dynamics, Inc.", "addr": "7575 W Jefferson Blvd", "city": "Fort Wayne", "state": "IN", "zip": "46804", "phone": "260-969-3500", "buyer": "Karen Lee", "email": "klee@steeldynamics.com", "source": "coupa",
     "lines": [("T10540036", "Pall Replacement Cartridge", 100, 73.21), ("490.646.1Y.BC", "Lechler Nozzle Brass", 50, 24.73)]},

    # 6: Grain Processing Corp — YELLOW (known customer, 1 unknown part)
    {"po": "GPC-44521", "ship_name": "Grain Processing Corp.", "addr": "1600 Oregon St", "city": "Muscatine", "state": "IA", "zip": "52761", "phone": "563-264-4265", "buyer": "Dave Miller", "email": "dmiller@grainprocessing.com", "source": "ariba",
     "lines": [("9000238", "Pall Seal Kit", 6, 96.52), ("CUSTOM-GASKET-77X", "Custom Gasket Assembly Special", 2, 450.00)]},

    # 7: Ashland Specialty — YELLOW (known customer, mixed parts)
    {"po": "ASH-PO-2026-339", "ship_name": "Ashland Specialty Ingredients", "addr": "1005 Route US 46", "city": "Calvert City", "state": "KY", "zip": "42029", "phone": "270-395-4171", "buyer": "Lisa Park", "email": "lpark@ashland.com", "source": "coupa",
     "lines": [("3000TAEUC", "Teledyne Analytical Sensor", 10, 66.15), ("VALVE-REPAIR-KIT-99", "Valve Repair Kit Unknown", 5, 320.00), ("PH0260", "Pall Filter HP", 8, 185.50)]},

    # 8: Unknown customer — RED (ship-to doesn't match any P21 customer)
    {"po": "XYZ-99001", "ship_name": "Acme Industrial Supply Co", "addr": "999 Unknown Blvd", "city": "Nowhere", "state": "TX", "zip": "79999", "phone": "", "buyer": "John Doe", "email": "jdoe@acme-fake.com", "source": "direct",
     "lines": [("WIDGET-A1", "Industrial Widget Type A", 100, 12.50), ("WIDGET-B2", "Industrial Widget Type B", 50, 25.00)]},

    # 9: South Bend Ethanol — GREEN (recent orders)
    {"po": "SBE-26-009912", "ship_name": "South Bend Ethanol, LLC", "addr": "3201 W Calvert St", "city": "South Bend", "state": "IN", "zip": "46613-1010", "phone": "574-703-3360", "buyer": "Mark Jensen", "email": "mjensen@southbendethanol.com", "source": "ariba",
     "lines": [("T10540036", "Pall Cartridge Industrial", 25, 73.21)]},

    # 10: Xcel Energy — YELLOW (known customer, 2 unknown parts out of 3)
    {"po": "XCEL-CC-2026-0403", "ship_name": "Xcel Energy", "addr": "13999 Industrial Blvd", "city": "Becker", "state": "MN", "zip": "55308-8800", "phone": "800-895-1999", "buyer": "Hailey Horvath", "email": "hhorvath@xcelenergy.com", "source": "coupa",
     "lines": [("9000238", "Pall Seal Kit", 12, 96.52), ("TURBINE-BEARING-X9", "Turbine Bearing Assembly", 2, 1250.00), ("GENERATOR-FILTER-Z3", "Generator Air Filter Custom", 4, 875.00)]},
]

TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE cXML SYSTEM "http://xml.cxml.org/schemas/cXML/1.2.069/cXML.dtd">
<cXML payloadID="sample-{po}@test" timestamp="2026-04-03T12:00:00-05:00" version="1.2.069">
  <Header>
    <From><Credential domain="TestNetwork"><Identity>TEST-{po}</Identity></Credential></From>
    <To><Correspondent><Contact><Name>EnPro Industries</Name><Email>orders@enproinc.com</Email></Contact></Correspondent></To>
    <Sender><Credential domain="TestNetwork"><Identity>TEST-SENDER</Identity></Credential></Sender>
  </Header>
  <Request>
    <OrderRequest>
      <OrderRequestHeader orderID="{po}" orderDate="2026-04-03" orderType="regular">
        <Total><Money currency="USD">{total:.2f}</Money></Total>
        <ShipTo>
          <Address>
            <Name>{ship_name}</Name>
            <PostalAddress>
              <Street>{addr}</Street>
              <City>{city}</City>
              <State>{state}</State>
              <PostalCode>{zip}</PostalCode>
              <Country isoCountryCode="US">United States</Country>
            </PostalAddress>
          </Address>
        </ShipTo>
        <Contact role="purchasingAgent">
          <Name>{buyer}</Name>
          <Email>{email}</Email>
        </Contact>
        <Comments>Sample PO for testing</Comments>
      </OrderRequestHeader>
{item_xml}
    </OrderRequest>
  </Request>
</cXML>"""

LINE_TEMPLATE = """      <ItemOut lineNumber="{ln}" quantity="{qty}" requestedDeliveryDate="2026-04-15">
        <ItemID><SupplierPartID>{part}</SupplierPartID></ItemID>
        <ItemDetail>
          <UnitPrice><Money currency="USD">{price:.2f}</Money></UnitPrice>
          <Description>{desc}</Description>
          <UnitOfMeasure>EA</UnitOfMeasure>
        </ItemDetail>
      </ItemOut>"""

out_dir = os.path.dirname(__file__)
for i, s in enumerate(SAMPLES, 4):
    lines_xml = "\n".join(
        LINE_TEMPLATE.format(ln=j+1, part=l[0], desc=l[1], qty=l[2], price=l[3])
        for j, l in enumerate(s["lines"])
    )
    total = sum(l[2]*l[3] for l in s["lines"])
    xml = TEMPLATE.format(
        po=s["po"], ship_name=s["ship_name"], addr=s["addr"],
        city=s["city"], state=s["state"], zip=s["zip"],
        buyer=s["buyer"], email=s["email"], total=total,
        item_xml=lines_xml,
    )
    fname = f"sample_po_{i}_{s['source']}.xml"
    with open(os.path.join(out_dir, fname), "w") as f:
        f.write(xml)
    print(f"Generated {fname}: {s['ship_name']} ({s['source']}) - {len(s['lines'])} lines")
