"""
po_parser.py — Parse cXML and PDF purchase orders into structured models.
Handles Ariba cXML OrderRequest, Coupa cXML, and PDF via Azure Document Intelligence.
MRO-specific: blanket POs, releases, line-level ShipTo, GL codes, Extrinsics.

Models aligned to real P21 po_hdr/po_line schema (validated March 2026).
"""

import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional

from models import POHeader, POLineItem, OrderType, SourceSystem, VEGA_SOURCE_TYPE


def parse_cxml(content: str) -> tuple[POHeader, list[POLineItem], str]:
    """Parse Ariba/Coupa cXML OrderRequest into structured header + lines."""
    root = ET.fromstring(content)

    orh = root.find('.//OrderRequestHeader')
    if orh is None:
        raise ValueError("No OrderRequestHeader found in cXML")

    # ── Header ───────────────────────────────────────────────────────────
    order_type_raw = orh.get('orderType', 'regular')
    order_type = OrderType.REGULAR
    if order_type_raw == 'blanket':
        order_type = OrderType.BLANKET
    elif order_type_raw == 'release':
        order_type = OrderType.RELEASE

    header = POHeader(
        po_no=orh.get('orderID', ''),
        order_date=orh.get('orderDate', '')[:10],
        order_type=order_type,
        order_version=orh.get('orderVersion', '1'),
        source_system=SourceSystem.EMAIL,  # Primary inflow is orders@enpro inbox
        # P21 defaults for Vega
        po_type='D',
        source_type=VEGA_SOURCE_TYPE,
        company_no=1,
        location_id=10,
        branch_id='000',
        ship2_country='US',
        approved='Y',
    )

    # Currency
    total_money = orh.find('.//Total/Money')
    if total_money is not None:
        header.currency_id = total_money.get('currency', 'USD')

    # Release info (blanket PO reference)
    release_info = orh.find('.//ReleaseInfo')
    if release_info is not None:
        header.blanket_po_no = release_info.get('masterOrderID', '')
        header.release_no = release_info.get('releaseNumber', '')

    # Ship To (header level) — maps to po_hdr ship2_* columns
    ship_to = orh.find('.//ShipTo/Address')
    if ship_to is not None:
        header.ship2_name = _get_text(ship_to, 'Name')
        postal = ship_to.find('.//PostalAddress')
        if postal is not None:
            streets = postal.findall('Street')
            if len(streets) >= 1:
                header.ship2_add1 = (streets[0].text or '').strip()
            if len(streets) >= 2:
                header.ship2_add2 = (streets[1].text or '').strip()
            if len(streets) >= 3:
                header.ship2_add3 = (streets[2].text or '').strip()
            header.ship2_city = _get_text(postal, 'City')
            header.ship2_state = _get_text(postal, 'State')
            header.ship2_zip = _get_text(postal, 'PostalCode')
            country = _get_text(postal, 'Country')
            if country:
                header.ship2_country = country

    # Buyer / Purchasing Agent — stored in notes (not direct po_hdr columns)
    buyer_contact = orh.find('.//Contact[@role="purchasingAgent"]')
    if buyer_contact is not None:
        header.buyer = _get_text(buyer_contact, 'Name')
        header.buyer_email = _get_text(buyer_contact, 'Email')
        phone = buyer_contact.find('.//TelephoneNumber')
        if phone is not None:
            area = _get_text(phone, 'AreaOrCityCode')
            number = _get_text(phone, 'Number')
            header.buyer_phone = f"{area}-{number}" if area else number

    # Delivery Terms → maps to fob / freight_terms
    tod = orh.find('.//TermsOfDelivery/TransportTerms')
    if tod is not None:
        header.freight_terms = (tod.get('value', '') + ' - ' + (tod.text or '')).strip(' -')
    fob_elem = orh.find('.//TermsOfDelivery/ShippingPaymentMethod')
    if fob_elem is not None:
        header.fob = fob_elem.get('value', '')

    # Extrinsics (MRO-specific fields)
    for ext in orh.findall('Extrinsic'):
        name = ext.get('name', '')
        val = ext.text or ''
        if name == 'AribaNetwork.PaymentTermsExplanation':
            header.terms = val
        elif name in ('ContractID', 'AgreementNumber', 'ContractNumber'):
            header.contract_no = val
        elif name == 'ExternalPONumber':
            header.external_po_no = val

    # Comments → po_desc (shipping instructions)
    comments = orh.find('Comments')
    if comments is not None:
        comment_text = (comments.text or '').strip()
        header.po_desc = comment_text  # shipping instructions go in po_desc
        header.comments = comment_text

    # Supplier info
    correspondent = root.find('.//To/Correspondent/Contact')
    if correspondent is not None:
        header.supplier_name = _get_text(correspondent, 'Name')
        header.supplier_email = _get_text(correspondent, 'Email')

    # ── Line Items ───────────────────────────────────────────────────────
    lines: list[POLineItem] = []
    for item_out in root.findall('.//ItemOut'):
        line = POLineItem(
            line_no=int(item_out.get('lineNumber', '0')),
            qty_ordered=float(item_out.get('quantity', '0')),
            date_due=item_out.get('requestedDeliveryDate', '')[:10] if item_out.get('requestedDeliveryDate') else '',
            # P21 defaults for Vega lines
            source_type=VEGA_SOURCE_TYPE,
            calc_type='MULTIPLIER',
            calc_value=1.0,
            unit_size=1.0,
            unit_quantity=1.0,
            pricing_unit_size=1.0,
            inventory_flag='N',
        )

        # Set required_date = date_due if present
        if line.date_due:
            line.required_date = line.date_due

        # Item ID
        item_id = item_out.find('.//ItemID')
        if item_id is not None:
            line.supplier_part_id = _get_text(item_id, 'SupplierPartID')

        # Item Detail
        detail = item_out.find('.//ItemDetail')
        if detail is not None:
            price_money = detail.find('.//UnitPrice/Money')
            if price_money is not None:
                line.unit_price = float(price_money.text or 0)
                line.unit_price_display = line.unit_price
                line.base_ut_price = line.unit_price

            line.item_description = _get_text(detail, 'Description')
            line.unit_of_measure = _get_text(detail, 'UnitOfMeasure') or 'EA'
            line.pricing_unit = line.unit_of_measure

            mfg_part = detail.find('ManufacturerPartID')
            if mfg_part is not None:
                line.mfg_part_no = mfg_part.text or ''

        # Line-level Ship To (MRO override)
        line_ship = item_out.find('.//ShipTo/Address')
        if line_ship is not None:
            line.ship_to_name = _get_text(line_ship, 'Name')
            line_postal = line_ship.find('.//PostalAddress')
            if line_postal is not None:
                line.ship_to_address = _get_text(line_postal, 'Street')
                line.ship_to_city = _get_text(line_postal, 'City')
                line.ship_to_state = _get_text(line_postal, 'State')
                line.ship_to_zip = _get_text(line_postal, 'PostalCode')

        # Accounting / Distribution (MRO GL codes) → maps to account_no
        accounting = item_out.find('.//Accounting')
        if accounting is not None:
            for seg in accounting.findall('.//AccountingSegment'):
                seg_type = seg.get('type', '')
                seg_val = _get_text(seg, 'Name') or _get_text(seg, 'Description')
                if seg_type == 'CostCenter':
                    line.cost_center = seg_val
                elif seg_type in ('Account', 'GLAccount'):
                    line.gl_account = seg_val
                    line.account_no = seg_val  # direct P21 mapping

        # Line Extrinsics (MRO work orders, asset IDs)
        for ext in item_out.findall('Extrinsic'):
            name = ext.get('name', '')
            val = ext.text or ''
            if name in ('WorkOrderNumber', 'MaintenanceOrder'):
                line.work_order = val
            elif name in ('AssetID', 'EquipmentID'):
                line.asset_id = val

        # Line Comments → extended_desc
        line_comments = item_out.find('Comments')
        if line_comments is not None:
            comment_text = (line_comments.text or '').strip()
            line.notes = comment_text
            line.extended_desc = comment_text

        lines.append(line)

    return header, lines, content


def parse_pdf(file_path: str, doc_intel_endpoint: str, doc_intel_key: str) -> tuple[POHeader, list[POLineItem], str]:
    """Parse PO PDF using Azure Document Intelligence prebuilt invoice model."""
    from azure.ai.formrecognizer import DocumentAnalysisClient
    from azure.core.credentials import AzureKeyCredential

    client = DocumentAnalysisClient(
        endpoint=doc_intel_endpoint,
        credential=AzureKeyCredential(doc_intel_key)
    )

    with open(file_path, 'rb') as f:
        poller = client.begin_analyze_document('prebuilt-invoice', f)
    result = poller.result()

    if not result.documents:
        raise ValueError(f"No invoice/PO data found in {file_path}")

    doc = result.documents[0]
    fields = doc.fields

    def _field_val(name, default=''):
        f = fields.get(name)
        if f is None:
            return default
        if f.value_type == 'currency':
            return f.value.amount if f.value else 0
        return f.value if f.value else (f.content if f.content else default)

    header = POHeader(
        po_no=str(_field_val('PurchaseOrder') or _field_val('InvoiceId', '')),
        order_date='',
        source_system=SourceSystem.EMAIL,  # PDF attachments come via email too
        # P21 defaults for Vega
        po_type='D',
        source_type=VEGA_SOURCE_TYPE,
        company_no=1,
        location_id=10,
        branch_id='000',
        ship2_country='US',
        approved='Y',
    )

    # Date
    inv_date = fields.get('InvoiceDate')
    if inv_date and inv_date.value:
        header.order_date = str(inv_date.value)[:10]

    header.currency_id = 'USD'

    # Vendor
    vendor = fields.get('VendorName')
    if vendor:
        header.supplier_name = vendor.value or vendor.content or ''

    # Ship To / Customer → ship2_* columns
    customer = fields.get('CustomerName')
    if customer:
        header.ship2_name = customer.value or customer.content or ''
    cust_addr = fields.get('CustomerAddress')
    if cust_addr and cust_addr.value:
        addr = cust_addr.value
        header.ship2_add1 = addr.street_address or ''
        header.ship2_city = addr.city or ''
        header.ship2_state = addr.state or ''
        header.ship2_zip = addr.postal_code or ''

    # Ship To override (more specific)
    ship_addr = fields.get('ShippingAddress')
    if ship_addr and ship_addr.value:
        addr = ship_addr.value
        if not header.ship2_name:
            header.ship2_name = str(_field_val('ShippingAddressRecipient', ''))
        header.ship2_add1 = addr.street_address or ''
        header.ship2_city = addr.city or ''
        header.ship2_state = addr.state or ''
        header.ship2_zip = addr.postal_code or ''

    header.terms = str(_field_val('PaymentTerm', ''))

    # Line items
    lines: list[POLineItem] = []
    items_field = fields.get('Items')
    if items_field and items_field.value:
        for i, item in enumerate(items_field.value):
            item_fields = item.value if item.value else {}
            line = POLineItem(
                line_no=(i + 1) * 10,
                date_due=header.order_date,
                # P21 defaults
                source_type=VEGA_SOURCE_TYPE,
                calc_type='MULTIPLIER',
                calc_value=1.0,
                unit_size=1.0,
                unit_quantity=1.0,
                pricing_unit_size=1.0,
                inventory_flag='N',
            )

            if line.date_due:
                line.required_date = line.date_due

            desc = item_fields.get('Description')
            if desc:
                line.item_description = desc.value or desc.content or ''

            qty = item_fields.get('Quantity')
            if qty and qty.value:
                line.qty_ordered = float(qty.value)

            price = item_fields.get('UnitPrice')
            if price and price.value:
                line.unit_price = price.value.amount if hasattr(price.value, 'amount') else float(price.value)
                line.unit_price_display = line.unit_price
                line.base_ut_price = line.unit_price

            amount = item_fields.get('Amount')
            if amount and amount.value:
                amt = amount.value.amount if hasattr(amount.value, 'amount') else float(amount.value)
                if line.unit_price == 0 and line.qty_ordered > 0:
                    line.unit_price = amt / line.qty_ordered
                    line.unit_price_display = line.unit_price
                    line.base_ut_price = line.unit_price

            prod_code = item_fields.get('ProductCode')
            if prod_code:
                line.supplier_part_id = prod_code.value or prod_code.content or ''

            uom = item_fields.get('Unit')
            if uom and uom.value:
                line.unit_of_measure = uom.value
            line.pricing_unit = line.unit_of_measure

            date_f = item_fields.get('Date')
            if date_f and date_f.value:
                line.date_due = str(date_f.value)[:10]
                line.required_date = line.date_due

            lines.append(line)

    raw_text = result.content[:8000] if result.content else ''
    return header, lines, raw_text


def generate_cxml_response(payload_id: str, status_code: int = 200, status_text: str = "OK") -> str:
    """Generate a cXML Response document for Ariba Network.
    Must be returned within 60 seconds of receiving the OrderRequest.
    """
    timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S+00:00')
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE cXML SYSTEM "http://xml.cxml.org/schemas/cXML/1.2.069/cXML.dtd">
<cXML payloadID="{payload_id}_response" timestamp="{timestamp}" version="1.2.069">
  <Response>
    <Status code="{status_code}" text="{status_text}">
      {status_text}
    </Status>
  </Response>
</cXML>"""


def _get_text(element, tag: str) -> str:
    """Safely get text from child element, handling namespaces."""
    child = element.find(tag)
    if child is not None and child.text:
        return child.text.strip()
    for child in element:
        if child.tag.endswith(tag) and child.text:
            return child.text.strip()
    return ''
