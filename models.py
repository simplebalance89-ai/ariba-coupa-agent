"""
models.py — Pydantic models for Ariba/Coupa PO Automation Agent.
Forked from Vega MRO agent, extended with crosswalk + confidence fields.
Aligned to real P21 po_hdr / po_line schema (validated March 2026).
"""

from datetime import datetime
from enum import Enum
from typing import Optional, Literal
from pydantic import BaseModel, Field


class POStatus(str, Enum):
    RECEIVED = "RECEIVED"
    PARSED = "PARSED"
    VALIDATED = "VALIDATED"
    IMPORTED = "IMPORTED"
    DUPLICATE = "DUPLICATE"
    ERROR = "ERROR"
    REJECTED = "REJECTED"
    APPROVED = "APPROVED"
    PENDING_REVIEW = "PENDING_REVIEW"


class OrderType(str, Enum):
    REGULAR = "regular"
    BLANKET = "blanket"
    RELEASE = "release"


class SourceSystem(str, Enum):
    EMAIL = "EMAIL"
    ARIBA = "ARIBA"
    COUPA = "COUPA"
    VEGA = "VEGA"
    PDF = "PDF"
    MANUAL = "MANUAL"
    DIRECT = "DIRECT"


P21_PO_TYPE_MAP = {
    OrderType.REGULAR: "D",
    OrderType.BLANKET: "D",
    OrderType.RELEASE: "D",
}

VEGA_SOURCE_TYPE = 951


class POLineItem(BaseModel):
    """Maps to dbo.po_line columns in P21."""
    line_no: int = 0
    line_type: str = ""
    inv_mast_uid: Optional[int] = None

    # Item info
    supplier_part_id: str = ""
    item_description: str = ""
    extended_desc: str = ""
    mfg_part_no: str = ""

    # Units & pricing
    unit_of_measure: str = "EA"
    unit_size: float = 1.0
    unit_quantity: float = 1.0
    pricing_unit: str = ""
    pricing_unit_size: float = 1.0
    qty_ordered: float = 0
    unit_price: float = 0
    unit_price_display: float = 0
    base_ut_price: float = 0
    list_price_multiplier: float = 0
    calc_type: str = "MULTIPLIER"
    calc_value: float = 1.0

    # Dates
    date_due: str = ""
    required_date: str = ""
    expected_ship_date: str = ""

    # Line metadata
    source_type: int = VEGA_SOURCE_TYPE
    account_no: str = ""
    contract_number: str = ""
    entered_as_code: str = ""
    country_of_origin: str = ""
    desired_receipt_location_id: Optional[int] = None

    # MRO-specific
    cost_center: str = ""
    gl_account: str = ""
    work_order: str = ""
    asset_id: str = ""
    inventory_flag: str = "N"

    # Line-level ship-to override
    ship_to_name: str = ""
    ship_to_address: str = ""
    ship_to_city: str = ""
    ship_to_state: str = ""
    ship_to_zip: str = ""

    # Notes
    notes: str = ""

    # === NEW: Crosswalk + Confidence fields ===
    item_id_p21: Optional[str] = None
    crosswalk_match_score: Optional[float] = None
    confidence: Optional[Literal["green", "yellow", "red"]] = None


class POHeader(BaseModel):
    """Maps to dbo.po_hdr columns in P21."""
    po_no: str = ""
    order_date: str = ""
    order_type: OrderType = OrderType.REGULAR
    po_type: str = "D"
    source_type: int = VEGA_SOURCE_TYPE
    company_no: int = 1
    order_version: str = "1"
    currency_id: str = ""
    source_system: SourceSystem = SourceSystem.EMAIL

    # Supplier
    supplier_id: Optional[int] = None
    supplier_name: str = ""
    supplier_email: str = ""
    vendor_id: Optional[int] = None

    # Ship To
    ship2_name: str = ""
    ship2_add1: str = ""
    ship2_add2: str = ""
    ship2_add3: str = ""
    ship2_city: str = ""
    ship2_state: str = ""
    ship2_zip: str = ""
    ship2_country: str = "US"

    # PO metadata
    po_desc: str = ""
    requested_by: str = ""
    contact_id: Optional[int] = None
    location_id: int = 10
    division_id: Optional[int] = None
    branch_id: str = "000"
    purchase_group_id: Optional[int] = None
    external_po_no: str = ""

    # Shipping
    terms: str = ""
    fob: str = ""
    carrier_id: Optional[int] = None
    ship_via_desc: str = ""
    freight_terms: str = ""

    # Buyer info
    buyer: str = ""
    buyer_email: str = ""
    buyer_phone: str = ""
    comments: str = ""

    transmission_method: str = ""
    approved: str = "Y"

    # MRO-specific
    blanket_po_no: str = ""
    contract_no: str = ""
    release_no: str = ""

    # === Crosswalk + Confidence fields ===
    vendor_id_raw: Optional[str] = None
    vendor_id_p21: Optional[str] = None
    vendor_match_score: Optional[float] = None

    # === Customer-focused crosswalk (SO creation) ===
    customer_id_p21: Optional[str] = None
    customer_name_p21: Optional[str] = None
    customer_match_score: Optional[float] = None
    customer_match_method: Optional[str] = None
    ship_to_id_p21: Optional[str] = None
    ship_to_match_score: Optional[float] = None


class POPayload(BaseModel):
    """Full PO with confidence scoring — used by processing pipeline."""
    intake_id: str
    source: SourceSystem
    format: Literal["cxml", "pdf", "text"]
    received_at: str
    header: POHeader
    lines: list[POLineItem]
    raw_content: str = ""
    classification_confidence: float = 0.0
    overall_confidence: Optional[Literal["green", "yellow", "red"]] = None
    review_required: bool = False
    cism_blob_path: Optional[str] = None
    status: POStatus = POStatus.RECEIVED


class POImportResult(BaseModel):
    status: POStatus
    po_no: str
    message: str = ""
    supplier: str = ""
    ship_to: str = ""
    total: float = 0
    lines: int = 0
    buyer: str = ""
    staging_id: Optional[int] = None
    confidence: Optional[str] = None
    cism_path: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.now)


class HealthResponse(BaseModel):
    status: str
    version: str
    environment: str
    services: dict[str, str]
