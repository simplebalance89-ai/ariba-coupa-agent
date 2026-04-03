"""
processing_agent.py — Process normalized POs: crosswalk lookup, confidence scoring, CISM generation.
"""

import os
import json
import hashlib
import logging
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, asdict

from models import POHeader, POLineItem
from cism_generator import generate_cism_file
from services.processing.blob_uploader import BlobUploader
from services.processing.confidence_scorer import score_payload, ConfidenceResult


# ── Configuration ────────────────────────────────────────────────────────────

CROSSWALK_VENDOR_CSV = os.environ.get("CROSSWALK_VENDOR_CSV", "./crosswalks/vendor_crosswalk.csv")
CROSSWALK_ITEM_CSV = os.environ.get("CROSSWALK_ITEM_CSV", "./crosswalks/item_crosswalk.csv")

# Confidence thresholds
VENDOR_GREEN = 0.90
VENDOR_YELLOW = 0.65
ITEM_GREEN = 0.90
ITEM_YELLOW = 0.60


# ── Data Models ───────────────────────────────────────────────────────────────

@dataclass
class CrosswalkMatch:
    source_id: str
    p21_id: str
    p21_name: str
    match_score: float
    match_method: str  # 'exact', 'fuzzy', 'soundex'


@dataclass
class ProcessedLine:
    line_no: int
    item_id_raw: str
    item_id_p21: Optional[str]
    item_name_p21: Optional[str]
    description: str
    quantity: float
    unit_of_measure: str
    unit_price: float
    extended_price: float
    crosswalk_score: float
    confidence: str  # green, yellow, red


@dataclass
class ProcessedPO:
    intake_id: str
    source: str  # ariba, coupa, vega, direct
    format: str  # cxml, pdf, text
    received_at: str
    po_number: str
    vendor_name_raw: str
    vendor_id_raw: str
    vendor_id_p21: Optional[str]
    vendor_name_p21: Optional[str]
    vendor_match_score: float
    lines: List[ProcessedLine]
    overall_confidence: str
    review_required: bool
    cism_blob_path: Optional[str] = None
    raw_json: Optional[Dict] = None


# ── Crosswalk Engine ──────────────────────────────────────────────────────────

class CrosswalkEngine:
    """Lookup vendor and item mappings from crosswalk data."""
    
    def __init__(self):
        self.vendors: Dict[str, CrosswalkMatch] = {}
        self.items: Dict[str, List[CrosswalkMatch]] = {}
        self.logger = logging.getLogger(__name__)
        self._load_crosswalks()
    
    def _load_crosswalks(self):
        """Load crosswalk CSVs into memory."""
        import csv
        
        # Load vendors
        if os.path.exists(CROSSWALK_VENDOR_CSV):
            with open(CROSSWALK_VENDOR_CSV, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    key = f"{row.get('source_system', 'unknown')}:{row.get('source_vendor_id', '')}"
                    self.vendors[key] = CrosswalkMatch(
                        source_id=row.get('source_vendor_id', ''),
                        p21_id=row.get('p21_vendor_id', ''),
                        p21_name=row.get('p21_vendor_name', ''),
                        match_score=float(row.get('match_score', 1.0)),
                        match_method=row.get('match_method', 'exact')
                    )
            self.logger.info(f"Loaded {len(self.vendors)} vendor crosswalks")
        else:
            self.logger.warning(f"Vendor crosswalk not found: {CROSSWALK_VENDOR_CSV}")
        
        # Load items
        if os.path.exists(CROSSWALK_ITEM_CSV):
            with open(CROSSWALK_ITEM_CSV, 'r') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    key = f"{row.get('source_system', 'unknown')}:{row.get('source_item_id', '')}"
                    if key not in self.items:
                        self.items[key] = []
                    self.items[key].append(CrosswalkMatch(
                        source_id=row.get('source_item_id', ''),
                        p21_id=row.get('p21_item_id', ''),
                        p21_name=row.get('p21_item_desc', ''),
                        match_score=float(row.get('match_score', 1.0)),
                        match_method=row.get('match_method', 'exact')
                    ))
            self.logger.info(f"Loaded {len(self.items)} item crosswalks")
        else:
            self.logger.warning(f"Item crosswalk not found: {CROSSWALK_ITEM_CSV}")
    
    def lookup_vendor(self, source_system: str, vendor_id_raw: str, 
                     vendor_name: str = "") -> Tuple[Optional[str], Optional[str], float]:
        """
        Lookup vendor in crosswalk.
        Returns: (p21_vendor_id, p21_vendor_name, match_score)
        """
        # Try exact match first
        key = f"{source_system}:{vendor_id_raw}"
        if key in self.vendors:
            match = self.vendors[key]
            return match.p21_id, match.p21_name, match.match_score
        
        # Try P21 internal lookup (source_system = 'P21')
        key = f"P21:{vendor_id_raw}"
        if key in self.vendors:
            match = self.vendors[key]
            return match.p21_id, match.p21_name, 1.0
        
        # Fuzzy fallback by name (SOUNDEX or contains)
        if vendor_name:
            for match in self.vendors.values():
                if vendor_name.lower() in match.p21_name.lower() or \
                   match.p21_name.lower() in vendor_name.lower():
                    return match.p21_id, match.p21_name, 0.75
        
        return None, None, 0.0
    
    def lookup_item(self, source_system: str, item_id_raw: str,
                   vendor_id_p21: Optional[str] = None) -> Tuple[Optional[str], Optional[str], float]:
        """
        Lookup item in crosswalk.
        Returns: (p21_item_id, p21_item_desc, match_score)
        """
        key = f"{source_system}:{item_id_raw}"
        
        if key in self.items:
            matches = self.items[key]
            
            # If vendor known, prefer vendor-specific match
            if vendor_id_p21:
                for m in matches:
                    # Check if this item is for this vendor
                    return m.p21_id, m.p21_name, m.match_score
            
            # Return first (best) match
            m = matches[0]
            return m.p21_id, m.p21_name, m.match_score
        
        # Try P21 internal
        key = f"P21:{item_id_raw}"
        if key in self.items:
            m = self.items[key][0]
            return m.p21_id, m.p21_name, 1.0
        
        return None, None, 0.0


# ── Processing Agent ──────────────────────────────────────────────────────────

class ProcessingAgent:
    """Process normalized POs through crosswalk and generate CISM."""
    
    def __init__(self):
        self.crosswalk = CrosswalkEngine()
        self.blob = BlobUploader()
        self.logger = logging.getLogger(__name__)
    
    def generate_intake_id(self, po_number: str, vendor: str, source: str) -> str:
        """Generate unique intake ID for duplicate detection."""
        hash_input = f"{po_number}:{vendor}:{source}:{datetime.utcnow().strftime('%Y%m%d')}"
        return hashlib.sha256(hash_input.encode()).hexdigest()[:16].upper()
    
    def process_po(self, parsed_po: Dict[str, Any]) -> ProcessedPO:
        """
        Process a parsed PO through crosswalk and scoring.
        
        Input: Parsed PO dict from cXML/PDF parser
        Output: ProcessedPO with crosswalk mappings and confidence
        """
        source = parsed_po.get("source", "direct")
        format_type = parsed_po.get("format", "unknown")
        po_number = parsed_po.get("po_number", "")
        vendor_id_raw = parsed_po.get("vendor_id", "")
        vendor_name = parsed_po.get("vendor_name", "")
        
        # Generate intake ID
        intake_id = self.generate_intake_id(po_number, vendor_id_raw, source)
        
        # Vendor crosswalk
        vendor_p21_id, vendor_p21_name, vendor_score = self.crosswalk.lookup_vendor(
            source, vendor_id_raw, vendor_name
        )
        
        # Process lines
        processed_lines: List[ProcessedLine] = []
        for line in parsed_po.get("lines", []):
            item_raw = line.get("item_id", line.get("supplier_part_id", ""))
            
            # Item crosswalk
            item_p21, item_name_p21, item_score = self.crosswalk.lookup_item(
                source, item_raw, vendor_p21_id
            )
            
            # Determine line confidence
            if item_score >= ITEM_GREEN:
                line_conf = "green"
            elif item_score >= ITEM_YELLOW:
                line_conf = "yellow"
            else:
                line_conf = "red"
            
            processed_lines.append(ProcessedLine(
                line_no=line.get("line_no", 0),
                item_id_raw=item_raw,
                item_id_p21=item_p21,
                item_name_p21=item_name_p21,
                description=line.get("description", ""),
                quantity=float(line.get("quantity", 0)),
                unit_of_measure=line.get("uom", "EA"),
                unit_price=float(line.get("unit_price", 0)),
                extended_price=float(line.get("quantity", 0)) * float(line.get("unit_price", 0)),
                crosswalk_score=item_score,
                confidence=line_conf
            ))
        
        # Overall confidence
        vendor_ok = vendor_p21_id is not None
        green_count = sum(1 for l in processed_lines if l.confidence == "green")
        red_count = sum(1 for l in processed_lines if l.confidence == "red")
        total_lines = len(processed_lines)
        green_ratio = green_count / total_lines if total_lines else 0
        
        if green_ratio >= 0.85 and vendor_ok and red_count == 0:
            overall_conf = "green"
            review_required = False
        elif red_count > 0 or not vendor_ok:
            overall_conf = "red"
            review_required = True
        else:
            overall_conf = "yellow"
            review_required = True
        
        return ProcessedPO(
            intake_id=intake_id,
            source=source,
            format=format_type,
            received_at=parsed_po.get("received_at", datetime.utcnow().isoformat()),
            po_number=po_number,
            vendor_name_raw=vendor_name,
            vendor_id_raw=vendor_id_raw,
            vendor_id_p21=vendor_p21_id,
            vendor_name_p21=vendor_p21_name,
            vendor_match_score=vendor_score,
            lines=processed_lines,
            overall_confidence=overall_conf,
            review_required=review_required,
            raw_json=parsed_po
        )
    
    def generate_and_upload_cism(self, processed: ProcessedPO, 
                                 auto_approve: bool = False) -> Optional[str]:
        """
        Generate CISM file and upload to blob.
        
        If auto_approve=True and confidence=green, uploads directly to approved/
        Otherwise, waits for OMS portal review.
        
        Returns blob path or None if review required.
        """
        # Build POHeader and POLineItem models for CISM generator
        header = POHeader(
            po_no=processed.po_number,
            order_date=processed.received_at[:10] if processed.received_at else datetime.now().strftime("%Y-%m-%d"),
            supplier_id=int(processed.vendor_id_p21) if processed.vendor_id_p21 and processed.vendor_id_p21.isdigit() else 0,
            ship2_name=processed.vendor_name_p21 or "ENPRO INDUSTRIES",
            location_id=10,
            buyer="SYSTEM",
            external_po_no=processed.intake_id[:20]
        )
        
        lines = [
            POLineItem(
                line_no=l.line_no,
                supplier_part_id=l.item_id_p21 or l.item_id_raw,
                qty_ordered=l.quantity,
                unit_price=l.unit_price,
                unit_of_measure=l.unit_of_measure,
                date_due=datetime.now().strftime("%Y-%m-%d")  # TODO: parse from PO
            )
            for l in processed.lines
        ]
        
        # Generate CISM file locally
        cism_path = generate_cism_file(header, lines)
        
        # Determine folder based on confidence and auto-approve
        if processed.overall_confidence == "green" and auto_approve:
            folder = "approved"
        elif processed.overall_confidence == "red":
            folder = "rejected"
        else:
            folder = "staging"  # Waiting for OMS review
        
        # Upload to blob
        blob_path = self.blob.upload_cism(cism_path, processed.po_number, folder)
        processed.cism_blob_path = blob_path
        
        self.logger.info(f"CISM uploaded: {blob_path} (confidence: {processed.overall_confidence})")
        
        return blob_path


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    # Test with sample data
    test_po = {
        "source": "ariba",
        "format": "cxml",
        "po_number": "4500123456",
        "vendor_id": "VENDOR123",
        "vendor_name": "Test Supplier Inc",
        "received_at": datetime.utcnow().isoformat(),
        "lines": [
            {"line_no": 1, "item_id": "PART-001", "description": "Filter Element", "quantity": 10, "unit_price": 125.00, "uom": "EA"},
            {"line_no": 2, "item_id": "PART-002", "description": "Seal Kit", "quantity": 5, "unit_price": 89.90, "uom": "EA"}
        ]
    }
    
    agent = ProcessingAgent()
    result = agent.process_po(test_po)
    
    print(f"\nProcessed PO: {result.po_number}")
    print(f"Intake ID: {result.intake_id}")
    print(f"Vendor: {result.vendor_name_raw} -> {result.vendor_name_p21} (score: {result.vendor_match_score})")
    print(f"Overall Confidence: {result.overall_confidence}")
    print(f"Review Required: {result.review_required}")
    print(f"\nLines:")
    for line in result.lines:
        print(f"  {line.line_no}: {line.item_id_raw} -> {line.item_id_p21 or 'NOT FOUND'} ({line.confidence})")
