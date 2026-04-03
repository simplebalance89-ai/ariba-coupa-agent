"""
confidence_scorer.py — Score PO confidence based on crosswalk matches.
"""

from dataclasses import dataclass
from typing import List, Literal


# ── Thresholds ───────────────────────────────────────────────────────────────

VENDOR_GREEN = 0.90
VENDOR_YELLOW = 0.65
ITEM_GREEN = 0.90
ITEM_YELLOW = 0.60
GREEN_RATIO_REQUIRED = 0.85  # 85%+ lines must be green for auto-approve


@dataclass
class LineConfidence:
    line_no: int
    item_raw: str
    item_p21: str
    crosswalk_score: float
    confidence: Literal["green", "yellow", "red"]


@dataclass
class ConfidenceResult:
    vendor_score: float
    vendor_confidence: Literal["green", "yellow", "red"]
    line_results: List[LineConfidence]
    green_ratio: float
    red_count: int
    overall: Literal["green", "yellow", "red"]
    review_required: bool
    reason: str


def score_line(crosswalk_score: float) -> Literal["green", "yellow", "red"]:
    """Score a single line based on crosswalk match."""
    if crosswalk_score >= ITEM_GREEN:
        return "green"
    elif crosswalk_score >= ITEM_YELLOW:
        return "yellow"
    else:
        return "red"


def score_vendor(vendor_score: float) -> Literal["green", "yellow", "red"]:
    """Score vendor match."""
    if vendor_score >= VENDOR_GREEN:
        return "green"
    elif vendor_score >= VENDOR_YELLOW:
        return "yellow"
    else:
        return "red"


def score_payload(lines_crosswalk_scores: List[float], 
                 vendor_score: float,
                 vendor_matched: bool) -> ConfidenceResult:
    """
    Score entire PO payload for confidence.
    
    Args:
        lines_crosswalk_scores: List of match scores for each line (0.0 - 1.0)
        vendor_score: Vendor crosswalk match score
        vendor_matched: Whether vendor was found in crosswalk
    
    Returns:
        ConfidenceResult with overall scoring
    """
    # Score each line
    line_results = []
    for i, score in enumerate(lines_crosswalk_scores, 1):
        line_conf = score_line(score)
        line_results.append(LineConfidence(
            line_no=i,
            item_raw="",  # Filled by caller if needed
            item_p21="",
            crosswalk_score=score,
            confidence=line_conf
        ))
    
    # Calculate ratios
    total = len(line_results)
    green_count = sum(1 for l in line_results if l.confidence == "green")
    yellow_count = sum(1 for l in line_results if l.confidence == "yellow")
    red_count = sum(1 for l in line_results if l.confidence == "red")
    
    green_ratio = green_count / total if total > 0 else 0.0
    
    # Vendor confidence
    vendor_conf = score_vendor(vendor_score) if vendor_matched else "red"
    
    # Overall confidence logic
    reason = ""
    
    if not vendor_matched:
        overall = "red"
        review_required = True
        reason = "Vendor not found in crosswalk"
    elif red_count > 0:
        overall = "red"
        review_required = True
        reason = f"{red_count} line(s) with no item match"
    elif vendor_conf == "red":
        overall = "red"
        review_required = True
        reason = "Vendor match confidence too low"
    elif green_ratio >= GREEN_RATIO_REQUIRED and vendor_conf == "green":
        overall = "green"
        review_required = False
        reason = "All matches high confidence"
    elif green_ratio >= 0.5:
        overall = "yellow"
        review_required = True
        reason = f"Partial matches: {green_count} green, {yellow_count} yellow"
    else:
        overall = "red"
        review_required = True
        reason = "Too many unmatched items"
    
    return ConfidenceResult(
        vendor_score=vendor_score,
        vendor_confidence=vendor_conf,
        line_results=line_results,
        green_ratio=green_ratio,
        red_count=red_count,
        overall=overall,
        review_required=review_required,
        reason=reason
    )


# ── 4-Dimension Customer PO Scoring ──────────────────────────────────────────

CUSTOMER_GREEN = 0.90
CUSTOMER_YELLOW = 0.65
SHIPTO_GREEN = 0.85
SHIPTO_YELLOW = 0.60


@dataclass
class CustomerPOResult:
    customer_score: float
    customer_confidence: Literal["green", "yellow", "red"]
    shipto_score: float
    shipto_confidence: Literal["green", "yellow", "red"]
    line_results: List[LineConfidence]
    green_ratio: float
    red_count: int
    is_duplicate: bool
    overall: Literal["green", "yellow", "red"]
    review_required: bool
    reason: str


def _score_dimension(score: float, green_thresh: float, yellow_thresh: float) -> Literal["green", "yellow", "red"]:
    if score >= green_thresh:
        return "green"
    elif score >= yellow_thresh:
        return "yellow"
    return "red"


def score_customer_po(
    customer_score: float,
    shipto_score: float,
    item_scores: List[float],
    is_duplicate: bool = False,
) -> CustomerPOResult:
    """
    4-dimension confidence scoring for customer PO processing.
    Weights: customer 0.30, ship-to 0.15, items 0.45, dedup 0.10
    """
    customer_conf = _score_dimension(customer_score, CUSTOMER_GREEN, CUSTOMER_YELLOW)
    shipto_conf = _score_dimension(shipto_score, SHIPTO_GREEN, SHIPTO_YELLOW)

    line_results = []
    for i, s in enumerate(item_scores, 1):
        line_results.append(LineConfidence(
            line_no=i, item_raw="", item_p21="",
            crosswalk_score=s, confidence=score_line(s),
        ))

    total = len(line_results)
    green_count = sum(1 for l in line_results if l.confidence == "green")
    red_count = sum(1 for l in line_results if l.confidence == "red")
    green_ratio = green_count / total if total else 0.0
    avg_item = sum(item_scores) / len(item_scores) if item_scores else 0.0

    dedup_pass = 0.0 if is_duplicate else 1.0
    weighted = (
        customer_score * 0.30
        + shipto_score * 0.15
        + avg_item * 0.45
        + dedup_pass * 0.10
    )

    # Overall determination
    if is_duplicate:
        overall = "red"
        review_required = True
        reason = "Duplicate PO detected"
    elif weighted >= 0.88 and customer_conf == "green" and red_count == 0:
        overall = "green"
        review_required = False
        reason = "All matches high confidence"
    elif weighted >= 0.60 and customer_conf != "red" and red_count <= 1:
        overall = "yellow"
        review_required = True
        reason = f"Partial matches: {green_count}/{total} green lines, customer={customer_conf}"
    else:
        overall = "red"
        review_required = True
        reasons = []
        if customer_conf == "red":
            reasons.append("customer not matched")
        if red_count > 0:
            reasons.append(f"{red_count} unmatched items")
        if not reasons:
            reasons.append("low overall score")
        reason = "; ".join(reasons)

    return CustomerPOResult(
        customer_score=customer_score,
        customer_confidence=customer_conf,
        shipto_score=shipto_score,
        shipto_confidence=shipto_conf,
        line_results=line_results,
        green_ratio=green_ratio,
        red_count=red_count,
        is_duplicate=is_duplicate,
        overall=overall,
        review_required=review_required,
        reason=reason,
    )


def get_review_priority(result: ConfidenceResult) -> int:
    """
    Get review queue priority (lower = higher priority).
    Used to sort review queue in OMS portal.
    """
    if result.overall == "red":
        return 1  # Urgent
    elif result.overall == "yellow":
        return 2  # Normal
    else:
        return 3  # Low (shouldn't be in queue)


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Test cases
    test_cases = [
        # Perfect match
        {"name": "Perfect", "vendor": 1.0, "vendor_ok": True, "lines": [1.0, 1.0, 1.0]},
        # One yellow
        {"name": "Mostly Good", "vendor": 1.0, "vendor_ok": True, "lines": [1.0, 0.9, 0.7]},
        # Missing vendor
        {"name": "Unknown Vendor", "vendor": 0.0, "vendor_ok": False, "lines": [1.0, 1.0]},
        # One red line
        {"name": "Missing Item", "vendor": 0.95, "vendor_ok": True, "lines": [1.0, 0.5, 0.0]},
        # All yellow
        {"name": "All Partial", "vendor": 0.8, "vendor_ok": True, "lines": [0.7, 0.75, 0.8]},
    ]
    
    print("Confidence Scoring Tests:\n")
    for test in test_cases:
        result = score_payload(test["lines"], test["vendor"], test["vendor_ok"])
        print(f"{test['name']:<15} | Overall: {result.overall:<6} | Review: {result.review_required} | {result.reason}")
        print(f"                Vendor: {result.vendor_confidence} ({result.vendor_score:.2f}) | Lines: {len(result.line_results)} | Green: {result.green_ratio:.0%}")
        print()
