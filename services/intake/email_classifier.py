"""
email_classifier.py — Classify incoming emails by source and format.
No ML — regex only. Fast, deterministic, no training needed.
"""

import re
from dataclasses import dataclass
from typing import Literal, List


@dataclass
class EmailClassification:
    source: Literal["ariba", "coupa", "vega", "direct"]
    format: Literal["cxml", "pdf", "text", "unknown"]
    confidence: float  # 0.0 - 1.0


# ── Source Detection Patterns ─────────────────────────────────────────────────

SOURCE_PATTERNS = {
    "ariba": [
        r"@ariba\.com",
        r"ordersender.*ariba",
        r"SAP\s*Ariba",
        r"cxml.*ariba",
        r"ariba.*network",
        r"<cXML",
        r"OrderRequest.*cXML",
    ],
    "coupa": [
        r"@coupa\.com",
        r"coupahost\.com",
        r"Coupa\s*Procurement",
        r"X-Coupa",
        r"coupa.*supplier",
    ],
    "vega": [
        r"@vega[-.]?mro\.com",
        r"Vega.*PO",
        r"Vega.*Purchase",
        r"vegamro",
    ],
}

FORMAT_PATTERNS = {
    "cxml": [
        r"\.xml$",
        r"\.cxml$",
        r"<cXML",
        r"OrderRequest",
        r"Content-Type.*xml",
    ],
    "pdf": [
        r"\.pdf$",
        r"application/pdf",
    ],
    "text": [
        r"\.txt$",
        r"\.csv$",
        r"text/plain",
    ],
}


def classify_email(sender: str, subject: str, attachments: List[str]) -> EmailClassification:
    """
    Classify email by source system and attachment format.
    
    Args:
        sender: From email address
        subject: Email subject line
        attachments: List of attachment filenames
    
    Returns:
        EmailClassification with source, format, and confidence
    """
    combined = f"{sender} {subject}".lower()
    
    # ── Source Classification ─────────────────────────────────────────────────
    source = "direct"
    source_conf = 0.5
    
    for src, patterns in SOURCE_PATTERNS.items():
        hits = sum(1 for p in patterns if re.search(p, combined, re.IGNORECASE))
        if hits:
            source = src
            source_conf = min(0.7 + hits * 0.15, 1.0)
            break
    
    # Boost confidence for known domain patterns
    if source == "ariba" and "@ariba.com" in sender.lower():
        source_conf = 1.0
    elif source == "coupa" and "@coupa.com" in sender.lower():
        source_conf = 1.0
    
    # ── Format Classification ─────────────────────────────────────────────────
    fmt = "unknown"
    fmt_conf = 0.5
    
    # Check attachments first
    att_str = " ".join(attachments).lower()
    for ftype, patterns in FORMAT_PATTERNS.items():
        hits = sum(1 for p in patterns if re.search(p, att_str, re.IGNORECASE))
        if hits:
            fmt = ftype
            fmt_conf = min(0.8 + hits * 0.1, 1.0)
            break
    
    # Default to PDF if attachments present but unknown type
    if fmt == "unknown" and attachments:
        # Check if any attachment looks like PDF
        if any(a.lower().endswith('.pdf') for a in attachments):
            fmt = "pdf"
            fmt_conf = 0.9
        # Check for XML/cXML
        elif any(a.lower().endswith(('.xml', '.cxml')) for a in attachments):
            fmt = "cxml"
            fmt_conf = 0.9
    
    # No attachments = text body only
    if not attachments:
        fmt = "text"
        fmt_conf = 0.6
    
    # ── Combined Confidence ───────────────────────────────────────────────────
    # Weight source and format equally
    overall_conf = (source_conf + fmt_conf) / 2
    
    return EmailClassification(
        source=source,
        format=fmt,
        confidence=round(overall_conf, 2)
    )


def is_po_email(subject: str, sender: str, body_preview: str = "") -> bool:
    """
    Quick filter: is this likely a purchase order email?
    Used before full classification to skip non-PO emails.
    """
    combined = f"{subject} {sender} {body_preview}".lower()
    
    po_indicators = [
        r"purchase.?order",
        r"po.?#?\s*\d+",
        r"order.?confirmation",
        r"new.?order",
        r"order.?request",
        r"cxml",
        r"orderequest",
    ]
    
    po_hits = sum(1 for p in po_indicators if re.search(p, combined, re.IGNORECASE))
    
    # Negative indicators (likely not a PO)
    negative_indicators = [
        r"quotation",
        r"quote.?request",
        r"rfq",
        r"spam",
        r"unsubscribe",
        r"marketing",
        r"newsletter",
    ]
    
    neg_hits = sum(1 for p in negative_indicators if re.search(p, combined, re.IGNORECASE))
    
    return po_hits > 0 and po_hits > neg_hits


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Test cases
    test_cases = [
        ("ordersender@ariba.com", "New Purchase Order #4500123456", ["order_4500123456.xml"]),
        ("supplier@coupa.com", "Purchase Order from EnPro Industries", ["PO_12345.pdf"]),
        ("orders@vega-mro.com", "Vega MRO PO 789012", ["PO_789012.pdf"]),
        ("supplier@example.com", "Order Confirmation", ["invoice.pdf"]),
        ("random@unknown.com", "Hello", []),
    ]
    
    print("Email Classification Tests:\n")
    for sender, subject, attachments in test_cases:
        result = classify_email(sender, subject, attachments)
        is_po = is_po_email(subject, sender)
        print(f"From: {sender[:30]:<30} | Subject: {subject[:35]:<35}")
        print(f"  -> Source: {result.source:<8} | Format: {result.format:<6} | Conf: {result.confidence:.2f} | IsPO: {is_po}")
        print()
