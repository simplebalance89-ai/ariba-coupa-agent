"""
local_store.py -- File-based PO store for sandbox/Render (no SQL dependency).

Stores processed POs as JSON files in ./po_store/.
Provides the same interface as the SQL staging log.
"""

import json
import logging
import os
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

STORE_DIR = os.environ.get("PO_STORE_DIR", "/app/data/po_store")


def _ensure_dir():
    os.makedirs(STORE_DIR, exist_ok=True)


def save_po(intake_id: str, data: dict):
    """Save a processed PO to local store."""
    _ensure_dir()
    data["_stored_at"] = datetime.utcnow().isoformat()
    path = os.path.join(STORE_DIR, f"{intake_id}.json")
    with open(path, "w") as f:
        json.dump(data, f, default=str)
    logger.info(f"Stored PO {intake_id} -> {path}")


def get_po(intake_id: str) -> Optional[dict]:
    """Get a single PO by intake_id."""
    path = os.path.join(STORE_DIR, f"{intake_id}.json")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def list_pos(status: str = None, confidence: str = None) -> list:
    """List all stored POs, optionally filtered."""
    _ensure_dir()
    results = []
    for fname in os.listdir(STORE_DIR):
        if not fname.endswith(".json"):
            continue
        path = os.path.join(STORE_DIR, fname)
        try:
            with open(path) as f:
                data = json.load(f)
            if status and data.get("review_status") != status:
                continue
            if confidence and data.get("confidence") != confidence:
                continue
            results.append(data)
        except Exception as e:
            logger.warning(f"Bad PO file {fname}: {e}")
    results.sort(key=lambda x: x.get("_stored_at", ""), reverse=True)
    return results


def update_po(intake_id: str, updates: dict):
    """Update fields on a stored PO."""
    data = get_po(intake_id)
    if not data:
        return False
    data.update(updates)
    data["_updated_at"] = datetime.utcnow().isoformat()
    path = os.path.join(STORE_DIR, f"{intake_id}.json")
    with open(path, "w") as f:
        json.dump(data, f, default=str)
    return True


def is_duplicate(intake_id: str, po_no: str, source: str) -> bool:
    """Check if this PO was already processed."""
    # Check by intake_id
    if get_po(intake_id):
        return True
    # Check by po_no + source
    for po in list_pos():
        if po.get("po_no") == po_no and po.get("source") == source:
            return True
    return False


def get_stats() -> dict:
    """Get summary statistics."""
    all_pos = list_pos()
    total = len(all_pos)
    green = sum(1 for p in all_pos if p.get("confidence") == "green")
    yellow = sum(1 for p in all_pos if p.get("confidence") == "yellow")
    red = sum(1 for p in all_pos if p.get("confidence") == "red")
    approved = sum(1 for p in all_pos if p.get("review_status") == "approved")
    rejected = sum(1 for p in all_pos if p.get("review_status") == "rejected")
    pending = sum(1 for p in all_pos if p.get("review_status") == "pending")

    return {
        "total": total,
        "green": green,
        "yellow": yellow,
        "red": red,
        "approved": approved,
        "rejected": rejected,
        "pending": pending,
    }
