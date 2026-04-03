"""
customer_crosswalk_engine.py -- Customer-focused crosswalk matching engine.

Matches incoming Ariba/Coupa PO data to P21 customers, items, and ship-to addresses
using CSV-based crosswalk tables generated from P21 SO exports.
"""

import csv
import logging
import os
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

from services.processing.address_normalizer import (
    normalize_name, normalize_address, normalize_zip,
    name_similarity, composite_address_score,
)

logger = logging.getLogger(__name__)

# Paths (overridable via env)
CROSSWALK_DIR = os.environ.get("CROSSWALK_DIR", "./crosswalks")


@dataclass
class CustomerMatch:
    p21_customer_id: str = ""
    p21_customer_name: str = ""
    p21_address_id: str = ""
    match_score: float = 0.0
    match_method: str = ""  # learned, exact_name_zip, fuzzy, po_pattern
    candidates: list = field(default_factory=list)  # top alternatives


@dataclass
class ItemMatch:
    p21_inv_mast_uid: str = ""
    p21_item_desc: str = ""
    unit_of_measure: str = ""
    product_group: str = ""
    match_score: float = 0.0
    match_method: str = ""  # customer_part, global_part, direct_item, fuzzy_desc
    price_in_range: bool = True


@dataclass
class DuplicateCheck:
    is_duplicate: bool = False
    existing_order_no: str = ""
    existing_order_date: str = ""
    completed: str = ""


def _read_csv(filename: str) -> list[dict]:
    path = os.path.join(CROSSWALK_DIR, filename)
    if not os.path.exists(path):
        logger.warning(f"Crosswalk file not found: {path}")
        return []
    with open(path, "r", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


class CustomerCrosswalkEngine:
    """CSV-based customer crosswalk matching engine."""

    def __init__(self, crosswalk_dir: str = None):
        global CROSSWALK_DIR
        if crosswalk_dir:
            CROSSWALK_DIR = crosswalk_dir

        self.customer_xw: list[dict] = []
        self.customer_items: dict[str, list[dict]] = defaultdict(list)  # cid → items
        self.global_items: dict[str, list[dict]] = defaultdict(list)  # part# → items
        self.po_history: dict[str, list[dict]] = defaultdict(list)  # po_no → records
        self.item_master: dict[str, dict] = {}  # inv_mast_uid → item
        self.customers_p21: dict[str, dict] = {}  # customer_id → customer record
        self.customer_defaults: dict[str, dict] = {}  # customer_id → {contact_id, address_id, terms, carrier_id}

        self._load()

    def _load(self):
        logger.info(f"Loading crosswalks from {CROSSWALK_DIR}")

        # Customer crosswalk
        self.customer_xw = _read_csv("customer_crosswalk.csv")
        logger.info(f"  customer_crosswalk: {len(self.customer_xw)} entries")

        # Customer-item crosswalk — index by customer and globally by part#
        for row in _read_csv("customer_item_crosswalk.csv"):
            cid = row.get("p21_customer_id", "")
            cpn = row.get("customer_part_number", "")
            if cid:
                self.customer_items[cid].append(row)
            if cpn:
                self.global_items[cpn].append(row)
        logger.info(f"  customer_item_crosswalk: {sum(len(v) for v in self.customer_items.values())} entries")

        # PO history — index by po_no
        for row in _read_csv("customer_po_history.csv"):
            po = row.get("customer_po_no", "")
            if po:
                self.po_history[po].append(row)
        logger.info(f"  customer_po_history: {sum(len(v) for v in self.po_history.values())} entries")

        # Item master
        for row in _read_csv("item_master_index.csv"):
            uid = row.get("p21_inv_mast_uid", "")
            if uid:
                self.item_master[uid] = row
        logger.info(f"  item_master_index: {len(self.item_master)} items")

        # Customers P21
        for row in _read_csv("customers_p21.csv"):
            cid = row.get("customer_id", "")
            if cid:
                self.customers_p21[cid] = row
        logger.info(f"  customers_p21: {len(self.customers_p21)} customers")

        # Customer defaults (contact_id, address_id, terms, carrier)
        for row in _read_csv("customer_defaults.csv"):
            cid = row.get("customer_id", "")
            if cid:
                self.customer_defaults[cid] = row
        logger.info(f"  customer_defaults: {len(self.customer_defaults)} entries")

    # ------------------------------------------------------------------
    # Customer Matching
    # ------------------------------------------------------------------

    def match_customer(
        self,
        ship2_name: str,
        ship2_add1: str = "",
        ship2_city: str = "",
        ship2_state: str = "",
        ship2_zip: str = "",
        buyer_email: str = "",
        source_system: str = "",
        source_customer_id: str = "",
        po_no: str = "",
    ) -> CustomerMatch:
        """Match incoming buyer to P21 customer. Returns best match."""

        # Step 1: Learned exact match (source_system + source_customer_id)
        if source_system and source_customer_id:
            for row in self.customer_xw:
                if (row.get("source_system", "") == source_system and
                        row.get("source_customer_id", "") == source_customer_id):
                    return CustomerMatch(
                        p21_customer_id=row["p21_customer_id"],
                        p21_customer_name=row.get("p21_customer_name", ""),
                        match_score=1.0,
                        match_method="learned",
                    )

        # Step 2: Ship-to name + zip exact match
        name_norm = normalize_name(ship2_name)
        zip5 = normalize_zip(ship2_zip)

        if name_norm and zip5:
            for row in self.customer_xw:
                if (row.get("ship2_name_normalized", "") == name_norm and
                        row.get("ship2_zip", "") == zip5):
                    return CustomerMatch(
                        p21_customer_id=row["p21_customer_id"],
                        p21_customer_name=row.get("p21_customer_name", ""),
                        match_score=0.95,
                        match_method="exact_name_zip",
                    )

        # Step 3: Fuzzy address match — score all candidates, return best
        candidates = []
        for row in self.customer_xw:
            score = composite_address_score(
                ship2_name, ship2_add1, ship2_city, ship2_state, ship2_zip,
                row.get("ship2_name", ""), row.get("ship2_add1", ""),
                row.get("ship2_city", ""), row.get("ship2_state", ""),
                row.get("ship2_zip", ""),
            )
            if score >= 0.50:
                candidates.append((score, row))

        candidates.sort(key=lambda x: x[0], reverse=True)

        # Step 4: PO number pattern match
        if po_no and po_no in self.po_history:
            po_records = self.po_history[po_no]
            # Most common customer for this PO#
            cid_counts = defaultdict(int)
            for r in po_records:
                cid_counts[r.get("p21_customer_id", "")] += 1
            best_cid = max(cid_counts, key=cid_counts.get) if cid_counts else ""
            if best_cid:
                cust = self.customers_p21.get(best_cid, {})
                po_match = CustomerMatch(
                    p21_customer_id=best_cid,
                    p21_customer_name=cust.get("customer_name", ""),
                    match_score=0.85,
                    match_method="po_pattern",
                )
                # If fuzzy also found this customer with higher score, use that
                for score, row in candidates:
                    if row.get("p21_customer_id", "") == best_cid and score > 0.85:
                        po_match.match_score = score
                        po_match.match_method = "fuzzy+po_confirm"
                        break
                po_match.candidates = [
                    {"customer_id": r[1]["p21_customer_id"],
                     "customer_name": r[1].get("p21_customer_name", ""),
                     "score": round(r[0], 3)}
                    for r in candidates[:3]
                ]
                return po_match

        # Return best fuzzy candidate
        if candidates and candidates[0][0] >= 0.65:
            best_score, best_row = candidates[0]
            return CustomerMatch(
                p21_customer_id=best_row["p21_customer_id"],
                p21_customer_name=best_row.get("p21_customer_name", ""),
                match_score=round(best_score, 4),
                match_method="fuzzy",
                candidates=[
                    {"customer_id": r[1]["p21_customer_id"],
                     "customer_name": r[1].get("p21_customer_name", ""),
                     "score": round(r[0], 3)}
                    for r in candidates[:3]
                ],
            )

        # No match
        return CustomerMatch(
            match_score=0.0,
            match_method="none",
            candidates=[
                {"customer_id": r[1]["p21_customer_id"],
                 "customer_name": r[1].get("p21_customer_name", ""),
                 "score": round(r[0], 3)}
                for r in candidates[:3]
            ],
        )

    # ------------------------------------------------------------------
    # Item Matching
    # ------------------------------------------------------------------

    def match_item(
        self,
        supplier_part_id: str,
        item_description: str = "",
        unit_price: float = 0.0,
        uom: str = "",
        p21_customer_id: str = "",
        source_system: str = "",
    ) -> ItemMatch:
        """Match an incoming line item to P21 inventory."""

        part = supplier_part_id.strip()
        if not part:
            return ItemMatch(match_score=0.0, match_method="none")

        # Step 1: Customer-specific part number lookup
        if p21_customer_id and p21_customer_id in self.customer_items:
            for row in self.customer_items[p21_customer_id]:
                if row.get("customer_part_number", "").strip() == part:
                    price_ok = self._check_price_range(row, unit_price)
                    return ItemMatch(
                        p21_inv_mast_uid=row.get("p21_inv_mast_uid", ""),
                        p21_item_desc=row.get("p21_item_desc", ""),
                        unit_of_measure=row.get("unit_of_measure", ""),
                        product_group=row.get("product_group_id", ""),
                        match_score=1.0,
                        match_method="customer_part",
                        price_in_range=price_ok,
                    )

        # Step 2: Global part number lookup
        if part in self.global_items:
            rows = self.global_items[part]
            best = rows[0]  # highest seen_count (pre-sorted)
            price_ok = self._check_price_range(best, unit_price)
            return ItemMatch(
                p21_inv_mast_uid=best.get("p21_inv_mast_uid", ""),
                p21_item_desc=best.get("p21_item_desc", ""),
                unit_of_measure=best.get("unit_of_measure", ""),
                product_group=best.get("product_group_id", ""),
                match_score=0.90,
                match_method="global_part",
                price_in_range=price_ok,
            )

        # Step 3: Direct item_id match against item master
        for uid, item in self.item_master.items():
            desc_norm = item.get("p21_item_desc_normalized", "")
            part_norm = normalize_name(part)
            if part_norm and part_norm == desc_norm:
                return ItemMatch(
                    p21_inv_mast_uid=uid,
                    p21_item_desc=item.get("p21_item_desc", ""),
                    unit_of_measure=item.get("default_selling_unit", ""),
                    product_group=item.get("product_group", ""),
                    match_score=0.95,
                    match_method="direct_item",
                )

        # Step 4: Fuzzy description match (if description provided)
        if item_description:
            desc_norm = normalize_name(item_description)
            best_score = 0.0
            best_item = None
            for uid, item in self.item_master.items():
                item_desc_norm = item.get("p21_item_desc_normalized", "")
                if not item_desc_norm:
                    continue
                score = name_similarity(desc_norm, item_desc_norm)
                if score > best_score:
                    best_score = score
                    best_item = item

            if best_item and best_score >= 0.60:
                return ItemMatch(
                    p21_inv_mast_uid=best_item.get("p21_inv_mast_uid", ""),
                    p21_item_desc=best_item.get("p21_item_desc", ""),
                    unit_of_measure=best_item.get("default_selling_unit", ""),
                    product_group=best_item.get("product_group", ""),
                    match_score=round(best_score * 0.80, 4),  # cap fuzzy desc at 0.80
                    match_method="fuzzy_desc",
                )

        return ItemMatch(match_score=0.0, match_method="none")

    def _check_price_range(self, row: dict, unit_price: float) -> bool:
        """Check if price falls within historical range (±30%)."""
        if not unit_price:
            return True
        try:
            p_min = float(row.get("unit_price_min", 0) or 0)
            p_max = float(row.get("unit_price_max", 0) or 0)
            if p_min == 0 and p_max == 0:
                return True
            margin = max(p_max - p_min, p_max * 0.30)
            return (p_min - margin) <= unit_price <= (p_max + margin)
        except (ValueError, TypeError):
            return True

    # ------------------------------------------------------------------
    # PO Duplicate Check
    # ------------------------------------------------------------------

    def check_duplicate_po(self, po_no: str, p21_customer_id: str = "") -> DuplicateCheck:
        """Check if this PO# already exists for this customer."""
        if not po_no:
            return DuplicateCheck()

        records = self.po_history.get(po_no, [])
        if not records:
            return DuplicateCheck()

        # If customer known, check for exact match
        if p21_customer_id:
            for r in records:
                if r.get("p21_customer_id", "") == p21_customer_id:
                    return DuplicateCheck(
                        is_duplicate=True,
                        existing_order_no=r.get("p21_order_no", ""),
                        existing_order_date=r.get("order_date", ""),
                        completed=r.get("completed", ""),
                    )

        # PO exists but for different customer — possible cross-ship
        return DuplicateCheck(
            is_duplicate=False,
            existing_order_no=records[0].get("p21_order_no", ""),
            existing_order_date=records[0].get("order_date", ""),
        )

    # ------------------------------------------------------------------
    # Customer detail lookup
    # ------------------------------------------------------------------

    def get_customer_detail(self, p21_customer_id: str) -> dict:
        """Get full customer record from P21 customer master."""
        return self.customers_p21.get(p21_customer_id, {})

    def get_customer_defaults(self, p21_customer_id: str) -> dict:
        """Get CISM defaults: contact_id, address_id, terms, carrier_id."""
        return self.customer_defaults.get(p21_customer_id, {})
