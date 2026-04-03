"""
address_normalizer.py -- Address normalization and similarity for customer matching.
"""

import re
from difflib import SequenceMatcher

# Common address abbreviation expansions
ABBREVS = {
    "ST": "STREET", "AVE": "AVENUE", "BLVD": "BOULEVARD", "DR": "DRIVE",
    "LN": "LANE", "RD": "ROAD", "CT": "COURT", "CIR": "CIRCLE",
    "PL": "PLACE", "TER": "TERRACE", "PKWY": "PARKWAY", "HWY": "HIGHWAY",
    "STE": "SUITE", "APT": "APARTMENT", "BLDG": "BUILDING", "FL": "FLOOR",
    "N": "NORTH", "S": "SOUTH", "E": "EAST", "W": "WEST",
    "NE": "NORTHEAST", "NW": "NORTHWEST", "SE": "SOUTHEAST", "SW": "SOUTHWEST",
    "MT": "MOUNT", "FT": "FORT", "PT": "POINT",
}

# Tokens to strip (noise)
NOISE = {"INC", "LLC", "LTD", "CORP", "CO", "COMPANY", "INCORPORATED", "THE", "DBA"}


def normalize_text(text: str) -> str:
    """Uppercase, strip punctuation, collapse whitespace."""
    if not text:
        return ""
    t = text.upper().strip()
    t = re.sub(r"[.,;:'\"\-/\\()\[\]#&]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def normalize_name(name: str) -> str:
    """Normalize a company/person name for matching."""
    t = normalize_text(name)
    tokens = [tok for tok in t.split() if tok not in NOISE]
    return " ".join(tokens)


def normalize_address(addr: str) -> str:
    """Normalize an address line — expand abbreviations, strip noise."""
    t = normalize_text(addr)
    tokens = []
    for tok in t.split():
        tokens.append(ABBREVS.get(tok, tok))
    return " ".join(tokens)


def normalize_zip(zip_code: str) -> str:
    """Normalize zip to 5-digit base."""
    if not zip_code:
        return ""
    z = re.sub(r"[^0-9]", "", str(zip_code))
    return z[:5] if len(z) >= 5 else z


def _tokenize(text: str) -> set:
    return set(text.split()) if text else set()


def jaccard(a: str, b: str) -> float:
    """Jaccard similarity on token sets."""
    sa, sb = _tokenize(a), _tokenize(b)
    if not sa and not sb:
        return 1.0
    if not sa or not sb:
        return 0.0
    inter = sa & sb
    union = sa | sb
    return len(inter) / len(union)


def levenshtein_ratio(a: str, b: str) -> float:
    """SequenceMatcher ratio (0-1)."""
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def name_similarity(name1: str, name2: str) -> float:
    """Compare two company/person names."""
    n1 = normalize_name(name1)
    n2 = normalize_name(name2)
    # Blend Jaccard (word overlap) and Levenshtein (sequence)
    j = jaccard(n1, n2)
    l = levenshtein_ratio(n1, n2)
    return j * 0.4 + l * 0.6


def address_similarity(addr1: str, addr2: str) -> float:
    """Compare two address lines."""
    a1 = normalize_address(addr1)
    a2 = normalize_address(addr2)
    return jaccard(a1, a2) * 0.5 + levenshtein_ratio(a1, a2) * 0.5


def composite_address_score(
    name1: str, addr1: str, city1: str, state1: str, zip1: str,
    name2: str, addr2: str, city2: str, state2: str, zip2: str,
) -> float:
    """
    Full address composite score.
    Weights: name 0.40, address 0.30, zip 0.20, state 0.10
    """
    n_score = name_similarity(name1, name2)
    a_score = address_similarity(addr1, addr2)
    z_match = 1.0 if normalize_zip(zip1) == normalize_zip(zip2) and normalize_zip(zip1) else 0.0
    s_match = 1.0 if normalize_text(state1) == normalize_text(state2) and state1 else 0.0

    return n_score * 0.40 + a_score * 0.30 + z_match * 0.20 + s_match * 0.10
