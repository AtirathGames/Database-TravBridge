#!/usr/bin/env python3
"""
Validate PDP extraction for a single package (PKG013836).
- Gets a fresh token
- Fetches PDP
- Extracts key fields in a controlled way (like your prod extractor)
- Compares raw vs extracted (and cleaned for HTML-heavy fields)
- Prints a pass/fail report

Run:  python validate_pdp_extraction.py
"""

import requests
import re
import html
import json
import time
import logging
from typing import Any, Dict, List, Tuple

# ---------- Config ----------
PACKAGE_ID = "PKG013836"

TOKEN_URL = "https://services.thomascook.in/tcCommonRS/extnrt/getNewRequestToken"
PDP_BASE_URL = "https://services.thomascook.in/tcHolidayRS/packagedetails/pdp/"

DEFAULT_HEADERS = {"uniqueId": "172.63.176.111", "user": "paytm"}
REQUEST_TIMEOUT = 60

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# ---------- Helpers ----------
def remove_html_tags(text: str) -> str:
    if not text:
        return ""
    clean = re.compile("<.*?>")
    text = re.sub(clean, "", text)
    return html.unescape(text).strip()

def get_new_auth_token() -> Tuple[str, str]:
    """Return (request_id, token_id)."""
    resp = requests.get(TOKEN_URL, headers=DEFAULT_HEADERS, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    if data.get("errorCode") != 0:
        raise RuntimeError(f"Token error: {data.get('errorMsg')}")
    return data["requestId"], data["tokenId"]

def get_pdp_details(package_id: str, request_id: str, session_id: str) -> Dict[str, Any]:
    url = f"{PDP_BASE_URL}{package_id}"
    headers = {"requestid": request_id, "sessionid": session_id}
    resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    # PDP sometimes returns [ {...} ]; normalize to dict
    if isinstance(data, list):
        if not data:
            raise ValueError("Empty PDP array.")
        data = data[0]
    if not isinstance(data, dict):
        raise TypeError("Unexpected PDP payload type.")
    return data

# ---------- Minimal extractor (mirrors your logic for the fields under test) ----------
def extract_from_pdp(item: Dict[str, Any]) -> Dict[str, Any]:
    package_detail = item.get("packageDetail", {}) or {}
    # Fields to validate
    out = {
        "packageId": package_detail.get("packageId"),
        "pkgName": package_detail.get("pkgName"),
        "duration": package_detail.get("duration"),

        # flightDefaultMsg
        "flightDefaultMsg_raw": package_detail.get("flightDefaultMsg"),
        "flightDefaultMsg_clean": remove_html_tags(package_detail.get("flightDefaultMsg", "")),

        # tourManagerDescription
        "tourManagerDescription_raw": package_detail.get("tourManagerDescription"),
        "tourManagerDescription_clean": remove_html_tags(package_detail.get("tourManagerDescription", "")),

        # highlights
        "highlights_raw": package_detail.get("overviewHighlights") or package_detail.get("highlights"),
        "highlights_clean": remove_html_tags(
            package_detail.get("overviewHighlights") or package_detail.get("highlights") or ""
        ),

        # itinerary length sanity
        "itinerary_len": len(package_detail.get("tcilHolidayItineraryCollection") or []),
    }
    return out

# ---------- Validation logic ----------
def validate_extraction(raw: Dict[str, Any], extracted: Dict[str, Any]) -> List[str]:
    """
    Returns a list of human-readable PASS/FAIL lines.
    """
    lines = []
    pd = raw.get("packageDetail", {}) or {}

    def p(line: str):  # pretty print aggregator
        lines.append(line)

    # 1) Package ID
    exp_pkg_id = pd.get("packageId")
    got_pkg_id = extracted.get("packageId")
    p(f"[{'PASS' if exp_pkg_id == got_pkg_id else 'FAIL'}] packageId: expected={exp_pkg_id!r} got={got_pkg_id!r}")

    # 2) Duration
    exp_duration = pd.get("duration")
    got_duration = extracted.get("duration")
    p(f"[{'PASS' if exp_duration == got_duration else 'FAIL'}] duration: expected={exp_duration!r} got={got_duration!r}")

    # 3) flightDefaultMsg raw exact match
    exp_flight_raw = pd.get("flightDefaultMsg")
    got_flight_raw = extracted.get("flightDefaultMsg_raw")
    p(f"[{'PASS' if exp_flight_raw == got_flight_raw else 'FAIL'}] flightDefaultMsg (raw): expected={exp_flight_raw!r} got={got_flight_raw!r}")

    # 4) flightDefaultMsg cleaned (sanity: non-empty if raw had HTML list/li)
    got_flight_clean = extracted.get("flightDefaultMsg_clean")
    if isinstance(exp_flight_raw, str) and ("<li>" in exp_flight_raw or "<ul>" in exp_flight_raw or "<p>" in exp_flight_raw):
        p(f"[{'PASS' if got_flight_clean and '<' not in got_flight_clean else 'FAIL'}] flightDefaultMsg (clean): cleaned={got_flight_clean!r}")
    else:
        # If raw is empty or plain text, clean should equal raw (modulo trim)
        ok = (got_flight_clean or "") == (exp_flight_raw or "").strip()
        p(f"[{'PASS' if ok else 'FAIL'}] flightDefaultMsg (clean passthrough): cleaned={got_flight_clean!r}")

    # 5) tourManagerDescription raw exact match
    exp_tour_raw = pd.get("tourManagerDescription")
    got_tour_raw = extracted.get("tourManagerDescription_raw")
    p(f"[{'PASS' if exp_tour_raw == got_tour_raw else 'FAIL'}] tourManagerDescription (raw): expected={exp_tour_raw!r} got={got_tour_raw!r}")

    # 6) tourManagerDescription cleaned (HTML should be stripped if present)
    got_tour_clean = extracted.get("tourManagerDescription_clean")
    if isinstance(exp_tour_raw, str) and ("<" in exp_tour_raw and ">" in exp_tour_raw):
        p(f"[{'PASS' if got_tour_clean and '<' not in got_tour_clean else 'FAIL'}] tourManagerDescription (clean): cleaned={got_tour_clean!r}")
    else:
        ok = (got_tour_clean or "") == (exp_tour_raw or "").strip()
        p(f"[{'PASS' if ok else 'FAIL'}] tourManagerDescription (clean passthrough): cleaned={got_tour_clean!r}")

    # 7) Highlights presence/cleaning
    exp_highlights = pd.get("overviewHighlights") or pd.get("highlights")
    got_highlights_raw = extracted.get("highlights_raw")
    got_highlights_clean = extracted.get("highlights_clean")
    p(f"[{'PASS' if (exp_highlights or '') == (got_highlights_raw or '') else 'FAIL'}] highlights (raw match)")
    if exp_highlights:
        p(f"[{'PASS' if got_highlights_clean and '<' not in got_highlights_clean else 'FAIL'}] highlights (clean)")
    else:
        p(f"[PASS] highlights (absent)")

    # 8) Itinerary structure sanity
    exp_itin = pd.get("tcilHolidayItineraryCollection") or []
    got_len = extracted.get("itinerary_len")
    p(f"[{'PASS' if len(exp_itin) == got_len else 'FAIL'}] itinerary length: expected={len(exp_itin)} got={got_len}")

    return lines

# ---------- Runner ----------
def main() -> None:
    print(f"Validating PDP extraction for package: {PACKAGE_ID}")
    request_id, session_id = get_new_auth_token()
    raw = get_pdp_details(PACKAGE_ID, request_id, session_id)

    # Show a minimal snapshot of the raw parts we care about
    pd = raw.get("packageDetail", {}) or {}
    snapshot = {
        "packageDetail.packageId": pd.get("packageId"),
        "packageDetail.duration": pd.get("duration"),
        "packageDetail.flightDefaultMsg": pd.get("flightDefaultMsg"),
        "packageDetail.tourManagerDescription": pd.get("tourManagerDescription"),
        "packageDetail.overviewHighlights": pd.get("overviewHighlights"),
        "packageDetail.highlights": pd.get("highlights"),
        "itinerary_len": len(pd.get("tcilHolidayItineraryCollection") or []),
    }
    print("\n--- RAW SNAPSHOT (key fields) ---")
    print(json.dumps(snapshot, indent=2, ensure_ascii=False))

    extracted = extract_from_pdp(raw)
    print("\n--- EXTRACTED ---")
    print(json.dumps(extracted, indent=2, ensure_ascii=False))

    results = validate_extraction(raw, extracted)
    print("\n=== VALIDATION REPORT ===")
    for line in results:
        print(line)

    # Optional: explicit success/failure exit code
    failed = any(line.startswith("[FAIL]") for line in results)
    print("\nOverall:", "FAIL" if failed else "PASS")

if __name__ == "__main__":
    main()
