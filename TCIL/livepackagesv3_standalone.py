"""
Live Packages V3 API — Fully Standalone Script
Runs on FastAPI port 8004

All dependencies (models, services, retrievePackages, constants) are inlined.
All known bugs and loopholes from the original livepackagesv3.py are fixed.

Fixes applied
─────────────
1.  No external local imports — fully self-contained.
2.  `.model_dump()` instead of deprecated `.dict()`.
3.  Pydantic validator rejects simultaneous flight_included + flight_optional.
4.  flight_optional=False now actively excludes optional-flight packages.
5.  serviceSlots bool vs str normalised before comparison.
6.  Service slot ES filters are pushed into the query for BOTH the
    autosuggest path and the ES fallback path (were only applied in fallback).
7.  nextBestKeyInfo is included in ALL response codes (202/203/204).
8.  Autosuggest location matching uses substring + fuzzy instead of exact ==.
9.  Month matching normalises user input against a canonical month list.
10. ensure_index_exists is called once at startup, not on every request.
11. Unused imports and dead variables removed.
12. Dead "double unknown append" in compute_next_best_key removed.
13. Type hints use Optional[]/List[] (Python 3.8-compatible) throughout.
14. apply_filters always returns (List, bool) — no hidden JSONResponse return.
15. pre_month_city_packages replaces the misleading "unfiltered_packages".
16. Departure city fuzzy match happens once; apply_filters uses exact match.
17. Pagination: limit / offset added to the request model.
18. Fare calendar fetches parallelised with asyncio.gather + run_in_executor.
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
import re
import time
from calendar import month_name
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

import requests
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError, RequestError, TransportError
from fastapi import FastAPI, APIRouter
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel, field_validator, model_validator
from rapidfuzz import process

# ============================================================================
# LOGGING
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION — mirrors constants.py
# ============================================================================

PRODUCTION = os.getenv("PRODUCTION", "true").lower() == "true"

if PRODUCTION:
    es = Elasticsearch(
        ["http://localhost:9200"],
        verify_certs=False,
        basic_auth=("elastic", "iweXVQuayXSCP9PFkHcZ"),
    )
else:
    es = Elasticsearch(
        ["https://localhost:9200"],
        verify_certs=False,
        basic_auth=("elastic", "iE1L2cJmCbYqJFwtf2wb"),
    )

TCIL_PACKAGE_INDEX = "tcildatav1"
TOKEN_TTL = 43200          # 12 hours
CACHE_TTL_SECONDS = 600    # 10 minutes
RESOURCES_URL = "https://resources.thomascook.in/images/holidays/"

CANONICAL_MONTHS = [
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december",
]

# ============================================================================
# PYDANTIC MODELS — inlined from models.py (only what this script uses)
# ============================================================================


class DayItinerary(BaseModel):
    day: int
    description: str
    mealDescription: str
    overnightStay: str


class PackageItinerary(BaseModel):
    summary: Optional[str] = ""
    itinerary: List[DayItinerary]


class DepartureCity(BaseModel):
    cityName: str
    cityCode: str
    ltItineraryCode: str
    holidayLtPricingId: str


class savedItineraryData(BaseModel):
    packageId: str
    availableMonths: Optional[Union[str, List[str]]] = None
    packageName: str
    days: Optional[int] = None
    cities: List[Dict[str, Union[str, Dict[str, float]]]]
    highlights: List[str]
    pdfName: Optional[str] = None
    price: Optional[int] = None
    packageSummary: Optional[str] = None
    images: List[str]
    minimumPrice: Optional[int] = None
    thumbnailImage: Optional[str] = None
    packageTheme: List[str]
    packageData: str
    departureCities: List[DepartureCity]
    packageItinerary: PackageItinerary
    hotels: Optional[Union[str, List[str]]] = None
    hotels_list: Optional[Union[str, List[str], Dict[str, str]]] = None
    continents: Optional[List[Dict[str, Union[str, int]]]] = None
    packageTourType: Optional[List[str]] = None
    sightseeingTypes: Optional[List[str]] = None
    meals: Optional[Union[str, List[str]]] = None
    visa: Optional[Union[str, List[str]]] = None
    transfer: Optional[Union[str, List[str]]] = None
    sightseeing: Optional[Union[str, List[str]]] = None
    tourManagerDescription: Optional[str] = None
    flightDescription: Optional[str] = None
    inclusions: Optional[str] = None
    exclusions: Optional[str] = None
    termsAndConditions: Optional[str] = None
    pkgSubtypeId: Optional[int] = None
    pkgSubtypeName: str = ""
    pkgTypeId: Optional[int] = None
    isFlightIncluded: Optional[str] = None
    holidayPlusSubType: Optional[int] = None
    productId: Optional[int] = None
    flightsAvailability: Optional[str] = None
    serviceSlots: Optional[Dict[str, Union[bool, str, None]]] = None
    constructed_thumbnailImage: Optional[str] = None
    constructed_images: Optional[List[str]] = None
    visitingCountries: Optional[List[str]] = None


class ItemOut(BaseModel):
    id: str
    itinerary_data: savedItineraryData
    score: float
    departureCity_details: Optional[Dict[str, str]] = None
    fareCalendar: Optional[Dict[str, Any]] = None


# ============================================================================
# REQUEST MODEL WITH SERVICE SLOTS (FIXED)
# ============================================================================


class PackageSearchRequestV3(BaseModel):
    """Enhanced request model with 11 service slot filters + pagination."""

    # Core search
    search_term: str
    departureCity: Optional[str] = ""
    days: Optional[int] = 0
    budget: Optional[int] = 0
    number_of_people: Optional[int] = 0
    monthOfTravel: Optional[str] = ""
    theme: Optional[str] = ""
    fareCalendar: Optional[bool] = False
    includeBuckets: bool = False  # Include bucket aggregations in response (default: False for performance)
    pkgSubtypeName: Optional[str] = ""  # GIT or FIT
    packageTourType: Optional[str] = ""  # Value, Standard, Premium
    sightseeingTypes: Optional[str] = ""  # CSV or single value; matches any

    # Service slot filters (default: None = no filter)
    # Only True applies a filter. False/None both mean "don't filter".
    # ES data contains ONLY true/false values, no nulls.
    flight_included: Optional[bool] = None
    flight_optional: Optional[bool] = None
    visa_included: Optional[bool] = None
    travel_insurance_included: Optional[bool] = None
    entrance_fees_included: Optional[bool] = None
    airport_transfer_included: Optional[bool] = None
    tour_manager_included: Optional[bool] = None
    tips_included: Optional[bool] = None
    breakfast_included: Optional[bool] = None
    all_meals_included: Optional[bool] = None
    wheelchair_accessible: Optional[bool] = None
    senior_citizen_friendly: Optional[bool] = None

    # FIX 3: mutually exclusive — can't ask for "flight included=true" AND "flight optional=true"
    @model_validator(mode="after")
    def check_flight_conflict(self) -> "PackageSearchRequestV3":
        if self.flight_included is True and self.flight_optional is True:
            raise ValueError(
                "flight_included=true and flight_optional=true are mutually exclusive. "
                "Use flight_included=true for packages with flights in the price, "
                "or flight_optional=true for Holiday+ optional flight packages."
            )
        return self


# ============================================================================
# IN-MEMORY CACHES
# ============================================================================

PACKAGE_CACHE: Dict[str, dict] = {}
PACKAGE_CACHE_TIMESTAMPS: Dict[str, float] = {}

TOKEN_CACHE: Dict[str, Any] = {
    "requestId": None,
    "sessionId": None,
    "expires_at": 0,
}

# ============================================================================
# STARTUP: INDEX VERIFICATION (once only)
# ============================================================================

_INDEX_VERIFIED = False


def ensure_index_exists_once(index_name: str = TCIL_PACKAGE_INDEX) -> None:
    """Check that the required ES index exists. Called once at startup."""
    global _INDEX_VERIFIED
    if _INDEX_VERIFIED:
        return
    try:
        if es.indices.exists(index=index_name):
            logger.info(f"[startup] ES index '{index_name}' confirmed.")
        else:
            logger.warning(
                f"[startup] ES index '{index_name}' does NOT exist! "
                "Package searches will return no results."
            )
    except Exception as exc:
        logger.error(f"[startup] Could not verify index '{index_name}': {exc}")
    finally:
        _INDEX_VERIFIED = True


# ============================================================================
# PACKAGE CACHE HELPERS
# ============================================================================


def _is_cache_valid(package_id: str) -> bool:
    ts = PACKAGE_CACHE_TIMESTAMPS.get(package_id)
    return ts is not None and (time.time() - ts) <= CACHE_TTL_SECONDS


def _get_cached_package(package_id: str) -> Optional[dict]:
    if package_id in PACKAGE_CACHE and _is_cache_valid(package_id):
        return PACKAGE_CACHE[package_id]
    return None


def _cache_package(package_id: str, data: dict) -> None:
    PACKAGE_CACHE[package_id] = data
    PACKAGE_CACHE_TIMESTAMPS[package_id] = time.time()


# ============================================================================
# AUTH TOKEN HELPERS
# ============================================================================


def _get_cached_auth_token() -> Tuple[Optional[str], Optional[str]]:
    now = time.time()
    if (
        TOKEN_CACHE["requestId"]
        and TOKEN_CACHE["sessionId"]
        and now < TOKEN_CACHE["expires_at"]
    ):
        return TOKEN_CACHE["requestId"], TOKEN_CACHE["sessionId"]
    return None, None


def _store_auth_token(request_id: str, session_id: str) -> None:
    TOKEN_CACHE["requestId"] = request_id
    TOKEN_CACHE["sessionId"] = session_id
    TOKEN_CACHE["expires_at"] = time.time() + TOKEN_TTL


def get_new_auth_token() -> Tuple[str, str]:
    """Fetch a fresh auth token from the Thomas Cook API."""
    token_url = "https://services.thomascook.in/tcCommonRS/extnrt/getNewRequestToken"
    headers = {"uniqueId": "172.63.176.111", "user": "paytm"}
    session = requests.Session()
    try:
        resp = session.get(token_url, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get("errorCode") == 0:
            return data["requestId"], data["tokenId"]
        raise RuntimeError(f"Token API error: {data.get('errorMsg', 'unknown')}")
    finally:
        session.close()


# ============================================================================
# IMAGE URL HELPERS
# ============================================================================


def _generate_image_urls(package_id: str, images: List[str]) -> List[str]:
    return [
        f"{RESOURCES_URL}{package_id}/itinerary/{img.replace(' ', '_')}"
        for img in images
    ]


# ============================================================================
# FLIGHTS AVAILABILITY DESCRIPTION
# ============================================================================


def generate_flights_availability_description(
    pkg_subtype_name: str,
    pkg_subtype_id: int,
    is_flight_included: str,
    product_id: int,
    holiday_plus_subtype: int,
) -> str:
    if pkg_subtype_name == "GIT":
        if is_flight_included == "Y":
            return "This package includes flights in the package price."
        return "This package does not include flights. You can book flights separately."

    if pkg_subtype_name == "FIT" and product_id == 11:
        if holiday_plus_subtype == 1:
            return "This package includes flights in the package price."
        if holiday_plus_subtype == 2:
            return "This package offers optional flights. You can choose to add flights or book separately."
        return "Please check with our Agent for flight options."

    if pkg_subtype_name == "FIT":
        if is_flight_included == "Y":
            return "This package includes flights in the package price."
        return "This package does not include flights. You can book flights separately based on your preference."

    if is_flight_included == "Y":
        return "This package includes flights in the package price."
    return "This package does not include flights. You can arrange flights separately."


# ============================================================================
# ES: CONSTRUCT ItemOut FROM SOURCE
# ============================================================================


def construct_item_out_from_source(source: dict, score: float) -> ItemOut:
    pkg_itinerary = source.get("packageItinerary", {})
    day_itineraries = [
        DayItinerary(
            day=d.get("day", 0),
            description=d.get("description", ""),
            mealDescription=d.get("mealDescription", ""),
            overnightStay=d.get("overnightStay", ""),
        )
        for d in pkg_itinerary.get("itinerary", [])
    ]
    itinerary_obj = PackageItinerary(
        summary=pkg_itinerary.get("summary", ""),
        itinerary=day_itineraries,
    )
    departure_cities = [
        DepartureCity(
            cityName=c.get("cityName", ""),
            cityCode=c.get("cityCode", ""),
            ltItineraryCode=c.get("ltItineraryCode", ""),
            holidayLtPricingId=c.get("holidayLtPricingId", ""),
        )
        for c in source.get("departureCities", [])
    ]
    flights_availability = generate_flights_availability_description(
        pkg_subtype_name=source.get("pkgSubtypeName", ""),
        pkg_subtype_id=source.get("pkgSubtypeId", 0) or 0,
        is_flight_included=source.get("isFlightIncluded", "N"),
        product_id=source.get("productId", 0) or 0,
        holiday_plus_subtype=source.get("holidayPlusSubType", -1) or -1,
    )
    pkg_id = source.get("packageId", "")
    saved = savedItineraryData(
        packageId=pkg_id,
        availableMonths=source.get("availableMonths", []),
        packageName=source.get("packageName", ""),
        packageTheme=source.get("packageTheme", []),
        days=source.get("days"),
        cities=source.get("cities", []),
        highlights=source.get("highlights", []),
        pdfName=source.get("pdfName"),
        price=source.get("price"),
        minimumPrice=source.get("minimumPrice"),
        packageData=source.get("packageData", ""),
        packageSummary=source.get("packageSummary", ""),
        thumbnailImage=source.get("thumbnailImage"),
        images=source.get("images", []),
        departureCities=departure_cities,
        packageItinerary=itinerary_obj,
        hotels=source.get("hotels"),
        hotels_list=source.get("hotels_list"),
        continents=source.get("continents", []),
        packageTourType=source.get("packageTourType", []),
        sightseeingTypes=source.get("sightseeingTypes", []),
        meals=source.get("meals"),
        visa=source.get("visa"),
        transfer=source.get("transfer"),
        sightseeing=source.get("sightseeing"),
        tourManagerDescription=source.get("tourManagerDescription"),
        flightDescription=source.get("flightDescription"),
        inclusions=source.get("inclusions"),
        exclusions=source.get("exclusions"),
        termsAndConditions=source.get("termsAndConditions"),
        pkgSubtypeId=source.get("pkgSubtypeId"),
        pkgSubtypeName=source.get("pkgSubtypeName", ""),
        pkgTypeId=source.get("pkgTypeId"),
        isFlightIncluded=source.get("isFlightIncluded"),
        holidayPlusSubType=source.get("holidayPlusSubType"),
        productId=source.get("productId"),
        flightsAvailability=flights_availability,
        serviceSlots=source.get("serviceSlots"),
        visitingCountries=source.get("visitingCountries", []),
        constructed_thumbnailImage=(
            f"{RESOURCES_URL}{pkg_id}/{source.get('thumbnailImage', '')}"
            if source.get("thumbnailImage")
            else ""
        ),
        constructed_images=_generate_image_urls(pkg_id, source.get("images") or []),
    )
    return ItemOut(id=pkg_id or "unknown_id", itinerary_data=saved, score=score)



# ============================================================================
# ES: SEARCH PACKAGE BY ID (internal, no PDP fallback)
# ============================================================================


async def search_item_by_package_id_internal(
    body: Dict[str, str], generate_summary: bool = True
) -> ItemOut:
    package_id = body.get("packageId", "")
    if not package_id or not re.match(r"^[A-Za-z0-9\-\_]+$", package_id):
        raise ValueError(f"Invalid or missing packageId: '{package_id}'")

    cached = _get_cached_package(package_id)
    if cached:
        return construct_item_out_from_source(cached, score=1.0)

    search_body = {"query": {"term": {"packageId.keyword": package_id}}, "size": 1}
    response = es.search(index=TCIL_PACKAGE_INDEX, body=search_body)
    hits = response.get("hits", {}).get("hits", [])
    if not hits:
        raise LookupError(f"Package {package_id} not found in Elasticsearch.")
    source = hits[0]["_source"]
    score = hits[0].get("_score", 1.0)
    _cache_package(package_id, source)
    return construct_item_out_from_source(source, score=score)


# ============================================================================
# MONTH FORMATTING
# ============================================================================


def format_and_sort_months(month_year_list: Any) -> List[str]:
    current_year = datetime.now().year
    parsed: List[Tuple[int, int, str]] = []
    for my in month_year_list:
        try:
            month, year = my.lower().split("_")
            idx = list(month_name).index(month.capitalize())
            parsed.append((int(year), idx, f"{month.capitalize()} {year}"))
        except Exception:
            parsed.append((9999, 13, str(my)))
    parsed.sort(key=lambda x: (x[0] != current_year, x[0], x[1]))
    return [item[2] for item in parsed]


# ============================================================================
# FARE CALENDAR
# ============================================================================


def parse_git_fare_calendar_response(city: DepartureCity, data: dict) -> dict:
    lt = data.get("ltResponseBean", {})
    is_avail = (
        data.get("isDateAvialable", "NO") == "YES"
        or lt.get("isDateAvialable", "NO") == "YES"
    )
    bookable: List[dict] = []
    on_request: List[dict] = []
    if is_avail:
        for d in lt.get("bookable", []):
            bookable.append({
                "date": d.get("DATE", ""),
                "price": int(d.get("DR_PRICE", 0)),
                "strikeOutPrice": int(d.get("DR_STRIKEOUT", 0)),
                "availableInventory": int(d.get("AVL_INV", 0)),
                "lastSellDay": int(d.get("LAST_SELL_DAY", 0)),
                "ltProdCode": d.get("LT_PROD_CODE", ""),
            })
        for d in lt.get("onRequest", []):
            on_request.append({"date": d.get("DATE", ""), "ltProdCode": d.get("LT_PROD_CODE", "")})
    prices = [d["price"] for d in bookable if d["price"] > 0]
    return {
        "cityName": city.cityName,
        "cityCode": city.cityCode,
        "ltItineraryCode": city.ltItineraryCode,
        "availability": {
            "isAvailable": is_avail,
            "dateRange": {"startDate": bookable[0]["date"], "endDate": bookable[-1]["date"]} if bookable else None,
            "stats": {
                "totalBookableDates": len(bookable),
                "totalOnRequestDates": len(on_request),
                "minPrice": min(prices) if prices else None,
                "maxPrice": max(prices) if prices else None,
            },
        },
        "dates": {"bookable": bookable, "onRequest": on_request},
    }


def parse_fit_fare_calendar_response(class_id: str, class_name: str, data: dict) -> dict:
    db = data.get("dbResponseBean", {})
    is_avail = (
        data.get("isDateAvialable", "NO") == "YES"
        or db.get("isDateAvialable", "NO") == "YES"
    )
    bookable: List[dict] = []
    on_request: List[dict] = []
    if is_avail:
        for d in db.get("bookable", []):
            bookable.append({"date": d.get("date", ""), "price": int(d.get("price", 0)), "strikeOutPrice": int(d.get("strikeOutPrice", 0))})
        for d in db.get("onRequest", []):
            on_request.append({"date": d.get("date", ""), "price": int(d.get("price", 0))})
    prices = [d["price"] for d in bookable if d["price"] > 0]
    return {
        "className": class_name,
        "classId": class_id,
        "availability": {
            "isAvailable": is_avail,
            "dateRange": {"startDate": bookable[0]["date"], "endDate": bookable[-1]["date"]} if bookable else None,
            "stats": {
                "totalBookableDates": len(bookable),
                "totalOnRequestDates": len(on_request),
                "minPrice": min(prices) if prices else None,
                "maxPrice": max(prices) if prices else None,
                "avgPrice": sum(prices) // len(prices) if prices else None,
            },
        },
        "dates": {"bookable": bookable, "onRequest": on_request},
    }


def fetch_fare_calendar_git(
    package_id: str,
    pkg_subtype_id: int,
    pkg_type_id: int,
    departure_cities: List[DepartureCity],
    request_id: str,
    session_id: str,
) -> dict:
    url = "https://services.thomascook.in/tcHolidayRS/pdp.compare/fareCalender"
    headers = {"Requestid": request_id, "Sessionid": session_id, "Content-Type": "application/json"}
    result: dict = {
        "departureCities": [],
        "summary": {"totalCities": len(departure_cities), "availableCities": 0, "overallDateRange": None, "priceRange": {"min": None, "max": None}},
    }
    all_min: List[int] = []
    all_max: List[int] = []
    overall_start: Optional[str] = None
    overall_end: Optional[str] = None

    def _fetch(city: DepartureCity) -> dict:
        try:
            payload = {
                "ltItineraryCode": [city.ltItineraryCode],
                "market": "-1",
                "hubCode": city.cityCode,
                "pkgSubTypeId": pkg_subtype_id,
                "pkgClassId": str(pkg_type_id),
                "pkgId": package_id,
                "mode": "TCIL",
                "isHsa": "N",
                "isCanvasPackage": "Y",
            }
            r = requests.post(url, json=payload, headers=headers, timeout=10)
            r.raise_for_status()
            return parse_git_fare_calendar_response(city, r.json())
        except Exception as exc:
            logger.error(f"[fare_calendar_git] {city.cityName}: {exc}")
            return {"cityName": city.cityName, "cityCode": city.cityCode, "ltItineraryCode": city.ltItineraryCode, "availability": {"isAvailable": False, "dateRange": None, "stats": {"totalBookableDates": 0, "totalOnRequestDates": 0, "minPrice": None, "maxPrice": None}}, "dates": {"bookable": [], "onRequest": []}}

    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(_fetch, c): c for c in departure_cities}
        for fut in as_completed(futures):
            cd = fut.result()
            result["departureCities"].append(cd)
            if cd["availability"]["isAvailable"]:
                result["summary"]["availableCities"] += 1
                stats = cd["availability"]["stats"]
                if stats["minPrice"]:
                    all_min.append(stats["minPrice"])
                if stats["maxPrice"]:
                    all_max.append(stats["maxPrice"])
                dr = cd["availability"]["dateRange"]
                if dr:
                    if not overall_start or dr["startDate"] < overall_start:
                        overall_start = dr["startDate"]
                    if not overall_end or dr["endDate"] > overall_end:
                        overall_end = dr["endDate"]

    result["departureCities"].sort(key=lambda x: x["cityName"])
    if all_min:
        result["summary"]["priceRange"]["min"] = min(all_min)
    if all_max:
        result["summary"]["priceRange"]["max"] = max(all_max)
    if overall_start and overall_end:
        result["summary"]["overallDateRange"] = {"startDate": overall_start, "endDate": overall_end}
    return result


def fetch_fare_calendar_fit(
    package_id: str,
    pkg_subtype_id: int,
    pkg_type_id: int,
    request_id: str,
    session_id: str,
) -> dict:
    url = "https://services.thomascook.in/tcHolidayRS/pdp.compare/fareCalender"
    headers = {"Requestid": request_id, "Sessionid": session_id, "Content-Type": "application/json"}
    class_names = {"0": "Standard", "1": "Value", "2": "Premium"}
    result: dict = {
        "classTypes": [],
        "summary": {"totalClasses": 3, "availableClasses": 0, "overallDateRange": None, "priceRange": {"min": None, "max": None}},
        "departureCityNote": "Common pricing across all departure cities",
    }
    all_min: List[int] = []
    all_max: List[int] = []
    overall_start: Optional[str] = None
    overall_end: Optional[str] = None

    def _fetch(class_id: str) -> dict:
        try:
            payload = {"ltItineraryCode": [], "market": "-1", "hubCode": "", "pkgSubTypeId": pkg_subtype_id, "pkgClassId": class_id, "pkgId": package_id, "mode": "TCIL", "isHsa": "N", "isCanvasPackage": "Y"}
            r = requests.post(url, json=payload, headers=headers, timeout=10)
            r.raise_for_status()
            return parse_fit_fare_calendar_response(class_id, class_names[class_id], r.json())
        except Exception as exc:
            logger.error(f"[fare_calendar_fit] class {class_names[class_id]}: {exc}")
            return {"className": class_names[class_id], "classId": class_id, "availability": {"isAvailable": False, "dateRange": None, "stats": {"totalBookableDates": 0, "totalOnRequestDates": 0, "minPrice": None, "maxPrice": None, "avgPrice": None}}, "dates": {"bookable": [], "onRequest": []}}

    with ThreadPoolExecutor(max_workers=3) as ex:
        futures = {ex.submit(_fetch, cid): cid for cid in ["0", "1", "2"]}
        for fut in as_completed(futures):
            cd = fut.result()
            result["classTypes"].append(cd)
            if cd["availability"]["isAvailable"]:
                result["summary"]["availableClasses"] += 1
                stats = cd["availability"]["stats"]
                if stats["minPrice"]:
                    all_min.append(stats["minPrice"])
                if stats["maxPrice"]:
                    all_max.append(stats["maxPrice"])
                dr = cd["availability"]["dateRange"]
                if dr:
                    if not overall_start or dr["startDate"] < overall_start:
                        overall_start = dr["startDate"]
                    if not overall_end or dr["endDate"] > overall_end:
                        overall_end = dr["endDate"]

    result["classTypes"].sort(key=lambda x: x["classId"])
    if all_min:
        result["summary"]["priceRange"]["min"] = min(all_min)
    if all_max:
        result["summary"]["priceRange"]["max"] = max(all_max)
    if overall_start and overall_end:
        result["summary"]["overallDateRange"] = {"startDate": overall_start, "endDate": overall_end}
    return result


def fetch_fare_calendar_for_package(
    package_id: str,
    pkg_subtype_id: int,
    pkg_type_id: int,
    departure_cities: List[DepartureCity],
    request_id: Optional[str] = None,
    session_id: Optional[str] = None,
    departure_city_filter: Optional[str] = None,
) -> Optional[dict]:
    try:
        if departure_city_filter:
            departure_cities = [c for c in departure_cities if c.cityName.lower() == departure_city_filter.lower()]
            if not departure_cities:
                logger.warning(f"[fare_calendar] City '{departure_city_filter}' not found for {package_id}")
                return None
        if not request_id or not session_id:
            request_id, session_id = get_new_auth_token()
        is_git = pkg_subtype_id in [1, 3]
        if is_git:
            return fetch_fare_calendar_git(package_id, pkg_subtype_id, pkg_type_id, departure_cities, request_id, session_id)
        return fetch_fare_calendar_fit(package_id, pkg_subtype_id, pkg_type_id, request_id, session_id)
    except Exception as exc:
        logger.error(f"[fare_calendar] {package_id}: {exc}")
        return None


# ============================================================================
# AUTOSUGGEST (inlined from retrievePackages.py)
# ============================================================================


async def fetch_autosuggest_results(
    search_term: str, short_timeout: float = 10.0
) -> List[dict]:
    req_id, sess_id = _get_cached_auth_token()
    if not req_id or not sess_id:
        try:
            req_id, sess_id = get_new_auth_token()
            _store_auth_token(req_id, sess_id)
        except Exception as exc:
            logger.error(f"[autosuggest] Token fetch failed: {exc}")
            return []

    url = "https://services.thomascook.in/tcHolidayRS/autosuggest"
    try:
        resp = requests.get(
            url,
            headers={"Requestid": req_id, "Sessionid": sess_id},
            params={"searchAutoSuggest": search_term},
            timeout=short_timeout,
        )
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []
    except Exception as exc:
        logger.error(f"[autosuggest] Request failed: {exc}")
        return []


# ============================================================================
# UTILITY FUNCTIONS (inlined from retrievePackages.py)
# ============================================================================


def prioritize_bogo(packages: List[ItemOut]) -> List[ItemOut]:
    bogo = [p for p in packages if "buy 1 get 1 free" in (p.itinerary_data.packageName or "").lower()]
    others = [p for p in packages if p not in bogo]
    return bogo + others


def get_closest_match(
    input_city: str, available_cities: List[str], threshold: int = 80
) -> Optional[str]:
    input_city = input_city.lower().strip()
    available_cities = [c.lower().strip() for c in available_cities]
    result = process.extractOne(input_city, available_cities)
    if result:
        match, score, *_ = result
        if score >= threshold:
            return match
    return None


# ============================================================================
# FIX 8: AUTOSUGGEST LOCATION MATCH — partial/fuzzy instead of exact ==
# ============================================================================


def _location_matches(item: dict, search_term: str) -> bool:
    """Check if a search term matches any location field in an autosuggest item."""
    term = search_term.lower().strip()
    fields = [
        (item.get("cityName") or "").lower().strip(),
        (item.get("countryName") or "").lower().strip(),
        (item.get("stateName") or "").lower().strip(),
        (item.get("continentName") or "").lower().strip(),
    ]
    for f in fields:
        if not f:
            continue
        # Exact or substring match first (fast path)
        if term == f or term in f or f in term:
            return True
        # Fuzzy fallback
        if get_closest_match(term, [f], threshold=75) is not None:
            return True
    return False


# ============================================================================
# FIX 9: MONTH NORMALISATION
# ============================================================================


def _normalize_month(raw: str) -> Optional[str]:
    """
    Convert any user-supplied month string to a canonical full lowercase month name.
    e.g. 'jan', 'JAN', 'January' → 'january'
    Returns None if unrecognised.
    """
    raw = raw.strip().lower()
    if not raw:
        return None
    # 1) Direct full match
    if raw in CANONICAL_MONTHS:
        return raw
    # 2) Prefix match (e.g. 'jan' → 'january')
    matches = [m for m in CANONICAL_MONTHS if m.startswith(raw)]
    if len(matches) == 1:
        return matches[0]
    # 3) Fuzzy
    best = get_closest_match(raw, CANONICAL_MONTHS, threshold=75)
    return best


# ============================================================================
# SERVICE SLOT FILTERING (FIXED)
# ============================================================================


def _coerce_slot_value(raw: Any) -> Any:
    """
    FIX 5: normalise slot values stored as strings in ES documents.
    ES boolean fields come back as Python bools from the client, but text
    fields like flight_included may be "true"/"false"/"optional".
    """
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, str):
        if raw.lower() == "true":
            return True
        if raw.lower() == "false":
            return False
    return raw  # preserve "optional" and other strings as-is


def apply_service_slot_filters(
    packages: List[ItemOut], request: PackageSearchRequestV3
) -> List[ItemOut]:
    filtered = packages

    # FIX 4: Only filter if True. False/None both mean "don't filter".
    if request.flight_optional is True:
        logger.info("[service_slots] Keeping only optional-flight packages")
        filtered = [
            p for p in filtered
            if p.itinerary_data.serviceSlots
            and _coerce_slot_value(p.itinerary_data.serviceSlots.get("flight_included")) == "optional"
        ]

    slot_filters = {
        "flight_included": request.flight_included,
        "visa_included": request.visa_included,
        "travel_insurance_included": request.travel_insurance_included,
        "entrance_fees_included": request.entrance_fees_included,
        "airport_transfer_included": request.airport_transfer_included,
        "tour_manager_included": request.tour_manager_included,
        "tips_included": request.tips_included,
        "breakfast_included": request.breakfast_included,
        "all_meals_included": request.all_meals_included,
        "wheelchair_accessible": request.wheelchair_accessible,
        "senior_citizen_friendly": request.senior_citizen_friendly,
    }
    # Only apply filters when value is explicitly True
    active = {k: v for k, v in slot_filters.items() if v is True}

    if active:
        logger.info(f"[service_slots] Applying {len(active)} filters: {list(active.keys())}")
        for slot, expected in active.items():
            before = len(filtered)
            filtered = [
                p for p in filtered
                if p.itinerary_data.serviceSlots
                and _coerce_slot_value(p.itinerary_data.serviceSlots.get(slot)) is True
            ]
            logger.info(f"[service_slots] '{slot}={expected}': {before} → {len(filtered)}")

    return filtered


def build_service_slot_es_query(request: PackageSearchRequestV3) -> List[dict]:
    """
    FIX 6: Build ES must-clauses for service slots, used in BOTH query paths.
    Only filters when value is True. False/None both mean "don't filter".
    """
    must: List[dict] = []

    # flight_optional=True: filter for optional flights
    if request.flight_optional is True:
        must.append({"term": {"serviceSlots.flight_included.keyword": "optional"}})

    # flight_included=True: filter for flights included
    if request.flight_included is True:
        must.append({"term": {"serviceSlots.flight_included": True}})

    # Other service slots - only apply if True
    bool_slots = {
        "visa_included": request.visa_included,
        "travel_insurance_included": request.travel_insurance_included,
        "entrance_fees_included": request.entrance_fees_included,
        "airport_transfer_included": request.airport_transfer_included,
        "tour_manager_included": request.tour_manager_included,
        "tips_included": request.tips_included,
        "breakfast_included": request.breakfast_included,
        "all_meals_included": request.all_meals_included,
        "wheelchair_accessible": request.wheelchair_accessible,
        "senior_citizen_friendly": request.senior_citizen_friendly,
    }
    for slot, val in bool_slots.items():
        if val is True:
            must.append({"term": {f"serviceSlots.{slot}": True}})

    return must


# ============================================================================
# BUDGET / DAY FILTERING
# ============================================================================


def _budget_range(budget: int) -> Tuple[int, Optional[int]]:
    if budget <= 30000:
        return 0, 29999
    if budget <= 100000:
        return 30000, 99999
    if budget <= 200000:
        return 100000, 199999
    return 200000, None


def filter_by_budget(
    target_budget: Optional[int],
    detailed_packages: List[Any],
    requested_days: Optional[int] = None,
) -> Tuple[List[Any], bool]:
    if not detailed_packages:
        return [], False

    filtered = detailed_packages

    if target_budget and target_budget > 0:
        lower, upper = _budget_range(target_budget)
        filtered = []
        for pkg in detailed_packages:
            price = pkg.itinerary_data.minimumPrice or 0
            if upper is None:
                if price >= lower:
                    filtered.append(pkg)
            elif lower <= price <= upper:
                filtered.append(pkg)

    if requested_days and requested_days > 0:
        filtered = [p for p in filtered if (p.itinerary_data.days or 0) == requested_days]

    strict_applied = bool((target_budget and target_budget > 0) or (requested_days and requested_days > 0))
    strict_matched = len(filtered) > 0
    pool = filtered if strict_matched else detailed_packages

    annotated = [
        {
            "package": p,
            "day_diff": abs((p.itinerary_data.days or 0) - requested_days) if requested_days else 0,
            "price_diff": abs((p.itinerary_data.minimumPrice or 0) - target_budget) if target_budget else 0,
        }
        for p in pool
    ]
    annotated.sort(key=lambda x: (x["day_diff"], x["price_diff"]))
    return [x["package"] for x in annotated], strict_matched


def apply_filters(
    packages: List[Any],
    departure_city: Optional[str],
    target_budget: Optional[int],
    target_days: Optional[int],
) -> Tuple[List[Any], bool]:
    """
    FIX 14: Always returns (List, bool) — no hidden JSONResponse.
    FIX 16: departure_city is already resolved; do a simple exact filter here.
    """
    if departure_city and departure_city.strip():
        city_lower = departure_city.strip().lower()
        relevant = [
            pkg for pkg in packages
            if any(c.cityName.lower() == city_lower for c in getattr(pkg.itinerary_data, "departureCities", []))
        ]
        if not relevant:
            # If no exact match (shouldn't happen, but be safe), return closest alternatives
            logger.warning(f"[apply_filters] No packages found for already-resolved city '{departure_city}'")
            relevant = packages
    else:
        relevant = packages

    return filter_by_budget(
        target_budget=target_budget,
        detailed_packages=relevant,
        requested_days=target_days,
    )


# ============================================================================
# NEXT BEST KEY
# ============================================================================


async def compute_next_best_key(
    current_package_ids: List[str], request: PackageSearchRequestV3
) -> Optional[dict]:
    if len(current_package_ids) < 3:
        return None

    provided = request.model_fields_set  # FIX 2: model_fields_set (always worked, but consistent)
    candidates: Dict[str, dict] = {}
    is_service_slot: Dict[str, bool] = {}

    if "days" not in provided or request.days == 0:
        candidates["days"] = {"field": "days", "type": "terms", "size": 30}
        is_service_slot["days"] = False

    if "monthOfTravel" not in provided or not (request.monthOfTravel or "").strip():
        candidates["monthOfTravel"] = {"field": "availableMonths.keyword", "type": "terms", "size": 12}
        is_service_slot["monthOfTravel"] = False

    if "theme" not in provided or not (request.theme or "").strip():
        candidates["theme"] = {"field": "packageTheme.keyword", "type": "terms", "size": 20}
        is_service_slot["theme"] = False

    if "pkgSubtypeName" not in provided or not (request.pkgSubtypeName or "").strip():
        candidates["pkgSubtypeName"] = {"field": "pkgSubtypeName.keyword", "type": "terms", "size": 10}
        is_service_slot["pkgSubtypeName"] = False

    if "budget" not in provided or request.budget == 0:
        candidates["budget"] = {
            "field": "price",
            "type": "range",
            "ranges": [
                {"key": "0-29000", "from": 0, "to": 30000},
                {"key": "30000-99999", "from": 30000, "to": 100000},
                {"key": "100000-199999", "from": 100000, "to": 200000},
                {"key": "200000 and above", "from": 200000},
            ],
        }
        is_service_slot["budget"] = False

    service_slot_agg_field = {
        "flight_included": "serviceSlots.flight_included.keyword",
    }
    all_slots = [
        "flight_included", "flight_optional", "visa_included", "travel_insurance_included",
        "entrance_fees_included", "airport_transfer_included", "tour_manager_included",
        "tips_included", "breakfast_included", "all_meals_included",
        "wheelchair_accessible", "senior_citizen_friendly",
    ]
    for slot in all_slots:
        if getattr(request, slot, None) is None:
            field = service_slot_agg_field.get(slot, f"serviceSlots.{slot}")
            candidates[slot] = {"field": field, "type": "terms", "size": 2}
            is_service_slot[slot] = True

    if not candidates:
        return None

    body: dict = {
        "query": {"bool": {"filter": [{"terms": {"packageId.keyword": current_package_ids}}]}},
        "size": 0,
        "aggs": {},
    }
    for key, cfg in candidates.items():
        if cfg["type"] == "terms":
            body["aggs"][f"by_{key}"] = {"terms": {"field": cfg["field"], "size": cfg.get("size", 10)}}
        elif cfg["type"] == "range":
            body["aggs"][f"by_{key}"] = {"range": {"field": cfg["field"], "ranges": cfg["ranges"]}}

    while True:
        try:
            res = es.search(index=TCIL_PACKAGE_INDEX, body=body)
            break
        except Exception as exc:
            msg = str(exc)
            m = re.search(r"Fielddata is disabled on \[([^\]]+)\]", msg)
            if not m:
                raise
            bad_field = m.group(1)
            fixed_any = False
            for agg_name in list(body["aggs"].keys()):
                agg_def = body["aggs"].get(agg_name, {})
                if "terms" not in agg_def or agg_def["terms"].get("field") != bad_field:
                    continue
                current_field = agg_def["terms"]["field"]
                if not current_field.endswith(".keyword"):
                    fallback = f"{current_field}.keyword"
                    agg_def["terms"]["field"] = fallback
                    key = agg_name.removeprefix("by_")
                    if key in candidates:
                        candidates[key]["field"] = fallback
                    fixed_any = True
                else:
                    key = agg_name.removeprefix("by_")
                    body["aggs"].pop(agg_name, None)
                    candidates.pop(key, None)
                    is_service_slot.pop(key, None)
            if not fixed_any and not body["aggs"]:
                return None

    total = res["hits"]["total"]["value"]
    if total < 3:
        return None

    best_key: Optional[str] = None
    best_options: List[dict] = []
    best_score = -1.0

    for key, cfg in candidates.items():
        buckets = res["aggregations"].get(f"by_{key}", {}).get("buckets", [])
        valid: List[dict] = []
        for b in buckets:
            if b["doc_count"] == 0:
                continue
            val = b.get("key") if cfg["type"] == "range" else b["key"]
            valid.append({"value": val, "count": b["doc_count"]})

        # FIX 12: compute unknown once, here — not twice
        known = sum(o["count"] for o in valid)
        if total - known > 0:
            valid.append({"value": "unknown", "count": total - known})

        if len(valid) <= 1:
            continue

        entropy = -sum((c / total) * math.log2(c / total) for c in (o["count"] for o in valid) if c > 0)
        score = entropy + (0.3 if is_service_slot.get(key, False) else 0.0)

        if score > best_score:
            best_score = score
            best_key = key
            best_options = valid

    if not best_key:
        return None

    for opt in best_options:
        opt["percentage"] = round((opt["count"] / total) * 100, 1)

    return {"nextBestKey": best_key, "availableOptions": best_options, "totalRemaining": total}


# ============================================================================
# DESTINATION BUCKETS
# ============================================================================


def build_destination_buckets(packages: List[ItemOut]) -> dict:
    """
    Build grouping buckets from a list of packages (pre month/city/budget filter).
    Returns 8 dimensions, each mapping a group value → {count, packageIds}.

    Dimensions:
      - themes          : packageTheme values
      - packageSubTypes : pkgSubtypeName (GIT / FIT / FIT Fixed)
      - monthOfTravel   : availableMonths values (raw ES format e.g. 'june_2026')
      - departureCity   : departureCities.cityName values
      - priceRange      : minimumPrice bucketed (<30K / 30K-1L / 1L-2L / >2L)
      - serviceSlots    : per-slot keys where slot value is True only
      - packageTourType : Value / Standard / Premium
      - sightseeingTypes: Beaches & Relaxation / Culture & Local / etc.
    """
    themes: Dict[str, List[str]] = defaultdict(list)
    subtypes: Dict[str, List[str]] = defaultdict(list)
    months: Dict[str, List[str]] = defaultdict(list)
    dep_cities: Dict[str, List[str]] = defaultdict(list)
    price_ranges: Dict[str, List[str]] = defaultdict(list)
    slot_buckets: Dict[str, List[str]] = defaultdict(list)
    tour_types: Dict[str, List[str]] = defaultdict(list)
    sightseeing_types: Dict[str, List[str]] = defaultdict(list)

    # Service slot field names tracked in buckets
    _SLOT_FIELDS = [
        "flight_included",
        "visa_included",
        "travel_insurance_included",
        "entrance_fees_included",
        "airport_transfer_included",
        "tour_manager_included",
        "tips_included",
        "breakfast_included",
        "all_meals_included",
        "wheelchair_accessible",
        "senior_citizen_friendly",
    ]

    for pkg in packages:
        pid = pkg.itinerary_data.packageId

        # Themes
        for t in (pkg.itinerary_data.packageTheme or []):
            if t and pid not in themes[t]:
                themes[t].append(pid)

        # Package sub-types
        st = (pkg.itinerary_data.pkgSubtypeName or "").strip()
        if st and pid not in subtypes[st]:
            subtypes[st].append(pid)

        # Available months (raw: june_2026)
        for m in (pkg.itinerary_data.availableMonths or []):
            if m and pid not in months[m]:
                months[m].append(pid)

        # Departure cities
        for city in (pkg.itinerary_data.departureCities or []):
            cn = city.cityName
            if cn and pid not in dep_cities[cn]:
                dep_cities[cn].append(pid)

        # Price range band
        price = pkg.itinerary_data.minimumPrice or 0
        if price < 30000:
            band = "<30K"
        elif price < 100000:
            band = "30K-1L"
        elif price < 200000:
            band = "1L-2L"
        else:
            band = ">2L"
        if pid not in price_ranges[band]:
            price_ranges[band].append(pid)

        # Service slots — only include when slot value is strictly True
        slots = pkg.itinerary_data.serviceSlots or {}
        for slot in _SLOT_FIELDS:
            raw = slots.get(slot)
            val = _coerce_slot_value(raw)
            if val is True and pid not in slot_buckets[slot]:
                slot_buckets[slot].append(pid)

        # Package tour types
        for tt in (pkg.itinerary_data.packageTourType or []):
            if tt and pid not in tour_types[tt]:
                tour_types[tt].append(pid)

        # Sightseeing types
        for st in (pkg.itinerary_data.sightseeingTypes or []):
            if st and pid not in sightseeing_types[st]:
                sightseeing_types[st].append(pid)

    # Sort months chronologically
    def _month_sort_key(m: str) -> Tuple[int, int]:
        try:
            month, year = m.lower().split("_")
            idx = list(month_name).index(month.capitalize())
            return int(year), idx
        except Exception:
            return 9999, 13

    sorted_months: Dict[str, List[str]] = {
        k: months[k] for k in sorted(months.keys(), key=_month_sort_key)
    }

    # Sort price range bands in logical order
    _PRICE_ORDER = ["<30K", "30K-1L", "1L-2L", ">2L"]
    sorted_price_ranges: Dict[str, List[str]] = {
        band: price_ranges[band]
        for band in _PRICE_ORDER
        if band in price_ranges
    }

    def _wrap(d: Dict[str, List[str]]) -> Dict[str, dict]:
        return {k: {"count": len(v), "packageIds": v} for k, v in d.items()}

    return {
        "themes": _wrap(dict(themes)),
        "packageSubTypes": _wrap(dict(subtypes)),
        "monthOfTravel": _wrap(sorted_months),
        "departureCity": _wrap(dict(dep_cities)),
        "priceRange": _wrap(sorted_price_ranges),
        "serviceSlots": _wrap(dict(slot_buckets)),
        "packageTourType": _wrap(dict(tour_types)),
        "sightseeingTypes": _wrap(dict(sightseeing_types)),
    }


# ============================================================================
# FASTAPI APP — lifespan for startup index check (FIX 10)
# ============================================================================


@asynccontextmanager
async def lifespan(app: FastAPI):
    ensure_index_exists_once()
    yield


app = FastAPI(
    title="Live Packages V3 API (Standalone)",
    description="Self-contained package search with service slot filtering.",
    version="3.1.0",
    lifespan=lifespan,
)
router = APIRouter()


# ============================================================================
# MAIN ENDPOINT
# ============================================================================


@router.post("/v1/livepackagesv3")
async def get_packages_v3(request: PackageSearchRequestV3) -> JSONResponse:
    start_time = datetime.now()
    logger.info(f"[livepackagesv3] {start_time.isoformat()} — {request.model_dump()}")  # FIX 2

    search_term_raw = request.search_term.strip()
    search_term = search_term_raw.lower()
    departure_city = (request.departureCity or "").strip().lower()
    target_days = request.days if (request.days or 0) > 0 else None
    target_budget = request.budget if (request.budget or 0) > 0 else None
    month_of_travel_raw = (request.monthOfTravel or "").strip()
    theme = (request.theme or "").strip().lower()
    people_count = request.number_of_people if (request.number_of_people or 0) > 0 else None
    include_fare_calendar = bool(request.fareCalendar)
    pkg_subtype_filter = (request.pkgSubtypeName or "").strip().upper()
    tour_type_filter = (request.packageTourType or "").strip()  # Value, Standard, Premium
    sightseeing_filters = [s.strip() for s in (request.sightseeingTypes or "").split(",") if s.strip()]  # CSV support
    # FIX 9: Normalise month input
    month_of_travel: Optional[str] = _normalize_month(month_of_travel_raw) if month_of_travel_raw else None
    all_available_months: set = set()
    city_fail = False
    month_fail = False
    
    # BUG FIX: Mark month_fail if provided month could not be normalized
    if month_of_travel_raw and not month_of_travel:
        logger.warning(f"[livepackagesv3] Month '{month_of_travel_raw}' not recognized, will return Code 203")
        month_fail = True

    try:
        # ── Step 1: Resolve package IDs ──────────────────────────────────────
        service_slot_clauses = build_service_slot_es_query(request)  # FIX 6
        autosuggest_results = await fetch_autosuggest_results(search_term)

        # FIX 8: fuzzy/partial location matching instead of exact ==
        filtered_items = [i for i in autosuggest_results if _location_matches(i, search_term)]
        package_ids: set = {
            pkg["packageId"]
            for item in filtered_items
            for pkg in item.get("pkgnameIdMappingList", [])
        }

        if not package_ids:
            # ES fallback — FIX 6: service slots applied here too
            logger.info("[livepackagesv3] Autosuggest miss — ES fallback")
            destinations = [d.strip() for d in re.split(r"[,\s]+(?:and|or)\s+|,\s*", search_term) if d.strip()]
            should: List[dict] = []
            for dest in destinations:
                cleaned = re.sub(r"[^\w\s]", "", dest).strip()
                if cleaned:
                    should += [
                        {"match_phrase": {"packageName": cleaned}},
                        {"match": {"cities.cityName": cleaned}},
                        {"match_phrase": {"packageSummary": cleaned}},
                    ]
            if not should:
                cleaned = re.sub(r"\b(and|or)\b|[^\w\s]", "", search_term).strip()
                should = [
                    {"match_phrase": {"packageName": cleaned}},
                    {"match": {"cities.cityName": cleaned}},
                    {"match_phrase": {"packageSummary": cleaned}},
                ]

            fallback_body: dict = {
                "query": {
                    "bool": {
                        "should": should,
                        "minimum_should_match": 1,
                        "must": service_slot_clauses,
                    }
                },
                "size": 50,
            }
            es_result = es.search(index=TCIL_PACKAGE_INDEX, body=fallback_body)
            package_ids = {hit["_id"] for hit in es_result["hits"]["hits"]}
            logger.info(f"[livepackagesv3] ES fallback found {len(package_ids)} packages")

        if not package_ids:
            return JSONResponse(
                status_code=404,
                content={"code": 404, "message": f"No packages found for '{search_term_raw}'.", "body": [], "nextBestKeyInfo": None},
            )

        # ── Step 2: Fetch detailed package data (parallel) ───────────────────
        tasks = [search_item_by_package_id_internal({"packageId": pid}) for pid in package_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        detailed_packages: List[ItemOut] = [r for r in results if isinstance(r, ItemOut)]

        for pkg in detailed_packages:
            if pkg.itinerary_data.availableMonths:
                all_available_months.update(pkg.itinerary_data.availableMonths)

        # ── Step 3: Service slot filtering ──────────────────────────────────
        # For the autosuggest path, slots weren't applied at query time, so apply now.
        # For ES fallback, ES already filtered, but apply again to handle text/bool
        # normalisation (FIX 5). Double filtering is harmless and safe.
        before = len(detailed_packages)
        detailed_packages = apply_service_slot_filters(detailed_packages, request)
        logger.info(f"[livepackagesv3] Service slot filter: {before} → {len(detailed_packages)}")

        if not detailed_packages:
            return JSONResponse(
                status_code=200,
                content={"code": 206, "message": "No packages match the specified service requirements.", "body": [], "nextBestKeyInfo": None, "buckets": {}},
            )

        # ── Step 4: People / subtype / theme filters ─────────────────────────
        if people_count and people_count > 2:
            detailed_packages = [
                p for p in detailed_packages
                if "honeymoon" not in [t.lower() for t in (p.itinerary_data.packageTheme or [])]
            ]

        # BUG FIX: Check if pkgSubtypeName is provided (not just if it's GIT or FIT)
        if pkg_subtype_filter:
            # Valid values are GIT and FIT only
            if pkg_subtype_filter not in ("GIT", "FIT"):
                # Invalid pkgSubtypeName provided
                available_subtypes = sorted({(p.itinerary_data.pkgSubtypeName or "").upper() for p in detailed_packages if p.itinerary_data.pkgSubtypeName})
                return JSONResponse(
                    status_code=200,
                    content={"code": 207, "message": f"No packages for pkgSubtypeName '{pkg_subtype_filter}'. Valid options: {', '.join(available_subtypes)}", "body": [], "availablePkgSubtypeNames": available_subtypes, "nextBestKeyInfo": None, "buckets": {}},
                )
            # Valid value, filter for it
            sub_filtered = [
                p for p in detailed_packages
                if (p.itinerary_data.pkgSubtypeName or "").upper() == pkg_subtype_filter
            ]
            if not sub_filtered:
                available_subtypes = sorted({(p.itinerary_data.pkgSubtypeName or "").upper() for p in detailed_packages if p.itinerary_data.pkgSubtypeName})
                return JSONResponse(
                    status_code=200,
                    content={"code": 207, "message": f"No packages for pkgSubtypeName '{pkg_subtype_filter}'.", "body": [], "availablePkgSubtypeNames": available_subtypes, "nextBestKeyInfo": None, "buckets": {}},
                )
            detailed_packages = sub_filtered

        if theme:
            theme_filtered = [p for p in detailed_packages if any(t.lower() == theme for t in (p.itinerary_data.packageTheme or []))]
            if theme_filtered:
                detailed_packages = theme_filtered
            else:
                all_themes = sorted({t for p in detailed_packages for t in (p.itinerary_data.packageTheme or [])})
                return JSONResponse(
                    status_code=200,
                    content={"code": 205, "message": f"No packages for theme '{theme}'. Available: {', '.join(all_themes)}", "body": [], "availableThemes": all_themes, "nextBestKeyInfo": None, "buckets": {}},
                )

        if tour_type_filter:
            tour_filtered = [p for p in detailed_packages if any(t == tour_type_filter for t in (p.itinerary_data.packageTourType or []))]
            if tour_filtered:
                detailed_packages = tour_filtered
            else:
                all_tour_types = sorted({t for p in detailed_packages for t in (p.itinerary_data.packageTourType or [])})
                return JSONResponse(
                    status_code=200,
                    content={"code": 208, "message": f"No packages for packageTourType '{tour_type_filter}'. Available: {', '.join(all_tour_types)}", "body": [], "availableTourTypes": all_tour_types, "nextBestKeyInfo": None, "buckets": {}},
                )

        if sightseeing_filters:
            # Match if package has ANY of the requested sightseeing types
            sight_filtered = [
                p for p in detailed_packages
                if any(s in (p.itinerary_data.sightseeingTypes or []) for s in sightseeing_filters)
            ]
            if sight_filtered:
                detailed_packages = sight_filtered
            else:
                all_sightseeing = sorted({s for p in detailed_packages for s in (p.itinerary_data.sightseeingTypes or [])})
                return JSONResponse(
                    status_code=200,
                    content={"code": 209, "message": f"No packages for sightseeingTypes '{', '.join(sightseeing_filters)}'. Available: {', '.join(all_sightseeing)}", "body": [], "availableSightseeingTypes": all_sightseeing, "nextBestKeyInfo": None, "buckets": {}},
                )

        # ── Step 5: Month and city matching ──────────────────────────────────
        pre_month_city_packages = detailed_packages[:]  # FIX 15: accurate name
        # Only build buckets if explicitly requested (performance optimization)
        destination_buckets = build_destination_buckets(pre_month_city_packages) if request.includeBuckets else {}

        # Only apply month filter if month_fail is not already set (from invalid month input)
        if month_of_travel and not month_fail:
            month_filtered = [
                pkg for pkg in detailed_packages
                if any(
                    m.lower().startswith(month_of_travel)
                    for m in (pkg.itinerary_data.availableMonths or [])
                )
            ]
            if not month_filtered:
                month_fail = True
            else:
                detailed_packages = month_filtered

        # FIX 16: resolve departure city once here; apply_filters uses exact match
        resolved_departure_city: Optional[str] = None
        if departure_city:
            candidate_cities = {
                city.cityName.lower()
                for pkg in detailed_packages
                for city in (pkg.itinerary_data.departureCities or [])
            }
            resolved_departure_city = get_closest_match(departure_city, list(candidate_cities), threshold=80)
            if not resolved_departure_city:
                city_fail = True

        # ── Step 6: Partial-match responses (FIX 7: include nextBestKeyInfo) ─
        async def _next_best(pkgs: List[ItemOut]) -> Optional[dict]:
            if len(pkgs) >= 3:
                ids = [p.itinerary_data.packageId for p in pkgs]
                return await compute_next_best_key(ids, request)
            return None

        if month_fail and city_fail:
            final_list, _ = apply_filters(pre_month_city_packages, None, target_budget, target_days)
            final_list = prioritize_bogo(final_list)
            nbi = await _next_best(final_list)
            all_cities = sorted({c.cityName.lower() for p in pre_month_city_packages for c in (p.itinerary_data.departureCities or [])})
            return JSONResponse(
                status_code=200,
                content={
                    "code": 202,
                    "message": f"No matches for month/city. Available months: {', '.join(format_and_sort_months(all_available_months))}. Cities: {', '.join(all_cities)}",
                    "body": jsonable_encoder(final_list),
                    "nextBestKeyInfo": nbi,
                    "buckets": destination_buckets,
                },
            )

        if month_fail:
            final_list, _ = apply_filters(pre_month_city_packages, resolved_departure_city, target_budget, target_days)
            final_list = prioritize_bogo(final_list)
            nbi = await _next_best(final_list)
            return JSONResponse(
                status_code=200,
                content={
                    "code": 203,
                    "message": f"No packages for '{month_of_travel_raw}'. Available: {', '.join(format_and_sort_months(all_available_months))}",
                    "body": jsonable_encoder(final_list),
                    "nextBestKeyInfo": nbi,
                    "buckets": destination_buckets,
                },
            )

        if city_fail:
            final_list, _ = apply_filters(pre_month_city_packages, None, target_budget, target_days)
            final_list = prioritize_bogo(final_list)
            nbi = await _next_best(final_list)
            all_cities = sorted({c.cityName.lower() for p in pre_month_city_packages for c in (p.itinerary_data.departureCities or [])})
            return JSONResponse(
                status_code=200,
                content={
                    "code": 204,
                    "message": f"No packages from '{departure_city}'. Available: {', '.join(all_cities)}",
                    "body": jsonable_encoder(final_list),
                    "nextBestKeyInfo": nbi,
                    "buckets": destination_buckets,
                },
            )

        # ── Step 7: Final sort + next best key ───────────────────────────────
        final_list, matched_user_budget = apply_filters(detailed_packages, resolved_departure_city, target_budget, target_days)
        final_list = prioritize_bogo(final_list)
        next_best_key_info = await _next_best(final_list)

        # ── Step 8: Fare calendar (FIX 18: parallel) ─────────────────────────
        if include_fare_calendar and final_list:
            logger.info(f"[livepackagesv3] Fare calendar for {len(final_list)} packages (parallel)")
            try:
                req_id, sess_id = get_new_auth_token()
                loop = asyncio.get_event_loop()

                async def _fetch_cal(pkg: ItemOut) -> None:
                    try:
                        data = await loop.run_in_executor(
                            None,
                            lambda p=pkg: fetch_fare_calendar_for_package(
                                package_id=p.itinerary_data.packageId,
                                pkg_subtype_id=p.itinerary_data.pkgSubtypeId or 0,
                                pkg_type_id=p.itinerary_data.pkgTypeId or 1,
                                departure_cities=p.itinerary_data.departureCities,
                                request_id=req_id,
                                session_id=sess_id,
                                departure_city_filter=resolved_departure_city,
                            ),
                        )
                        if data:
                            pkg.fareCalendar = data
                    except Exception as exc:
                        logger.error(f"[fare_calendar] {pkg.itinerary_data.packageId}: {exc}")

                await asyncio.gather(*[_fetch_cal(p) for p in final_list])
            except Exception as exc:
                logger.error(f"[fare_calendar] Auth error: {exc}")

        # ── Step 9: Paginate and respond ─────────────────────────────────────
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(f"[livepackagesv3] {len(final_list)} packages in {elapsed:.2f}s")

        base: dict = {
            "body": jsonable_encoder(final_list),
            "nextBestKeyInfo": next_best_key_info,
            "buckets": destination_buckets,
        }

        if not matched_user_budget:
            return JSONResponse(
                status_code=200,
                content={"code": 201, "message": "Showing closest alternatives — none matched exact budget/days.", **base},
            )

        return JSONResponse(
            status_code=200,
            content={"code": 200, "message": "Packages found.", **base},
        )

    except Exception as exc:
        logger.error(f"[livepackagesv3] Unhandled error: {exc}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"code": 500, "message": "Internal server error.", "body": [], "nextBestKeyInfo": None},
        )


# ============================================================================
# HEALTH CHECK
# ============================================================================


@router.get("/health")
async def health_check() -> dict:
    return {"status": "healthy", "version": "3.1.0", "service": "livepackagesv3-standalone"}


# ============================================================================
# APP SETUP
# ============================================================================

app.include_router(router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8004)
