from fastapi import (
    FastAPI,
    HTTPException,
    Query,
    UploadFile,
    BackgroundTasks,
    Body,
    APIRouter,
)
from elasticsearch import Elasticsearch, RequestError, TransportError, helpers
from elasticsearch.exceptions import (
    NotFoundError,
    ConflictError,
    RequestError,
    TransportError,
)
import logging, re, os, time
from NewESmapping import (
    conversation_index_mapping,
    visa_faq_mapping,
    package_index_mapping,
    bug_index_mapping,
)
from constants import production, VISA_INDEX, TCIL_PACKAGE_INDEX, es, BUG_INDEX_NAME
from models import ItemOut, savedItineraryData, DepartureCity
import json, requests
from typing import Dict, List, Optional, Union, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from models import (
    DayItinerary,
    PackageItinerary,
    savedItineraryData,
    ItemOut,
    SearchItem,
    QueryRequest,
    PackageSearchRequest,
    AutoBudgetRequest,
    ChatMessage,
    SavedPackages,
    UserIdRequest,
    UpdateChatNameRequest,
    Conversation,
    ConversationIdRequest,
    DeleteConversationRequest,
)
from fetchpackages import fetch_package_dynamically
from datetime import datetime
from requests.exceptions import Timeout as RequestsTimeout, RequestException

RESOURCES_URL = "https://resources.thomascook.in/images/holidays/"


def _generate_image_urls(package_id: str, images: List[str]) -> List[str]:
    """Build fully-qualified itinerary image URLs for a package."""
    result = []
    for image in images:
        modified = image.replace(" ", "_")
        result.append(f"{RESOURCES_URL}{package_id}/itinerary/{modified}")
    return result


# In-memory caches (optional)
PACKAGE_CACHE: Dict[str, dict] = {}


def generate_flights_availability_description(
    pkg_subtype_name: str,
    pkg_subtype_id: int,
    is_flight_included: str,
    product_id: int,
    holiday_plus_subtype: int,
) -> str:
    """
    Generate a descriptive sentence about flight availability based on package parameters.

    Classification:
    - GIT: pkgSubtypeId 1=Domestic, 3=International
    - FIT: pkgSubtypeId 2=Domestic, 4=International
    - Holiday+: productId=11, holidayPlusSubType 1=Flights Included, 2=Flights Optional
    """

    # Determine if it's domestic or international
    if pkg_subtype_id in [1, 2]:
        classification = "Domestic"
    elif pkg_subtype_id in [3, 4]:
        classification = "International"
    else:
        classification = "Special"

    # GIT packages
    if pkg_subtype_name == "GIT":
        if is_flight_included == "Y":
            return f"This package includes flights in the package price."
        else:
            return f"This package does not include flights. You can book flights separately."

    # FIT packages with Holiday+
    elif pkg_subtype_name == "FIT" and product_id == 11:
        if holiday_plus_subtype == 1:
            return f"This package includes flights in the package price."
        elif holiday_plus_subtype == 2:
            return f"This package offers optional flights. You can choose to add flights or book separately."
        else:
            return f"Please check with our Agent for flight options."

    # Standard FIT packages
    elif pkg_subtype_name == "FIT":
        if is_flight_included == "Y":
            return f"This package includes flights in the package price."
        else:
            return f"This package does not include flights. You can book flights separately based on your preference."

    # Default case
    else:
        if is_flight_included == "Y":
            return "This package includes flights in the package price."
        else:
            return "This package does not include flights. You can arrange flights separately."


PACKAGE_CACHE_TIMESTAMPS: Dict[str, float] = {}
CACHE_TTL_SECONDS = 60 * 10  # Cache packages for 10 min


def ensure_index_exists(index_name: str):
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body=package_index_mapping)
        logging.info(f"Index {index_name} created with mapping.")
    else:
        logging.info(f"Index {index_name} already exists.")


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def create_visa_faq_index():
    if not es.indices.exists(index=VISA_INDEX):
        es.indices.create(index=VISA_INDEX, body=visa_faq_mapping)


def create_bug_report_index():
    """
    Create the bug reporting index if it doesn't exist.
    Called during application startup.
    """
    try:
        if not es.indices.exists(index=BUG_INDEX_NAME):
            es.indices.create(index=BUG_INDEX_NAME, mappings=bug_index_mapping)
            logging.info(f"Bug index '{BUG_INDEX_NAME}' created successfully.")
        else:
            logging.info(f"Bug index '{BUG_INDEX_NAME}' already exists.")
    except Exception as e:
        logging.error(f"Error creating bug index: {str(e)}")


def get_new_auth_token():
    """
    Fetch a new auth token from Thomas Cook API.

    Returns:
        Tuple[str, str]: (requestId, tokenId)
        - requestId is forwarded as 'Requestid' header in downstream requests
        - tokenId is forwarded as 'Sessionid' header in downstream requests
    """
    token_url = "https://services.thomascook.in/tcCommonRS/extnrt/getNewRequestToken"
    headers = {"uniqueId": "172.63.176.111", "user": "paytm"}

    # Create fresh session for token request to avoid WAF blocking
    fresh_session = requests.Session()
    try:
        logging.debug(f"Fetching auth token from {token_url}")
        response = fresh_session.get(token_url, headers=headers, timeout=10)

        if response.status_code == 406:
            logging.error(
                f"406 Not Acceptable from Thomas Cook API. WAF blocked the token request."
            )
            logging.error(f"Response: {response.text[:300]}")
            raise HTTPException(
                status_code=503,
                detail="Thomas Cook API firewall rejected token request (406). Try again in a few moments.",
            )

        response.raise_for_status()
        token_data = response.json()

        if token_data.get("errorCode") == 0:
            request_id = token_data["requestId"]
            token_id = token_data["tokenId"]
            logging.info(
                f"Successfully obtained auth token (requestId: {request_id[:10]}...)"
            )
            return request_id, token_id
        else:
            error_msg = token_data.get("errorMsg", "Unknown error")
            logging.error(f"Token API error: {error_msg}")
            raise HTTPException(
                status_code=500, detail=f"Error retrieving token: {error_msg}"
            )

    except requests.exceptions.Timeout:
        logging.error(f"Timeout while fetching auth token (timeout=10s)")
        raise HTTPException(
            status_code=503, detail="Timeout fetching auth token from Thomas Cook API"
        )
    except requests.exceptions.RequestException as e:
        logging.error(
            f"Request error while fetching auth token: {type(e).__name__}: {e}"
        )
        raise HTTPException(status_code=503, detail=f"Failed to fetch auth token: {e}")
    finally:
        fresh_session.close()


async def retrieve_visa_faq(query: str):
    try:
        create_visa_faq_index()
        response = es.search(
            index=VISA_INDEX, body={"query": {"match": {"visitingCountry": query}}}
        )
        results = [hit["_source"] for hit in response["hits"]["hits"]]
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


async def index_package_in_es(package_data: dict):
    # Index the PDP-fetched package into Elasticsearch.
    doc_id = package_data["packageId"]
    document = {
        "packageId": package_data["packageId"],
        "availableMonths": package_data["availableMonths"],
        "packageName": package_data["packageName"],
        "packageTheme": package_data["packageTheme"],
        "days": package_data["days"],
        "cities": package_data["cities"],
        "highlights": package_data["highlights"],
        "pdfName": package_data["pdfName"],
        "price": package_data.get("minimumPrice"),
        "minimumPrice": package_data.get("minimumPrice"),
        "packageData": package_data["packageData"],
        "packageSummary": package_data.get("packageSummary"),
        "thumbnailImage": package_data["thumbnailImage"],
        "images": package_data["images"],
        "visitingCountries": package_data.get("visitingCountries", []),
        "departureCities": package_data.get("departureCities", []),
        "packageItinerary": package_data["packageItinerary"],
        "hotels": package_data.get("hotels"),
        "hotels_list": package_data.get("hotels_list"),
        "continents": package_data.get("continents", []),
        "meals": package_data.get("meals"),
        "sightseeing": package_data.get("sightseeing"),
        "visa": package_data.get("visa"),
        "transfer": package_data.get("transfer"),
        "tourManagerDescription": package_data.get("tourManagerDescription"),
        "flightDescription": package_data.get("flightDescription"),
        "inclusions": package_data.get("inclusions"),
        "exclusions": package_data.get("exclusions"),
        "termsAndConditions": package_data.get("termsAndConditions"),
        "hashKey": package_data.get("hashKey"),
        "pkgSubtypeId": package_data["pkgSubtypeId"],
        "pkgSubtypeName": package_data["pkgSubtypeName"],
        "pkgTypeId": package_data["pkgTypeId"],
        "isFlightIncluded": package_data.get("isFlightIncluded"),
        "holidayPlusSubType": package_data.get("holidayPlusSubType"),
        "productId": package_data.get("productId"),
    }
    es.index(index=TCIL_PACKAGE_INDEX, id=doc_id, document=document)
    logging.info(f"Indexed package {doc_id} into {TCIL_PACKAGE_INDEX}")


def is_cache_valid(package_id: str) -> bool:
    """Check if the cached data for a given package ID is still valid."""
    if package_id not in PACKAGE_CACHE_TIMESTAMPS:
        return False
    last_updated = PACKAGE_CACHE_TIMESTAMPS[package_id]
    return (time.time() - last_updated) <= CACHE_TTL_SECONDS


def get_cached_package(package_id: str) -> Optional[dict]:
    """Retrieve package data from the in-memory cache if valid."""
    if package_id in PACKAGE_CACHE and is_cache_valid(package_id):
        return PACKAGE_CACHE[package_id]
    return None


def cache_package(package_id: str, data: dict):
    """Store package data in the in-memory cache with a timestamp."""
    PACKAGE_CACHE[package_id] = data
    PACKAGE_CACHE_TIMESTAMPS[package_id] = time.time()


async def fetch_package_with_retry(
    package_id: str,
    do_generate_summary: bool,
    max_retries: int = 3,
    backoff_factor: float = 2.0,
) -> Optional[dict]:
    """
    Attempts to fetch a package from PDP with retries and simple exponential backoff.
    Raises HTTPException(503) if exhausted.
    """
    from fetchpackages import fetch_package_dynamically

    attempt = 0
    while attempt < max_retries:
        try:
            result = fetch_package_dynamically(
                package_id, do_generate_summary=do_generate_summary
            )
            # fetch_package_dynamically returns {"processed": ..., "raw": ...}
            return result.get("processed") if isinstance(result, dict) else None
        except (RequestsTimeout, RequestException) as e:
            attempt += 1
            if attempt < max_retries:
                sleep_time = backoff_factor**attempt
                logging.warning(
                    f"Attempt {attempt}/{max_retries} to fetch package {package_id} failed. "
                    f"Retrying in {sleep_time} seconds. Error: {str(e)}"
                )
                time.sleep(sleep_time)
            else:
                raise HTTPException(
                    status_code=503,
                    detail=(
                        f"Failed to fetch package {package_id} after {max_retries} retries "
                        f"due to network/PDP issues. Error: {str(e)}"
                    ),
                )
    return None  # Should not be reached if we raise on final attempt


def construct_item_out_from_source(source_data: dict, score: float) -> ItemOut:
    """
    Helper function to map a single `packageData` source (from ES or PDP)
    into the final ItemOut pydantic model, including savedItineraryData.
    """
    pkg_itinerary = source_data.get("packageItinerary", {})
    summary_str = pkg_itinerary.get("summary", "")
    itinerary_list = pkg_itinerary.get("itinerary", [])

    day_itineraries = []
    for day_item in itinerary_list:
        day_itineraries.append(
            DayItinerary(
                day=day_item.get("day", 0),
                description=day_item.get("description", ""),
                mealDescription=day_item.get("mealDescription", ""),
                overnightStay=day_item.get("overnightStay", ""),
            )
        )
    itinerary_obj = PackageItinerary(summary=summary_str, itinerary=day_itineraries)

    # Construct departureCities using the DepartureCity model
    departure_cities = [
        DepartureCity(
            cityName=city.get("cityName", ""),
            cityCode=city.get("cityCode", ""),
            ltItineraryCode=city.get("ltItineraryCode", ""),
            holidayLtPricingId=city.get("holidayLtPricingId", ""),
        )
        for city in source_data.get("departureCities", [])
    ]

    # Generate flights availability description
    flights_availability = generate_flights_availability_description(
        pkg_subtype_name=source_data.get("pkgSubtypeName", ""),
        pkg_subtype_id=source_data.get("pkgSubtypeId", 0),
        is_flight_included=source_data.get("isFlightIncluded", "N"),
        product_id=source_data.get("productId", 0),
        holiday_plus_subtype=source_data.get("holidayPlusSubType", -1),
    )

    saved_data = savedItineraryData(
        packageId=source_data.get("packageId", ""),
        availableMonths=source_data.get("availableMonths", []),
        packageName=source_data.get("packageName", ""),
        packageTheme=source_data.get("packageTheme", []),
        days=source_data.get("days", 0),
        cities=source_data.get("cities", []),
        highlights=source_data.get("highlights", []),
        pdfName=source_data.get("pdfName", ""),
        price=source_data.get("price"),
        minimumPrice=source_data.get("minimumPrice"),
        packageData=source_data.get("packageData", ""),
        packageSummary=source_data.get("packageSummary", ""),
        thumbnailImage=source_data.get("thumbnailImage"),
        images=source_data.get("images", []),
        departureCities=departure_cities,
        packageItinerary=itinerary_obj,
        hotels=source_data.get("hotels"),
        hotels_list=source_data.get("hotels_list"),
        continents=source_data.get("continents", []),
        meals=source_data.get("meals"),
        visa=source_data.get("visa"),
        transfer=source_data.get("transfer"),
        sightseeing=source_data.get("sightseeing"),
        tourManagerDescription=source_data.get("tourManagerDescription"),
        flightDescription=source_data.get("flightDescription"),
        inclusions=source_data.get("inclusions"),
        exclusions=source_data.get("exclusions"),
        termsAndConditions=source_data.get("termsAndConditions"),
        pkgSubtypeId=source_data.get("pkgSubtypeId"),
        pkgSubtypeName=source_data.get("pkgSubtypeName", ""),
        pkgTypeId=source_data.get("pkgTypeId"),
        isFlightIncluded=source_data.get("isFlightIncluded"),
        holidayPlusSubType=source_data.get("holidayPlusSubType"),
        productId=source_data.get("productId"),
        flightsAvailability=flights_availability,
        serviceSlots=source_data.get("serviceSlots"),  # Add service slots
        constructed_thumbnailImage=(
            f"{RESOURCES_URL}{source_data.get('packageId', '')}/{source_data.get('thumbnailImage', '')}"
            if source_data.get("thumbnailImage")
            else ""
        ),
        constructed_images=_generate_image_urls(
            source_data.get("packageId", ""), source_data.get("images") or []
        ),
    )

    return ItemOut(
        id=source_data.get("packageId", "unknown_id"),
        itinerary_data=saved_data,
        score=score,
    )


async def search_item_by_package_id_internal(
    body: Dict[str, str], generate_summary: bool
) -> ItemOut:
    """
    Helper function to search for a package by packageId with dynamic summary generation,
    but WITHOUT falling back to PDP if not found in Elasticsearch.

    Args:
        body (dict): Request body containing the packageId.
        generate_summary (bool): Whether to generate a summary for the package.

    Returns:
        ItemOut: The package details and its search score.
    """

    index = TCIL_PACKAGE_INDEX  # Adjust to your index name

    # Basic input validation
    package_id = body.get("packageId")
    if not package_id or not isinstance(package_id, str):
        raise HTTPException(
            status_code=400, detail="packageId is required and must be a string."
        )

    # Optional regex check to avoid malicious input
    if not re.match(r"^[A-Za-z0-9\-\_]+$", package_id):
        raise HTTPException(
            status_code=400,
            detail="Invalid packageId format. Only alphanumeric, hyphens, or underscores are allowed.",
        )

    logging.info(
        f"[search_item_by_package_id_internal] Searching for package ID: {package_id}, generate_summary={generate_summary}"
    )

    # Optional: check cache first
    cached = get_cached_package(package_id)
    if cached:
        logging.info(
            f"[search_item_by_package_id_internal] Package {package_id} found in cache. Returning cached result."
        )
        return construct_item_out_from_source(cached, score=1.0)

    search_body = {"query": {"term": {"packageId.keyword": package_id}}, "size": 1}

    try:
        response = es.search(index=index, body=search_body)
        hits = response.get("hits", {}).get("hits", [])

        # Collect brief info for logging
        hit_summaries = []
        for hit in hits:
            doc_id = hit["_id"]
            source = hit.get("_source", {})
            package_name = source.get("packageName", "N/A")
            hit_summaries.append(f"[{doc_id}:{package_name}]")

        # Log only the packageId plus each ID:packageName pair
        logging.info(
            f"[search_item_by_package_id_internal] ES hits for packageId={package_id}: {hit_summaries}"
        )

        if not hits:
            # No fallback to PDP, so just return 404
            logging.info(
                f"[search_item_by_package_id_internal] No package found in ES for {package_id}; returning 404."
            )
            raise HTTPException(
                status_code=404,
                detail=f"No package found in Elasticsearch for packageId={package_id}",
            )

        # Found at least one doc in ES
        source = hits[0]["_source"]
        score = hits[0].get("_score", 1.0)

        # Optional: cache the result
        cache_package(package_id, source)

        return construct_item_out_from_source(source, score=score)

    except RequestError as e:
        logging.error(
            f"[search_item_by_package_id_internal] Elasticsearch RequestError: {str(e)}"
        )
        raise HTTPException(
            status_code=400, detail=f"Elasticsearch request error: {str(e)}"
        )
    except TransportError as e:
        logging.error(
            f"[search_item_by_package_id_internal] Elasticsearch TransportError: {str(e)}"
        )
        raise HTTPException(status_code=500, detail="Elasticsearch transport error")
    except HTTPException:
        # Re-raise custom HTTP errors
        raise
    except Exception as e:
        logging.error(
            f"[search_item_by_package_id_internal] Unexpected error: {str(e)}"
        )
        raise HTTPException(status_code=500, detail="Unexpected Internal Server Error")


def setup_logging():
    """
    Configures logging to write logs to a file with a daily rolling mechanism.
    Logs are stored in 'logs/' folder with the filename format 'YYYY-MM-DD.log'.
    """
    log_dir = os.path.join(os.getcwd(), "logs")  # Absolute path of the logs folder
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)  # Ensure the logs directory exists

    log_filename = os.path.join(log_dir, f"{datetime.now().strftime('%Y-%m-%d')}.log")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(
                log_filename, mode="a", encoding="utf-8"
            ),  # Write logs to file
            logging.StreamHandler(),  # Print logs to console
        ],
    )

    logging.info("Logging setup complete. Logs will be stored in: %s", log_filename)
    print(
        f"✅ Logs are being saved at: {log_filename}"
    )  # Prints the log file path on startup


from datetime import datetime
from calendar import month_name


def format_and_sort_months(month_year_list):
    # Current year to prioritize current year's months first
    current_year = datetime.now().year

    # Build a list of tuples (year, month_index, "Month Year")
    parsed_months = []
    for month_year in month_year_list:
        try:
            month, year = month_year.lower().split("_")
            month_index = list(month_name).index(month.capitalize())
            display = f"{month.capitalize()} {year}"
            parsed_months.append((int(year), month_index, display))
        except Exception:
            # In case of bad format, push it to the end
            parsed_months.append((9999, 13, month_year))

    # Sort: current year months first, then by year and month number
    parsed_months.sort(key=lambda x: (x[0] != current_year, x[0], x[1]))

    # Return the formatted strings
    return [item[2] for item in parsed_months]


def get_price_tier(package_price: float, target_budget: float) -> int:
    """
    Classifies the package into budget tiers based on ±10% range.
    - S1: Within ±10% of the target budget
    - S2: Greater than +10%
    - S3: Less than -10%
    """
    if not target_budget or target_budget <= 0:
        return 1  # Treat as S1 if budget not provided

    lower_bound = target_budget * 0.9
    upper_bound = target_budget * 1.1

    if lower_bound <= package_price <= upper_bound:
        return 1  # S1
    elif package_price > upper_bound:
        return 2  # S2
    else:
        return 3  # S3


def get_days_tier(package_days: int, target_days: int) -> int:
    """
    Classifies the package into days tiers based on ±10% range.
    - D1: Within ±10% of the target days
    - D2: Greater than +10%
    - D3: Less than -10%
    """
    if not target_days or target_days <= 0:
        return 1  # Default to D1 if no target specified

    lower_bound = target_days * 0.9
    upper_bound = target_days * 1.1

    if lower_bound <= package_days <= upper_bound:
        return 1  # D1
    elif package_days > upper_bound:
        return 2  # D2
    else:
        return 3  # D3


# ============================================================================
# Fare Calendar Integration
# ============================================================================


def fetch_fare_calendar_for_package(
    package_id: str,
    pkg_subtype_id: int,
    pkg_type_id: int,
    departure_cities: List[DepartureCity],
    request_id: Optional[str] = None,
    session_id: Optional[str] = None,
    departure_city_filter: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Fetch fare calendar data for a package based on its type (GIT or FIT).

    Args:
        package_id: Package ID
        pkg_subtype_id: Package subtype ID (1=DOM GIT, 2=DOM FIT, 3=INT GIT, 4=INT FIT)
        pkg_type_id: Package type ID (class)
        departure_cities: List of departure cities with LT codes
        request_id: Auth token request ID (if not provided, will fetch new one)
        session_id: Auth token session ID (if not provided, will fetch new one)
        departure_city_filter: Optional city name to fetch fare calendar for only that city

    Returns:
        Fare calendar dict or None if fetch fails
    """
    try:
        # Filter departure cities if specific city requested
        filtered_cities = departure_cities
        if departure_city_filter:
            filtered_cities = [
                city
                for city in departure_cities
                if city.cityName.lower() == departure_city_filter.lower()
            ]
            if not filtered_cities:
                logging.warning(
                    f"[fetch_fare_calendar] City '{departure_city_filter}' not found for {package_id}"
                )
                return None
            logging.info(
                f"[fetch_fare_calendar] Filtering to city: {departure_city_filter} for {package_id}"
            )

        # Get auth token if not provided
        if not request_id or not session_id:
            request_id, session_id = get_new_auth_token()
            if not request_id or not session_id:
                logging.error(
                    f"[fetch_fare_calendar] Failed to get auth token for {package_id}"
                )
                return None

        # Determine if GIT or FIT based on pkgSubtypeId
        # GIT: 1 (DOM), 3 (INT)
        # FIT: 2 (DOM), 4 (INT)
        is_git = pkg_subtype_id in [1, 3]

        if is_git:
            return fetch_fare_calendar_git(
                package_id,
                pkg_subtype_id,
                pkg_type_id,
                filtered_cities,
                request_id,
                session_id,
            )
        else:
            return fetch_fare_calendar_fit(
                package_id, pkg_subtype_id, pkg_type_id, request_id, session_id
            )

    except Exception as e:
        logging.error(f"[fetch_fare_calendar] Error for {package_id}: {str(e)}")
        return None


def fetch_fare_calendar_git(
    package_id: str,
    pkg_subtype_id: int,
    pkg_type_id: int,
    departure_cities: List[DepartureCity],
    request_id: str,
    session_id: str,
) -> Dict[str, Any]:
    """
    Fetch fare calendar for GIT packages (city-based pricing) in parallel.
    """
    url = "https://services.thomascook.in/tcHolidayRS/pdp.compare/fareCalender"
    headers = {
        "Requestid": request_id,
        "Sessionid": session_id,
        "Content-Type": "application/json",
    }

    result = {
        "departureCities": [],
        "summary": {
            "totalCities": len(departure_cities),
            "availableCities": 0,
            "overallDateRange": None,
            "priceRange": {"min": None, "max": None},
        },
    }

    all_min_prices = []
    all_max_prices = []
    overall_start = None
    overall_end = None

    def fetch_city_data(city):
        """Fetch fare calendar for a single city"""
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

            response = requests.post(url, json=payload, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()

            city_data = parse_git_fare_calendar_response(city, data)
            return city_data

        except Exception as e:
            logging.error(
                f"[fetch_fare_calendar_git] Error for city {city.cityName}: {str(e)}"
            )
            # Return unavailable city entry
            return {
                "cityName": city.cityName,
                "cityCode": city.cityCode,
                "ltItineraryCode": city.ltItineraryCode,
                "availability": {
                    "isAvailable": False,
                    "dateRange": None,
                    "stats": {
                        "totalBookableDates": 0,
                        "totalOnRequestDates": 0,
                        "minPrice": None,
                        "maxPrice": None,
                    },
                },
                "dates": {"bookable": [], "onRequest": []},
            }

    # Parallel execution using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Submit all city requests
        future_to_city = {
            executor.submit(fetch_city_data, city): city for city in departure_cities
        }

        # Collect results as they complete
        for future in as_completed(future_to_city):
            city_data = future.result()
            result["departureCities"].append(city_data)

            if city_data["availability"]["isAvailable"]:
                result["summary"]["availableCities"] += 1
                stats = city_data["availability"]["stats"]
                if stats["minPrice"]:
                    all_min_prices.append(stats["minPrice"])
                if stats["maxPrice"]:
                    all_max_prices.append(stats["maxPrice"])

                date_range = city_data["availability"]["dateRange"]
                if date_range:
                    if not overall_start or date_range["startDate"] < overall_start:
                        overall_start = date_range["startDate"]
                    if not overall_end or date_range["endDate"] > overall_end:
                        overall_end = date_range["endDate"]

    # Sort departureCities to maintain consistent order (by cityName)
    result["departureCities"].sort(key=lambda x: x["cityName"])

    # Update summary
    if all_min_prices:
        result["summary"]["priceRange"]["min"] = min(all_min_prices)
    if all_max_prices:
        result["summary"]["priceRange"]["max"] = max(all_max_prices)
    if overall_start and overall_end:
        result["summary"]["overallDateRange"] = {
            "startDate": overall_start,
            "endDate": overall_end,
        }

    return result


def fetch_fare_calendar_fit(
    package_id: str,
    pkg_subtype_id: int,
    pkg_type_id: int,
    request_id: str,
    session_id: str,
) -> Dict[str, Any]:
    """
    Fetch fare calendar for FIT packages (class-based pricing) in parallel.
    Queries all 3 class types: Standard (0), Value (1), Premium (2)
    """
    url = "https://services.thomascook.in/tcHolidayRS/pdp.compare/fareCalender"
    headers = {
        "Requestid": request_id,
        "Sessionid": session_id,
        "Content-Type": "application/json",
    }

    class_names = {"0": "Standard", "1": "Value", "2": "Premium"}

    result = {
        "classTypes": [],
        "summary": {
            "totalClasses": 3,
            "availableClasses": 0,
            "overallDateRange": None,
            "priceRange": {"min": None, "max": None},
        },
        "departureCityNote": "Common pricing across all departure cities",
    }

    all_min_prices = []
    all_max_prices = []
    overall_start = None
    overall_end = None

    def fetch_class_data(class_id):
        """Fetch fare calendar for a single class type"""
        try:
            payload = {
                "ltItineraryCode": [],
                "market": "-1",
                "hubCode": "",
                "pkgSubTypeId": pkg_subtype_id,
                "pkgClassId": class_id,
                "pkgId": package_id,
                "mode": "TCIL",
                "isHsa": "N",
                "isCanvasPackage": "Y",
            }

            response = requests.post(url, json=payload, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()

            class_data = parse_fit_fare_calendar_response(
                class_id, class_names[class_id], data
            )
            return class_data

        except Exception as e:
            logging.error(
                f"[fetch_fare_calendar_fit] Error for class {class_names[class_id]}: {str(e)}"
            )
            # Return unavailable class entry
            return {
                "className": class_names[class_id],
                "classId": class_id,
                "availability": {
                    "isAvailable": False,
                    "dateRange": None,
                    "stats": {
                        "totalBookableDates": 0,
                        "totalOnRequestDates": 0,
                        "minPrice": None,
                        "maxPrice": None,
                        "avgPrice": None,
                    },
                },
                "dates": {"bookable": [], "onRequest": []},
            }

    # Parallel execution using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=3) as executor:
        # Submit all class requests
        future_to_class = {
            executor.submit(fetch_class_data, class_id): class_id
            for class_id in ["0", "1", "2"]
        }

        # Collect results as they complete
        for future in as_completed(future_to_class):
            class_data = future.result()
            result["classTypes"].append(class_data)

            if class_data["availability"]["isAvailable"]:
                result["summary"]["availableClasses"] += 1
                stats = class_data["availability"]["stats"]
                if stats["minPrice"]:
                    all_min_prices.append(stats["minPrice"])
                if stats["maxPrice"]:
                    all_max_prices.append(stats["maxPrice"])

                date_range = class_data["availability"]["dateRange"]
                if date_range:
                    if not overall_start or date_range["startDate"] < overall_start:
                        overall_start = date_range["startDate"]
                    if not overall_end or date_range["endDate"] > overall_end:
                        overall_end = date_range["endDate"]

    # Sort classTypes to maintain consistent order (by classId)
    result["classTypes"].sort(key=lambda x: x["classId"])

    # Update summary
    if all_min_prices:
        result["summary"]["priceRange"]["min"] = min(all_min_prices)
    if all_max_prices:
        result["summary"]["priceRange"]["max"] = max(all_max_prices)
    if overall_start and overall_end:
        result["summary"]["overallDateRange"] = {
            "startDate": overall_start,
            "endDate": overall_end,
        }

    return result


def parse_git_fare_calendar_response(
    city: DepartureCity, response_data: Dict
) -> Dict[str, Any]:
    """Parse GIT fare calendar API response (ltResponseBean structure)."""
    lt_response = response_data.get("ltResponseBean", {})
    # Check both root level and ltResponseBean level for isDateAvialable
    is_available = (
        response_data.get("isDateAvialable", "NO") == "YES"
        or lt_response.get("isDateAvialable", "NO") == "YES"
    )

    bookable_dates = []
    on_request_dates = []

    if is_available:
        # Parse bookable dates - API uses uppercase field names
        for date_item in lt_response.get("bookable", []):
            bookable_dates.append(
                {
                    "date": date_item.get("DATE", ""),
                    "price": int(date_item.get("DR_PRICE", 0)),
                    "strikeOutPrice": int(date_item.get("DR_STRIKEOUT", 0)),
                    "availableInventory": int(date_item.get("AVL_INV", 0)),
                    "lastSellDay": int(date_item.get("LAST_SELL_DAY", 0)),
                    "ltProdCode": date_item.get("LT_PROD_CODE", ""),
                }
            )

        # Parse on-request dates
        for date_item in lt_response.get("onRequest", []):
            on_request_dates.append(
                {
                    "date": date_item.get("DATE", ""),
                    "ltProdCode": date_item.get("LT_PROD_CODE", ""),
                }
            )

    # Calculate stats
    min_price = None
    max_price = None
    start_date = None
    end_date = None

    if bookable_dates:
        prices = [d["price"] for d in bookable_dates if d["price"] > 0]
        if prices:
            min_price = min(prices)
            max_price = max(prices)

        start_date = bookable_dates[0]["date"]
        end_date = bookable_dates[-1]["date"]

    return {
        "cityName": city.cityName,
        "cityCode": city.cityCode,
        "ltItineraryCode": city.ltItineraryCode,
        "availability": {
            "isAvailable": is_available,
            "dateRange": (
                {"startDate": start_date, "endDate": end_date} if start_date else None
            ),
            "stats": {
                "totalBookableDates": len(bookable_dates),
                "totalOnRequestDates": len(on_request_dates),
                "minPrice": min_price,
                "maxPrice": max_price,
            },
        },
        "dates": {"bookable": bookable_dates, "onRequest": on_request_dates},
    }


def parse_fit_fare_calendar_response(
    class_id: str, class_name: str, response_data: Dict
) -> Dict[str, Any]:
    """Parse FIT fare calendar API response (dbResponseBean structure)."""
    db_response = response_data.get("dbResponseBean", {})
    # Check both root level and dbResponseBean level for isDateAvialable
    is_available = (
        response_data.get("isDateAvialable", "NO") == "YES"
        or db_response.get("isDateAvialable", "NO") == "YES"
    )

    bookable_dates = []
    on_request_dates = []

    if is_available:
        # Parse bookable dates - API uses lowercase field names for FIT
        for date_item in db_response.get("bookable", []):
            bookable_dates.append(
                {
                    "date": date_item.get("date", ""),
                    "price": int(date_item.get("price", 0)),
                    "strikeOutPrice": int(date_item.get("strikeOutPrice", 0)),
                }
            )

        # Parse on-request dates
        for date_item in db_response.get("onRequest", []):
            on_request_dates.append(
                {
                    "date": date_item.get("date", ""),
                    "price": int(date_item.get("price", 0)),
                }
            )

    # Calculate stats
    min_price = None
    max_price = None
    avg_price = None
    start_date = None
    end_date = None

    if bookable_dates:
        prices = [d["price"] for d in bookable_dates if d["price"] > 0]
        if prices:
            min_price = min(prices)
            max_price = max(prices)
            avg_price = sum(prices) // len(prices)

        start_date = bookable_dates[0]["date"]
        end_date = bookable_dates[-1]["date"]

    return {
        "className": class_name,
        "classId": class_id,
        "availability": {
            "isAvailable": is_available,
            "dateRange": (
                {"startDate": start_date, "endDate": end_date} if start_date else None
            ),
            "stats": {
                "totalBookableDates": len(bookable_dates),
                "totalOnRequestDates": len(on_request_dates),
                "minPrice": min_price,
                "maxPrice": max_price,
                "avgPrice": avg_price,
            },
        },
        "dates": {"bookable": bookable_dates, "onRequest": on_request_dates},
    }
