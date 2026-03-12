from typing import List, Optional
import requests
import re
import html
import json
import hashlib
from datetime import datetime
import httpx
import time
import logging
import requests, time

# Global variable to store token data
cached_token = {
    "request_id": None,
    "token_id": None,
    "expiration_time": 0,  # Epoch timestamp
}


# Add SRP API constants
SRP_API_URL = "https://services.sotc.in/holidayRS/srp/packages"


TOKEN_URL = "https://services.sotc.in/commonRS/extnrt/getNewRequestToken"
PDP_BASE_URL = "https://services.sotc.in/holidayRS/packagedetails/pdp/"
SUMMARY_API_URL = "http://45.194.2.100:8000/v1/chat/completions"


def remove_html_tags(text):
    if text:
        clean = re.compile("<.*?>")
        text = re.sub(clean, "", text)
        return html.unescape(text)
    return ""


def clean_text(text):
    return " ".join(text.split()) if text else ""


session = (
    requests.Session()
)  # persist cookies for SRP/PDP requests (NOT for token requests)
COMMON_HEADERS = {
    "uniqueId": "172.63.176.111",
    "user": "pathfndr",
    "User-Agent": "PostmanRuntime/7.43.3",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}


def get_new_auth_token():
    """
    Fetch a new auth token from SOTC API.

    IMPORTANT: Uses a FRESH session for each request to avoid WAF blocking.
    The API's firewall (AppTrana) blocks repeated requests on the same session
    as suspicious behavior. Using fresh sessions for token requests avoids the 406 error.

    The persistent 'session' should only be used for SRP/PDP requests after we have
    a valid token.
    """
    global cached_token
    if cached_token["token_id"] and cached_token["expiration_time"] > time.time():
        logging.debug(
            f"Using cached token (expires in {cached_token['expiration_time'] - time.time():.0f}s)"
        )
        return cached_token["request_id"], cached_token["token_id"]

    # Create a FRESH session for token request to avoid 406 WAF blocking
    fresh_session = requests.Session()
    try:
        logging.debug(f"Fetching new auth token from {TOKEN_URL}")
        r = fresh_session.get(TOKEN_URL, headers=COMMON_HEADERS, timeout=15)

        if r.status_code == 406:
            logging.error(f"406 Not Acceptable from SOTC API. WAF blocked the request.")
            logging.error(f"Response preview: {r.text[:300]}")
            raise RuntimeError(
                "SOTC API firewall (AppTrana) rejected the request. "
                "This may be temporary. The API is blocking repeated requests as suspicious behavior."
            )

        r.raise_for_status()
        data = r.json()

        if data.get("errorCode") != 0:
            error_msg = data.get("errorMsg", "Unknown error")
            logging.error(f"API returned error: {error_msg}")
            raise RuntimeError(f"Token API error: {error_msg}")

        request_id = data.get("requestId")
        token_id = data.get("tokenId")

        if not request_id or not token_id:
            logging.error(f"Invalid token response: {data}")
            raise RuntimeError(
                f"Invalid token response from API: missing requestId or tokenId"
            )

        cached_token = {
            "request_id": request_id,
            "token_id": token_id,
            "expiration_time": time.time() + 3500,  # 58 min safety margin
        }
        logging.info(f"Successfully fetched new auth token. Expires in ~58 minutes.")
        return request_id, token_id

    except requests.exceptions.Timeout:
        logging.error(f"Timeout fetching auth token from {TOKEN_URL}")
        raise RuntimeError("Timeout while fetching auth token from SOTC API")
    except requests.exceptions.RequestException as e:
        logging.error(f"Request error fetching auth token: {e}")
        raise RuntimeError(f"Error fetching auth token: {e}")
    finally:
        fresh_session.close()


def _build_headers(request_id: str, session_id: str) -> dict:
    """Merge BASE_HEADERS with the per-request ids."""
    return {**COMMON_HEADERS, "requestid": request_id, "sessionid": session_id}


def get_pdp_details(package_id: str, request_id: str, session_id: str) -> dict:
    """
    Fetch package details from PDP endpoint using FRESH session.

    FIX FOR 406 ERROR: Uses fresh session for each request to avoid WAF blocking.
    The API's firewall blocks repeated requests on the same session as suspicious behavior.
    """
    url = f"{PDP_BASE_URL}{package_id}"
    headers = _build_headers(request_id, session_id)

    # Use FRESH session to avoid 406 WAF blocking
    fresh_session = requests.Session()
    try:
        resp = fresh_session.get(url, headers=headers, timeout=60)
        resp.raise_for_status()
        return resp.json()
    finally:
        fresh_session.close()


def construct_prompt_from_api_response(package_data):
    """Construct prompt based on the fetched package data."""
    package_detail = package_data.get("packageDetail", {})
    if not package_detail:
        raise Exception("Invalid package data structure")

    package_name = package_detail.get("pkgName", "Unnamed Package")
    duration = package_detail.get("duration", "N/A")
    city_collection = package_detail.get("tcilHolidayItineraryCollection", [])

    # Extract city names
    cities = [
        city.get("cityCode", {}).get("cityName", "Unknown City")
        for city in city_collection
    ]
    cities_text = ", ".join(cities)

    # Extract highlights
    highlights = [
        clean_text(remove_html_tags(package_detail.get("overviewHighlights")))
    ]
    highlights_text = ", ".join(highlights)

    # Construct the full prompt
    prompt = (
        f"Generate a travel package summary in one paragraph without bullet points or structured formatting. "
        f"The summary should include the package name, duration, cities covered, and key activities. "
        f"Example: The 'Scandinavian Dhamaka (Summer 2024)' package is an 8-day tour covering Copenhagen, Oslo, Geilo, "
        f"Stockholm, and Gothenburg. The itinerary includes visits to the Parliament building, Amalienborg Palace, "
        f"Gefion fountain, Little Mermaid statue, and many other interesting sights. Participants will enjoy guided "
        f"city tours, cruises, and visits to microbreweries and porcelain factories.\n\n"
        f"Package Name: '{package_name}'\n"
        f"Duration: {duration} days\n"
        f"Cities Covered: {cities_text}\n"
        f"Key Highlights: {highlights_text}\n\n"
        f"Summary:"
    )
    return prompt


def generate_summary_from_pdp_data(package_data, do_generate_summary: bool = True):
    """
    Generate a summary using the updated summary generation logic from summ_gen.py.
    Uses OSS model: openai/gpt-oss-120b
    """
    if not do_generate_summary:
        return ""

    try:
        prompt = construct_prompt_from_api_response(package_data)

        payload = {
            "model": "openai/gpt-oss-120b",
            "messages": [{"role": "user", "content": prompt}],
            "stream": False,
            "temperature": 0.1,
            "include_reasoning": False,
        }

        response = requests.post(SUMMARY_API_URL, json=payload, stream=False)

        if response.status_code == 200:
            return (
                response.json()
                .get("choices", [{}])[0]
                .get("message", {})
                .get("content", "")
            )
        else:
            logging.warning(
                f"Summary API returned {response.status_code} for package. Body: {response.text}"
            )
            return ""
    except Exception as e:
        logging.exception(f"Error generating summary: {e}")
        return "Error generating summary."


def extract_departure_cities(lt_pricing_collection):
    """Extract city info from the tcilHolidayLtPricingCollection field."""
    return [
        {
            "cityName": pricing.get("hubCode", {}).get("cityName", ""),
            "cityCode": pricing.get("hubCode", {}).get("cityCode", ""),
            "ltItineraryCode": pricing.get("ltItineraryCode", ""),
            "holidayLtPricingId": pricing.get("holidayLtPricingId", ""),
        }
        for pricing in lt_pricing_collection
        if pricing.get("hubCode", {}).get("cityName")
    ]


def extract_city_info(city_collection):
    return sorted(
        [
            {
                "day": city.get("itineraryDay"),
                "cityName": city.get("cityCode", {}).get("cityName", ""),
                "meals": remove_html_tags(city.get("mealDescription")),
                "description": remove_html_tags(city.get("itineraryDescription")),
                "mealDescription": remove_html_tags(city.get("mealDescription", "")),
                "overnightStay": remove_html_tags(city.get("overnightStay", "")),
                "geoLocation": {
                    "lat": city.get("cityCode", {}).get("latitude"),
                    "lon": city.get("cityCode", {}).get("longitude"),
                },
            }
            for city in city_collection
        ],
        key=lambda x: x["day"],
    )


def extract_highlights(city_collection):
    seen = set()
    result = []
    for city in city_collection:
        highlight = clean_text(remove_html_tags(city.get("highlights", "")))
        if highlight and highlight not in seen:
            seen.add(highlight)
            result.append(highlight)
    return result


def extract_hotels_envisaged(category_collection):
    seen = set()
    result = []
    for category in category_collection:
        if category.get("type") == "Accomodation":
            hotel = remove_html_tags(category.get("typeDefaultMsg", ""))
            if hotel and hotel not in seen:
                seen.add(hotel)
                result.append(hotel)
    return result


def extract_hotels_list(category_collection):
    """
    Extract hotel info segregated by packageClassId.
    Returns a dict: { packageClassId: typeDefaultMsg (raw HTML preserved for rendering) }
    Multiple entries with the same packageClassId are concatenated.
    """
    result = {}
    for category in category_collection:
        if category.get("type") == "Accomodation":
            class_id = str(category.get("packageClassId", "0"))
            msg = remove_html_tags(category.get("typeDefaultMsg", ""))
            if msg:
                result[class_id] = msg
    return result


def extract_sightseeing(category_collection):
    seen = set()
    result = []
    for category in category_collection:
        if category.get("type") == "Sightseeing":
            sight = remove_html_tags(category.get("typeDefaultMsg", ""))
            if sight and sight not in seen:
                seen.add(sight)
                result.append(sight)
    return result


def extract_meals(category_collection):
    seen = set()
    result = []
    for category in category_collection:
        if category.get("type") == "Meal":
            meal = remove_html_tags(category.get("typeDefaultMsg", ""))
            if meal and meal not in seen:
                seen.add(meal)
                result.append(meal)
    return result


def extract_visa_data(category_collection):
    seen = set()
    result = []
    for category in category_collection:
        if category.get("type") == "Visa":
            visa = remove_html_tags(category.get("typeDefaultMsg", ""))
            if visa and visa not in seen:
                seen.add(visa)
                result.append(visa)
    return result


def extract_transfer_data(category_collection):
    seen = set()
    result = []
    for category in category_collection:
        if category.get("type") == "Transfer":
            Transfer = remove_html_tags(category.get("typeDefaultMsg", ""))
            if Transfer and Transfer not in seen:
                seen.add(Transfer)
                result.append(Transfer)
    return result


def extract_inclusions_exclusions(inc_exc_collection):
    inclusions = (
        remove_html_tags(inc_exc_collection[0].get("includes", ""))
        if inc_exc_collection
        else ""
    )
    exclusions = (
        remove_html_tags(inc_exc_collection[0].get("excludes", ""))
        if inc_exc_collection
        else ""
    )
    return inclusions, exclusions


def extract_terms_and_conditions(payment_terms_collection):
    return (
        remove_html_tags(payment_terms_collection[0].get("notesDescription", ""))
        if payment_terms_collection
        else ""
    )


def format_package_data(package):
    formatted = (
        f"Package ID: {package['packageId']} is named '{package['packageName']}' and spans {package['days']} days. "
        "The itinerary includes:\n"
    )
    for city in package["itinerary"]:
        formatted += (
            f"Day {city['day']} - {city['cityName']}, Meals: {clean_text(city['meals']) if city['meals'] else 'N/A'}, "
            f"Description: {clean_text(city['description']) if city['description'] else 'N/A'}, "
            f"GeoLocation: (Lat: {city['geoLocation']['lat']}, Lon: {city['geoLocation']['lon']}).\n"
        )
    formatted += (
        f"The price is {package['standardPrice']}. Hotels envisaged include: {', '.join(package['Hotels Envisaged'])}. "
        f"Meals include: {', '.join(package['Meals'])}. Inclusions: {clean_text(package['Inclusions']) if package['Inclusions'] else 'N/A'}. "
        f"Exclusions: {clean_text(package['Exclusions']) if package['Exclusions'] else 'N/A'}. "
        f"Product Terms and Conditions: {clean_text(package['Product Terms and Conditions']) if package['Product Terms and Conditions'] else 'N/A'}. "
        f"Highlights of the package: {clean_text(' '.join(package['highlights']))}."
    )
    return formatted.strip()


def process_pdp_package_response(item, do_generate_summary: bool = True):
    def ensure_list_of_clean_strings(value):
        if isinstance(value, list):
            return [
                clean_text(remove_html_tags(str(v)))
                for v in value
                if isinstance(v, str)
            ]
        elif isinstance(value, str):
            return [clean_text(remove_html_tags(value))]
        elif value is not None:
            return [clean_text(remove_html_tags(str(value)))]
        return []

    try:
        packageDetail = item.get("packageDetail", {})
        packageId = packageDetail.get("packageId")
        packageName = packageDetail.get("pkgName")
        pkgSubtypeId = packageDetail.get("pkgSubtypeId", {})
        isFlightIncluded = packageDetail.get("isFlightIncluded")
        holidayPlusSubType = packageDetail.get("holidayPlusSubType")
        productId = packageDetail.get("productId")
        pkgSubtypeId_value = pkgSubtypeId.get("pkgSubtypeId")
        pkgSubtypeName = pkgSubtypeId.get("pkgSubtypeName")
        pkgTypeId = pkgSubtypeId.get("pkgTypeId")
        duration = packageDetail.get("duration")
        city_collection = packageDetail.get("tcilHolidayItineraryCollection", [])

        cities_info = extract_city_info(city_collection)
        for city in cities_info:
            city["geoLocation"]["lat"] = 0.0
            city["geoLocation"]["lon"] = 0.0

        highlights = ensure_list_of_clean_strings(
            packageDetail.get("overviewHighlights")
        )

        hotels_envisaged = extract_hotels_envisaged(
            packageDetail.get("tcilHolidayCategoryCollection", [])
        )
        hotels_list = extract_hotels_list(
            packageDetail.get("tcilHolidayCategoryCollection", [])
        )
        sightseeing = extract_sightseeing(
            packageDetail.get("tcilHolidayCategoryCollection", [])
        )
        mealsDetails = extract_meals(
            packageDetail.get("tcilHolidayCategoryCollection", [])
        )
        visa_data = extract_visa_data(
            packageDetail.get("tcilHolidayCategoryCollection", [])
        )
        transfer_data = extract_transfer_data(
            packageDetail.get("tcilHolidayCategoryCollection", [])
        )
        inclusions, exclusions = extract_inclusions_exclusions(
            packageDetail.get("tcilHolidayIncludeExcludeCollection", [{}])
        )
        termsAndConditions = extract_terms_and_conditions(
            packageDetail.get("tcilHolidayPaymentTermsCollection", [{}])
        )

        lt_pricing_collection = packageDetail.get("tcilHolidayLtPricingCollection", [])
        departure_cities = extract_departure_cities(lt_pricing_collection)

        standardPrice = item.get("minimumStartingPrice", 0)
        if standardPrice == 0:
            standardPrice = item.get("startingPriceStandard", 0)
        if standardPrice == 0:
            standardPrice = item.get("startingPricePremium", 0)

        strikeOutPrice = item.get("strikeoutPriceStandard", 0)

        all_images_with_days = [
            (city.get("image"), city.get("itineraryDay"))
            for city in city_collection
            if city.get("image")
        ]
        sorted_images = [
            image for image, day in sorted(all_images_with_days, key=lambda x: x[1])
        ]

        themes = packageDetail.get("tcilHolidayThemeCollection", [])
        packageThemes = [
            remove_html_tags(theme.get("tcilMstHolidayThemes", {}).get("name", ""))
            for theme in themes
            if "tcilMstHolidayThemes" in theme
        ]

        unique_countries = set()
        for city in city_collection:
            country_name = (
                city.get("cityCode", {})
                .get("tcilMstCountryStateMapping", {})
                .get("tcilMstCountryContinentMapping", {})
                .get("countryName")
            )
            if country_name:
                unique_countries.add(country_name)

        thumbnailImage = packageDetail.get("packageThumbnailImage")

        package_data = format_package_data(
            {
                "packageId": packageId,
                "packageName": packageName,
                "days": duration,
                "itinerary": cities_info,
                "highlights": highlights,
                "Hotels Envisaged": hotels_envisaged,
                "Meals": mealsDetails,
                "Inclusions": inclusions,
                "Exclusions": exclusions,
                "Product Terms and Conditions": termsAndConditions,
                "standardPrice": standardPrice,
                "strikeOutPrice": strikeOutPrice,
            }
        )

        # clean_hotels = [clean_text(remove_html_tags(h)) for h in hotels_envisaged]
        # clean_sightseeing = [clean_text(remove_html_tags(s)) for s in sightseeing]
        # clean_meals = [clean_text(remove_html_tags(m)) for m in mealsDetails]
        # clean_visa_data = [clean_text(remove_html_tags(v)) for v in visa_data]
        # clean_transfer_data = [clean_text(remove_html_tags(t)) for t in transfer_data]
        # clean_inclusions = clean_text(remove_html_tags(inclusions))
        # clean_exclusions = clean_text(remove_html_tags(exclusions))
        # clean_terms = clean_text(remove_html_tags(termsAndConditions))

        summary = generate_summary_from_pdp_data(item, do_generate_summary)
        summary_generated = bool(summary)

        logging.info(
            f"Summary generated for package {packageId}: {'Yes' if summary_generated else 'No'}"
        )

        return {
            "packageId": packageId,
            "packageName": packageName,
            "days": duration,
            "cities": [
                {"cityName": city["cityName"], "geoLocation": city["geoLocation"]}
                for city in cities_info
            ],
            "visitingCountries": list(unique_countries),
            "images": sorted_images,
            "thumbnailImage": thumbnailImage,
            "packageTheme": packageThemes,
            "minimumPrice": standardPrice,
            "highlights": highlights,
            "pdfName": packageDetail.get("standardAutoPdf"),
            "departureCities": departure_cities,
            "packageData": package_data,
            "packageSummary": summary,
            "packageItinerary": {
                "summary": summary,
                "itinerary": [
                    {
                        "day": city["day"],
                        "description": city["description"],
                        "mealDescription": city["mealDescription"],
                        "overnightStay": city["overnightStay"],
                    }
                    for city in cities_info
                ],
            },
            "hotels": hotels_envisaged,
            "hotels_list": hotels_list,
            "meals": mealsDetails,
            "visa": visa_data,
            "transfer": transfer_data,
            "sightseeing": sightseeing,
            "inclusions": inclusions,
            "exclusions": exclusions,
            "termsAndConditions": termsAndConditions,
            "pkgSubtypeId": pkgSubtypeId_value,
            "pkgSubtypeName": pkgSubtypeName,
            "pkgTypeId": pkgTypeId,
            "isFlightIncluded": isFlightIncluded,
            "holidayPlusSubType": holidayPlusSubType,
            "productId": productId,
        }

    except Exception as e:
        print(f"Error processing item: {e}")
        return None


def fetch_package_dynamically(package_id, do_generate_summary: bool = True):
    """
    Fetch package details from PDP endpoint with retry logic and exponential backoff.

    FIX FOR 406 ERROR: Implements comprehensive error handling:
    1. Fresh sessions for PDP requests (avoid WAF blocking)
    2. Exponential backoff on 406/429 errors (1s → 2s → 4s)
    3. Automatic retry on transient failures
    4. Proper session cleanup to prevent connection pool exhaustion

    Args:
        package_id: Package ID to fetch
        do_generate_summary: Whether to generate AI summary

    Returns:
        dict: Processed package data or None if all retries fail
    """
    max_retries = 3
    base_delay = 1.0  # seconds

    for attempt in range(max_retries):
        try:
            # Get/refresh auth token
            request_id, session_id = get_new_auth_token()
            if not request_id or not session_id:
                logging.error(f"Failed to obtain auth token for package {package_id}")
                return None

            # Add exponential backoff delay on retry (but not on first attempt)
            if attempt > 0:
                delay = base_delay * (2 ** (attempt - 1))  # 1s, 2s, 4s
                logging.info(
                    f"[Retry {attempt + 1}/{max_retries}] Backing off {delay}s before retry for {package_id}..."
                )
                time.sleep(delay)

            # Fetch PDP data with fresh session (uses fresh session internally)
            pdp_data = get_pdp_details(package_id, request_id, session_id)

            if isinstance(pdp_data, list):
                if len(pdp_data) == 0:
                    return None
                pdp_data = pdp_data[0]

            if not isinstance(pdp_data, dict):
                return None

            # Compute hashKey from the raw pdp_data
            pdp_data_str = json.dumps(pdp_data, sort_keys=True)
            hash_obj = hashlib.sha256(pdp_data_str.encode("utf-8"))
            hash_key = hash_obj.hexdigest()

            processed_data = process_pdp_package_response(
                pdp_data, do_generate_summary=do_generate_summary
            )
            if processed_data is not None:
                processed_data["hashKey"] = hash_key

            logging.info(f"Successfully fetched package {package_id}")
            return {"processed": processed_data, "raw": pdp_data}

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 406:
                logging.warning(
                    f"[Attempt {attempt + 1}] 406 WAF blocking for {package_id}. Retrying with backoff..."
                )
                if attempt < max_retries - 1:
                    continue  # Retry with next iteration's exponential backoff
                else:
                    logging.error(
                        f"Error fetching package {package_id}: 406 Client Error after {max_retries} attempts"
                    )
                    return None
            elif e.response.status_code == 429:
                logging.warning(
                    f"[Attempt {attempt + 1}] 429 Rate limited for {package_id}. Retrying with backoff..."
                )
                if attempt < max_retries - 1:
                    continue
                else:
                    logging.error(
                        f"Error fetching package {package_id}: 429 Too Many Requests after {max_retries} attempts"
                    )
                    return None
            else:
                logging.error(
                    f"[Attempt {attempt + 1}] HTTP {e.response.status_code} for {package_id}: {str(e)}"
                )
                return None

        except requests.exceptions.Timeout:
            logging.warning(
                f"[Attempt {attempt + 1}] Timeout fetching package {package_id}"
            )
            if attempt < max_retries - 1:
                continue
            else:
                logging.error(
                    f"Error fetching package {package_id}: Timeout after {max_retries} attempts"
                )
                return None

        except requests.exceptions.RequestException as e:
            logging.warning(
                f"[Attempt {attempt + 1}] Request error for {package_id}: {str(e)}"
            )
            if attempt < max_retries - 1:
                continue
            else:
                logging.error(f"Error fetching package {package_id}: {str(e)}")
                return None

        except Exception as e:
            logging.error(f"Unexpected error fetching package {package_id}: {str(e)}")
            return None

    return None


def get_srp_packages(
    package_ids: List[str], month_of_travel: Optional[str] = None
) -> List[str]:
    """
    Query the SRP endpoint and return the unique package IDs it sends back.

    FIX FOR 406 ERROR: Uses fresh session for SRP requests to avoid WAF blocking.
    When multiple parallel tasks call SRP simultaneously, reusing persistent session
    triggers WAF bot detection. Fresh sessions avoid this.
    """
    request_id, session_id = get_new_auth_token()

    params = {"searchType": ",".join(package_ids)}
    if month_of_travel:
        params["monthOfTravel"] = month_of_travel  # MM-YYYY

    # Use FRESH session for SRP to avoid 406 WAF blocking on parallel calls
    fresh_session = requests.Session()
    try:
        resp = fresh_session.get(
            SRP_API_URL,
            headers=_build_headers(request_id, session_id),
            params=params,
            timeout=60,
        )
        resp.raise_for_status()

        srp_data = resp.json()
        result = [
            pkg["packageDetail"]["packageId"]
            for pkg in srp_data
            if "packageDetail" in pkg
        ]
        logging.info(
            f"SRP returned {len(result)} package IDs for month {month_of_travel}"
        )
        return result
    finally:
        fresh_session.close()


def fetch_packages_for_month(month_of_travel: str) -> List[str]:
    """
    Return ALL package IDs available in the given month (MM-YYYY).

    FIX FOR 406 ERROR: Uses fresh session for SRP requests to avoid WAF blocking.
    When multiple parallel tasks fetch packages for different months simultaneously,
    reusing persistent session triggers WAF bot detection. Fresh sessions prevent this.
    """
    request_id, session_id = get_new_auth_token()

    # Use FRESH session for SRP to avoid 406 WAF blocking on parallel calls
    fresh_session = requests.Session()
    try:
        resp = fresh_session.get(
            SRP_API_URL,
            headers=_build_headers(request_id, session_id),
            params={"searchType": "", "monthOfTravel": month_of_travel},
            timeout=60,
        )
        resp.raise_for_status()

        srp_data = resp.json()
        package_ids = [
            pkg["packageDetail"]["packageId"]
            for pkg in srp_data
            if "packageDetail" in pkg
        ]
        logging.info(f"Fetched {len(package_ids)} packages for month {month_of_travel}")
        return package_ids
    finally:
        fresh_session.close()


def month_name_to_index(month_name: str) -> str:
    """
    Converts month name to its corresponding numeric index (01 for January, 02 for February, etc.).
    """
    month_map = {
        "january": "01",
        "february": "02",
        "march": "03",
        "april": "04",
        "may": "05",
        "june": "06",
        "july": "07",
        "august": "08",
        "september": "09",
        "october": "10",
        "november": "11",
        "december": "12",
    }
    return month_map.get(month_name.lower(), None)
