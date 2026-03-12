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


# Global variable to store token data
cached_token = {
    "request_id": None,
    "token_id": None,
    "expiration_time": 0,  # Epoch timestamp
}


# Add SRP API constants
SRP_API_URL = "https://services.thomascook.in/tcHolidayRS/srp/packages"


TOKEN_URL = "https://services.thomascook.in/tcCommonRS/extnrt/getNewRequestToken"
PDP_BASE_URL = "https://services.thomascook.in/tcHolidayRS/packagedetails/pdp/"
SUMMARY_API_URL = "http://45.194.2.100:8000/v1/chat/completions"


def determine_flight_included_from_logic(
    pkg_subtype_name: str,
    pkg_subtype_id: int,
    is_flight_included: str,
    product_id: int,
    holiday_plus_subtype: int
):
    """
    Determine if flights are included using the same logic as generate_flights_availability_description.
    
    Returns:
        True: Flights included in package price
        False: Flights not included (book separately)
        "optional": Optional flights (can be added to package)
        None: Unclear status
    """
    # GIT packages
    if pkg_subtype_name == "GIT":
        if is_flight_included == "Y":
            return True
        else:
            return False
    
    # FIT packages with Holiday+
    elif pkg_subtype_name == "FIT" and product_id == 11:
        if holiday_plus_subtype == 1:
            return True  # Flights Included
        elif holiday_plus_subtype == 2:
            return "optional"  # Flights Optional
        else:
            return None  # Unclear
    
    # Standard FIT packages
    elif pkg_subtype_name == "FIT":
        if is_flight_included == "Y":
            return True
        else:
            return False
    
    # Default case
    else:
        if is_flight_included == "Y":
            return True
        else:
            return False


def extract_service_slots_with_llm(
    inclusions: str,
    exclusions: str,
    meals: list,
    visa: list,
    visiting_countries: list,
    pkg_subtype_name: str,
    pkg_subtype_id: int,
    is_flight_included: str,
    product_id: int,
    holiday_plus_subtype: int,
    package_id: str = "Unknown"
):
    """
    Extract service-level boolean slots using the same LLM as summary generation.
    Uses existing logic for flight_included instead of LLM extraction.
    
    Returns:
        dict: Service slots with all 11 attributes or None if extraction fails
    """
    try:
        # Determine flight_included using existing logic (NOT LLM)
        flight_included = determine_flight_included_from_logic(
            pkg_subtype_name=pkg_subtype_name,
            pkg_subtype_id=pkg_subtype_id,
            is_flight_included=is_flight_included,
            product_id=product_id,
            holiday_plus_subtype=holiday_plus_subtype
        )
        
        # Clean and truncate source fields for LLM
        def clean_for_llm(text, max_length=800):
            if not text:
                return ""
            cleaned = clean_text(remove_html_tags(str(text)))
            return cleaned[:max_length]
        
        inclusions_clean = clean_for_llm(inclusions, max_length=800)
        exclusions_clean = clean_for_llm(exclusions, max_length=600)
        meals_str = str(meals)[:300]
        visa_str = str(visa)[:300]
        
        # Determine package type
        package_type = "domestic" if (len(visiting_countries) == 1 and visiting_countries[0] == "India") else "international"
        
        # Construct prompt for service slot extraction (10 slots - excluding flight_included)
        prompt = f"""Extract service-level boolean attributes from this travel package.

Package Type: {package_type}
Package Subtype: {pkg_subtype_name} (ID: {pkg_subtype_id})
Countries: {visiting_countries}

Inclusions:
{inclusions_clean}

Exclusions:
{exclusions_clean}

Meals Info: {meals_str}
Visa Info: {visa_str}

Analyze the inclusions and exclusions to determine which services are included. Return ONLY valid JSON (no markdown):

{{
  "visa_included": true/false,
  "travel_insurance_included": true/false,
  "entrance_fees_included": true/false,
  "airport_transfer_included": true/false,
  "tour_manager_included": true/false,
  "tips_included": true/false,
  "breakfast_included": true/false,
  "all_meals_included": true/false,
  "wheelchair_accessible": true/false,
  "senior_citizen_friendly": true/false
}}

Rules:
- true: Service explicitly mentioned in inclusions
- false: Service explicitly mentioned in exclusions
- false: Service not mentioned (no information)

Return ONLY the JSON object."""

        # Call LLM API (same endpoint as summary generation)
        payload = {
            "model": "openai/gpt-oss-120b",
            "messages": [{"role": "user", "content": prompt}],
            "stream": False,
            "temperature": 0.1,
            "response_format": {"type": "json_object"}
        }
        
        response = requests.post(SUMMARY_API_URL, json=payload, timeout=30)
        
        if response.status_code != 200:
            logging.warning(f"Service slot extraction failed for {package_id}: HTTP {response.status_code}")
            return None
        
        content = response.json().get("choices", [{}])[0].get("message", {}).get("content", "")
        
        # Parse JSON response
        content_clean = content.strip()
        if content_clean.startswith("```json"):
            content_clean = content_clean[7:]
        if content_clean.startswith("```"):
            content_clean = content_clean[3:]
        if content_clean.endswith("```"):
            content_clean = content_clean[:-3]
        content_clean = content_clean.strip()
        
        service_slots = json.loads(content_clean)
        
        # Add flight_included from logic-based determination
        service_slots["flight_included"] = flight_included
        
        logging.info(f"✅ Successfully extracted service slots for {package_id} (flight_included from logic: {flight_included})")
        return service_slots
        
    except Exception as e:
        logging.warning(f"Error extracting service slots for {package_id}: {str(e)}")
        return None


SIGHTSEEING_CATEGORIES = [
    "Beaches & Relaxation",
    "Nature & Scenic Spots",
    "Culture & Local",
    "Wellness & Spa",
    "Nightlife & Vibe",
    "Food & Local Cafes",
]

SIGHTSEEING_SKIP_PHRASES = {"sightseeing as per itinerary"}


def extract_sightseeing_types_with_llm(
    sightseeing: list,
    package_id: str = "Unknown"
) -> list:
    """
    Classify the sightseeing activities of a package into one or more predefined
    categories using the LLM.

    Categories:
        - Beaches & Relaxation
        - Nature & Scenic Spots
        - Culture & Local
        - Wellness & Spa
        - Nightlife & Vibe
        - Food & Local Cafes

    Returns:
        list: Subset of the above categories that apply, or [] if sightseeing
              data is empty / only placeholder text.
    """
    try:
        if not sightseeing:
            return []

        # Filter out empty / placeholder entries; entries are already HTML-stripped by extract_sightseeing
        valid_entries = [
            clean_text(entry)
            for entry in sightseeing
            if entry and entry.strip().lower() not in SIGHTSEEING_SKIP_PHRASES
        ]

        if not valid_entries:
            return []

        sightseeing_text = "\n".join(valid_entries)[:1500]  # cap to avoid token overflow

        categories_str = "\n".join(f"- {c}" for c in SIGHTSEEING_CATEGORIES)

        # response_format json_object requires the model to return a JSON object,
        # so we wrap the result in {"categories": [...]}.
        prompt = f"""You are a travel package classifier. Analyze the sightseeing activities listed below and decide which of the following categories apply to this package.

Available categories:
{categories_str}

Sightseeing activities:
{sightseeing_text}

Rules:
- Select ONLY categories that are clearly supported by the sightseeing data.
- A category may be selected if at least one activity strongly matches it.
- Use EXACT category names from the list above.
- Return ONLY a valid JSON object with a single key "categories" whose value is an array of matching category names. If none apply, use an empty array.

Example output: {{"categories": ["Culture & Local", "Nature & Scenic Spots"]}}

Output:"""

        payload = {
            "model": "openai/gpt-oss-120b",
            "messages": [{"role": "user", "content": prompt}],
            "stream": False,
            "temperature": 0.1,
            "response_format": {"type": "json_object"},
        }

        response = requests.post(SUMMARY_API_URL, json=payload, timeout=30)

        if response.status_code != 200:
            logging.warning(f"Sightseeing type extraction failed for {package_id}: HTTP {response.status_code}")
            return []

        content = response.json().get("choices", [{}])[0].get("message", {}).get("content", "")

        content_clean = content.strip()
        if content_clean.startswith("```json"):
            content_clean = content_clean[7:]
        if content_clean.startswith("```"):
            content_clean = content_clean[3:]
        if content_clean.endswith("```"):
            content_clean = content_clean[:-3]
        content_clean = content_clean.strip()

        parsed = json.loads(content_clean)

        # Handle both {"categories": [...]} and direct [...] responses
        if isinstance(parsed, dict):
            result = parsed.get("categories", [])
        elif isinstance(parsed, list):
            result = parsed
        else:
            result = []

        # Validate — only keep known categories
        valid_categories = [c for c in result if c in SIGHTSEEING_CATEGORIES]

        logging.info(f"✅ Sightseeing types for {package_id}: {valid_categories}")
        return valid_categories

    except Exception as e:
        logging.warning(f"Error extracting sightseeing types for {package_id}: {str(e)}")
        return []


def remove_html_tags(text):
    if text:
        clean = re.compile("<.*?>")
        text = re.sub(clean, "", text)
        return html.unescape(text)
    return ""


def clean_text(text):
    return " ".join(text.split()) if text else ""


def get_new_auth_token():
    """
    Fetch a new auth token from Thomas Cook API.

    IMPORTANT: Uses a FRESH session for each request to avoid WAF blocking.
    The API's firewall (AppTrana) blocks repeated requests on the same session
    as suspicious behavior. Using fresh sessions for token requests avoids the 406 error.

    Token is cached for 58 minutes to reduce API calls.
    """
    global cached_token

    current_time = time.time()  # Current time in seconds
    if cached_token["token_id"] and cached_token["expiration_time"] > current_time:
        # Return cached token if it's still valid
        logging.debug(
            f"Using cached token (expires in {cached_token['expiration_time'] - current_time:.0f}s)"
        )
        return cached_token["request_id"], cached_token["token_id"]

    # Create FRESH session for token request to avoid WAF blocking
    fresh_session = requests.Session()
    try:
        headers = {"uniqueId": "172.63.176.111", "user": "paytm"}
        logging.debug(f"Fetching new auth token from {TOKEN_URL}")
        response = fresh_session.get(TOKEN_URL, headers=headers, timeout=10)

        if response.status_code == 406:
            logging.error(
                f"406 Not Acceptable from Thomas Cook API. WAF blocked the request."
            )
            logging.error(f"Response preview: {response.text[:300]}")
            raise RuntimeError(
                "Thomas Cook API firewall (AppTrana) rejected the request. "
                "This may be temporary. The API is blocking repeated requests as suspicious behavior."
            )

        response.raise_for_status()
        token_data = response.json()

        if token_data.get("errorCode") == 0:
            request_id = token_data["requestId"]
            token_id = token_data["tokenId"]
            # 58 min safety margin (3480 seconds)
            expiration_time = current_time + 3480

            # Update the cache
            cached_token = {
                "request_id": request_id,
                "token_id": token_id,
                "expiration_time": expiration_time,
            }
            logging.info(
                f"Successfully fetched new auth token. Expires in ~58 minutes."
            )
            return request_id, token_id
        else:
            logging.error(f"Token API error: {token_data.get('errorMsg')}")
            raise Exception(f"Error retrieving token: {token_data.get('errorMsg')}")

    except requests.exceptions.Timeout:
        logging.error(f"Timeout fetching auth token from {TOKEN_URL}")
        raise RuntimeError("Timeout while fetching auth token from Thomas Cook API")
    except requests.exceptions.RequestException as e:
        logging.error(f"Request error fetching auth token: {e}")
        raise RuntimeError(f"Error fetching auth token: {e}")
    finally:
        fresh_session.close()


def get_pdp_details(package_id, request_id, session_id):
    """
    Fetch package details from PDP endpoint using FRESH session.

    FIX FOR 406 ERROR: Uses fresh session for each request to avoid WAF blocking.
    The API's firewall blocks repeated requests on the same session as suspicious behavior.
    """
    url = f"{PDP_BASE_URL}{package_id}"
    headers = {"requestid": request_id, "sessionid": session_id}

    # Use FRESH session to avoid 406 WAF blocking
    fresh_session = requests.Session()
    try:
        response = fresh_session.get(url, headers=headers, timeout=60)
        response.raise_for_status()
        return response.json()
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
    Returns a dict: { packageClassId: typeDefaultMsg (stripped of HTML tags) }
    """
    result = {}
    for category in category_collection:
        if category.get("type") == "Accomodation":
            class_id = str(category.get("packageClassId", "0"))
            msg = remove_html_tags(category.get("typeDefaultMsg", ""))
            if msg:
                result[class_id] = msg
    return result


def extract_continent_info(city_collection):
    """
    Extract unique country + continent info from tcilHolidayCityCollection.
    Deduplicates by countryCode.
    Returns a list of dicts: [{ "continentId": int, "continentName": str, "countryCode": str, "countryName": str }]
    """
    seen_codes = set()
    result = []
    for city in city_collection:
        country_continent = (
            city.get("cityCode", {})
            .get("tcilMstCountryStateMapping", {})
            .get("tcilMstCountryContinentMapping", {})
        )
        country_code = country_continent.get("countryCode")
        country_name = country_continent.get("countryName")
        continent = country_continent.get("countryContinentId", {})
        continent_id = continent.get("continentId")
        continent_name = continent.get("continentName")
        if country_code and country_code not in seen_codes:
            seen_codes.add(country_code)
            result.append({
                "continentId": continent_id,
                "continentName": continent_name,
                "countryCode": country_code,
                "countryName": country_name,
            })
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
        tourManagerDescription = packageDetail.get("tourManagerDescription")
        flightDescription = packageDetail.get("flightDefaultMsg")
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
        continents = extract_continent_info(
            packageDetail.get("tcilHolidayCityCollection", [])
        )

        # Extract package tour types based on class availability flags
        package_tour_type = []
        if packageDetail.get("isPackageClassStandard") == "Y":
            package_tour_type.append("Standard")
        if packageDetail.get("isPackageClassDelux") == "Y":
            package_tour_type.append("Value")
        if packageDetail.get("isPackageClassPremium") == "Y":
            package_tour_type.append("Premium")

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

        # Extract service slots (Phase 1: 8 slots)
        service_slots = extract_service_slots_with_llm(
            inclusions=inclusions,
            exclusions=exclusions,
            meals=mealsDetails,
            visa=visa_data,
            visiting_countries=list(unique_countries),
            pkg_subtype_name=pkgSubtypeName,
            pkg_subtype_id=pkgSubtypeId_value,
            is_flight_included=isFlightIncluded,
            product_id=productId,
            holiday_plus_subtype=holidayPlusSubType,
            package_id=packageId
        ) if do_generate_summary else None  # Only extract if summary generation is enabled

        # Extract sightseeing types
        sightseeing_types = extract_sightseeing_types_with_llm(
            sightseeing=sightseeing,
            package_id=packageId
        ) if do_generate_summary else []

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
            "continents": continents,
            "packageTourType": package_tour_type,
            "meals": mealsDetails,
            "visa": visa_data,
            "transfer": transfer_data,
            "sightseeing": sightseeing,
            "tourManagerDescription": tourManagerDescription,
            "flightDescription": flightDescription,
            "inclusions": inclusions,
            "exclusions": exclusions,
            "termsAndConditions": termsAndConditions,
            "pkgSubtypeId": pkgSubtypeId_value,
            "isFlightIncluded": isFlightIncluded,
            "holidayPlusSubType": holidayPlusSubType,
            "productId": productId,
            "pkgSubtypeName": pkgSubtypeName,
            "pkgTypeId": pkgTypeId,
            "serviceSlots": service_slots,  # Add service slots to package data
            "sightseeingTypes": sightseeing_types,
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
    Call the SRP API to filter packages based on availability and month of travel.

    FIX FOR 406 ERROR: Uses fresh session to avoid WAF blocking on parallel calls.
    """
    request_id, session_id = get_new_auth_token()

    # Prepare the searchType parameter (comma-separated package IDs)
    search_type = ",".join(package_ids)

    headers = {
        "uniqueId": "172.63.176.111",
        "user": "paytm",
        "requestid": request_id,
        "sessionid": session_id,
        "Accept": "application/json",
        "User-Agent": "package-service/1.0",
    }

    params = {"searchType": search_type}
    if month_of_travel:
        params["monthOfTravel"] = month_of_travel

    # Use FRESH session for SRP to avoid 406 WAF blocking on parallel calls
    fresh_session = requests.Session()
    try:
        response = fresh_session.get(
            SRP_API_URL, headers=headers, params=params, timeout=300
        )
        response.raise_for_status()

        srp_data = response.json()
        unique_package_ids = {
            pkg["packageDetail"]["packageId"]
            for pkg in srp_data
            if "packageDetail" in pkg
        }
        logging.info(
            f"SRP returned {len(unique_package_ids)} package IDs for month {month_of_travel}"
        )
        return list(unique_package_ids)

    except requests.HTTPError as http_err:
        logging.error(f"HTTP error: {http_err}")
        return []
    except requests.RequestException as req_err:
        logging.error(f"Request error: {req_err}")
        return []
    except Exception as e:
        logging.error(f"Unexpected error with SRP API: {e}")
        return []
    finally:
        fresh_session.close()


def fetch_packages_for_month(month_of_travel: str) -> List[str]:
    """
    Fetch package IDs for the given month from the SRP API.

    FIX FOR 406 ERROR: Uses fresh session to avoid WAF blocking on parallel calls.
    When multiple tasks fetch different months simultaneously, fresh sessions prevent
    the WAF from detecting repeated requests as bot behavior.
    """
    request_id, session_id = get_new_auth_token()

    headers = {
        "uniqueId": "172.63.176.111",
        "user": "paytm",
        "requestid": request_id,
        "sessionid": session_id,
        "Accept": "application/json",
    }
    params = {"searchType": "", "monthOfTravel": month_of_travel}

    # Use FRESH session for SRP to avoid 406 WAF blocking on parallel calls
    fresh_session = requests.Session()
    try:
        response = fresh_session.get(
            SRP_API_URL, headers=headers, params=params, timeout=300
        )
        response.raise_for_status()

        srp_data = response.json()
        package_ids = [
            pkg["packageDetail"]["packageId"]
            for pkg in srp_data
            if "packageDetail" in pkg
        ]
        logging.info(f"Fetched {len(package_ids)} packages for month {month_of_travel}")
        return package_ids

    except requests.HTTPError as http_err:
        logging.error(
            f"HTTP error fetching packages for {month_of_travel}: {str(http_err)}"
        )
        return []
    except requests.RequestException as req_err:
        logging.error(
            f"Request error fetching packages for {month_of_travel}: {str(req_err)}"
        )
        return []
    except Exception as e:
        logging.error(f"Unexpected error with SRP API for {month_of_travel}: {str(e)}")
        return []
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
