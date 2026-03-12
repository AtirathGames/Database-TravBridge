import requests
import re
import html
import json
import hashlib
from datetime import datetime

TOKEN_URL = "https://services.thomascook.in/tcCommonRS/extnrt/getNewRequestToken"
PDP_BASE_URL = "https://services.thomascook.in/tcHolidayRS/packagedetails/pdp/"
SUMMARY_API_URL = "https://travbridge.atirath.com/api/generate"

def remove_html_tags(text):
    if text:
        clean = re.compile('<.*?>')
        text = re.sub(clean, '', text)
        return html.unescape(text)
    return ""

def clean_text(text):
    return ' '.join(text.split()) if text else ""

def get_new_auth_token():
    headers = {
        "uniqueId": "172.63.176.111",
        "user": "paytm"
    }
    response = requests.get(TOKEN_URL, headers=headers)
    response.raise_for_status()
    token_data = response.json()
    if token_data.get("errorCode") == 0:
        return token_data["requestId"], token_data["tokenId"]
    else:
        raise Exception(f"Error retrieving token: {token_data.get('errorMsg')}")

def get_pdp_details(package_id, request_id, session_id):
    url = f"{PDP_BASE_URL}{package_id}"
    headers = {
        "requestid": request_id,
        "sessionid": session_id
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def generate_summary(prompt):
    payload = {
        "model": "llama3.1:8b-instruct-fp16",
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0.1}
    }
    resp = requests.post(SUMMARY_API_URL, json=payload)
    if resp.status_code == 200:
        return resp.json().get("response", "")
    else:
        return f"Error: Unable to generate summary (Status Code: {resp.status_code})."

def extract_city_info(city_collection):
    return sorted([
        {
            "day": city.get("itineraryDay"),
            "cityName": city.get("cityCode", {}).get("cityName", ""),
            "meals": remove_html_tags(city.get("mealDescription")),
            "description": remove_html_tags(city.get("itineraryDescription")),
            "geoLocation": {
                "lat": city.get("cityCode", {}).get("latitude"),
                "lon": city.get("cityCode", {}).get("longitude")
            }
        } for city in city_collection
    ], key=lambda x: x['day'])

def extract_highlights(city_collection):
    return [
        clean_text(remove_html_tags(city.get("highlights", "")))
        for city in city_collection if city.get("highlights")
    ]

def extract_hotels_envisaged(category_collection):
    return [
        remove_html_tags(category.get("typeDefaultMsg", ""))
        for category in category_collection if category.get("type") == "Accomodation"
    ]

def extract_meals(category_collection):
    return [
        remove_html_tags(category.get("typeDefaultMsg", ""))
        for category in category_collection if category.get("type") == "Meal"
    ]

def extract_inclusions_exclusions(inc_exc_collection):
    inclusions = remove_html_tags(inc_exc_collection[0].get("includes", "")) if inc_exc_collection else ""
    exclusions = remove_html_tags(inc_exc_collection[0].get("excludes", "")) if inc_exc_collection else ""
    return inclusions, exclusions

def extract_terms_and_conditions(payment_terms_collection):
    return remove_html_tags(payment_terms_collection[0].get("notesDescription", "")) if payment_terms_collection else ""

def format_package_data(package):
    formatted = (
        f"Package ID: {package['packageId']} is named '{package['packageName']}' and spans {package['days']} days. "
        "The itinerary includes:\n"
    )
    for city in package['itinerary']:
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

BASE_PROMPT = """
    Generate a concise and structured summary for the given package data using exactly the following format. Do not include any introductory statements or explanations—only the summary in the specified format.

    Example Format:
    The '[PACKAGE NAME]' package is a [DURATION]-day tour covering [KEY DESTINATIONS]. The itinerary includes [HIGHLIGHTS AND ACTIVITIES]. Participants will enjoy [SPECIAL EXPERIENCES OR FEATURES].

    Now, summarize the provided package data directly using this format.
"""

def process_pdp_package_response(item, do_generate_summary: bool = True):
    try:
        packageDetail = item.get("packageDetail", {})
        packageId = packageDetail.get("packageId")
        packageName = packageDetail.get("pkgName")
        duration = packageDetail.get("duration")
        city_collection = packageDetail.get("tcilHolidayItineraryCollection", [])

        cities_info = extract_city_info(city_collection)
        for city in cities_info:
            city["geoLocation"]["lat"] = 0.0
            city["geoLocation"]["lon"] = 0.0

        highlights = extract_highlights(city_collection)
        hotels_envisaged = extract_hotels_envisaged(packageDetail.get("tcilHolidayCategoryCollection", []))
        mealsDetails = extract_meals(packageDetail.get("tcilHolidayCategoryCollection", []))
        inclusions, exclusions = extract_inclusions_exclusions(packageDetail.get("tcilHolidayIncludeExcludeCollection", [{}]))
        termsAndConditions = extract_terms_and_conditions(packageDetail.get("tcilHolidayPaymentTermsCollection", [{}]))

        standardPrice = item.get("startingPriceStandard", 0)
        strikeOutPrice = item.get("strikeoutPriceStandard", 0)

        all_images_with_days = [
            (city.get("image"), city.get("itineraryDay"))
            for city in city_collection if city.get("image")
        ]
        sorted_images = [image for image, day in sorted(all_images_with_days, key=lambda x: x[1])]

        themes = packageDetail.get("tcilHolidayThemeCollection", [])
        packageThemes = [
            remove_html_tags(theme.get("tcilMstHolidayThemes", {}).get("name", ""))
            for theme in themes if "tcilMstHolidayThemes" in theme
        ]

        unique_countries = set()
        for city in city_collection:
            country_name = (city.get("cityCode", {})
                                .get("tcilMstCountryStateMapping", {})
                                .get("tcilMstCountryContinentMapping", {})
                                .get("countryName"))
            if country_name:
                unique_countries.add(country_name)

        thumbnailImage = packageDetail.get("packageThumbnailImage")

        package_data = format_package_data({
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
            "strikeOutPrice": strikeOutPrice
        })

        full_prompt = f"{BASE_PROMPT}\n\n{package_data}"
        summary = ""
        if do_generate_summary:
            summary = generate_summary(full_prompt)

        return {
            "packageId": packageId,
            "packageName": packageName,
            "days": duration,
            "cities": [
                {"cityName": city["cityName"], "geoLocation": city["geoLocation"]} for city in cities_info
            ],
            "visitingCountries": list(unique_countries),
            "images": sorted_images,
            "thumbnailImage": thumbnailImage,
            "packageTheme": packageThemes,
            "minimumPrice": standardPrice,
            "highlights": highlights,
            "pdfName": packageDetail.get("standardAutoPdf"),
            "packageData": package_data,
            "packageSummary": summary,
            "packageItinerary": {
                "summary": summary,
                "itinerary": [clean_text(city["description"]) for city in cities_info]
            }
        }

    except Exception as e:
        print(f"Error processing item: {e}")
        return None

def fetch_package_dynamically(package_id, do_generate_summary: bool = True):
    request_id, session_id = get_new_auth_token()
    pdp_data = get_pdp_details(package_id, request_id, session_id)

    if isinstance(pdp_data, list):
        if len(pdp_data) == 0:
            return None
        pdp_data = pdp_data[0]

    if not isinstance(pdp_data, dict):
        return None

    # Compute hashKey from the raw pdp_data
    pdp_data_str = json.dumps(pdp_data, sort_keys=True)
    hash_obj = hashlib.sha256(pdp_data_str.encode('utf-8'))
    hash_key = hash_obj.hexdigest()

    processed_data = process_pdp_package_response(pdp_data, do_generate_summary=do_generate_summary)
    if processed_data is not None:
        processed_data["hashKey"] = hash_key

    return processed_data
