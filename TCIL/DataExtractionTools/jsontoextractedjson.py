import json
import re
import os
import html

# File path to the input JSON file
input_file_path = "/Users/atirathgaming/Downloads/package_details.json"
output_file_path = "/Users/atirathgaming/Downloads/updatedPackageslist.json"

# Initialize itineraries list to store all package details
itineraries = []

# Initialize a counter to limit the size of itineraries list
counter = 0

def remove_html_tags(text):
    """Remove HTML tags and decode HTML entities from a string."""
    if text:
        clean = re.compile('<.*?>')
        text = re.sub(clean, '', text)
        return html.unescape(text)
    return ""

def clean_text(text):
    """Clean up text by removing extra whitespace and newline characters."""
    return ' '.join(text.split())

def format_package(package):
    """Helper function to format data into readable text."""
    formatted = (
        f"Package ID: {package['packageId']} "
        f"Package Name: {package['packageName']} "
        f"Duration (Days): {package['days']} "
        f"Itinerary: "
    )
    for city in package['itinerary']:
        formatted += (
            f"Day {city['day']} - {city['cityName']} "
            f"Meals: {clean_text(city['meals']) if city['meals'] else ''} "
            f"Description: {clean_text(city['description']) if city['description'] else ''} "
            f"GeoLocation: Lat: {city['geoLocation']['lat']}, Lon: {city['geoLocation']['lon']} "
        )
    formatted += (
        f"Discounted Price: {package['standardPrice']} "
        f"Actual Price: {package['strikeOutPrice']} "
        f"Hotels Envisaged: {', '.join(package['Hotels Envisaged'])} "
        f"Meals: {', '.join(package['Meals'])} "
        f"Inclusions: {clean_text(package['Inclusions']) if package['Inclusions'] else ''} "
        f"Exclusions: {clean_text(package['Exclusions']) if package['Exclusions'] else ''} "
        f"Product Terms and Conditions: {clean_text(package['Product Terms and Conditions']) if package['Product Terms and Conditions'] else ''} "
        f"Highlights: {clean_text(' '.join(package['highlights']))} "
    )
    return formatted.strip()

# Open and read the JSON file
with open(input_file_path, "r") as file:
    data = json.load(file)

# Iterate through each item in the data
for arr in data:
    for item in arr:
        try:
            # Initialize packageId, packageName, price, duration, cities_info, and all_highlights for each iteration
            packageId = None
            packageName = None
            price = None
            strikeOutPrice = None
            pdf = None
            duration = None
            cities_info = []
            all_highlights = []
            hotels_envisaged = []
            mealsDetails = []
            sightSeeing = []
            inclusions = None
            exclusions = None
            termsAndConditions = None

            if counter < 1010:  # Check if the counter is less than 1010
                if "packageDetail" in item:
                    # Extract the packageId, packageName, duration, and pdf
                    packageId = item["packageDetail"].get("packageId")
                    packageName = item["packageDetail"].get("pkgName")
                    duration = item["packageDetail"].get("duration")
                    pdf = item["packageDetail"].get("standardAutoPdf")

                    # Extract the list of cities from the packageDetail
                    city_collection = item["packageDetail"].get("tcilHolidayItineraryCollection", [])

                    # Iterate through each city in the city collection
                    for city in city_collection:
                        city_code = city["cityCode"]["cityCode"]
                        city_name = city["cityCode"]["cityName"]
                        latitude = city["cityCode"]["latitude"]
                        longitude = city["cityCode"]["longitude"]
                        dayCount = city.get("itineraryDay")
                        description = city.get("itineraryDescription")
                        meals = city.get("mealDescription")

                        # Construct the geoLocation dictionary
                        geo_location = {"lat": latitude, "lon": longitude}

                        # Construct the city information dictionary
                        city_info = {
                            "day": dayCount,
                            "cityName": city_name,
                            "meals": remove_html_tags(meals),
                            "description": remove_html_tags(description),
                            "geoLocation": geo_location
                        }

                        # Append the city information to the list
                        cities_info.append(city_info)

                        # Extract highlights if available
                        city_highlights = city.get("highlights")
                        if city_highlights:
                            all_highlights.append(remove_html_tags(city_highlights))

                    # Extract hotel accommodations
                    category_collection = item["packageDetail"].get("tcilHolidayCategoryCollection", [])
                    for category in category_collection:
                        if category.get("type") == "Accomodation":
                            hotels_envisaged.append(remove_html_tags(category.get("typeDefaultMsg", "")))
                        if category.get("type") == "Meal":
                            mealsDetails.append(remove_html_tags(category.get("typeDefaultMsg", "")))
                        if category.get("type") == "Sightseeing":
                            sightSeeing.append(remove_html_tags(category.get("typeDefaultMsg", "")))

                    if "tcilHolidayIncludeExcludeCollection" in item["packageDetail"]:
                        include_exclude_collection = item["packageDetail"]["tcilHolidayIncludeExcludeCollection"]
                        if include_exclude_collection:
                            inclusions = remove_html_tags(include_exclude_collection[0].get("includes", ""))
                            exclusions = remove_html_tags(include_exclude_collection[0].get("excludes", ""))

                    if "tcilHolidayPaymentTermsCollection" in item["packageDetail"]:
                        terms_and_conditions = item["packageDetail"]["tcilHolidayPaymentTermsCollection"]
                        if terms_and_conditions:
                            termsAndConditions = remove_html_tags(terms_and_conditions[0].get("notesDescription", ""))

                    # Extract price if available
                    if "startingPriceStandard" in item:
                        price = item["startingPriceStandard"]

                    if "strikeoutPriceStandard" in item:
                        strikeOutPrice = item["strikeoutPriceStandard"]

                    # Sort the cities_info list by day in ascending order
                    cities_info = sorted(cities_info, key=lambda x: x['day'])

                    # Append package information to itineraries list
                    itineraries.append({
                        "packageId": packageId,
                        "packageName": packageName,
                        "days": duration,
                        "itinerary": cities_info,
                        "highlights": all_highlights,
                        "Hotels Envisaged": hotels_envisaged,
                        "Meals": mealsDetails,
                        "Inclusions": inclusions,
                        "Exclusions": exclusions,
                        "Sightseeing": sightSeeing,
                        "Product Terms and Conditions": termsAndConditions,
                        "pdfName": pdf,
                        "standardPrice": price,
                        "strikeOutPrice": strikeOutPrice
                    })

                    # Increment the counter
                    counter += 1
            else:
                break  # Break out of the loop if the counter reaches 1010
        except Exception as e:
            print(f"Error processing item: {e}")

# Write all packages to a single JSON file
formatted_itineraries = [format_package(package) for package in itineraries]
with open(output_file_path, "w") as output_file:
    json.dump(formatted_itineraries, output_file, indent=4)

print("Single JSON file created successfully.")
