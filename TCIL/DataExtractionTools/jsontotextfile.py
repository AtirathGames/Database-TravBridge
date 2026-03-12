import json
import re
import os
import html

# File path to the input JSON file
input_file_path = "/Users/atirathgaming/Downloads/package_details.json"
output_dir = "/Users/atirathgaming/Downloads/updated1Packagestextfiles"

# Create the output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Initialize itineraries list to store all package details
itineraries = []

# Initialize a counter to limit the size of itineraries list
counter = 0

def remove_html_tags(text):
    """Remove HTML tags and decode HTML entities from a string."""
    clean = re.compile('<.*?>')
    text = re.sub(clean, '', text)
    return html.unescape(text)

def format_package(package):
    """Helper function to format data into readable text."""
    formatted = (
        "Package ID: " + package['packageId'] + "\n" +
        "Package Name: " + package['packageName'] + "\n" +
        "Duration (Days): " + str(package['days']) + "\n" +
        "Price: " + str(package['standardPrice']) + "\n" +
        "Itinerary:\n"
    )
    for city in package['itinerary']:
        formatted += (
            "Day " + str(city['day']) + " - " + city['cityName'] + "\n" +
            "    Meals: " + (city['meals'] if city['meals'] else "") + "\n" +
            "    Description: " + (city['description'] if city['description'] else "") + "\n" +
            "    GeoLocation: Lat: " + str(city['geoLocation']['lat']) + ", Lon: " + str(city['geoLocation']['lon']) + "\n"
        )
    formatted += (
        "Hotels Envisaged: " + ', '.join(package['Hotels Envisaged']) + "\n" +
        "Meals: " + ', '.join(package['Meals']) + "\n" +
        "Inclusions: " + (package['Inclusions'] if package['Inclusions'] else "") + "\n" +
        "Exclusions: " + (package['Exclusions'] if package['Exclusions'] else "") + "\n" +
        "Product Terms and Conditions: " + (package['Product Terms and Conditions'] if package['Product Terms and Conditions'] else "") + "\n\n" +
        "Highlights:\n" +
        '\n'.join(package['highlights']) + "\n"
    )
    return formatted.strip()

def write_package_to_text(package):
    """Function to write package data to a text file."""
    output_file_path = os.path.join(output_dir, f"{package['packageId']}.txt")
    with open(output_file_path, "w") as file:
        file.write(format_package(package))

# Open and read the JSON file
with open(input_file_path, "r") as file:
    data = json.load(file)

# Iterate through each item in the data
for arr in data:
    for item in arr:
        # Initialize packageId, packageName, price, pdf, duration, cities_info, and all_highlights for each iteration
        packageId = None
        packageName = None
        price = None
        strikeOutPrice = None
        minimumStartingPrice = None
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
                minimumStartingPrice = item["minimumStartingPrice"]

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
                        "meals": remove_html_tags(meals) if meals else None,
                        "description": remove_html_tags(description) if description else None,
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
                    "standardPrice": minimumStartingPrice,
                    "strikeOutPrice": strikeOutPrice
                })

                # Increment the counter
                counter += 1
        else:
            break  # Break out of the loop if the counter reaches 1010

# Generate text files for each package
for package in itineraries:
    write_package_to_text(package)

print("Text files created successfully.")
