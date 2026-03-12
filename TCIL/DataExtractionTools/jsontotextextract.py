import json
import re

# File path to the input JSON file
input_file_path = "/Users/atirathgaming/Downloads/package_details.json"

# Open and read the JSON file
with open(input_file_path, "r") as file:
    data = json.load(file)

# Initialize itineraries list to store all package details
itineraries = []

# Initialize a counter to limit the size of itineraries list
counter = 0

def remove_html_tags(text):
    """Remove HTML tags from a string"""
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)

# Helper function to format data into readable text
def format_package(package):
    formatted = """
Package ID: {packageId}
Package Name: {packageName}
Duration (Days): {days}
Itinerary:
""".format(
        packageId=package['packageId'],
        packageName=package['packageName'],
        days=package['days']
    )

    for city in package['itinerary']:
        formatted += """
Day {day} - {cityName}
    Meals: {meals}
    Description: {description}
    GeoLocation: Lat: {lat}, Lon: {lon}
""".format(
            day=city['day'],
            cityName=city['cityName'],
            meals=city['meals'],
            description=city['description'],
            lat=city['geoLocation']['lat'],
            lon=city['geoLocation']['lon']
        )

    formatted += """
Price: {price}
PDF: {pdfName}
Hotels Envisaged: {hotels}
Meals: {meals}
Sightseeing: {sightseeing}
Inclusions: {inclusions}
Exclusions: {exclusions}
Product Terms and Conditions: {terms}

Highlights:
{highlights}
""".format(
        price=package['price'],
        pdfName=package['pdfName'],
        hotels=', '.join(package['Hotels Envisaged']),
        meals=', '.join(package['Meals']),
        sightseeing=', '.join(package['Sightseeing']),
        inclusions=package['Inclusions'],
        exclusions=package['Exclusions'],
        terms=package['Product Terms and Conditions'],
        highlights='\n'.join(package['highlights'])
    )

    return formatted.strip()

# Iterate through each item in the data
for arr in data:
    # Iterate through each item in the array
    for item in arr:
        # Initialize packageId, packageName, price, pdf, duration, cities_info, and all_highlights for each iteration
        packageId = None
        packageName = None
        price = None
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

        if counter < 10:  # Check if the counter is less than 10
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
                    "price": price
                })

                # Increment the counter
                counter += 1
        else:
            break  # Break out of the loop if the counter reaches 10

# Output file path
output_file_path = "/Users/atirathgaming/Downloads/output_cities.txt"

# Write the formatted package details to a text file
with open(output_file_path, "w") as output_file:
    for package in itineraries:
        output_file.write(format_package(package) + "\n\n" + "-"*80 + "\n\n")
