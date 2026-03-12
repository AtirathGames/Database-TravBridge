import json
import re
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib import colors
import html

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
    """Remove HTML tags and decode HTML entities from a string"""
    clean = re.compile('<.*?>')
    text = re.sub(clean, '', text)
    return html.unescape(text)  # This will handle entities like &nbsp;

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
Discounted Price: {price}
Actual Price: {strikeOutPrice}
Hotels Envisaged: {hotels}
Meals: {meals}
Inclusions: {inclusions}
Exclusions: {exclusions}
Product Terms and Conditions: {terms}

Highlights:
{highlights}
""".format(
        price=package['standardPrice'],
        strikeOutPrice = package['strikeOutPrice'],
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

# Function to write package data to a PDF
def write_package_to_pdf(package):
    output_file_path = f"/Users/atirathgaming/Downloads/updatedPackages/{package['packageId']}.pdf"
    # Create a new canvas for each PDF
    c = canvas.Canvas(output_file_path, pagesize=letter)
    width, height = letter

    # Add content to the PDF
    #c.setFont("Helvetica-Bold", 16)
    #c.drawString(40, height - 40, f"Duration: {package['days']} days")
    c.setFont("Helvetica-Bold", 14)
    c.drawString(40, height - 60, f"Package Name: {package['packageName']} for {package['days']} days")

    # Ensure that text does not overflow the page
    y_position = height - 100
    c.setFont("Helvetica", 10)
    text = format_package(package)
    lines = text.split('\n')
    for line in lines:
        if y_position < 40:  # Check for page end
            c.showPage()
            y_position = height - 40  # Reset y position
        c.drawString(40, y_position, line)
        y_position -= 12

    # Save the PDF
    c.showPage()
    c.save()

# Iterate through each item in the data
for arr in data:
    # Iterate through each item in the array
    for item in arr:
        # Initialize packageId, packageName, price, pdf, duration, cities_info, and all_highlights for each iteration
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

        if counter < 1010:  # Check if the counter is less than 10
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
                    "strikeOutPrice" :strikeOutPrice
                })

                # Increment the counter
                counter += 1
        else:
            break  # Break out of the loop if the counter reaches 10

# Generate PDFs for each package
for package in itineraries:
    write_package_to_pdf(package)

print("PDF files created successfully.")
