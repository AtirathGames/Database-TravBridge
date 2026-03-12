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
    """Remove html tags from a string"""
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)

print(len(data))
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
        
        if counter < 1:  # Check if the counter is less than 10
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

                    # Construct the geoLocation dictionary
                    geo_location = {"lat": latitude, "lon": longitude}

                    # Construct the city information dictionary
                    city_info = {"cityName": city_name, "geoLocation": geo_location}

                    # Append the city information to the list
                    cities_info.append(city_info)
                # Extract highlights if available and not empty
                    city_highlights = city.get("highlights")
                    if city_highlights:
                        # Split the highlights by '<p>' tag and extract the text between them
                        #highlights_text = [highlight.split('<p>')[-1].split('</p>')[0] for highlight in city_highlights]
                        print("highkights are there -------------------------------------------------")
                        print(city_highlights)
                        #highlights_text = [remove_html_tags(highlight) for highlight in city_highlights]
                        # Add extracted highlight text to the all_highlights list
                        all_highlights.append(city_highlights)
                        print("highkights are there -------------------------------------------------")
                    else:
                        print("no highligths")
                
                # Extract price if available
                if "startingPriceStandard" in item:
                    price = item["startingPriceStandard"]

                cleaned_highlights = [re.sub(r'</?p>', '', highlight) for highlight in all_highlights]
                # Append package information to itineraries list
                itineraries.append({
                    "packageId": packageId,
                    "packageName": packageName,
                    "days": duration,
                    "cities": cities_info,
                    "highlights": cleaned_highlights,
                    "pdfName": pdf,
                    "price": price
                })

                # Increment the counter
                counter += 1
        else:
            break  # Break out of the loop if the counter reaches 10

# Output dictionary with "itineraries" key
output_dict = {"itineraries": itineraries}

# Output file path
output_file_path = "/Users/atirathgaming/Downloads/output_cities1234.json"

# Serialize the output dictionary to a JSON string
json_string = json.dumps(output_dict, indent=4)

# Write the JSON string to the output file
with open(output_file_path, "w") as output_file:
    output_file.write(json_string)
