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

print(len(data))

# Iterate through each item in the data
for arr in data:
    # Iterate through each item in the array
    for item in arr:
        # Initialize packageId, packageName, price, pdf, duration, cities_info, and all_highlights for each iteration
        packageId = None
        packageName = None
        packageTheme = []
        price = None
        minimumStartingPrice = None
        pdf = None
        duration = None
        cities_info = []
        all_highlights = []
        all_images=[]
        thumbnailImage = None
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
                thumbnailImage = item["packageDetail"].get("packageThumbnailImage")
                minimumStartingPrice = item["minimumStartingPrice"]
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
                    countryName =city["cityCode"]["tcilMstCountryStateMapping"]["tcilMstCountryContinentMapping"]["countryName"]

                    # Construct the geoLocation dictionary
                    geo_location = {"lat": latitude, "lon": longitude}

                    # Construct the city information dictionary
                    city_info = {
                        #"day": dayCount,
                        "cityName": city_name,
                        "countryName":countryName
                        #"meals": remove_html_tags(meals) if meals else None,
                        #"description": remove_html_tags(description) if description else None,
                        #"geoLocation": geo_location
                    }

                    # Append the city information to the list
                    cities_info.append(city_info)

                    # Extract highlights if available
                    city_highlights = city.get("highlights")
                    if city_highlights:
                        all_highlights.append((city_highlights))

                    itinenary_images = city.get("image")
                    if itinenary_images:
                        all_images.append((itinenary_images))    



                    # Extract image and day value into a list of tuples
                    all_images_with_days = [(city.get("image"), city.get("itineraryDay")) for city in city_collection if city.get("image")]

                    # Sort the list of tuples by the day value
                    sorted_images_with_days = sorted(all_images_with_days, key=lambda x: x[1])

                    # Extract sorted images from the sorted list of tuples
                    sorted_images = [image for image, day in sorted_images_with_days]

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

                if "tcilHolidayThemeCollection" in item["packageDetail"]:
                    themes = item["packageDetail"]["tcilHolidayThemeCollection"]
                    packageThemes = []
                    for theme in themes:
                        if "tcilMstHolidayThemes" in theme:
                            theme_name = theme["tcilMstHolidayThemes"].get("name", "")
                        if theme_name:
                            packageThemes.append(remove_html_tags(theme_name))



                # Extract price if available
                if "startingPriceStandard" in item:
                    price = item["startingPriceStandard"]

                # Append package information to itineraries list
                itineraries.append({
                    "packageId": packageId,
                    "cities": cities_info
                    
                })

                # Increment the counter
                counter += 1
        else:
            break  # Break out of the loop if the counter reaches 10

# Output dictionary with "itineraries" key
output_dict = {"itineraries_with_images": itineraries}

# Output file path
output_file_path = "/Users/atirathgaming/Downloads/countryInfo.json"

# Serialize the output dictionary to a JSON string
json_string = json.dumps(output_dict, indent=4)

# Write the JSON string to the output file
with open(output_file_path, "w") as output_file:
    output_file.write(json_string)
