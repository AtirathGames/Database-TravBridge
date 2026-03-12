import json
import pandas as pd

# File path to the input JSON file
input_file_path = "/Users/atirathgaming/Downloads/package_details.json"

# Open and read the JSON file
with open(input_file_path, "r") as file:
    data = json.load(file)

# Initialize itineraries list to store all package details
itineraries = []

# Initialize a counter to limit the size of itineraries list
counter = 0

# Iterate through each item in the data
for arr in data:
    # Iterate through each item in the array
    for item in arr:
        # Initialize packageId, packageName, price, pdf, duration, cities_info, and all_highlights for each iteration
        packageId = None
        packageName = None
        packageType = None
        visaIncluded = None
        price = None
        pdf = None
        duration = None
        destinations = []
        all_highlights = []

        if counter < 1010:  # Check if the counter is less than 10
            if "packageDetail" in item:
                # Extract the packageId, packageName, duration, and pdf
                packageId = item["packageDetail"].get("packageId")
                packageName = item["packageDetail"].get("pkgName")
                packageType = item["packageDetail"]["pkgSubtypeId"].get("pkgSubtypeName")
                visaIncluded = item["packageDetail"].get("isVisaIncluded")
                duration = item["packageDetail"].get("duration")
                pdf = item["packageDetail"].get("standardAutoPdf")

                # Extract the list of cities from the packageDetail
                city_collection = item["packageDetail"].get("tcilHolidayItineraryCollection", [])

                # Iterate through each city in the city collection
                for city in city_collection:
                    city_name = city["cityCode"]["cityName"]
                    destinations.append(city_name)

                destinations_str = ', '.join(destinations)


                # Extract price if available
                if "startingPriceStandard" in item:
                    price = item["startingPriceStandard"]

                # Append package information to itineraries list
                itineraries.append({
                    "packageId": packageId,
                    "packageName": packageName,
                    "packageType" : packageType,
                    "days": duration,
                    "destinations": destinations_str,
                    "pdfName": pdf,
                    "price": price
                })

                # Increment the counter
                counter += 1
        else:
            break  # Break out of the loop if the counter reaches 10

# Convert itineraries list to a pandas DataFrame
df = pd.DataFrame(itineraries)

# Output file path
output_file_path = "/Users/atirathgaming/Downloads/out_cities.xlsx"

# Write the DataFrame to an Excel file
df.to_excel(output_file_path, index=False)
