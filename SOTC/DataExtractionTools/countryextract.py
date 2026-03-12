import json

# Function to transform the JSON
def transform_json(input_data):
    transformed_data = []
    for package in input_data["itineraries_with_images"]:
        package_id = package["packageId"]
        unique_countries = {city["countryName"] for city in package["cities"]}
        transformed_package = {
            "packageId": package_id,
            "visitingCountries": list(unique_countries)
        }
        transformed_data.append(transformed_package)
    return transformed_data

# Read data from local JSON file
input_file_path = '/Users/atirathgaming/Downloads/countryInfo.json'
with open(input_file_path, 'r') as f:
    data = json.load(f)

# Transform the input data
transformed_data = transform_json(data)

# Convert the transformed data to JSON
transformed_json = json.dumps(transformed_data, indent=2)

# Save the transformed JSON to a file
output_file_path = '/Users/atirathgaming/Downloads/countrydata.json'
with open(output_file_path, 'w') as f:
    f.write(transformed_json)

print("Transformed JSON data has been saved to", output_file_path)
