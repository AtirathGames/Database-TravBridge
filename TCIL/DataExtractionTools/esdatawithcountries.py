import json

# Function to load JSON data from a file
def load_json(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)

# Function to save JSON data to a file
def save_json(data, file_path):
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=2)

# Function to add visitingCountries to the original JSON data based on packageId
def add_visiting_countries(original_data, transformed_data):
    transformed_dict = {item["packageId"]: item["visitingCountries"] for item in transformed_data}
    
    for package in original_data["itineraries"]:
        package_id = package["packageId"]
        if package_id in transformed_dict:
            package["visitingCountries"] = transformed_dict[package_id]

    return original_data

# Paths to the input JSON files
transformed_file_path = '/Users/atirathgaming/Downloads/countrydata.json'
original_file_path = '/Users/atirathgaming/Documents/thomascook-travelplanner/Elastic Search/data/output_cities_with_images.json'
output_file_path = '/Users/atirathgaming/Documents/thomascook-travelplanner/Elastic Search/data/output_cities_with_countries.json'

# Load data from JSON files
transformed_data = load_json(transformed_file_path)
original_data = load_json(original_file_path)

# Add visitingCountries to the original data
updated_data = add_visiting_countries(original_data, transformed_data)

# Save the updated data to a new JSON file
save_json(updated_data, output_file_path)

print("Updated JSON data has been saved to", output_file_path)
