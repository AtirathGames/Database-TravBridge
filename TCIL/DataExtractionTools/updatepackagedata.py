import json

# Load the current file (structured data)
with open('/Users/atirathgaming/Documents/thomascook-travelplanner/Elastic Search/data/output_cities_with_images.json', 'r') as f:
    current_data = json.load(f)

# Load the new file (mapping of packageId to packageData)
with open('/Users/atirathgaming/Downloads/updated_package_details12thjuly.json', 'r') as f:
    new_data = json.load(f)

# Iterate over each itinerary in the current file
for itinerary in current_data['itineraries']:
    package_id = itinerary['packageId']
    
    # If the packageId exists in the new data, update the packageData field
    if package_id in new_data:
        itinerary['packageData'] = new_data[package_id]

# Save the updated data back to a file
with open('/Users/atirathgaming/Documents/thomascook-travelplanner/Elastic Search/data/output_cities_with_images.json', 'w') as f:
    json.dump(current_data, f, indent=4)

print("Package data updated successfully.")
