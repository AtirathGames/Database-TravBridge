import json

# Load JSON data from files
with open("/Users/atirathgaming/Downloads/thumbnailimageandminiprice.json", 'r') as f:
    itineraries_with_images = json.load(f)

with open("/Users/atirathgaming/Documents/thomascook-travelplanner/Elastic Search/data/output_cities_with_images.json", 'r') as f:
    itineraries = json.load(f)

# Adding images to the second JSON by matching packageId
#package_images = {item['packageId']: item['images'] for item in itineraries_with_images['itineraries_with_images']}
themes = {item['packageId']: item['packageTheme'] for item in itineraries_with_images['itineraries_with_images']}
for itinerary in itineraries['itineraries']:
    package_id = itinerary['packageId']
    if package_id in themes:
        itinerary['packageTheme'] = themes[package_id]

# Output the modified JSON
output_path = "/Users/atirathgaming/Documents/thomascook-travelplanner/Elastic Search/data/output_cities_with_images.json"
with open(output_path, 'w') as f:
    json.dump(itineraries, f, indent=2)

print(f"Modified JSON has been saved to {output_path}")
