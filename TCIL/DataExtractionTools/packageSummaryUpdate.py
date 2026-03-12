import json

# Load the first JSON file
with open('/Users/atirathgaming/Documents/thomascook-travelplanner/Elastic Search/data/output_cities_with_countries.json', 'r') as file:
    itineraries_data = json.load(file)

# Load the second JSON file
with open('/Users/atirathgaming/Documents/thomascook-travelplanner/Elastic Search/data/filteredIssuepackage6.json', 'r') as file:
    summaries_data = json.load(file)

# Create a dictionary from the second JSON file for quick lookup
summary_dict = {item['packageId']: item['packageSummary'] for item in summaries_data}

# Iterate over the itineraries and update the packageSummary
for itinerary in itineraries_data['itineraries']:
    package_id = itinerary['packageId']
    if package_id in summary_dict:
        itinerary['packageSummary'] = summary_dict[package_id]

# Save the updated JSON back to a file
with open('thomascook-travelplanner/Elastic Search/data/output_cities_with_countries.json', 'w') as file:
    json.dump(itineraries_data, file, indent=4)

print("Package summaries updated successfully!")
