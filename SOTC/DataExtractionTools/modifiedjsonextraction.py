import json

# Load the packages JSON file
with open('/Users/atirathgaming/Documents/thomascook-travelplanner/Elastic Search/data/output_cities.json', 'r') as f:
    packages = json.load(f)

# Load the package data JSON file
with open('/Users/atirathgaming/Downloads/6thjune.json', 'r') as f:
    summaries_data = json.load(f)

# Convert summaries_data to a dictionary for easy lookup
summaries_dict = {item_id: summary for summary_dict in summaries_data for item_id, summary in summary_dict.items()}

# Add the packageSummary field to each package
for package in packages['itineraries']:
    package_id = package['packageId']
    if package_id in summaries_dict:
        package['packageSummary'] = summaries_dict[package_id]

# Save the updated packages data to a new JSON file
with open('/Users/atirathgaming/Downloads/updated_packages6thjune.json', 'w') as f:
    json.dump(packages, f, indent=4)

print("Updated packages with summaries have been saved to 'updated_packages.json'.")
