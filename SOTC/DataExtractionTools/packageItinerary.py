import json

def add_package_itinerary(source_file, target_file, output_file):
    # Load the source JSON file with itineraries that contain the "packageItinerary" field
    with open(source_file, 'r') as src_file:
        source_data = json.load(src_file)
    
    # Load the target JSON file where "packageItinerary" needs to be added
    with open(target_file, 'r') as tgt_file:
        target_data = json.load(tgt_file)
    
    # Create a dictionary for quick lookup of packageItinerary by packageId
    itinerary_dict = {itinerary['packageId']: itinerary['packageItinerary'] for itinerary in source_data['itineraries']}
    
    # Iterate over the target data and add the packageItinerary if packageId matches
    for package in target_data['itineraries']:
        package_id = package['packageId']
        if package_id in itinerary_dict:
            package['packageItinerary'] = itinerary_dict[package_id]
    
    # Save the updated target data to a new JSON file
    with open(output_file, 'w') as out_file:
        json.dump(target_data, out_file, indent=4)
    
    print(f"packageItinerary fields have been successfully added to {output_file}.")

# Example usage:
source_file = '/Users/atirathgaming/Documents/thomascook-travelplanner/Elastic Search/data/output_cities_with_countries_modified.json'
target_file = '/Users/atirathgaming/Documents/thomascook-travelplanner/Elastic Search/data/output_cities_with_countries.json'
output_file = '/Users/atirathgaming/Documents/thomascook-travelplanner/Elastic Search/data/output_cities_with_itinerary.json'

add_package_itinerary(source_file, target_file, output_file)
