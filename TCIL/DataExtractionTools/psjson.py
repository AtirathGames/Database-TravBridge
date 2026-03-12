import json
import re

def convert_package_summary(package_summary):
    # Initialize variables
    summary_text = ""
    itinerary_list = []

    # Extract summary text
    summary_match = re.search(r"\*\*Summary:\*\*([\s\S]*?)(\*\*Itinerary:\*\*|\n\n|\Z)", package_summary)
    if summary_match:
        summary_text = summary_match.group(1).strip()

    # Extract itinerary items
    itinerary_match = re.search(r"\*\*Itinerary:\*\*([\s\S]*)", package_summary)
    if itinerary_match:
        itinerary_text = itinerary_match.group(1).strip()
        # Split the itinerary into separate days
        day_items = re.findall(r"\*\*Day \d+:\*\*([\s\S]*?)(?=(\*\*Day \d+:|\Z))", itinerary_text)
        for item in day_items:
            # Clean and format each day description
            cleaned_item = re.sub(r"\s+", " ", item[0].strip())  # Replace multiple spaces/newlines with a single space
            itinerary_list.append(cleaned_item)

    return {
        "summary": summary_text,
        "itinerary": itinerary_list
    }

def process_itineraries(data):
    for itinerary in data["itineraries"]:
        package_summary = itinerary.get("packageSummary", "")
        if package_summary:
            itinerary["packageSummary"] = convert_package_summary(package_summary)
    return data

def main():
    input_file = '/Users/atirathgaming/Documents/thomascook-travelplanner/Elastic Search/data/output_cities_with_countries.json'
    output_file = '/Users/atirathgaming/Documents/thomascook-travelplanner/Elastic Search/data/output_cities_with_countries_modified.json'
    
    # Load the JSON data from the file
    with open(input_file, 'r') as file:
        data = json.load(file)
    
    # Process the itineraries
    updated_data = process_itineraries(data)
    
    # Save the updated JSON data back to a file
    with open(output_file, 'w') as file:
        json.dump(updated_data, file, indent=4)

if __name__ == "__main__":
    main()
