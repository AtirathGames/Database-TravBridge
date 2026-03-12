import json
import re
from collections import defaultdict

def verify_package_days(package):
    package_id = package['packageId']
    package_summary = package['packageSummary']
    
    # Extract day count from the summary
    match = re.search(r"(\d+)-day", package_summary)
    if not match:
        print(f"[{package_id}] Failed: No day count found in summary.")
        return False

    summary_day_count = int(match.group(1))

    # Count the number of days in the itinerary
    itinerary_days = len(re.findall(r"Day \d+:", package_summary))

    # Debugging statements
    if summary_day_count == itinerary_days:
        #print(f"[{package_id}] Passed: Summary day count ({summary_day_count}) matches itinerary day count ({itinerary_days}).")
        return True
    else:
        print(f"[{package_id}] Failed: Summary day count ({summary_day_count}) does not match itinerary day count ({itinerary_days}).")
        return False

def check_duplicate_summaries(packages):
    summary_dict = defaultdict(list)
    
    # Collect package summaries
    for package in packages:
        summary = package['packageSummary']
        summary_dict[summary].append(package['packageId'])
    
    # Check for duplicates
    for summary, package_ids in summary_dict.items():
        if len(package_ids) > 1:
            print(f"Duplicate package summaries found in packages: {', '.join(package_ids)}")
        
def main():
    # Load JSON data
    with open('/Users/atirathgaming/Documents/thomascook-travelplanner/Elastic Search/data/output_cities_with_countries.json', 'r') as file:
        data = json.load(file)
    
    packages = data['itineraries']
    
    # Verify each package
    for package in packages:
        verify_package_days(package)
    
    # Check for duplicate summaries
    check_duplicate_summaries(packages)

if __name__ == "__main__":
    main()
