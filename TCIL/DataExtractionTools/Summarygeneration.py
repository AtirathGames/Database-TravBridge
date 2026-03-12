import json
import os
import requests
import time
from datetime import datetime

# Replace this with the actual URL of your LLaMA model endpoint
LLAMA_ENDPOINT = "http://35.198.207.144:8000/v1/chat/completions"

# Example summary to guide the LLaMA model
example_summary = """
**Summary:**\nThe 'Highlights of Scandinavia Fjord Cruise with 4 Nights Denmark (Summer 2024)' package is a 12-day tour that explores the breathtaking landscapes of Scandinavia. The itinerary includes visits to iconic landmarks such as the Great Ocean Road, the Great Barrier Reef, the Sydney Opera House, and the Blue Mountains, as well as a tour of the city of Copenhagen. Participants will enjoy scenic drives, guided tours, and a range of exciting activities, including a jet boat ride, a visit to the famous Sea World, and a hot air balloon ride.\n\n**Itinerary:**\n\n**Day 1:** Copenhagen, Meals: Dinner\n- Arrive at Copenhagen airport and check-in at the hotel.\n- Spend the day acclimatizing to the city and its surroundings.\n\n**Day 2:** Copenhagen Excursion, Meals: Breakfast and Dinner\n- Visit the famous Rosklide Cathedral and the Viking Ship Museum.\n- Visit the famous Odense city and explore its historic sites.\n\n**Day 3:** Fjord Cruise, Meals: Breakfast, Lunch, Dinner On-board the Cruise\n- Depart from Copenhagen and board your cruise.\n- Enjoy the scenic views of the Fjords and the surrounding waters.\n\n**Day 4:** Fjord Cruise, Meals: Breakfast, Lunch, Dinner On-board the Cruise\n- Continue your cruise and enjoy the scenic views of the Fjords.\n- Visit the famous city of Bergen and explore its historic sites.\n\n**Day 5:** Fjord Cruise, Meals: Breakfast, Lunch, Dinner On-board the Cruise\n- Continue your cruise and enjoy the scenic views of the Fjords.\n- Visit the famous city of Kiel and explore its historic sites.\n\n**Day 6:** Fjord Cruise, Meals: Breakfast, Lunch, Dinner On-board the Cruise\n- Continue your cruise and enjoy the scenic views of the Fjords.\n- Visit the famous city of Kiel and explore its historic sites.\n\n**Day 7:** Fjord Cruise, Meals: Breakfast, Lunch, Dinner On-board the Cruise\n- Continue your cruise and enjoy the scenic views of the Fjords.\n- Visit the famous city of Kiel and explore its historic sites.\n\n**Day 8:** Fjord Cruise, Meals: Breakfast, Lunch, Dinner On-board the Cruise\n- Continue your cruise and enjoy the scenic views of the Fjords.\n- Visit the famous city of Kiel and explore its historic sites.\n\n**Day 9:** Fjord Cruise, Meals: Breakfast, Lunch, Dinner On-board the Cruise\n- Continue your cruise and enjoy the scenic views of the Fjords.\n- Visit the famous city of Kiel and explore its historic sites.\n\n**Day 10:** Fjord Cruise, Meals: Breakfast, Lunch, Dinner On-board the Cruise\n- Continue your cruise and enjoy the scenic views of the Fjords.\n- Visit the famous city of Kiel and explore its historic sites.\n\n**Day 11:** Copenhagen, Meals: Cruise Breakfast and Dinner\n- Disembark from the cruise and proceed for a guided city tour of Copenhagen.\n- Visit the famous Parliament building, Amalienborg Palace, Gefion fountain, Little Mermaid statue and many other interesting sights.\n\n**Day 12:** Copenhagen, Meals: Breakfast\n- Depart from Copenhagen airport.\n\n**Inclusions:**\n\n- Return economy class group airfare as per the itinerary.\n- All local taxes and services as of 1st September, 2023.\n- Cost of Visa and Medical Insurance for the duration of the tour.\n- Sightseeing and accommodation as per the above-mentioned itinerary.\n- All transfers and extensive excursions with entrance fees (as specified in the itinerary) by a deluxe air-conditioned coach.\n- Services of a professional, friendly and multilingual Tour Manager or a local Representative (Subject to minimum 15 customers).\n\n**Exclusions:**\n\n- If you wish to travel in advance i.e., before the published departure date as mentioned in the brochure or like to come back on a later date after the tour ends, there will be an additional charge that will be applicable (subject to availability of seats for the given dates & ticket validity) which will be advised to you by our sales staff / travel agent.\n- Tipping is expected from anyone providing your service e.g. Coach Driver, Local Guides, Hotel & Restaurant staff etc. It is mandatory to tip a nominal amount of EUR 3 per person/per day.\n- Mandatory Gratuities on Cruise of EUR 10 per person per night.\n- Cost of Basic Travel Quota equivalent to USD 250,000.\n- Any increase in the airfare charged by the airline on your air ticket (presently calculated as on 1st September, 2023).\n- Government Taxes as applicable are extra.\n- The tour cost does not include POE charges and passport charges.\n- No porterage included.\n- Any expenses of a personal nature such as porterage, laundry expenses, wines, mineral water, food and drink not in the regular menu provided by us, minibar, telephone calls, etc.\n- Meals other than that are mentioned in your itinerary.\n- Cost of excursions, city sightseeing, entrance fees and local guides availed of by the passengers, other than that mentioned in your itinerary.\n- Private airport transfers, Cost of Suggested / Optional Excursions.
"""

# List of specific package IDs to process
specific_package_ids = ["PKG009793"]

def generate_summary(package_data):
    request_body = {
        "model": "convo",
        "messages": [
            {
                "role": "user",
                "content": f"Here is an example of a package summary: {example_summary}\n\nNow, generate a summary for the following package:\n\n{package_data}"
            }
        ],
        "do_sample": True,
        "temperature": 0.9,
        "top_p": 0.9,
        "n": 1,
        "max_tokens": 4096,
        "stream": False
    }
    headers = {"Content-Type": "application/json"}
    response = requests.post(LLAMA_ENDPOINT, json=request_body, headers=headers)
    return response.json().get("choices", [{}])[0].get("message", {}).get("content", "")

def process_packages(input_file, output_file, specific_package_ids):
    with open(input_file, "r", encoding="utf-8") as file:
        data = json.load(file)

    itineraries = data["itineraries"]
    filtered_itineraries = [pkg for pkg in itineraries if pkg["packageId"] in specific_package_ids]
    total_packages = len(filtered_itineraries)

    start_time_total = time.time()
    output_data = []

    print(f"Processing {total_packages} packages for the specified IDs.")

    for package in filtered_itineraries:
        package_id = package["packageId"]
        print(f"Processing package ID: {package_id}")
        package_data = package["packageData"]
        package_summary = generate_summary(package_data)
        output_data.append({
            "packageId": package_id,
            "packageSummary": package_summary
        })

    with open(output_file, "w", encoding="utf-8") as outfile:
        json.dump(output_data, outfile, indent=4)

    end_time_total = time.time()
    elapsed_time_total = end_time_total - start_time_total
    print(f"Finished processing, output written to {output_file}")
    print(f"Total processing time: {elapsed_time_total:.2f} seconds")

# Example usage
input_file = "/Users/atirathgaming/Documents/thomascook-travelplanner/Elastic Search/data/output_cities_with_countries.json"
output_file = "/Users/atirathgaming/Documents/thomascook-travelplanner/Elastic Search/data/filteredIssuepackage6.json"
process_packages(input_file, output_file, specific_package_ids)
