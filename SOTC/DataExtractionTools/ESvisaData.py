import json
import re

# Function to remove HTML tags
def remove_html_tags(text):
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)

# Read JSON data from a file
with open('/Users/atirathgaming/Downloads/all_country_data.json', 'r') as f:
    data = json.load(f)

# Function to restructure the data
def restructure_data(data):
    result = []
    for item in data:
        if item["status"]:
            for visa_info in item["data"]:
                new_entry = {
                    "country_id": visa_info["country_id"],
                    "visitingCountry":visa_info["visiting_country"],
                    "visa_info": ""
                }
                visa_info_text = f'''
                Name: {remove_html_tags(visa_info["name"])}
                Visiting Country: {remove_html_tags(visa_info["visiting_country"])}
                Description: {remove_html_tags(visa_info["description"])}
                Visa Fees: {remove_html_tags(visa_info["visa_fees"])}
                Visa Needed: {remove_html_tags(visa_info["visa_needed"])}
                Type: {remove_html_tags(visa_info["type"])}
                Documents Required: {remove_html_tags(visa_info["documents_required"])}
                '''
                new_entry["visa_info"] = visa_info_text.strip()
                result.append(new_entry)
    return result

# Get the restructured data
restructured_data = restructure_data(data)

# Print the restructured data
for entry in restructured_data:
    print(entry)

# Optionally, save the restructured data to a JSON file
with open('/Users/atirathgaming/Downloads/restructured_data.json', 'w') as f:
    json.dump(restructured_data, f, indent=4)
