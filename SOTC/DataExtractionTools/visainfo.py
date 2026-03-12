import pandas as pd
import requests
import json
import time
from random import uniform

# Load the Excel file
file_path = '/Users/atirathgaming/Downloads/Visa Country Codes.xlsx'
df = pd.read_excel(file_path)

# Initialize an empty list to store the results
all_country_data = []

# Define the API URL and headers
api_url = 'https://content.ctrlvisa.com/api/country'
headers = {
    'Content-Type': 'application/json'
}

# Function to make a GET request with retry logic
def make_request(country_code):
    payload = {'country_id': country_code}
    for attempt in range(5):  # Retry up to 5 times
        response = requests.get(api_url, headers=headers, json=payload)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            wait_time = 2 ** attempt  # Exponential backoff
            print(f"Rate limit exceeded. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
        else:
            print(f"Failed to retrieve data for country code {country_code}: {response.status_code}")
            return None
    return None

# Iterate over each country code in the DataFrame
for index, row in df.iterrows():
    country_code = row['CTRL_VISA_COUNTRY_CODE']
    country_data = make_request(country_code)
    if country_data:
        all_country_data.append(country_data)
    # Random sleep to avoid hitting the rate limit
    time.sleep(uniform(1, 3))

# Save the combined data to a JSON file
output_file_path = '/Users/atirathgaming/Downloads/all_country_data.json'
with open(output_file_path, 'w') as json_file:
    json.dump(all_country_data, json_file, indent=4)

print(f"Data for all countries has been saved to {output_file_path}")
