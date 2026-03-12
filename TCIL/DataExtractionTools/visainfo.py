import pandas as pd
import requests
import json
import time
from random import uniform
import urllib3
import logging
from datetime import datetime

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Load the Excel file
file_path = '/Users/atirathgaming/Downloads/Visa Country Codes.xlsx'
logger.info(f"Loading Excel file from: {file_path}")
df = pd.read_excel(file_path)
logger.info(f"Loaded {len(df)} country records")

# Initialize an empty list to store the results
all_country_data = []

# Define the API URL and headers
api_url = 'https://content.ctrlvisa.com/api/country'
headers = {
    'Content-Type': 'application/json'
}

# Function to make a GET request with retry logic
def make_request(country_code, index, total):
    payload = {'country_id': country_code}
    logger.info(f"[{index}/{total}] Fetching data for country code: {country_code}")
    for attempt in range(5):  # Retry up to 5 times
        try:
            response = requests.get(api_url, headers=headers, json=payload, verify=False, timeout=10)
            if response.status_code == 200:
                logger.info(f"[{index}/{total}] SUCCESS: Retrieved data for {country_code}")
                return response.json()
            elif response.status_code == 429:
                wait_time = 2 ** attempt  # Exponential backoff
                logger.warning(f"[{index}/{total}] Rate limit exceeded for {country_code}. Retrying in {wait_time} seconds... (Attempt {attempt + 1}/5)")
                time.sleep(wait_time)
            else:
                logger.error(f"[{index}/{total}] Failed to retrieve data for country code {country_code}: HTTP {response.status_code}")
                return None
        except requests.exceptions.Timeout:
            logger.error(f"[{index}/{total}] Timeout error for country code {country_code} (Attempt {attempt + 1}/5)")
            if attempt < 4:
                time.sleep(2 ** attempt)
        except Exception as e:
            logger.error(f"[{index}/{total}] Error fetching {country_code} (Attempt {attempt + 1}/5): {str(e)}")
            if attempt < 4:
                time.sleep(2 ** attempt)
    logger.error(f"[{index}/{total}] FAILED: Max retries exceeded for {country_code}")
    return None

# Iterate over each country code in the DataFrame
logger.info("=" * 60)
logger.info("Starting country data collection...")
logger.info("=" * 60)
total_countries = len(df)
for index, row in df.iterrows():
    country_code = row['CTRL_VISA_COUNTRY_CODE']
    country_data = make_request(country_code, index + 1, total_countries)
    if country_data:
        all_country_data.append(country_data)
        logger.info(f"Total records collected so far: {len(all_country_data)}")
    else:
        logger.warning(f"Skipped country code {country_code} - no data retrieved")
    # Random sleep to avoid hitting the rate limit
    sleep_time = uniform(1, 3)
    logger.debug(f"Sleeping for {sleep_time:.2f} seconds before next request...")
    time.sleep(sleep_time)

# Save the combined data to a JSON file
output_file_path = '/Users/atirathgaming/Downloads/all_country_data.json'
logger.info("=" * 60)
logger.info(f"Collection complete! Total records: {len(all_country_data)}")
logger.info(f"Saving data to: {output_file_path}")
logger.info("=" * 60)
with open(output_file_path, 'w') as json_file:
    json.dump(all_country_data, json_file, indent=4)

logger.info(f"SUCCESS: Data for all countries has been saved to {output_file_path}")
logger.info(f"Processed {len(all_country_data)} out of {total_countries} countries")
