import os
from fastapi import FastAPI, HTTPException, Query, UploadFile, BackgroundTasks,Body,APIRouter
from elasticsearch import Elasticsearch, RequestError, TransportError, helpers
from elasticsearch.exceptions import NotFoundError, ConflictError
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut



app = FastAPI()

# Set the production flag (You can set this using an environment variable or directly in the code)
production = os.getenv("PRODUCTION", "true").lower() == "true"
# Elasticsearch configuration based on the production flag
if production:
    es = Elasticsearch(
        ['http://localhost:9200'],
        verify_certs=False,
        basic_auth=('elastic', 'iweXVQuayXSCP9PFkHcZ')
    )
    uvicorn_host = "0.0.0.0"
    uvicorn_port = 8000
else:
    es = Elasticsearch(
        ['https://localhost:9200'],
        verify_certs=False,
        basic_auth=('elastic', 'iE1L2cJmCbYqJFwtf2wb')
    )
    uvicorn_host = "127.0.0.1"
    uvicorn_port = 8000

TCIL_PACKAGE_INDEX = "tcildatav1"
TCIL_PACKAGE_INDEX_HI = "tcildatav1_hi"
TCIL_RAW_INDEX = "tcildatav1_raw"
VISA_INDEX = "visa_faq"
CHAT_INDEX_NAME = "user_conversations"
TCIL_CAMPAIGN_INDEX = "tcil_campaigns"
DESTINATIONS_FAQ_INDEX = "destinations_faq"
ALL_UNIQUE_DESTINATIONS_INDEX = "all_unique_destinations"
MAX_RETRIES = 3  # Number of retries for failed requests
TIMEOUT = 300  # Increase timeout to 120 seconds

geolocator = Nominatim(user_agent="geoapi", timeout=10)

visa_data = '/home/gcp-admin/thomascook-travelplanner/Elastic Search/data/visa_data.json'

TOKEN_TTL = 43200

GEOCODE_TTL_SECONDS = 300  

MONTHS = ["january", "february", "march", "april", "may", "june",
                   "july", "august", "september", "october", "november", "december"]

# -----------------------------------------------------------------------
#  Config Flags
# -----------------------------------------------------------------------
ENABLE_SCHEDULED_JOB = True  # Set to False to disable scheduled job
ENABLE_DESTINATION_SCRAPING = True  # Set to False to disable destination FAQ scraping in batch process
