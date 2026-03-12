import os
from fastapi import (
    FastAPI,
    HTTPException,
    Query,
    UploadFile,
    BackgroundTasks,
    Body,
    APIRouter,
)
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
        ["http://localhost:9200"], basic_auth=("elastic", "iweXVQuayXSCP9PFkHcZ")
    )
    uvicorn_host = "0.0.0.0"
    uvicorn_port = 8001
else:
    es = Elasticsearch(
        ["https://localhost:9200"],
        verify_certs=False,
        basic_auth=("elastic", "iE1L2cJmCbYqJFwtf2wb"),
    )
    uvicorn_host = "127.0.0.1"
    uvicorn_port = 8000

SOTC_PACKAGE_INDEX = "sotcdatav1"
SOTC_RAW_INDEX = "sotcdatav1_raw"
VISA_INDEX = "visa_faq"
SOTC_CHAT_INDEX_NAME = "sotc_user_conversations"

MAX_RETRIES = 3  # Number of retries for failed requests
TIMEOUT = 120  # Increase timeout to 120 seconds

geolocator = Nominatim(user_agent="geoapi", timeout=10)

visa_data = "/home/gcp-admin/sotc-travelplanner/Elastic Search/data/visa_data.json"

TOKEN_TTL = 43200

GEOCODE_TTL_SECONDS = 300

MONTHS = [
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
]

# -----------------------------------------------------------------------
#  Config Flags
# -----------------------------------------------------------------------
ENABLE_SCHEDULED_JOB = True  # Set to False to disable scheduled job
