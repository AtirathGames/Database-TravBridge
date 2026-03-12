from fastapi import FastAPI, HTTPException, Query, UploadFile, BackgroundTasks,Body,APIRouter
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Union,Any
from elasticsearch import Elasticsearch, RequestError, TransportError, helpers
from elasticsearch.exceptions import NotFoundError, ConflictError
import logging
import json, requests
import traceback
import os
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from datetime import datetime
from fetchpackages import fetch_package_dynamically,fetch_packages_for_month
import asyncio
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
import re
from fastapi.middleware.cors import CORSMiddleware
from oldesmapping import conversation_index_mapping ,index_mapping , visa_faq_mapping
import time
from requests.exceptions import ReadTimeout, ConnectionError, HTTPError

MAX_RETRIES = 3  # Number of retries for failed requests
TIMEOUT = 120  # Increase timeout to 120 seconds

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

router = APIRouter()

# Set the production flag (You can set this using an environment variable or directly in the code)
production = os.getenv("PRODUCTION", "true").lower() == "true"

# Elasticsearch configuration based on the production flag
if production:
    es = Elasticsearch(
        ['http://localhost:9200'],
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

indexNameForPackages = "tcildatav1"
INDEX_NAME = "visa_faq"
CHAT_INDEX_NAME = "user_conversations"

# Pydantic models
class GeoLocation(BaseModel):
    lat: float
    lon: float

class DayItinerary(BaseModel):
    day: int
    description: str
    mealDescription: str
    overnightStay: str

class PackageItinerary(BaseModel):
    summary: str
    itinerary: List[DayItinerary]

class ItineraryData(BaseModel):
    packageId: str
    packageName: str
    days: Optional[int]
    cities: List[Dict[str, Union[str, Dict[str, float]]]]
    highlights: List[str]
    pdfName: Optional[str]
    price: Optional[int]
    packageData: str
    packageSummary: Optional[str] = None
    images: List[str]
    minimumPrice: Optional[int]
    thumbnailImage: Optional[str]
    packageTheme: List[str]
    visitingCountries: List[str]
    departureCities: List[str]  
    packageItinerary: PackageItinerary
    hotels: Optional[Union[str, List[str]]] = None
    meals: Optional[Union[str, List[str]]] = None
    sightseeing : Optional[Union[str, List[str]]] = None
    inclusions: Optional[str] = None
    exclusions: Optional[str] = None
    termsAndConditions: Optional[str] = None
    pkgSubtypeId:Optional[int]
    pkgSubtypeName:str
    pkgTypeId:Optional[int]
    

class savedItineraryData(BaseModel):
    packageId: str
    packageName: str
    days: Optional[int]
    cities: List[Dict[str, Union[str, Dict[str, float]]]]
    highlights: List[str]
    pdfName: Optional[str]
    price: Optional[int]
    packageSummary: Optional[str] = None
    images: List[str]
    minimumPrice: Optional[int]
    thumbnailImage: Optional[str]
    packageTheme: List[str]
    packageData: str
    departureCities: List[str]  
    packageItinerary: PackageItinerary
    hotels: Optional[Union[str, List[str]]] = None
    meals: Optional[Union[str, List[str]]] = None
    sightseeing : Optional[Union[str, List[str]]] = None
    inclusions: Optional[str] = None
    exclusions: Optional[str] = None
    termsAndConditions: Optional[str] = None
    pkgSubtypeId:Optional[int]
    pkgSubtypeName:str
    pkgTypeId:Optional[int]
    

class ItemOut(BaseModel):
    id: str
    itinerary_data: savedItineraryData
    score: float

class SearchItem(BaseModel):
    text: str
    index: str
    geoLocation: Optional[GeoLocation] = None
    search_type: Optional[str] = "match"
    search_results_size: Optional[int] = 10

class QueryRequest(BaseModel):
    text: str
    index: str
    days: Optional[int]
    budget: Optional[int]

class PackageSearchRequest(BaseModel):
    search_term: str
    departureCity: Optional[str] = "" 
    days: Optional[int] = 0
    budget: Optional[int] = 0
    monthOfTravel: Optional[str] = ""  # New field for SRP filtering

class AutoBudgetRequest(BaseModel):
    search_term: str

class ChatMessage(BaseModel):
    chat_id: str
    chat_time: datetime
    content: str
    message_id: str
    modified_time: datetime
    rating: Optional[float] = None
    role: str
    sequence_id: Optional[int] = None
    type: str

class SavedPackages(BaseModel):
    packageId: str
    saved_time: datetime

class Conversation(BaseModel):
    conversationId: str
    userId: str

    booking_date: Optional[datetime] = None
    chat_channel: Optional[str] = None
    chat_model_name: Optional[str] = None
    chat_model_version: Optional[str] = None
    chat_modified: datetime
    chat_name: str
    chat_started: datetime
    chat_summary: Optional[str] = None

    conversation: List[ChatMessage]

    customerId: Optional[str] = None
    dataset_version: Optional[str] = None
    opportunity_id: Optional[str] = None

    packages_saved: Optional[List[SavedPackages]] = None

class UserIdRequest(BaseModel):
    userId: str

class ConversationIdRequest(BaseModel):
    conversationId: str

class UpdateChatNameRequest(BaseModel):
    conversationId: str
    new_chat_name: str

class DeleteConversationRequest(BaseModel):
    conversationId: str


def ensure_index_exists(index_name: str):
    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name, body=index_mapping)
        logging.info(f"Index {index_name} created with mapping.")
    else:
        logging.info(f"Index {index_name} already exists.")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

geolocator = Nominatim(user_agent="geoapi", timeout=10)

@app.on_event("startup")
async def startup_event():
    create_visa_faq_index()

def create_visa_faq_index():
    if not es.indices.exists(index=INDEX_NAME):
        es.indices.create(index=INDEX_NAME, body=visa_faq_mapping)

###### VISA Handling Functions ######

@app.post("/createVisaFAQ")
async def create_visa_faq():
    try:
        create_visa_faq_index()
        with open('/home/gcp-admin/thomascook-travelplanner/Elastic Search/data/visa_data.json', 'r') as f:
            restructured_data = json.load(f)
        actions = [
            {
                "_index": INDEX_NAME,
                "_source": doc,
            }
            for doc in restructured_data
        ]

        helpers.bulk(es, actions)
        return {"message": "Data inserted successfully"}
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="visa_data.json file not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/searchVisaFAQ")
async def search_visa_faq(query: str):
    try:
        create_visa_faq_index()
        response = es.search(
            index=INDEX_NAME,
            body={
                "query": {
                    "match": {
                        "visitingCountry": query
                    }
                }
            }
        )
        results = [hit["_source"] for hit in response['hits']['hits']]
        return {"results": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def index_package_in_es(package_data: dict):
    # Index the PDP-fetched package into Elasticsearch.
    doc_id = package_data["packageId"]
    document = {
        "packageId": package_data["packageId"],
        "packageName": package_data["packageName"],
        "packageTheme": package_data["packageTheme"],
        "days": package_data["days"],
        "cities": package_data["cities"],
        "highlights": package_data["highlights"],
        "pdfName": package_data["pdfName"],
        "price": package_data.get("minimumPrice"),
        "minimumPrice": package_data.get("minimumPrice"),
        "packageData": package_data["packageData"],
        "packageSummary": package_data.get("packageSummary"),
        "thumbnailImage": package_data["thumbnailImage"],
        "images": package_data["images"],
        "visitingCountries": package_data.get("visitingCountries", []),
        "departureCities": package_data.get("departureCities", []),
        "packageItinerary": package_data["packageItinerary"],
        "hotels": package_data.get("hotels"),
        "meals": package_data.get("meals"),
        "sightseeing": package_data.get("sightseeing"),
        "inclusions": package_data.get("inclusions"),
        "exclusions": package_data.get("exclusions"),
        "termsAndConditions": package_data.get("termsAndConditions"),
        "hashKey": package_data.get("hashKey"),
        "pkgSubtypeId":package_data["pkgSubtypeId"],
        "pkgSubtypeName":package_data["pkgSubtypeName"],
        "pkgTypeId":package_data["pkgTypeId"]
    }
    es.index(index=indexNameForPackages, id=doc_id, document=document)
    logging.info(f"Indexed package {doc_id} into {indexNameForPackages}")

###### Elastic Search Package retrieval endpoints ######

@app.post("/v1/search_by_package_id", response_model=ItemOut)
async def search_item_by_package_id(
    body: Dict[str, str] = Body(..., example={"packageId": "12345"})
) -> ItemOut:
    """
    Search for a package by packageId. If not found in Elasticsearch, fetch from PDP and index it.

    Args:
        body (dict): Request body containing the packageId.

    Returns:
        ItemOut: The package details and its search score.
    """
    try:
        index = indexNameForPackages  # Default index
        generate_summary = True  # Default value for summary generation

        package_id = body.get("packageId")
        if not package_id:
            raise HTTPException(status_code=400, detail="packageId is required in the request body.")
        
        logging.info(f"Searching for package with packageId: {package_id} in index: {index}")

        ensure_index_exists(index)

        search_body = {
            "query": {
                "term": {
                    "packageId": package_id
                }
            },
            "size": 1
        }

        response = es.search(index=index, body=search_body)
        hits = response.get("hits", {}).get("hits", [])
        logging.info(f"Elasticsearch response: {response}")

        if not hits:
            logging.info(f"No package found in ES for {package_id}, attempting PDP fallback.")
            package_data = fetch_package_dynamically(package_id, do_generate_summary=generate_summary)
            if not package_data:
                raise HTTPException(status_code=404, detail="Package not found even in PDP source")
            
            # Index the fetched package into Elasticsearch
            await index_package_in_es(package_data)

            pdp_itinerary = package_data.get("packageItinerary", {"summary": "", "itinerary": []})
            return ItemOut(
                    id=package_data["packageId"],
                    itinerary_data=savedItineraryData(
                        packageId=package_data["packageId"],
                        packageName=package_data["packageName"],
                        packageTheme=package_data["packageTheme"],
                        days=package_data["days"],
                        cities=package_data["cities"],
                        highlights=package_data["highlights"],
                        thumbnailImage=package_data["thumbnailImage"],
                        images=package_data["images"],
                        pdfName=package_data["pdfName"],
                        price=package_data.get("minimumPrice"),
                        minimumPrice=package_data.get("minimumPrice"),
                        packageData=package_data["packageData"],
                        departureCities=package_data.get("departureCities", []),
                        packageSummary=package_data.get("packageSummary"),
                        packageItinerary=PackageItinerary(
                            summary=package_data["packageItinerary"].get("summary", ""),
                            itinerary=[
                                DayItinerary(
                                    day=itinerary_item.get("day", 0),
                                    description=itinerary_item.get("description", ""),
                                    mealDescription=itinerary_item.get("mealDescription", ""),
                                    overnightStay=itinerary_item.get("overnightStay", "")
                                )
                                for itinerary_item in package_data["packageItinerary"].get("itinerary", [])
                            ]
                        ),
                        hotels=package_data.get("hotels"),
                        meals=package_data.get("meals"),
                        sightseeing=package_data.get("sightseeing"),
                        inclusions=package_data.get("inclusions"),
                        exclusions=package_data.get("exclusions"),
                        termsAndConditions=package_data.get("termsAndConditions"),
                        pkgSubtypeId=package_data.get("pkgSubtypeId"),
                        pkgSubtypeName=package_data.get("pkgSubtypeName"),
                        pkgTypeId=package_data.get("pkgTypeId")
                    ),
                    score=1.0
                )

        source = hits[0]["_source"]
        logging.info(f"Package found in ES: {source}")

        if "visitingCountries" in source and "India" not in source["visitingCountries"]:
            visa_info_text = []
            for country in source["visitingCountries"]:
                visa_search_result = await search_visa_faq(country)
                logging.info(f"Visa search result for {country}: {visa_search_result}")
                if visa_search_result and "results" in visa_search_result and visa_search_result["results"]:
                    result_info = visa_search_result["results"][0]
                    if "visa_info" in result_info:
                        visa_info_text.append(result_info["visa_info"])
            if visa_info_text:
                source["packageData"] += "\n\nVisa Information:\n" + "\n".join(visa_info_text)

        es_itinerary = source.get("packageItinerary", {"summary": "", "itinerary": []})

        item_out = ItemOut(
            id=hits[0]["_id"],
            itinerary_data=savedItineraryData(
                packageId=source["packageId"],
                packageName=source["packageName"],
                packageTheme=source["packageTheme"],
                days=source["days"],
                cities=source["cities"],
                highlights=source["highlights"],
                thumbnailImage=source["thumbnailImage"],
                images=source["images"],
                pdfName=source["pdfName"],
                price=source["price"],
                minimumPrice=source["minimumPrice"],
                packageData=source["packageData"],
                departureCities=source.get("departureCities", []),
                packageSummary=source.get("packageSummary"),
                packageItinerary=PackageItinerary(
                    summary=source["packageItinerary"].get("summary", ""),
                    itinerary=[
                        DayItinerary(
                            day=day_data.get("day"),
                            description=day_data.get("description", ""),
                            mealDescription=day_data.get("mealDescription", ""),
                            overnightStay=day_data.get("overnightStay", "")
                        )
                        for day_data in source["packageItinerary"].get("itinerary", [])
                    ]
                ),
                hotels=source.get("hotels"),
                meals=source.get("meals"),
                sightseeing=source.get("sightseeing"),
                inclusions=source.get("inclusions"),
                exclusions=source.get("exclusions"),
                termsAndConditions=source.get("termsAndConditions"),
                pkgSubtypeId=source["pkgSubtypeId"],
                pkgSubtypeName=source["pkgSubtypeName"],
                pkgTypeId=source["pkgTypeId"]
            ),
            score=hits[0]["_score"]
        )


        logging.info(f"ItemOut constructed: {item_out}")
        return item_out

    except RequestError as e:
        logging.error(f"Request error: {str(e)}")
        raise HTTPException(status_code=400, detail="Request error")
    except TransportError as e:
        logging.error(f"Elasticsearch transport error: {str(e)}")
        raise HTTPException(status_code=500, detail="Elasticsearch transport error")
    except Exception as e:
        logging.error(f"Internal Server Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

@app.post("/v1/search_by_package_name", response_model=List[ItemOut])
async def search_by_package_name(
     body: Dict[str, str] = Body(..., example={"packageName": "Amazing Goa"})
):
    """
    Search for packages by the given text in the packageName field.
    Returns up to 6 relevant results.
    """
    try:
        packageName = body.get("packageName")
        if not packageName:
            raise HTTPException(status_code=400, detail="packageName is required in the request body.")

        ensure_index_exists(indexNameForPackages)

        search_body = {
            "query": {
                "match": {
                    "packageName": packageName
                }
            },
            "size": 6
        }

        response = es.search(index=indexNameForPackages, body=search_body)
        hits = response.get("hits", {}).get("hits", [])

        results = []
        for hit in hits:
            source = hit["_source"]
            es_itinerary = source.get("packageItinerary", {"summary": "", "itinerary": []})
            
            item_out = ItemOut(
                id=hit["_id"],
                itinerary_data=savedItineraryData(
                    packageId=source["packageId"],
                    packageName=source["packageName"],
                    packageTheme=source.get("packageTheme", []),
                    days=source.get("days"),
                    cities=source.get("cities", []),
                    highlights=source.get("highlights", []),
                    thumbnailImage=source.get("thumbnailImage"),
                    images=source.get("images", []),
                    pdfName=source.get("pdfName"),
                    price=source.get("price"),
                    minimumPrice=source.get("minimumPrice"),
                    packageData=source.get("packageData", ""),
                    packageSummary=source.get("packageSummary"),
                    departureCities=source.get("departureCities", []),
                    packageItinerary=PackageItinerary(
                        summary=source.get("packageItinerary", {}).get("summary", ""),  
                        itinerary=[
                            DayItinerary(
                                day=itinerary_item.get("day", 0),
                                description=itinerary_item.get("description", ""),
                                mealDescription=itinerary_item.get("mealDescription", ""),
                                overnightStay=itinerary_item.get("overnightStay", "")
                            )
                            for itinerary_item in source.get("packageItinerary", {}).get("itinerary", [])
                        ] 
                    ),
                    hotels=source.get("hotels"),
                    meals=source.get("meals"),
                    sightseeing=source.get("sightseeing"),
                    inclusions=source.get("inclusions"),
                    exclusions=source.get("exclusions"),
                    termsAndConditions=source.get("termsAndConditions"),
                    pkgSubtypeId=source.get("pkgSubtypeId"),
                    pkgSubtypeName=source.get("pkgSubtypeName", ""),
                    pkgTypeId=source.get("pkgTypeId")
                ),
                score=hit["_score"]
            )

            results.append(item_out)

        return results

    except RequestError as e:
        logging.error(f"Request error: {str(e)}")
        raise HTTPException(status_code=400, detail="Request error")
    except TransportError as e:
        logging.error(f"Elasticsearch transport error: {str(e)}")
        raise HTTPException(status_code=500, detail="Elasticsearch transport error")
    except Exception as e:
        logging.error(f"Internal Server Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

def get_new_auth_token():
    token_url = "https://services.thomascook.in/tcCommonRS/extnrt/getNewRequestToken"
    headers = {
        "uniqueId": "172.63.176.111",
        "user": "paytm"
    }
    response = requests.get(token_url, headers=headers)
    response.raise_for_status()
    token_data = response.json()
    if token_data.get("errorCode") == 0:
        return token_data["requestId"], token_data["tokenId"]
    else:
        raise HTTPException(status_code=500, detail=f"Error retrieving token: {token_data.get('errorMsg')}")

@app.post("/v1/livepackages")
async def get_packages(request: PackageSearchRequest) -> JSONResponse:
    """
    Fetch packages based on search term, departure city, days, budget, and month of travel.
    Utilizes pre-cached monthly active indices for faster retrieval.
    Fails gracefully with custom codes at 3 stages:
      - Stage 1 (AutoSuggest): 404 if no packages found in AutoSuggest
      - Stage 2 (Month Intersection): 401 if no active packages found for that month
      - Stage 3 (Departure City): 402 if no packages match the requested departure city
    Otherwise returns code 200 with the final list.
    """
    start_time = datetime.now()
    logging.info(f"Request received for /v1/livepackages at {start_time.isoformat()}")
    logging.info(f"Request payload: {request.dict()}")

    # Extract parameters
    search_term = request.search_term.strip().lower()
    departure_city = request.departureCity.strip().lower() if request.departureCity else None
    target_days = request.days if request.days > 0 else None
    target_budget = request.budget if request.budget > 0 else None
    month_of_travel = request.monthOfTravel.lower() if request.monthOfTravel else None

    try:
        logging.info(
            f"Filters Applied -> "
            f"Search Term: {search_term}, Departure City: {departure_city}, "
            f"Days: {target_days}, Budget: {target_budget}, Month of Travel: {month_of_travel}"
        )

        # Stage 1: month_of_travel index pre-check (just load monthly_package_ids)
        monthly_package_ids = set()
        if month_of_travel:
            if not re.match(
                r"^(january|february|march|april|may|june|july|august|september|october|november|december)$",
                month_of_travel, flags=re.IGNORECASE
            ):
                logging.error(f"Invalid monthOfTravel: {month_of_travel}")
                return JSONResponse(
                    status_code=400,
                    content={
                        "code": 400,
                        "message": f"Invalid monthOfTravel value: {month_of_travel}",
                        "body": []
                    }
                )
            current_month = datetime.now().month
            current_year = datetime.now().year
            month_number = list(map(str.lower, [
                "january", "february", "march", "april", "may", "june", "july", "august",
                "september", "october", "november", "december"
            ])).index(month_of_travel) + 1
            target_year = current_year if month_number >= current_month else current_year + 1

            index_name = f"{month_of_travel}_{target_year}"
            logging.info(f"[Stage0] Checking monthly index: {index_name}")
            try:
                response = es.search(index=index_name, body={"query": {"match_all": {}}}, size=10000)
                monthly_package_ids = {hit["_source"]["packageId"] for hit in response["hits"]["hits"]}
                logging.info(f"Loaded {len(monthly_package_ids)} from {index_name}.")
            except Exception as e:
                logging.error(f"Error searching monthly index {index_name}: {str(e)}")

        # Stage 2: AutoSuggest
        request_id, session_id = get_new_auth_token()
        url = "https://services.thomascook.in/tcHolidayRS/autosuggest"
        params = {"searchAutoSuggest": search_term}
        headers = {"Requestid": request_id, "Sessionid": session_id}

        logging.info("[Stage1] Calling AutoSuggest API")
        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            autosuggest_data = response.json()
            logging.info(f"AutoSuggest returned {len(autosuggest_data)} results.")
        except requests.exceptions.RequestException as e:
            logging.error(f"AutoSuggest error: {str(e)}")
            return JSONResponse(
                status_code=500,
                content={"code": 500, "message": "Error with autosuggest API", "body": []}
            )

        if not isinstance(autosuggest_data, list):
            logging.error("AutoSuggest API invalid format.")
            return JSONResponse(
                status_code=500,
                content={"code": 500, "message": "Invalid response from AutoSuggest API", "body": []}
            )

        # Filter by City/Country/State/Continent name
        filtered_packages = [
            item for item in autosuggest_data
            if search_term in [
                item.get("cityName", "").strip().lower(),
                item.get("countryName", "").strip().lower(),
                item.get("stateName", "").strip().lower(),
                item.get("continentName", "").strip().lower()
            ]
        ]

        package_ids = {
            pkg["packageId"]
            for item in filtered_packages
            for pkg in item.get("pkgnameIdMappingList", [])
        }
        logging.info(f"[Stage1] Package IDs from AutoSuggest: {package_ids}")

        if not package_ids:
            # Stage 1 fail => Return 404
            logging.warning("No matching packages from AutoSuggest. Stage 1 fail.")
            return JSONResponse(
                content={"code": 404, "message": "No matching packages found in AutoSuggest API.", "body": []}
            )

        # Stage 2: Month Intersection
        if month_of_travel:
            logging.info(f"[Stage2] Intersect with monthly_package_ids. Before: {len(package_ids)}")
            package_ids = package_ids.intersection(monthly_package_ids)
            logging.info(f"After: {len(package_ids)}")
            if not package_ids:
                # Stage 2 fail => Return 401
                logging.warning("No monthly active packages matched. Stage 2 fail.")
                return JSONResponse(
                    status_code=401,
                    content={
                        "code": 401,
                        "message": f"No active packages found for '{search_term}' in {index_name}",
                        "body": []
                    }
                )

        # Stage 3: Retrieve full packages from ES
        logging.info("[Stage3] Retrieving package details from ES.")
        detailed_packages = []
        for pid in package_ids:
            try:
                body = {"packageId": pid}
                pkg_data = await search_item_by_package_id_internal(body=body, generate_summary=True)
                if pkg_data is not None:
                    detailed_packages.append(pkg_data)
            except Exception as e:
                logging.error(f"Error retrieving {pid}: {str(e)}")

        if not detailed_packages:
            # Could happen if ES had IDs but search_item_by_package_id_internal fails
            logging.warning("Stage 3: No valid packages found in ES.")
            return JSONResponse(
                status_code=404,
                content={
                    "code": 404,
                    "message": "No valid packages found from final retrieval.",
                    "body": []
                }
            )

        # Stage 4: Filtering by departureCity
        #  We do the GIT vs FIT logic from your code, then apply your apply_filters
        git_packages = [p for p in detailed_packages if p.itinerary_data.pkgSubtypeName == "GIT"]
        fit_packages = [p for p in detailed_packages if p.itinerary_data.pkgSubtypeName == "FIT"]
        combined_packages = git_packages + fit_packages

        if not combined_packages:
            logging.warning("No GIT or FIT packages found. Stage 3 fail.")
            return JSONResponse(
                content={
                    "code": 404,
                    "message": "No matching packages found (No GIT/FIT).",
                    "body": []
                }
            )

        logging.info(f"Combining GIT & FIT => {len(combined_packages)} packages total.")
        filtered_packages = apply_filters(combined_packages, departure_city, target_days, target_budget)

        # if apply_filters returns JSONResponse, pass it along:
        if isinstance(filtered_packages, JSONResponse):
            return filtered_packages

        # Check final
        if not filtered_packages:
            # Stage 4 fail => Return 402 with a message listing available departure cities
            # We'll gather all unique departure cities from combined_packages
            all_dep_cities = set()
            for pkg in combined_packages:
                all_dep_cities.update({c.lower() for c in pkg.itinerary_data.departureCities})
            all_dep_cities_str = ", ".join(sorted(all_dep_cities))
            logging.warning(f"Stage 4: No packages match departureCity={departure_city}.")
            return JSONResponse(
                status_code=402,
                content={
                    "code": 402,
                    "message": (
                        f"No packages found from your base city '{departure_city}'. "
                        f"Available departure cities: {all_dep_cities_str}"
                    ),
                    "body": []
                }
            )

        # Stage final => success
        logging.info(f"Total filtered packages after all logic: {len(filtered_packages)}")
        serialized_data = jsonable_encoder(filtered_packages)
        return JSONResponse(
            status_code=200,
            content={
                "code": 200,
                "message": "Here are the available packages matching your travel details.",
                "body": serialized_data
            }
        )

    except Exception as e:
        logging.error(f"Internal server error: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={
                "code": 500,
                "message": "Internal server error",
                "body": []
            }
        )


@app.post("/v1/livepackagesv2")
async def get_packages_v2(request: PackageSearchRequest) -> JSONResponse:
    """
    Enhanced version of /v1/livepackages with Elasticsearch fallback + custom codes:
      1) Attempt AutoSuggest first → code=200 if success.
      2) If AutoSuggest fails, fallback to ES → code=201 if success.
      3) If both fail, code=404.
      4) Intersect with monthly active packages → if none, code=401.
      5) Filter by departureCity → if none, code=402 with a message listing possible departure cities.
      6) Final success → either code=200 or code=201 depending on which path gave us results.
    """
    start_time = datetime.now()
    logging.info(f"[livepackagesv2] Request at {start_time.isoformat()} | Payload: {request.dict()}")

    # Extract parameters
    search_term = request.search_term.strip().lower()
    departure_city = (request.departureCity or "").strip().lower()
    target_days = request.days if request.days > 0 else None
    target_budget = request.budget if request.budget > 0 else None
    month_of_travel = (request.monthOfTravel or "").strip().lower()

    # We'll track if fallback was used. If True => final code=201. If not => final code=200.
    fallback_used = False

    logging.info(
        f"Filters -> search_term={search_term}, departureCity={departure_city}, "
        f"days={target_days}, budget={target_budget}, monthOfTravel={month_of_travel}"
    )

    try:
        # STEP 1: Month-of-travel => Pre-cached monthly index
        monthly_package_ids = set()
        if month_of_travel:
            # Validate
            if not re.match(
                r"^(january|february|march|april|may|june|july|august|september|october|november|december)$",
                month_of_travel, flags=re.IGNORECASE):
                return JSONResponse(
                    status_code=400,
                    content={"code": 400, "message": f"Invalid monthOfTravel={month_of_travel}.", "body": []}
                )

            current_month = datetime.now().month
            current_year = datetime.now().year
            month_number = [
                "january", "february", "march", "april", "may",
                "june", "july", "august", "september", "october", "november", "december"
            ].index(month_of_travel) + 1
            target_year = current_year if month_number >= current_month else current_year + 1

            index_name = f"{month_of_travel}_{target_year}"
            logging.info(f"Checking monthly index: {index_name} for pre-cached data...")

            try:
                resp = es.search(index=index_name, body={"query": {"match_all": {}}}, size=10000)
                monthly_package_ids = {hit["_source"]["packageId"] for hit in resp["hits"]["hits"]}
                logging.info(f"Retrieved {len(monthly_package_ids)} monthly-active package IDs.")
            except Exception as e:
                logging.error(f"Error searching monthly index: {str(e)}")

        # STEP 2: Attempt AutoSuggest
        request_id, session_id = get_new_auth_token()
        url = "https://services.thomascook.in/tcHolidayRS/autosuggest"
        params = {"searchAutoSuggest": search_term}
        headers = {"Requestid": request_id, "Sessionid": session_id}

        logging.info(f"Calling AutoSuggest with params={params}, headers={headers}")
        try:
            auto_response = requests.get(url, params=params, headers=headers)
            auto_response.raise_for_status()
            auto_data = auto_response.json()
            logging.info(f"AutoSuggest returned {len(auto_data)} items.")
        except Exception as e:
            logging.error(f"AutoSuggest error: {e}")
            auto_data = []

        if not isinstance(auto_data, list):
            logging.error("Invalid AutoSuggest format => treat as empty.")
            auto_data = []

        # Filter by city/country/state/continent
        filtered_autosuggest = [
            item for item in auto_data
            if search_term in [
                (item.get("cityName") or "").lower().strip(),
                (item.get("countryName") or "").lower().strip(),
                (item.get("stateName") or "").lower().strip(),
                (item.get("continentName") or "").lower().strip()
            ]
        ]
        autosuggest_pkg_ids = {
            pkg["packageId"]
            for item in filtered_autosuggest
            for pkg in item.get("pkgnameIdMappingList", [])
        }
        logging.info(f"Package IDs from AutoSuggest: {autosuggest_pkg_ids}")

        # If autoSuggest is empty => fallback to ES
        if not autosuggest_pkg_ids:
            logging.info("No packages from AutoSuggest => fallback to ES.")
            fallback_used = True  # We'll set code=201 if successful

            fallback_body = {
                "query": {
                    "bool": {
                        "should": [
                            {
                                "nested": {
                                    "path": "cities",
                                    "query": {
                                        "bool": {
                                            "should": [
                                                {"match": {"cities.aliasCityName": search_term}},
                                                {"wildcard": {"cities.aliasCityName": f"*{search_term}*"}},
                                                {"fuzzy": {"cities.aliasCityName": {"value": search_term, "fuzziness": "AUTO"}}}
                                            ]
                                        }
                                    }
                                }
                            },
                            {
                                "multi_match": {
                                    "query": search_term,
                                    "fields": ["packageName", "packageSummary", "cities.aliasCityName"],
                                    "type": "best_fields"
                                }
                            }
                        ],
                        "minimum_should_match": 1
                    }
                },
                "size": 15,
                "sort": [{"_score": "desc"}]
            }

            try:
                fallback_resp = es.search(index=indexNameForPackages, body=fallback_body)
                fallback_hits = fallback_resp.get("hits", {}).get("hits", [])
                fallback_ids = {hit["_source"]["packageId"] for hit in fallback_hits}
                logging.info(f"ES fallback found {len(fallback_ids)} package IDs.")
            except Exception as e:
                logging.error(f"Error in ES fallback search: {str(e)}")
                fallback_ids = set()

            if not fallback_ids:
                # => TOTALLY NO PACKAGES => 404
                return JSONResponse(
                    content={
                        "code": 404,
                        "message": f"No matching packages found for '{search_term}'.",
                        "body": []
                    }
                )
            final_ids = fallback_ids
        else:
            final_ids = autosuggest_pkg_ids

        # STEP 3: If month_of_travel => intersect
        if month_of_travel:
            before_count = len(final_ids)
            final_ids = final_ids.intersection(monthly_package_ids)
            logging.info(f"Intersecting with monthly => from {before_count} to {len(final_ids)}")
            if not final_ids:
                # => 401 => no active packages
                return JSONResponse(
                    content={
                        "code": 401,
                        "message": f"No active packages found for {search_term} in {month_of_travel}.",
                        "body": []
                    }
                )

        # STEP 4: Retrieve full package details
        detailed_packages = []
        for pid in final_ids:
            try:
                body = {"packageId": pid}
                data = await search_item_by_package_id_internal(body=body, generate_summary=True)
                detailed_packages.append(data)
            except Exception as e:
                logging.error(f"Package {pid} retrieval error => {str(e)}")

        if not detailed_packages:
            # => 404 => No valid packages found
            return JSONResponse(
                content={
                    "code": 404,
                    "message": f"No valid packages found after PDP retrieval for {search_term}.",
                    "body": []
                }
            )

        # Step 5: GIT/FIT filtering
        git_packs = [pkg for pkg in detailed_packages if pkg.itinerary_data.pkgSubtypeName == "GIT"]
        fit_packs = [pkg for pkg in detailed_packages if pkg.itinerary_data.pkgSubtypeName == "FIT"]
        combined = git_packs + fit_packs
        if not combined:
            return JSONResponse(
                content={
                    "code": 404,
                    "message": "No GIT or FIT packages found after classification.",
                    "body": []
                }
            )

        # Step 6: Apply standard filters => departureCity, days, budget
        filtered_packages = apply_filters(combined, departure_city, target_days, target_budget)
        if isinstance(filtered_packages, JSONResponse):
            # => If your 'apply_filters' returns JSONResponse in some condition
            return filtered_packages
        if not filtered_packages:
            # => 402 => No packages from that base city => list all departure cities
            # Extract all departure cities
            all_depart_cities = set()
            for p in combined:
                all_depart_cities.update([c.lower() for c in p.itinerary_data.departureCities])
            return JSONResponse(
                content={
                    "code": 402,
                    "message": f"No packages from base city '{departure_city}'. Packages available from: {', '.join(sorted(all_depart_cities))}",
                    "body": []
                }
            )

        # Final success => code=200 if from AutoSuggest, code=201 if fallback
        final_code = 201 if fallback_used else 200
        response_msg = f"Here are the available packages for '{search_term}' for base city '{departure_city}'"
        serialized_data = jsonable_encoder(filtered_packages)
        return JSONResponse(
            content={"code": final_code, "message": response_msg, "body": serialized_data}
        )

    except Exception as e:
        logging.error(f"Internal Server Error in livepackagesv2: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"code": 500, "message": "Internal server error", "body": []}
        )

def apply_filters(detailed_packages, departure_city, target_days, target_budget):
    """
    Applies departure_city, days, and budget filters. Returns a list or JSONResponse if something fails.
    1) Filter by departure city
    2) Filter by days
    3) Filter by budget
    """
    # 1) Filter by departure city
    if departure_city:
        detailed_packages = [
            p for p in detailed_packages
            if departure_city in [c.lower() for c in p.itinerary_data.departureCities]
        ]

    # 2) Filter by days if needed
    if target_days:
        detailed_packages = [p for p in detailed_packages if p.itinerary_data.days and p.itinerary_data.days <= target_days]

    # 3) Filter by budget
    filtered_packages = filter_by_budget(target_budget, detailed_packages)

    return filtered_packages


def filter_by_budget(target_budget, detailed_packages):
    """
    Filters packages based on the target budget.
    Prioritizes closest budget matches first, then shows other categories in ascending order.
    """
    budget_ranges = {
        "Low Budget": lambda pkg: (pkg.itinerary_data.price or 0) < 30000,
        "Mid-Range Budget": lambda pkg: 30000 <= (pkg.itinerary_data.price or 0) <= 100000,
        "High Budget": lambda pkg: 100000 < (pkg.itinerary_data.price or 0) <= 200000,
        "Luxury Budget": lambda pkg: (pkg.itinerary_data.price or 0) > 200000
    }

    if target_budget is None:
        return sorted(detailed_packages, key=lambda x: x.itinerary_data.price or float('inf'))

    # Determine which budget category the target budget falls into
    if target_budget <= 30000:
        primary_category = "Low Budget"
    elif 30000 < target_budget <= 100000:
        primary_category = "Mid-Range Budget"
    elif 100000 < target_budget <= 200000:
        primary_category = "High Budget"
    else:
        primary_category = "Luxury Budget"

    # Step 1: Extract packages in the matching budget category and sort by closest match
    matching_budget_packages = sorted(
        [pkg for pkg in detailed_packages if budget_ranges[primary_category](pkg)],
        key=lambda x: abs((x.itinerary_data.price or 0) - target_budget)  # Sort by closest match
    )

    # Step 2: Extract packages from higher budget categories (sorted ascending)
    higher_budget_packages = sorted(
        [pkg for category in ["High Budget", "Luxury Budget"]
         if category != primary_category for pkg in detailed_packages if budget_ranges[category](pkg)],
        key=lambda x: x.itinerary_data.price or float('inf')
    )

    # Step 3: Extract packages from lower budget categories (sorted ascending)
    lower_budget_packages = sorted(
        [pkg for category in ["Low Budget"]
         if category != primary_category for pkg in detailed_packages if budget_ranges[category](pkg)],
        key=lambda x: x.itinerary_data.price or float('inf')
    )

    # Concatenate results: matching (closest first) → higher (ascending) → lower (ascending)
    return matching_budget_packages + higher_budget_packages + lower_budget_packages

async def search_item_by_package_id_internal(
    body: Dict[str, str],
    generate_summary: bool
) -> ItemOut:
    """
    Helper function to search for a package by packageId with dynamic summary generation.

    Args:
        body (dict): Request body containing the packageId.
        generate_summary (bool): Whether to generate a summary for the package.

    Returns:
        ItemOut: The package details and its search score.
    """
    index = indexNameForPackages  # Default index

    package_id = body.get("packageId")
    if not package_id:
        raise HTTPException(status_code=400, detail="packageId is required in the request body.")
    
    logging.info(f"Searching for package with packageId: {package_id} in index: {index}, generate_summary={generate_summary}")

    ensure_index_exists(index)

    search_body = {
        "query": {
            "term": {
                "packageId": package_id
            }
        },
        "size": 1
    }

    response = es.search(index=index, body=search_body)
    hits = response.get("hits", {}).get("hits", [])
    logging.info(f"Elasticsearch response: {response}")

    if not hits:
        logging.info(f"No package found in ES for {package_id}, attempting PDP fallback.")
        package_data = fetch_package_dynamically(package_id, do_generate_summary=generate_summary)
        if not package_data:
            raise HTTPException(status_code=404, detail="Package not found even in PDP source")
        
        # Index the fetched package into Elasticsearch
        await index_package_in_es(package_data)

        pdp_itinerary = package_data.get("packageItinerary", {"summary": "", "itinerary": []})

        return ItemOut(
            id=package_data["packageId"],
            itinerary_data=savedItineraryData(
                packageId=package_data["packageId"],
                packageName=package_data["packageName"],
                packageTheme=package_data["packageTheme"],
                days=package_data["days"],
                cities=package_data["cities"],
                highlights=package_data["highlights"],
                thumbnailImage=package_data["thumbnailImage"],
                images=package_data["images"],
                pdfName=package_data["pdfName"],
                price=package_data.get("minimumPrice"),
                minimumPrice=package_data.get("minimumPrice"),
                packageData=package_data["packageData"],
                departureCities=package_data.get("departureCities", []),
                packageSummary=package_data.get("packageSummary"),
                packageItinerary=PackageItinerary(
                    summary=pdp_itinerary.get("summary", ""),
                    itinerary=[
                        DayItinerary(
                            day=day_data.get("day", 0),
                            description=day_data.get("description", ""),
                            mealDescription=day_data.get("mealDescription", ""),
                            overnightStay=day_data.get("overnightStay", "")
                        )
                        for day_data in pdp_itinerary.get("itinerary", [])
                    ]
                ),
                hotels=package_data.get("hotels"),
                meals=package_data.get("meals"),
                sightseeing=package_data.get("sightseeing"),
                inclusions=package_data.get("inclusions"),
                exclusions=package_data.get("exclusions"),
                termsAndConditions=package_data.get("termsAndConditions"),
                pkgSubtypeId=package_data["pkgSubtypeId"],
                pkgSubtypeName=package_data["pkgSubtypeName"],
                pkgTypeId=package_data["pkgTypeId"]
            ),
            score=1.0
        )


    source = hits[0]["_source"]
    logging.info(f"Package found in ES: {source}")

    es_itinerary = source.get("packageItinerary", {"summary": "", "itinerary": []})

    item_out = ItemOut(
    id=hits[0]["_id"],
    itinerary_data=savedItineraryData(
        packageId=source["packageId"],
        packageName=source["packageName"],
        packageTheme=source["packageTheme"],
        days=source["days"],
        cities=source["cities"],
        highlights=source["highlights"],
        thumbnailImage=source["thumbnailImage"],
        images=source["images"],
        pdfName=source["pdfName"],
        price=source["price"],
        minimumPrice=source["minimumPrice"],
        packageData=source["packageData"],
        packageSummary=source.get("packageSummary"),
        departureCities=source.get("departureCities", []),
        packageItinerary=PackageItinerary(
            summary=source["packageItinerary"].get("summary", ""),
            itinerary=[
                DayItinerary(
                    day=day_data.get("day"),
                    description=day_data.get("description", ""),
                    mealDescription=day_data.get("mealDescription", ""),
                    overnightStay=day_data.get("overnightStay", "")
                )
                for day_data in source["packageItinerary"].get("itinerary", [])
            ]
        ),
        hotels=source.get("hotels"),
        meals=source.get("meals"),
        sightseeing=source.get("sightseeing"),
        inclusions=source.get("inclusions"),
        exclusions=source.get("exclusions"),
        termsAndConditions=source.get("termsAndConditions"),
        pkgSubtypeId=source["pkgSubtypeId"],
        pkgSubtypeName=source["pkgSubtypeName"],
        pkgTypeId=source["pkgTypeId"]
    ),
    score=hits[0]["_score"]
)


    logging.info(f"ItemOut constructed: {item_out}")
    return item_out

######## Destination Details ######


@app.post("/v1/get_destination_details")
async def get_destination_details(request: AutoBudgetRequest):
    """
    Fetches package IDs from the AutoSuggest API, retrieves package details from Elasticsearch (no PDP fallback),
    provides budget ranges, and fetches visa information if required.
    """
    start_time = datetime.now()
    logging.info(f"Request received for /v1/get_destination_details with searchTerm: {request.search_term}")

    search_term = request.search_term.strip().lower()
    visa_information = []  # Initialize visa information

    try:
        # Step 1: Get the country of the destination using Geopy with English language enforced
        geolocator = Nominatim(user_agent="geoapi", timeout=10)
        country_name = None

        try:
            location = geolocator.geocode(search_term, exactly_one=True, language="en")
            logging.debug(f"Geopy raw output for '{search_term}': {location}")

            if location:
                address_parts = location.address.split(",")
                country_name = address_parts[-1].strip()
                logging.info(f"Extracted country for '{search_term}': {country_name}")
            else:
                logging.warning(f"Geopy could not determine the country for '{search_term}'")

        except GeocoderTimedOut:
            logging.error("Geopy request timed out while fetching country information")
            country_name = None

        # Step 2: If not India, fetch Visa info
        if country_name and country_name.lower() != "india":
            logging.info(f"Fetching visa information for {country_name}...")
            visa_search_result = await search_visa_faq(country_name)
            logging.debug(f"Visa search result for {country_name}: {visa_search_result}")

            visa_information = [
                result.get("visa_info", "") for result in visa_search_result.get("results", [])
            ]
            logging.info(f"Visa info fetched for {country_name}: {visa_information}")
        else:
            logging.info("Visa information not required (Destination is in India).")

        # Step 3: Call AutoSuggest API to get package suggestions
        request_id, session_id = get_new_auth_token()
        url = "https://services.thomascook.in/tcHolidayRS/autosuggest"
        params = {"searchAutoSuggest": search_term}
        headers = {"Requestid": request_id, "Sessionid": session_id}

        logging.info(f"Calling AutoSuggest API with params: {params}")
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        autosuggest_data = response.json()

        logging.debug(f"AutoSuggest API raw response: {autosuggest_data}")

        if not isinstance(autosuggest_data, list):
            logging.error("Invalid AutoSuggest API response format.")
            raise HTTPException(status_code=500, detail="Invalid response from AutoSuggest API.")

        logging.info(f"Total autosuggest entries for '{search_term}': {len(autosuggest_data)}")

        # Step 4: Filter results by city, country, state, continent
        filtered_results = [
            item for item in autosuggest_data
            if search_term == item.get("cityName", "").strip().lower()
            or search_term == item.get("countryName", "").strip().lower()
            or search_term == item.get("stateName", "").strip().lower()
            or search_term == item.get("continentName", "").strip().lower()
        ]
        logging.info(f"Filtered AutoSuggest results count: {len(filtered_results)}")

        # Step 5: Extract package IDs from filtered results
        package_ids = {
            pkg["packageId"]
            for item in filtered_results
            for pkg in item.get("pkgnameIdMappingList", [])
        }
        logging.info(f"Filtered package IDs based on '{search_term}': {package_ids}")

        if not package_ids:
            return JSONResponse({
                "message": "No matching packages found.",
                "available_budget_ranges": [],
                "visa_information": visa_information
            })

        # Step 6: Fetch package details from Elasticsearch (NO PDP fallback)
        detailed_packages = []
        failed_packages_count = 0

        for pkg_id in package_ids:
            try:
                # Directly check ES; if not found in ES, skip
                search_body = {
                    "query": {
                        "term": {
                            "packageId": pkg_id
                        }
                    },
                    "size": 1
                }
                es_response = es.search(index=indexNameForPackages, body=search_body)
                hits = es_response.get("hits", {}).get("hits", [])
                if not hits:
                    logging.warning(f"Package {pkg_id} not found in ES; skipping.")
                    continue

                # Construct the package data from ES hit
                source = hits[0]["_source"]

                # Build a minimal object for cost extraction
                item_price = source.get("price") or source.get("minimumPrice")
                # Build a small structure that mimics your existing ItemOut
                item_obj = ItemOut(
                    id=hits[0]["_id"],
                    itinerary_data=savedItineraryData(
                        packageId=source["packageId"],
                        packageName=source["packageName"],
                        packageTheme=source.get("packageTheme", []),
                        days=source.get("days"),
                        cities=source.get("cities", []),
                        highlights=source.get("highlights", []),
                        thumbnailImage=source.get("thumbnailImage"),
                        images=source.get("images", []),
                        pdfName=source.get("pdfName"),
                        price=item_price,
                        minimumPrice=source.get("minimumPrice"),
                        packageData=source.get("packageData", ""),
                        packageSummary=source.get("packageSummary"),
                        departureCities=source.get("departureCities", []),
                        packageItinerary=PackageItinerary(
                            summary=source.get("packageItinerary", {}).get("summary", ""),
                            itinerary=[]
                        ),
                        hotels=source.get("hotels"),
                        meals=source.get("meals"),
                        sightseeing=source.get("sightseeing"),
                        inclusions=source.get("inclusions"),
                        exclusions=source.get("exclusions"),
                        termsAndConditions=source.get("termsAndConditions"),
                        pkgSubtypeId=source.get("pkgSubtypeId"),
                        pkgSubtypeName=source.get("pkgSubtypeName", ""),
                        pkgTypeId=source.get("pkgTypeId")
                    ),
                    score=hits[0]["_score"]
                )
                detailed_packages.append(item_obj)

            except Exception as e:
                failed_packages_count += 1
                logging.error(f"Error processing package {pkg_id}: {str(e)}")
                continue

        # Step 7: Extract prices for budget range from the ES-based packages
        prices = sorted(
            [pkg.itinerary_data.price for pkg in detailed_packages if pkg.itinerary_data.price]
        )
        logging.info(f"Extracted prices for destination: {prices}")

        if not prices:
            return JSONResponse({
                "message": "No price data available for the packages (or no valid ES packages).",
                "available_budget_ranges": [],
                "visa_information": visa_information
            })

        # Step 8: Define dynamic budget ranges with your custom labels
        # We only include the labels if at least 1 package falls into that range
        budget_labels = []
        # For readability, we define the intervals manually:
        # - Less than 30000
        # - 30000 to 100000
        # - 100000 to 200000
        # - more than 200000
        if any(p <= 30000 for p in prices):
            budget_labels.append("Less than ₹30,000")
        if any(30000 < p <= 100000 for p in prices):
            budget_labels.append("₹30,000 - ₹1 Lac")
        if any(100000 < p <= 200000 for p in prices):
            budget_labels.append("₹1 Lac - ₹2 Lac")
        if any(p > 200000 for p in prices):
            budget_labels.append("More than ₹2 Lac")

        logging.info(f"Available budget ranges: {budget_labels}")

        end_time = datetime.now()
        elapsed_time = (end_time - start_time).total_seconds()
        logging.info(f"Request processed in {elapsed_time} seconds")

        return {
            "search_term": request.search_term,
            "country_detected": country_name,
            "available_budget_ranges": budget_labels,
            "visa_information": visa_information
        }

    except Exception as e:
        logging.error(f"Internal server error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")





########### Save and Retrieve conversations #########    

@app.post("/v1/save_conversation")
def save_conversation(conversation_data: dict = Body(...)):
    """
    Save (index) the provided conversation into Elasticsearch.
    """
    try:
        # ✅ Convert empty string to `None` before validation
        if conversation_data.get("booking_date") == "":
            conversation_data["booking_date"] = None

        # ✅ Ensure `saved_time` exists in `packages_saved`
        for pkg in conversation_data.get("packages_saved", []):
            if "saved_time" not in pkg or not pkg["saved_time"]:
                pkg["saved_time"] = datetime.utcnow().isoformat()  # Set default time if missing

        # Ensure index exists
        if not es.indices.exists(index=CHAT_INDEX_NAME):
            es.indices.create(index=CHAT_INDEX_NAME, body=conversation_index_mapping)
            logging.info(f"Index {CHAT_INDEX_NAME} created with mapping.")

        doc_id = conversation_data["conversationId"]
        response = es.index(
            index=CHAT_INDEX_NAME,
            id=doc_id,
            document=conversation_data
        )
        return {"status": "success", "result": response}

    except Exception as e:
        logging.error(f"Error saving conversation: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/v1/get_conversation")
def get_conversation(body: ConversationIdRequest):
    """
    Retrieve the conversation document with the given conversationId.
    (Changed from GET to POST to accept a JSON body.)
    """
    try:
        if not es.indices.exists(index=CHAT_INDEX_NAME):
            raise HTTPException(status_code=404, detail=f"Index '{CHAT_INDEX_NAME}' not found.")

        response = es.get(index=CHAT_INDEX_NAME, id=body.conversationId)
        if response.get("found"):
            return {"status": "success", "conversation": response["_source"]}
        else:
            return {"status": "not_found", "message": f"No conversation found for {body.conversationId}"}

    except Exception as e:
        logging.error(f"Error retrieving conversation {body.conversationId}: {str(e)}")
        # Check for a "not_found" phrase in the error to return a 404, if needed
        if "not_found" in str(e).lower():
            raise HTTPException(status_code=404, detail=f"Conversation {body.conversationId} not found.")
        raise HTTPException(status_code=500, detail=str(e))
   
@app.post("/v1/get_conversation_summaries")
def get_conversation_summaries(body: UserIdRequest):
    """
    Given a userId, return a list of documents with:
      - conversationId
      - chat_name
      - chat_started
      - chat_modified
      - packages_saved (number of saved packages in each conversation)
    sorted by the latest chat_modified in descending order.
    """
    try:
        ensure_index_exists(CHAT_INDEX_NAME)

        search_body = {
            "size": 1000,
            "sort": [
                {"chat_modified": {"order": "desc", "missing": "_last"}}
            ],
            "query": {
                "term": {
                    "userId": body.userId
                }
            },
            "_source": ["conversationId", "chat_name", "chat_started", "chat_modified", "packages_saved"]
        }

        response = es.search(index=CHAT_INDEX_NAME, body=search_body)
        hits = response.get("hits", {}).get("hits", [])

        results = []
        for hit in hits:
            src = hit["_source"]
            item = {
                "conversationId": src.get("conversationId"),
                "chat_name": src.get("chat_name"),
                "chat_started": src.get("chat_started"),
                "chat_modified": src.get("chat_modified", None),
                "packages_saved": len(src.get("packages_saved", []))  # ✅ Count of saved packages
            }
            results.append(item)

        return {
            "status": "success",
            "total": len(results),
            "conversations": results
        }

    except Exception as e:
        logging.error(f"Error retrieving conversation summaries: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

def fetch_package_details_internally(package_id: str) -> Dict[str, Any]:
    """
    Fetch package details from Elasticsearch by packageId.
    """
    try:
        search_body = {
            "query": {
                "term": {
                    "packageId": package_id  
                }
            },
            "size": 1
        }

        logging.info(f"Sending Elasticsearch Query: {json.dumps(search_body, indent=2)}")

        response = es.search(index=indexNameForPackages, body=search_body)

        response_dict = response.body if hasattr(response, "body") else response
        logging.info(f"Elasticsearch Response: {json.dumps(response_dict, indent=2)}")

        hits = response_dict.get("hits", {}).get("hits", [])

        if not hits:
            logging.warning(f"Package ID {package_id} not found in Elasticsearch.")
            return {}  

        package_data = hits[0]["_source"]

        return {
            "packageName": package_data.get("packageName", "Unknown Package"),
            "days": package_data.get("days", None),
            "cities": package_data.get("cities", []),
            "price": package_data.get("price", None)
        }

    except Exception as e:
        logging.error(f"Error fetching package details for {package_id}: {str(e)}")
        return {}

@app.post("/v1/get_packages_saved")
def get_packages_saved(body: UserIdRequest):
    """
    Given a userId, return a unique list of saved packages 
    from all documents with the same userId, sorted by latest "chat_modified".
    """
    ensure_index_exists(CHAT_INDEX_NAME)

    search_body = {
        "size": 1000,
        "sort": [{"chat_modified": {"order": "desc", "missing": "_last"}}],  # ✅ Handle missing sorting field
        "query": {"term": {"userId": body.userId}},
        "_source": ["packages_saved", "conversationId"]
    }

    response = es.search(index=CHAT_INDEX_NAME, body=search_body)
    hits = response.get("hits", {}).get("hits", [])

    combined_results = []
    seen = set()

    for hit in hits:
        doc = hit["_source"]
        conv_id = doc["conversationId"]
        packages = doc.get("packages_saved", [])

        for pkg_obj in packages:
            pkg_id = pkg_obj["packageId"]
            saved_time = pkg_obj.get("saved_time", datetime.utcnow().isoformat())  # ✅ Prevent KeyError

            combo_key = (conv_id, pkg_id)
            if combo_key in seen:
                continue
            seen.add(combo_key)

            details = fetch_package_details_internally(pkg_id) 

            package_info = {
                "packageID": pkg_id,
                "conversationId": conv_id,
                "saved_time": saved_time,
                "packageName": details.get("packageName", "Unknown Package"),
                "Destination": list({c["cityName"] for c in details.get("cities", []) if "cityName" in c}),
                "duration": details.get("days"),
                "estimatedcost": details.get("price"),
            }

            combined_results.append(package_info)

    return {
        "status": "success",
        "packages_saved": combined_results
    }

@app.post("/v1/update_chat_name")
def update_chat_name(body: UpdateChatNameRequest):
    """
    Update the chat name for a given conversationId.
    """
    try:
        if not es.indices.exists(index=CHAT_INDEX_NAME):
            raise HTTPException(status_code=404, detail=f"Index '{CHAT_INDEX_NAME}' not found.")

        # Check if the conversation exists
        response = es.get(index=CHAT_INDEX_NAME, id=body.conversationId)
        if not response.get("found"):
            raise HTTPException(status_code=404, detail=f"Conversation {body.conversationId} not found.")

        # Update the chat name
        update_body = {
            "doc": {
                "chat_name": body.new_chat_name,
                "chat_modified": datetime.utcnow().isoformat()  # Update modification timestamp
            }
        }
        es.update(index=CHAT_INDEX_NAME, id=body.conversationId, body=update_body)

        return {"status": "success", "message": "Chat name updated successfully."}

    except Exception as e:
        logging.error(f"Error updating chat name: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/v1/delete_conversation")
def delete_conversation(body: DeleteConversationRequest):
    """
    Delete a conversation document by conversationId.
    """
    try:
        if not es.indices.exists(index=CHAT_INDEX_NAME):
            raise HTTPException(status_code=404, detail=f"Index '{CHAT_INDEX_NAME}' not found.")

        # Check if the conversation exists
        response = es.get(index=CHAT_INDEX_NAME, id=body.conversationId)
        if not response.get("found"):
            raise HTTPException(status_code=404, detail=f"Conversation {body.conversationId} not found.")

        # Delete the conversation
        es.delete(index=CHAT_INDEX_NAME, id=body.conversationId)

        return {"status": "success", "message": "Conversation deleted successfully."}

    except Exception as e:
        logging.error(f"Error deleting conversation: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/v1/get_user_conversations")
def get_user_conversations(body: UserIdRequest):
    """
    Retrieve all conversations for the given userId, sorted by latest chat_modified descending.
    """
    try:
        # Ensure the index exists (optional, if you want to be safe)
        if not es.indices.exists(index=CHAT_INDEX_NAME):
            raise HTTPException(
                status_code=404,
                detail=f"Index '{CHAT_INDEX_NAME}' not found."
            )

        # Build the Elasticsearch query
        search_body = {
            "size": 1000,
            "sort": [
                {"chat_modified": {"order": "desc", "missing": "_last"}}
            ],
            "query": {
                "term": {
                    "userId": body.userId
                }
            }
        }

        # Execute the search
        response = es.search(index=CHAT_INDEX_NAME, body=search_body)
        hits = response.get("hits", {}).get("hits", [])

        # Extract the full _source for each matching document
        conversations = [hit["_source"] for hit in hits]

        return {
            "status": "success",
            "total": len(conversations),
            "conversations": conversations
        }

    except Exception as e:
        logging.error(f"Error retrieving conversations for userId={body.userId}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )
    

######## Batch Processing Logic starts from here ########

@app.post("/batch_process_all_packages")
async def batch_process_all_packages(background_tasks: BackgroundTasks):
    """
    1. Fetches available package IDs for all months and stores them in respective monthly indices.
    2. Filters all unique package IDs across months.
    3. Fetches package details in parallel and indexes them into `tcildatav1`.

    Uses background processing to prevent request timeouts.
    """
    try:
        months = [
            "january", "february", "march", "april", "may", "june",
            "july", "august", "september", "october", "november", "december"
        ]
        current_year = datetime.now().year

        background_tasks.add_task(process_all_packages, months, current_year)

        return {"message": "Unified batch processing started for all packages."}
    except Exception as e:
        logging.error(f"Error starting batch processing: {str(e)}")
        raise HTTPException(status_code=500, detail="Error starting batch processing.")

async def process_all_packages(months: List[str], current_year: int):
    """
    1. Fetches package IDs for all months in parallel.
    2. Deduplicates package IDs.
    3. Fetches package details and indexes them into `tcildatav1`.
    """
    logging.info("Starting parallel batch processing for monthly packages.")
    
    failed_months = []
    unique_package_ids = set()
    current_month = datetime.now().month - 1  # Convert to 0-based index

    # **Use asyncio.gather() to fetch monthly packages concurrently**
    tasks = [
        fetch_and_store_monthly_packages(month, current_year, current_month, unique_package_ids)
        for month in months
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Handle any failed months
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logging.error(f"Failed to process month {months[i]}: {result}")
            failed_months.append(months[i])

    if failed_months:
        logging.error(f"Failed months: {', '.join(failed_months)}")
    else:
        logging.info("All months processed successfully.")

    # Now process package details in parallel
    await process_unique_packages(unique_package_ids)


async def fetch_and_store_monthly_packages(month: str, current_year: int, current_month: int, unique_package_ids: set):
    """
    Fetch package IDs for a given month, clear previous data, and store in Elasticsearch.
    """
    try:
        month_index = ["january", "february", "march", "april", "may", "june", 
                       "july", "august", "september", "october", "november", "december"].index(month)
        target_year = current_year if month_index >= current_month else current_year + 1
        month_of_travel = f"{str(month_index + 1).zfill(2)}-{target_year}"
        index_name = f"{month}_{target_year}"

        # **Step 1: Delete previous documents from the index**
        logging.info(f"Clearing previous data in index: {index_name}")
        await asyncio.to_thread(es.delete_by_query, index=index_name, body={"query": {"match_all": {}}})
        logging.info(f"Deleted old documents from {index_name}.")

        # **Step 2: Fetch new package IDs**
        attempt = 0
        package_ids = None

        while attempt < MAX_RETRIES:
            try:
                logging.info(f"Fetching packages for {month_of_travel} (Attempt {attempt + 1}/{MAX_RETRIES})...")
                package_ids = await fetch_packages_with_timeout(month_of_travel)

                if package_ids:
                    logging.info(f"Fetched {len(package_ids)} package IDs for {month_of_travel}.")
                    break  # ✅ Success, exit retry loop

            except ReadTimeout:
                logging.warning(f"Read timeout fetching packages for {month_of_travel}, retrying...")
            except ConnectionError:
                logging.warning(f"Connection error fetching packages for {month_of_travel}, retrying...")
            except HTTPError as http_err:
                logging.error(f"HTTP error fetching packages for {month_of_travel}: {http_err}")
                break  # ❌ No retry for HTTP errors
            except Exception as e:
                logging.error(f"Unknown error fetching packages for {month_of_travel}: {str(e)}")
                break

            attempt += 1
            await asyncio.sleep(2 ** attempt)  # ⏳ Exponential backoff

        if not package_ids:
            logging.error(f"Failed to fetch packages for {month_of_travel} after {MAX_RETRIES} attempts.")
            return  # Stop processing this month

        # **Step 3: Store package IDs in Elasticsearch**
        unique_package_ids.update(package_ids)

        bulk_actions = [{"_index": index_name, "_source": {"packageId": package_id}} for package_id in package_ids]
        if bulk_actions:
            await asyncio.to_thread(helpers.bulk, es, bulk_actions)
            logging.info(f"Indexed {len(bulk_actions)} packages for {month_of_travel} in {index_name}.")

    except Exception as e:
        logging.error(f"Error processing month {month}: {str(e)}")



async def fetch_packages_with_timeout(month_of_travel):
    """
    Wrapper to enforce a timeout when calling `fetch_packages_for_month()`.
    """
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(fetch_packages_for_month, month_of_travel),
            timeout=TIMEOUT
        )
    except asyncio.TimeoutError:
        logging.error(f"Timeout error: Fetching packages for {month_of_travel} took longer than {TIMEOUT} seconds.")
        return []

async def process_unique_packages(unique_package_ids: set):
    """
    Fetches package details concurrently for unique package IDs and stores them in the main index (`tcildatav1`).
    Uses batching to prevent too many concurrent requests.
    """
    logging.info(f"🚀 Processing {len(unique_package_ids)} unique packages for indexing.")

    if not unique_package_ids:
        logging.warning("⚠️ No unique packages found after filtering. Skipping detailed processing.")
        return

    # ✅ Limit concurrency to prevent overload (adjust as needed)
    SEMAPHORE_LIMIT = 50  # Max concurrent fetches
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)

    async def limited_fetch_and_store(package_id):
        async with semaphore:
            return await fetch_and_store_package(package_id)

    # ✅ Batch Processing (Limits Memory & Prevents API Overload)
    batch_size = 500  # Process in chunks
    package_list = list(unique_package_ids)
    results = []

    for i in range(0, len(package_list), batch_size):
        batch = package_list[i : i + batch_size]
        logging.info(f"🔄 Processing batch {i // batch_size + 1}: {len(batch)} packages.")

        # ✅ Run tasks with concurrency limits
        tasks = [limited_fetch_and_store(pkg) for pkg in batch]
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        results.extend(batch_results)

        # ✅ Pause to avoid overloading API (if needed)
        await asyncio.sleep(1)

    # ✅ Bulk index valid results into Elasticsearch
    es_actions = [res for res in results if isinstance(res, dict)]
    failed_packages = [res for res in results if isinstance(res, Exception)]

    if es_actions:
        try:
            success, failed = await asyncio.to_thread(helpers.bulk, es, es_actions, chunk_size=500, stats_only=True)
            logging.info(f"📌 Successfully indexed {success} packages into {indexNameForPackages}. Failed: {failed}")
        except Exception as e:
            logging.error(f"❌ Error during bulk indexing: {str(e)}")

    logging.info(f"✅ Batch processing completed. Processed: {len(es_actions)}, Failed: {len(failed_packages)}")


async def fetch_and_store_package(package_id: str):
    """
    Fetches package details for a single package and prepares it for bulk indexing.
    Runs in parallel for multiple package IDs.
    """
    try:
        logging.info(f"Fetching details for package ID: {package_id}")
        package_data = await asyncio.to_thread(fetch_package_dynamically, package_id)

        if not package_data:
            logging.warning(f"Skipping package {package_id} due to missing data.")
            return None

        # **Ensure packageItinerary follows the correct nested structure**
        itinerary_data = package_data.get("packageItinerary", {"summary": "", "itinerary": []})
        formatted_itinerary = [
            {
                "day": item.get("day"),
                "description": item.get("description", ""),
                "mealDescription": item.get("mealDescription", ""),
                "overnightStay": item.get("overnightStay", "")
            }
            for item in itinerary_data.get("itinerary", [])
        ]

        # **Return full package structure for Elasticsearch bulk indexing**
        return {
            "_index": indexNameForPackages,
            "_id": package_data["packageId"],
            "_source": {
                "packageId": package_data["packageId"],
                "packageName": package_data["packageName"],
                "packageTheme": package_data.get("packageTheme", []),
                "days": package_data["days"],
                "cities": package_data.get("cities", []),
                "highlights": package_data.get("highlights", []),
                "pdfName": package_data.get("pdfName"),
                "price": package_data.get("minimumPrice"),
                "minimumPrice": package_data.get("minimumPrice"),
                "packageData": package_data["packageData"],
                "packageSummary": package_data.get("packageSummary"),
                "thumbnailImage": package_data["thumbnailImage"],
                "images": package_data.get("images", []),
                "visitingCountries": package_data.get("visitingCountries", []),
                "departureCities": package_data.get("departureCities", []),
                "packageItinerary": {
                    "summary": itinerary_data.get("summary", ""),
                    "itinerary": formatted_itinerary
                },
                "hotels": package_data.get("hotels"),
                "meals": package_data.get("meals"),
                "sightseeing": package_data.get("sightseeing"),
                "inclusions": package_data.get("inclusions"),
                "exclusions": package_data.get("exclusions"),
                "termsAndConditions": package_data.get("termsAndConditions"),
                "hashKey": package_data.get("hashKey"),
                "pkgSubtypeId": package_data["pkgSubtypeId"],
                "pkgSubtypeName": package_data["pkgSubtypeName"],
                "pkgTypeId": package_data["pkgTypeId"]
            }
        }
    except Exception as e:
        logging.error(f"Error fetching package {package_id}: {str(e)}")
        return e

############# previous batch processing logic

@app.post("/batch_process_packages")
async def batch_process_packages(file: UploadFile, background_tasks: BackgroundTasks):
    """
    Accepts a JSON file with a list of destinations, processes each destination, 
    fetches related packages from PDP API, and stores them in Elasticsearch.
    """
    try:
        # Parse the uploaded JSON file
        content = await file.read()
        destinations = json.loads(content)

        if not isinstance(destinations, list):
            raise HTTPException(status_code=400, detail="Invalid file format. Expected a list of destinations.")

        # Add the task to background processing
        background_tasks.add_task(process_destinations_batch, destinations)

        return {"message": "Batch processing started. Data will be stored in Elasticsearch."}
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON file.")
    except Exception as e:
        logging.error(f"Error processing batch: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error during batch processing.")
    


async def process_destinations_batch(destinations: List[str]):
    """
    Processes each destination to fetch packages and store them in Elasticsearch.
    Logs the progress, including the number of unique packages found and their processing status.
    """
    logging.info(f"Starting batch process for destinations: {destinations}")

    # List to store actions for bulk insertion into Elasticsearch
    es_actions = []

    # Set to keep track of processed package IDs
    processed_package_ids = set()

    for destination in destinations:
        try:
            logging.info(f"Processing destination: {destination}")
            
            # Use the `fetch_package_dynamically` to fetch package data for the destination
            search_term = destination
            request_id, session_id = get_new_auth_token()

            # Call PDP API for the destination
            url = "https://services.thomascook.in/tcHolidayRS/autosuggest"
            params = {"searchAutoSuggest": search_term}
            headers = {
                "Requestid": request_id,
                "Sessionid": session_id,
            }

            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()

            # Parse JSON response
            data = response.json()

            # Track total packages found for the destination
            total_packages = 0

            for item in data:
                pkgname_mapping = item.get("pkgnameIdMappingList", [])
                total_packages += len(pkgname_mapping)

                for idx, pkg in enumerate(pkgname_mapping, start=1):
                    package_id = pkg["packageId"]

                    # Skip processing if package ID is already processed
                    if package_id in processed_package_ids:
                        logging.info(f"Skipping already processed package ID: {package_id}")
                        continue

                    logging.info(f"Processing package {idx}/{len(pkgname_mapping)} for destination '{destination}' (Package ID: {package_id})")

                    package_data = fetch_package_dynamically(package_id)

                    if package_data:
                        # Add package ID to the processed set
                        processed_package_ids.add(package_id)

                        itinerary_data = package_data.get("packageItinerary", {"summary": "", "itinerary": []})
                        formatted_itinerary = [
                            {
                                "day": item.get("day"),
                                "description": item.get("description", ""),
                                "mealDescription": item.get("mealDescription", ""),
                                "overnightStay": item.get("overnightStay", "")
                            }
                            for item in itinerary_data.get("itinerary", [])
                        ]

                        # **Return full package structure for Elasticsearch bulk indexing**
                        return {
                            "_index": indexNameForPackages,
                            "_id": package_data["packageId"],
                            "_source": {
                                "packageId": package_data["packageId"],
                                "packageName": package_data["packageName"],
                                "packageTheme": package_data.get("packageTheme", []),
                                "days": package_data["days"],
                                "cities": package_data.get("cities", []),
                                "highlights": package_data.get("highlights", []),
                                "pdfName": package_data.get("pdfName"),
                                "price": package_data.get("minimumPrice"),
                                "minimumPrice": package_data.get("minimumPrice"),
                                "packageData": package_data["packageData"],
                                "packageSummary": package_data.get("packageSummary"),
                                "thumbnailImage": package_data["thumbnailImage"],
                                "images": package_data.get("images", []),
                                "visitingCountries": package_data.get("visitingCountries", []),
                                "departureCities": package_data.get("departureCities", []),
                                "packageItinerary": {
                                    "summary": itinerary_data.get("summary", ""),
                                    "itinerary": formatted_itinerary
                                },
                                "hotels": package_data.get("hotels"),
                                "meals": package_data.get("meals"),
                                "sightseeing": package_data.get("sightseeing"),
                                "inclusions": package_data.get("inclusions"),
                                "exclusions": package_data.get("exclusions"),
                                "termsAndConditions": package_data.get("termsAndConditions"),
                                "hashKey": package_data.get("hashKey"),
                                "pkgSubtypeId": package_data["pkgSubtypeId"],
                                "pkgSubtypeName": package_data["pkgSubtypeName"],
                                "pkgTypeId": package_data["pkgTypeId"]
                            }
                        }
            logging.info(f"Package {package_id} successfully processed and prepared for indexing.")

            logging.info(f"Destination '{destination}' processing completed. Total packages found: {total_packages}")

        except Exception as e:
            logging.error(f"Error processing destination '{destination}': {str(e)}")

    # Bulk insert into Elasticsearch
    if es_actions:
        try:
            helpers.bulk(es, es_actions)
            logging.info(f"Successfully indexed {len(es_actions)} unique packages into Elasticsearch.")
        except Exception as e:
            logging.error(f"Error during Elasticsearch bulk insert: {str(e)}")
    else:
        logging.info("No packages to index.")

    # Log the total number of unique packages processed
    logging.info(f"Batch processing completed. Total unique packages processed: {len(processed_package_ids)}")

@app.post("/batch_process_monthly_packages")
async def batch_process_monthly_packages(background_tasks: BackgroundTasks):
    """
    Batch processes the SRP API for all months in a year and stores active package IDs in respective monthly indices.
    """
    try:
        # Define months and initialize a log
        months = [
            "january", "february", "march", "april", "may", "june",
            "july", "august", "september", "october", "november", "december"
        ]
        current_year = datetime.now().year

        # Add task to background processing
        background_tasks.add_task(process_monthly_packages, months, current_year)

        return {"message": "Batch processing started for monthly active packages."}
    except Exception as e:
        logging.error(f"Error starting batch processing: {str(e)}")
        raise HTTPException(status_code=500, detail="Error starting batch processing.")

async def process_monthly_packages(months: List[str], current_year: int):
    """
    Fetch package IDs for all months and store them in Elasticsearch indices.
    Handles failures and logs failed months.
    """
    logging.info("Starting monthly package processing.")
    failed_months = []
    current_month = datetime.now().month  # 1-indexed (1 for January, 12 for December)

    for month_index, month in enumerate(months):
        try:
            # Corrected logic to determine target year
            if month_index + 1 < current_month:  # If the month has already passed this year, it's for next year
                target_year = current_year + 1
            else:
                target_year = current_year

            month_number = month_index + 1  # Convert to 1-indexed format
            month_of_travel = f"{str(month_number).zfill(2)}-{target_year}"
            index_name = f"{month}_{target_year}"

            # Clear the existing index (if it exists)
            clear_index(index_name)

            # Fetch packages for the target month
            package_ids = fetch_packages_for_month(month_of_travel)
            print(f"package_ids months {month_of_travel} and id: {package_ids}")

            # Index package IDs into Elasticsearch
            bulk_actions = [
                {"_index": index_name, "_source": {"packageId": package_id}}
                for package_id in package_ids
            ]
            if bulk_actions:
                helpers.bulk(es, bulk_actions)
                logging.info(f"Indexed {len(bulk_actions)} packages for {month_of_travel} into {index_name}.")
            else:
                logging.info(f"No packages found for {month_of_travel}.")
        except Exception as e:
            logging.error(f"Error processing month {month}: {str(e)}")
            failed_months.append(month)

    if failed_months:
        logging.error(f"Failed to process the following months: {', '.join(failed_months)}")
    else:
        logging.info("All months processed successfully.")
    logging.info("Monthly package processing completed.")




def clear_index(index_name: str):
    """
    Clear all documents from a specific Elasticsearch index.
    Ensures that only month-based indices are targeted for deletion.
    """
    # Validate that the index name matches month-based patterns
    pattern = r"^(january|february|march|april|may|june|july|august|september|october|november|december)_\d{4}$"
    if not re.match(pattern, index_name, flags=re.IGNORECASE):
        logging.error(f"Attempt to clear non-monthly index: {index_name}. Operation aborted.")
        return

    try:
        # Check if the index exists
        if es.indices.exists(index=index_name):
            es.delete_by_query(index=index_name, body={"query": {"match_all": {}}})
            logging.info(f"Cleared all documents from index: {index_name}")
        else:
            logging.info(f"Index {index_name} does not exist. No action taken.")
    except Exception as e:
        logging.error(f"Error clearing index {index_name}: {str(e)}")

################################################################

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=uvicorn_host, port=uvicorn_port)