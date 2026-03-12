from fastapi import FastAPI, HTTPException, Query, UploadFile, BackgroundTasks
from pydantic import BaseModel, Field
from typing import Dict, List, Optional, Union
from elasticsearch import Elasticsearch, RequestError, TransportError, helpers
from elasticsearch.exceptions import NotFoundError, ConflictError
import logging
import json, requests
import traceback
import os
from geopy.geocoders import Nominatim
from datetime import datetime
from pdp_api import fetch_package_dynamically
import asyncio

app = FastAPI()

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

indexNameForPackages = "tcildatav6"
INDEX_NAME = "visa_faq"

# Pydantic models
class GeoLocation(BaseModel):
    lat: float
    lon: float

class PackageItinerary(BaseModel):
    summary: str
    itinerary: List[str]

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
    packageItinerary: PackageItinerary

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
    packageItinerary: PackageItinerary

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
    days: Optional[int] = 0
    budget: Optional[int] = 0

# Visa FAQ Index Mapping
visa_faq_mapping = {
    "mappings": {
        "properties": {
            "country_id": {"type": "integer"},
            "visa_info": {"type": "text"}
        }
    }
}

def ensure_index_exists(index_name: str):
    if not es.indices.exists(index=index_name):
        from NewESmapping import index_mapping
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

@app.post("/createVisaFAQ")
async def create_visa_faq():
    try:
        create_visa_faq_index()
        with open('/home/jayanth/thomascook-travelplanner/Elastic Search/data/visa_data.json', 'r') as f:
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
                        "visa_info": query
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
        "packageItinerary": package_data["packageItinerary"],
        # Include hashKey if present
        "hashKey": package_data.get("hashKey")
    }
    es.index(index=indexNameForPackages, id=doc_id, document=document)
    logging.info(f"Indexed package {doc_id} into {indexNameForPackages}")

@app.get("/v1/embeddings/search_by_package_id/{package_id}", response_model=ItemOut)
async def search_item_by_package_id(package_id: str, index: str, generate_summary: bool = True) -> ItemOut:
    try:
        logging.info(f"Searching for package with packageId: {package_id} in index: {index}")

        ensure_index_exists(indexNameForPackages)

        search_body = {
            "query": {
                "term": {
                    "packageId": package_id
                }
            },
            "size": 1
        }

        response = es.search(index=indexNameForPackages, body=search_body)
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
                    packageSummary=package_data.get("packageSummary"),
                    packageItinerary=PackageItinerary(
                        summary=pdp_itinerary.get("summary", ""),
                        itinerary=pdp_itinerary.get("itinerary", [])
                    )
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
                packageSummary=source.get("packageSummary"),
                packageItinerary=PackageItinerary(
                    summary=es_itinerary.get("summary", ""),
                    itinerary=es_itinerary.get("itinerary", [])
                )
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
async def get_packages(request: PackageSearchRequest) -> List[ItemOut]:
    start_time = datetime.now()
    logging.info(f"Request received for /v1/livepackages at {start_time.isoformat()}")

    search_term = request.search_term
    target_days = request.days
    target_budget = request.budget

    request_id, session_id = get_new_auth_token()

    url = "https://services.thomascook.in/tcHolidayRS/autosuggest"
    params = {"searchAutoSuggest": search_term}
    headers = {
        "Requestid": request_id,
        "Sessionid": session_id,
    }

    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
    except requests.exceptions.HTTPError as http_err:
        raise HTTPException(status_code=response.status_code, detail=f"HTTP error occurred: {http_err}")
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"Other error occurred: {err}")

    try:
        data = response.json()
    except ValueError as json_err:
        raise HTTPException(status_code=500, detail=f"Error parsing JSON: {json_err}")

    search_term_lower = search_term.lower()
    matching_packages = []
    # For exact matches only, not partial:
    for item in data:
        if (search_term_lower == item.get("cityName", "").lower() or
            search_term_lower == item.get("stateName", "").lower() or
            search_term_lower == item.get("countryName", "").lower() or
            search_term_lower == item.get("searchString", "").lower() or
            any(search_term_lower == pkg["packageName"].lower() for pkg in item.get("pkgnameIdMappingList", []))):
            matching_packages.append(item)

    if not matching_packages:
        return {"message": "No matching packages found for the search term."}

    seen_packages = set()
    filtered_packages = []
    for item in matching_packages:
        for pkg in item.get("pkgnameIdMappingList", []):
            if pkg["packageName"] not in seen_packages:
                seen_packages.add(pkg["packageName"])
                filtered_packages.append({
                    "isDynamicPackage": "N",
                    "packageId": pkg["packageId"],
                    "packageName": pkg["packageName"],
                    "productId": pkg.get("productId", 11)
                })

    multiple_packages = len(filtered_packages) > 1
    generate_summary = multiple_packages

    ensure_index_exists(indexNameForPackages)

    coroutines = [
        search_item_by_package_id(pkg["packageId"], index=indexNameForPackages, generate_summary=generate_summary)
        for pkg in filtered_packages
    ]

    results = await asyncio.gather(*coroutines, return_exceptions=True)
    detailed_packages = []
    for pkg, result in zip(filtered_packages, results):
        if isinstance(result, Exception):
            logging.error(f"Error retrieving package details for {pkg['packageId']}: {str(result)}")
            continue
        detailed_packages.append(result)

    # Define weights for days and budget
    days_weight = 0.3
    budget_weight = 0.7

    if target_days > 0 or target_budget > 0:
        detailed_packages = sorted(detailed_packages, key=lambda x: (
            days_weight * abs((x.itinerary_data.days or 0) - target_days) if target_days else 0,
            budget_weight * abs((x.itinerary_data.price or 0) - target_budget) if target_budget else 0
        ))

    end_time = datetime.now()
    elapsed_time = (end_time - start_time).total_seconds()
    logging.info(f"Request received time: {start_time.isoformat()}")
    logging.info(f"Response sent back time: {end_time.isoformat()}")
    logging.info(f"Total elapsed time: {elapsed_time} seconds")

    return detailed_packages


# Batch processing endpoint
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
    Logs the progress, including the number of packages found and their processing status.
    """
    logging.info(f"Starting batch process for destinations: {destinations}")

    # List to store actions for bulk insertion into Elasticsearch
    es_actions = []

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
                    logging.info(f"Processing package {idx}/{len(pkgname_mapping)} for destination '{destination}' (Package ID: {package_id})")

                    package_data = fetch_package_dynamically(package_id)

                    if package_data:
                        # Create an Elasticsearch bulk action
                        es_actions.append({
                            "_index": indexNameForPackages,
                            "_id": package_data["packageId"],
                            "_source": {
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
                                "packageItinerary": package_data["packageItinerary"],
                                "hashKey": package_data["hashKey"],
                            }
                        })
                        logging.info(f"Package {package_id} successfully processed and prepared for indexing.")

            logging.info(f"Destination '{destination}' processing completed. Total packages found: {total_packages}")

        except Exception as e:
            logging.error(f"Error processing destination '{destination}': {str(e)}")

    # Bulk insert into Elasticsearch
    if es_actions:
        try:
            helpers.bulk(es, es_actions)
            logging.info("Successfully indexed all packages into Elasticsearch.")
        except Exception as e:
            logging.error(f"Error during Elasticsearch bulk insert: {str(e)}")
    else:
        logging.info("No packages to index.")

    logging.info("Batch processing completed.")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=uvicorn_host, port=uvicorn_port)
