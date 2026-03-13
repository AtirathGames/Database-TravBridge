from fastapi import (
    FastAPI,
    HTTPException,
    Query,
    UploadFile,
    BackgroundTasks,
    Body,
    APIRouter,
)
from typing import Dict, List, Optional, Union, Any, Set, Tuple
from elasticsearch import Elasticsearch, RequestError, TransportError, helpers
from elasticsearch.exceptions import NotFoundError, ConflictError
import logging
import json, requests
import time
import re
from datetime import datetime
from models import (
    DayItinerary,
    PackageItinerary,
    ItineraryData,
    savedItineraryData,
    ItemOut,
    TOKEN_CACHE,
    PackageSearchRequest,
    GEOCODE_CACHE,
    AutoBudgetRequest,
    DepartureCity,
)
from constants import (
    TCIL_PACKAGE_INDEX,
    TCIL_RAW_INDEX,
    es,
    TOKEN_TTL,
    GEOCODE_TTL_SECONDS,
    geolocator,
)
from services import (
    ensure_index_exists,
    index_package_in_es,
    get_new_auth_token,
    search_item_by_package_id_internal,
    format_and_sort_months,
    get_days_tier,
    get_price_tier,
    generate_flights_availability_description,
    fetch_fare_calendar_for_package,
    RESOURCES_URL,
    _generate_image_urls,
)
from fetchpackages import fetch_package_dynamically
from fetchvisainfo import search_visa_faq
from fetchfaqinfo import get_destination_faq_internal
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from rapidfuzz import process
import asyncio
import os
from pathlib import Path


router = APIRouter()


@router.post("/v1/search_by_package_id", response_model=ItemOut)
async def search_item_by_package_id(
    body: Dict[str, Union[str, Optional[str], Optional[bool]]] = Body(
        ...,
        example={
            "packageId": "12345",
            "departureCity": "Mumbai",
            "fareCalendar": False,
        },
    )
) -> ItemOut:
    """
    Search for a package by packageId. If not found in Elasticsearch, fetch from PDP and index it.
    If departureCity is provided, append the departureCity_details for the matching city.
    If fareCalendar=true, fetch and include fare calendar data in the response.

    Args:
        body (dict): Request body containing the packageId, optional departureCity, and optional fareCalendar flag.

    Returns:
        ItemOut: The package details and its search score, with departureCity_details and fareCalendar if requested.
    """
    try:
        index = TCIL_PACKAGE_INDEX  # Default index
        generate_summary = True  # Default value for summary generation

        package_id = body.get("packageId")
        departure_city = body.get("departureCity")  # Optional field
        include_fare_calendar = body.get(
            "fareCalendar", False
        )  # Optional field, default False

        if not package_id:
            raise HTTPException(
                status_code=400, detail="packageId is required in the request body."
            )

        logging.info(
            f"Searching for package with packageId: {package_id} in index: {index}"
        )

        ensure_index_exists(index)

        search_body = {"query": {"term": {"packageId.keyword": package_id}}, "size": 1}

        response = es.search(index=index, body=search_body)
        hits = response.get("hits", {}).get("hits", [])

        # Extract just the _id values
        hit_ids = [hit["_id"] for hit in hits]
        logging.info(f"Elasticsearch hits found: {hit_ids}")

        if not hits:
            logging.info(
                f"No package found in ES for {package_id}, attempting PDP fallback."
            )
            pdp_result = fetch_package_dynamically(
                package_id, do_generate_summary=generate_summary
            )
            package_data = (
                pdp_result.get("processed") if isinstance(pdp_result, dict) else None
            )
            if not package_data:
                raise HTTPException(
                    status_code=404, detail="Package not found even in PDP source"
                )

            # Index the fetched package into Elasticsearch
            await index_package_in_es(package_data)

            pdp_itinerary = package_data.get(
                "packageItinerary", {"summary": "", "itinerary": []}
            )

            # Generate flights availability description for PDP data
            flights_availability_pdp = generate_flights_availability_description(
                pkg_subtype_name=package_data.get("pkgSubtypeName", ""),
                pkg_subtype_id=package_data.get("pkgSubtypeId", 0),
                is_flight_included=package_data.get("isFlightIncluded", "N"),
                product_id=package_data.get("productId", 0),
                holiday_plus_subtype=package_data.get("holidayPlusSubType", -1),
            )

            item_out_pdp = ItemOut(
                id=package_data["packageId"],
                itinerary_data=savedItineraryData(
                    packageId=package_data["packageId"],
                    availableMonths=package_data.get("availableMonths", []),
                    packageName=package_data["packageName"],
                    packageTheme=package_data.get("packageTheme", []),
                    days=package_data.get("days"),
                    cities=package_data.get("cities", []),
                    highlights=package_data.get("highlights", []),
                    thumbnailImage=package_data.get("thumbnailImage"),
                    images=package_data.get("images", []),
                    pdfName=package_data.get("pdfName"),
                    price=package_data.get("price"),
                    minimumPrice=package_data.get("minimumPrice"),
                    packageData=package_data.get("packageData", ""),
                    packageSummary=package_data.get("packageSummary", ""),
                    departureCities=[
                        DepartureCity(
                            cityName=city.get("cityName", ""),
                            cityCode=city.get("cityCode", ""),
                            ltItineraryCode=city.get("ltItineraryCode", ""),
                            holidayLtPricingId=city.get("holidayLtPricingId", ""),
                        )
                        for city in package_data.get("departureCities", [])
                    ],
                    packageItinerary=PackageItinerary(
                        summary=pdp_itinerary.get("summary", ""),
                        itinerary=[
                            DayItinerary(
                                day=itinerary_item.get("day", 0),
                                description=itinerary_item.get("description", ""),
                                mealDescription=itinerary_item.get(
                                    "mealDescription", ""
                                ),
                                overnightStay=itinerary_item.get("overnightStay", ""),
                            )
                            for itinerary_item in pdp_itinerary.get("itinerary", [])
                        ],
                    ),
                    hotels=package_data.get("hotels", []),
                    hotels_list=package_data.get("hotels_list"),
                    continents=package_data.get("continents", []),
                    packageTourType=package_data.get("packageTourType", []),
                    meals=package_data.get("meals", []),
                    visa=package_data.get("visa", []),
                    transfer=package_data.get("transfer", []),
                    sightseeing=package_data.get("sightseeing", []),
                    tourManagerDescription=package_data.get(
                        "tourManagerDescription", ""
                    ),
                    flightDescription=package_data.get("flightDescription", ""),
                    inclusions=package_data.get("inclusions", []),
                    exclusions=package_data.get("exclusions", []),
                    termsAndConditions=package_data.get("termsAndConditions", ""),
                    pkgSubtypeId=package_data.get("pkgSubtypeId"),
                    pkgSubtypeName=package_data.get("pkgSubtypeName", ""),
                    pkgTypeId=package_data.get("pkgTypeId"),
                    isFlightIncluded=package_data.get("isFlightIncluded"),
                    holidayPlusSubType=package_data.get("holidayPlusSubType"),
                    productId=package_data.get("productId"),
                    flightsAvailability=flights_availability_pdp,
                    constructed_thumbnailImage=(
                        f"{RESOURCES_URL}{package_data['packageId']}/{package_data.get('thumbnailImage', '')}"
                        if package_data.get("thumbnailImage")
                        else ""
                    ),
                    constructed_images=_generate_image_urls(
                        package_data["packageId"], package_data.get("images") or []
                    ),
                ),
                score=1.0,
            )

            # Fetch fare calendar if requested
            if include_fare_calendar:
                logging.info(f"Fetching fare calendar for PDP package {package_id}")
                fare_calendar_data = fetch_fare_calendar_for_package(
                    package_id=package_data["packageId"],
                    pkg_subtype_id=package_data.get("pkgSubtypeId", 0),
                    pkg_type_id=package_data.get("pkgTypeId", 1),
                    departure_cities=[
                        DepartureCity(
                            cityName=city.get("cityName", ""),
                            cityCode=city.get("cityCode", ""),
                            ltItineraryCode=city.get("ltItineraryCode", ""),
                            holidayLtPricingId=city.get("holidayLtPricingId", ""),
                        )
                        for city in package_data.get("departureCities", [])
                    ],
                    departure_city_filter=departure_city,  # Only fetch for specified city
                )
                if fare_calendar_data:
                    item_out_pdp.fareCalendar = fare_calendar_data
                    logging.info(
                        f"Fare calendar fetched successfully for PDP {package_id}"
                    )

            return item_out_pdp

        source = hits[0]["_source"]
        logging.info(f"Package found in ES: {source}")

        # Check if departureCity is provided and find matching city details
        departure_city_details = None
        if departure_city:
            departure_city_lower = departure_city.strip().lower()
            for city in source.get("departureCities", []):
                if city.get("cityName", "").lower() == departure_city_lower:
                    departure_city_details = {
                        "cityName": city.get("cityName", ""),
                        "cityCode": city.get("cityCode", ""),
                        "ltItineraryCode": city.get("ltItineraryCode", ""),
                    }
                    break

        # Add visa information if applicable
        if "visitingCountries" in source and "India" not in source["visitingCountries"]:
            visa_info_text = []
            for country in source["visitingCountries"]:
                visa_search_result = await search_visa_faq(country)
                logging.info(f"Visa search result for {country}: {visa_search_result}")
                if (
                    visa_search_result
                    and "results" in visa_search_result
                    and visa_search_result["results"]
                ):
                    result_info = visa_search_result["results"][0]
                    if "visa_info" in result_info:
                        visa_info_text.append(result_info["visa_info"])
            if visa_info_text:
                source["packageData"] += "\n\nVisa Information:\n" + "\n".join(
                    visa_info_text
                )

        es_itinerary = source.get("packageItinerary", {"summary": "", "itinerary": []})

        # Generate flights availability description
        flights_availability = generate_flights_availability_description(
            pkg_subtype_name=source.get("pkgSubtypeName", ""),
            pkg_subtype_id=source.get("pkgSubtypeId", 0),
            is_flight_included=source.get("isFlightIncluded", "N"),
            product_id=source.get("productId", 0),
            holiday_plus_subtype=source.get("holidayPlusSubType", -1),
        )

        item_out = ItemOut(
            id=hits[0]["_id"],
            itinerary_data=savedItineraryData(
                packageId=source["packageId"],
                availableMonths=source.get("availableMonths", []),
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
                packageSummary=source.get("packageSummary", ""),
                departureCities=[
                    DepartureCity(
                        cityName=city.get("cityName", ""),
                        cityCode=city.get("cityCode", ""),
                        ltItineraryCode=city.get("ltItineraryCode", ""),
                        holidayLtPricingId=city.get("holidayLtPricingId", ""),
                    )
                    for city in source.get("departureCities", [])
                ],
                packageItinerary=PackageItinerary(
                    summary=es_itinerary.get("summary", ""),
                    itinerary=[
                        DayItinerary(
                            day=day_data.get("day"),
                            description=day_data.get("description", ""),
                            mealDescription=day_data.get("mealDescription", ""),
                            overnightStay=day_data.get("overnightStay", ""),
                        )
                        for day_data in es_itinerary.get("itinerary", [])
                    ],
                ),
                hotels=source.get("hotels", []),
                hotels_list=source.get("hotels_list"),
                continents=source.get("continents", []),
                packageTourType=source.get("packageTourType", []),
                meals=source.get("meals", []),
                visa=source.get("visa", []),
                transfer=source.get("transfer", []),
                sightseeing=source.get("sightseeing", []),
                tourManagerDescription=source.get("tourManagerDescription", ""),
                flightDescription=source.get("flightDescription", ""),
                inclusions=source.get("inclusions", []),
                exclusions=source.get("exclusions", []),
                termsAndConditions=source.get("termsAndConditions", ""),
                pkgSubtypeId=source.get("pkgSubtypeId"),
                pkgSubtypeName=source.get("pkgSubtypeName", ""),
                pkgTypeId=source.get("pkgTypeId"),
                isFlightIncluded=source.get("isFlightIncluded"),
                holidayPlusSubType=source.get("holidayPlusSubType"),
                productId=source.get("productId"),
                flightsAvailability=flights_availability,
                constructed_thumbnailImage=(
                    f"{RESOURCES_URL}{source['packageId']}/{source.get('thumbnailImage', '')}"
                    if source.get("thumbnailImage")
                    else ""
                ),
                constructed_images=_generate_image_urls(
                    source["packageId"], source.get("images") or []
                ),
            ),
            score=hits[0]["_score"],
            departureCity_details=departure_city_details,  # Append departureCity_details if found
        )

        # Fetch fare calendar if requested
        if include_fare_calendar:
            logging.info(f"Fetching fare calendar for package {package_id}")
            fare_calendar_data = fetch_fare_calendar_for_package(
                package_id=source["packageId"],
                pkg_subtype_id=source.get("pkgSubtypeId", 0),
                pkg_type_id=source.get("pkgTypeId", 1),
                departure_cities=[
                    DepartureCity(
                        cityName=city.get("cityName", ""),
                        cityCode=city.get("cityCode", ""),
                        ltItineraryCode=city.get("ltItineraryCode", ""),
                        holidayLtPricingId=city.get("holidayLtPricingId", ""),
                    )
                    for city in source.get("departureCities", [])
                ],
                departure_city_filter=departure_city,  # Only fetch for specified city
            )
            if fare_calendar_data:
                item_out.fareCalendar = fare_calendar_data
                logging.info(f"Fare calendar fetched successfully for {package_id}")
            else:
                logging.warning(f"Failed to fetch fare calendar for {package_id}")

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


@router.post("/v1/search_by_package_name", response_model=List[ItemOut])
async def search_by_package_name(
    body: Dict[str, Union[str, bool]] = Body(
        ..., example={"packageName": "", "departureCity": "", "fareCalendar": False}
    )
):
    """
    Search for packages by the given text in the packageName and cities.cityName fields.
    Returns up to 6 relevant results.
    If fareCalendar=true, fetch and include fare calendar data for each package.
    Optional departureCity parameter to filter fare calendar for specific city only.
    """
    try:
        packageName = body.get("packageName")
        departure_city = body.get("departureCity")  # Optional field
        include_fare_calendar = body.get("fareCalendar", False)

        if not packageName:
            raise HTTPException(
                status_code=400, detail="packageName is required in the request body."
            )

        ensure_index_exists(TCIL_PACKAGE_INDEX)

        # Search in both packageName and cities.cityName fields
        search_body = {
            "query": {
                "bool": {
                    "should": [
                        {"match": {"packageName": packageName}},
                        {"match": {"cities.cityName": packageName}},
                    ],
                    "minimum_should_match": 1,
                }
            },
            "size": 10,
        }

        response = es.search(index=TCIL_PACKAGE_INDEX, body=search_body)
        hits = response.get("hits", {}).get("hits", [])

        results = []
        for hit in hits:
            source = hit["_source"]
            es_itinerary = source.get(
                "packageItinerary", {"summary": "", "itinerary": []}
            )

            # Generate flights availability description
            flights_availability = generate_flights_availability_description(
                pkg_subtype_name=source.get("pkgSubtypeName", ""),
                pkg_subtype_id=source.get("pkgSubtypeId", 0),
                is_flight_included=source.get("isFlightIncluded", "N"),
                product_id=source.get("productId", 0),
                holiday_plus_subtype=source.get("holidayPlusSubType", -1),
            )

            item_out = ItemOut(
                id=hit["_id"],
                itinerary_data=savedItineraryData(
                    packageId=source["packageId"],
                    availableMonths=source.get("availableMonths", []),
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
                    packageSummary=source.get("packageSummary", ""),
                    departureCities=[
                        DepartureCity(
                            cityName=city.get("cityName", ""),
                            cityCode=city.get("cityCode", ""),
                            ltItineraryCode=city.get("ltItineraryCode", ""),
                            holidayLtPricingId=city.get("holidayLtPricingId", ""),
                        )
                        for city in source.get("departureCities", [])
                    ],
                    packageItinerary=PackageItinerary(
                        summary=es_itinerary.get("summary", ""),
                        itinerary=[
                            DayItinerary(
                                day=itinerary_item.get("day", 0),
                                description=itinerary_item.get("description", ""),
                                mealDescription=itinerary_item.get(
                                    "mealDescription", ""
                                ),
                                overnightStay=itinerary_item.get("overnightStay", ""),
                            )
                            for itinerary_item in es_itinerary.get("itinerary", [])
                        ],
                    ),
                    hotels=source.get("hotels", []),
                    hotels_list=source.get("hotels_list"),
                    continents=source.get("continents", []),
                    packageTourType=source.get("packageTourType", []),
                    meals=source.get("meals", []),
                    visa=source.get("visa", []),
                    transfer=source.get("transfer", []),
                    sightseeing=source.get("sightseeing", []),
                    tourManagerDescription=source.get("tourManagerDescription", ""),
                    flightDescription=source.get("flightDescription", ""),
                    inclusions=source.get("inclusions", []),
                    exclusions=source.get("exclusions", []),
                    termsAndConditions=source.get("termsAndConditions", ""),
                    pkgSubtypeId=source.get("pkgSubtypeId"),
                    pkgSubtypeName=source.get("pkgSubtypeName", ""),
                    pkgTypeId=source.get("pkgTypeId"),
                    isFlightIncluded=source.get("isFlightIncluded"),
                    holidayPlusSubType=source.get("holidayPlusSubType"),
                    productId=source.get("productId"),
                    flightsAvailability=flights_availability,
                    constructed_thumbnailImage=(
                        f"{RESOURCES_URL}{source['packageId']}/{source.get('thumbnailImage', '')}"
                        if source.get("thumbnailImage")
                        else ""
                    ),
                    constructed_images=_generate_image_urls(
                        source["packageId"], source.get("images") or []
                    ),
                ),
                score=hit["_score"],
            )

            # Fetch fare calendar if requested
            if include_fare_calendar:
                logging.info(
                    f"Fetching fare calendar for package {source['packageId']}"
                )
                fare_calendar_data = fetch_fare_calendar_for_package(
                    package_id=source["packageId"],
                    pkg_subtype_id=source.get("pkgSubtypeId", 0),
                    pkg_type_id=source.get("pkgTypeId", 1),
                    departure_cities=[
                        DepartureCity(
                            cityName=city.get("cityName", ""),
                            cityCode=city.get("cityCode", ""),
                            ltItineraryCode=city.get("ltItineraryCode", ""),
                            holidayLtPricingId=city.get("holidayLtPricingId", ""),
                        )
                        for city in source.get("departureCities", [])
                    ],
                    departure_city_filter=departure_city,  # Only fetch for specified city
                )
                if fare_calendar_data:
                    item_out.fareCalendar = fare_calendar_data

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


@router.post("/v1/get_raw_response")
async def get_raw_response(
    body: Dict[str, str] = Body(..., example={"packageId": "12345"})
):
    """
    Retrieve the raw PDP API response (with HTML tags preserved) for a given packageId
    from the tcildatav1_raw index.

    Args:
        body (dict): Request body containing the packageId.

    Returns:
        JSON with packageId, fetchedAt, and the full raw_response object.
    """
    try:
        package_id = body.get("packageId")
        if not package_id:
            raise HTTPException(
                status_code=400, detail="packageId is required in the request body."
            )

        logging.info(
            f"[get_raw_response] Fetching raw response for packageId: {package_id}"
        )

        search_body = {"query": {"term": {"packageId": package_id}}, "size": 1}

        response = es.search(index=TCIL_RAW_INDEX, body=search_body)
        hits = response.get("hits", {}).get("hits", [])

        if not hits:
            raise HTTPException(
                status_code=404,
                detail=f"No raw response found for packageId: {package_id}",
            )

        source = hits[0]["_source"]
        logging.info(
            f"[get_raw_response] Found raw response for packageId: {package_id}"
        )

        return JSONResponse(
            status_code=200,
            content={
                "code": 200,
                "packageId": source.get("packageId"),
                "fetchedAt": source.get("fetchedAt"),
                "raw_response": source.get("raw_response"),
            },
        )

    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"[get_raw_response] Internal Server Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.get("/v1/get_all_package_themes")
async def get_all_package_themes():
    """
    Returns all unique package themes from the index with their document counts.
    Uses Elasticsearch aggregation for efficient retrieval.
    """
    try:
        ensure_index_exists(TCIL_PACKAGE_INDEX)

        # Aggregation query to get all unique themes with counts
        search_body = {
            "size": 0,
            "aggs": {
                "unique_themes": {
                    "terms": {
                        "field": "packageTheme.keyword",
                        "size": 1000,  # Get all themes
                    }
                }
            },
        }

        response = es.search(index=TCIL_PACKAGE_INDEX, body=search_body)

        # Extract themes and counts from aggregation
        buckets = (
            response.get("aggregations", {}).get("unique_themes", {}).get("buckets", [])
        )

        themes_with_counts = [
            {"theme": bucket["key"], "packageCount": bucket["doc_count"]}
            for bucket in buckets
        ]

        # Sort by package count (descending)
        themes_with_counts.sort(key=lambda x: x["packageCount"], reverse=True)

        logging.info(
            f"[get_all_package_themes] Found {len(themes_with_counts)} unique themes"
        )

        return JSONResponse(
            status_code=200,
            content={
                "code": 200,
                "message": f"Found {len(themes_with_counts)} unique package themes",
                "themes": themes_with_counts,
                "totalThemes": len(themes_with_counts),
            },
        )

    except RequestError as e:
        logging.error(f"Request error: {str(e)}")
        raise HTTPException(status_code=400, detail="Request error")
    except TransportError as e:
        logging.error(f"Elasticsearch transport error: {str(e)}")
        raise HTTPException(status_code=500, detail="Elasticsearch transport error")
    except Exception as e:
        logging.error(f"Internal Server Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.post("/v1/get_countries_by_theme")
async def get_countries_by_theme(
    body: Dict[str, str] = Body(..., example={"packageTheme": "Family"})
):
    """
    Returns all countries that have packages matching the specified theme.
    Uses Elasticsearch aggregation on visitingCountries for packages with the given theme.
    """
    try:
        package_theme = body.get("packageTheme")

        if not package_theme:
            raise HTTPException(
                status_code=400, detail="packageTheme is required in the request body."
            )

        ensure_index_exists(TCIL_PACKAGE_INDEX)

        # Query to filter by theme and aggregate countries
        search_body = {
            "size": 0,
            "query": {"term": {"packageTheme.keyword": package_theme}},
            "aggs": {
                "unique_countries": {
                    "terms": {
                        "field": "visitingCountries.keyword",
                        "size": 1000,  # Get all countries
                    }
                }
            },
        }

        response = es.search(index=TCIL_PACKAGE_INDEX, body=search_body)

        # Extract countries and counts from aggregation
        buckets = (
            response.get("aggregations", {})
            .get("unique_countries", {})
            .get("buckets", [])
        )

        countries_with_counts = [
            {"country": bucket["key"], "packageCount": bucket["doc_count"]}
            for bucket in buckets
        ]

        # Sort by package count (descending)
        countries_with_counts.sort(key=lambda x: x["packageCount"], reverse=True)

        total_packages = response.get("hits", {}).get("total", {}).get("value", 0)

        logging.info(
            f"[get_countries_by_theme] Found {len(countries_with_counts)} countries "
            f"for theme '{package_theme}' across {total_packages} packages"
        )

        return JSONResponse(
            status_code=200,
            content={
                "code": 200,
                "message": f"Found {len(countries_with_counts)} countries for theme '{package_theme}'",
                "theme": package_theme,
                "countries": countries_with_counts,
                "totalCountries": len(countries_with_counts),
                "totalPackages": total_packages,
            },
        )

    except RequestError as e:
        logging.error(f"Request error: {str(e)}")
        raise HTTPException(status_code=400, detail="Request error")
    except TransportError as e:
        logging.error(f"Elasticsearch transport error: {str(e)}")
        raise HTTPException(status_code=500, detail="Elasticsearch transport error")
    except Exception as e:
        logging.error(f"Internal Server Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.get("/v1/get_all_packages")
async def get_all_packages():
    """
    Retrieve ALL packages from the index with minimal details.
    
    Returns for each package:
    - packageId
    - packageName
    - availableMonths
    - packageTheme
    - aliasCityName (extracted from cities array)
    - days
    - isFlightIncluded
    - pkgSubtypeName
    
    Uses Elasticsearch scroll for efficient retrieval of large result sets.
    """
    try:
        ensure_index_exists(TCIL_PACKAGE_INDEX)

        # Initial search with scroll to get all packages
        search_body = {
            "query": {"match_all": {}},
            "size": 500,  # batch size per scroll page
            "_source": [
                "packageId",
                "packageName",
                "availableMonths",
                "packageTheme",
                "cities",
                "days",
                "isFlightIncluded",
                "pkgSubtypeName",
            ],
            "track_total_hits": True,
        }

        results = []

        # Initial search with scroll
        response = es.search(index=TCIL_PACKAGE_INDEX, body=search_body, scroll="2m")

        scroll_id = response.get("_scroll_id")
        hits = response.get("hits", {}).get("hits", [])
        total_packages = response.get("hits", {}).get("total", {}).get("value", 0)

        logging.info(
            f"[get_all_packages] Starting scroll retrieval. Total packages: {total_packages}"
        )

        def extract_alias_city_name(cities: List[Dict]) -> str:
            """Extract aliasCityName from cities array. Returns first city's alias or empty string."""
            if cities and len(cities) > 0:
                return cities[0].get("aliasCityName", "")
            return ""

        def format_package_minimal(hit) -> Dict[str, Any]:
            """Format a package document into minimal details."""
            source = hit["_source"]
            cities = source.get("cities", [])

            return {
                "packageId": source.get("packageId"),
                "packageName": source.get("packageName"),
                "availableMonths": source.get("availableMonths", []),
                "packageTheme": source.get("packageTheme", []),
                "aliasCityName": extract_alias_city_name(cities),
                "days": source.get("days"),
                "isFlightIncluded": source.get("isFlightIncluded", "N"),
                "pkgSubtypeName": source.get("pkgSubtypeName", ""),
            }

        # Process first page
        for hit in hits:
            results.append(format_package_minimal(hit))

        # Scroll through remaining pages
        while True:
            if not hits:
                break
            response = es.scroll(scroll_id=scroll_id, scroll="2m")
            scroll_id = response.get("_scroll_id")
            hits = response.get("hits", {}).get("hits", [])
            if not hits:
                break
            for hit in hits:
                results.append(format_package_minimal(hit))

        # Clear scroll context (best-effort)
        try:
            if scroll_id:
                es.clear_scroll(scroll_id=scroll_id)
        except Exception as e:
            logging.warning(f"[get_all_packages] Could not clear scroll: {str(e)}")

        logging.info(
            f"[get_all_packages] Successfully retrieved {len(results)} packages"
        )

        return JSONResponse(
            status_code=200,
            content={
                "code": 200,
                "message": f"Retrieved {len(results)} packages",
                "total": len(results),
                "packages": results,
            },
        )

    except RequestError as e:
        logging.error(f"[get_all_packages] Request error: {str(e)}")
        raise HTTPException(status_code=400, detail="Request error")
    except TransportError as e:
        logging.error(f"[get_all_packages] Elasticsearch transport error: {str(e)}")
        raise HTTPException(status_code=500, detail="Elasticsearch transport error")
    except Exception as e:
        logging.error(f"[get_all_packages] Internal Server Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.get("/v1/get_all_BOGO_packages", response_model=List[ItemOut])
async def get_all_bogo_packages():
    """
    Retrieve ALL packages that have the exact phrase "Buy 1 Get 1 Free" in the packageName.
    Uses Elasticsearch scroll to return the full result set.
    """
    try:
        ensure_index_exists(TCIL_PACKAGE_INDEX)

        # Exact phrase match in packageName (case-insensitive by default analyzer)
        search_body = {
            "query": {"match_phrase": {"packageName": "Buy 1 Get 1 Free"}},
            "size": 500,  # batch size per scroll page
            "track_total_hits": True,
        }

        results: List[ItemOut] = []

        # Initial search with scroll
        response = es.search(index=TCIL_PACKAGE_INDEX, body=search_body, scroll="2m")

        scroll_id = response.get("_scroll_id")
        hits = response.get("hits", {}).get("hits", [])

        def to_item_out(hit) -> ItemOut:
            source = hit["_source"]

            # Generate flights availability description
            flights_availability = generate_flights_availability_description(
                pkg_subtype_name=source.get("pkgSubtypeName", ""),
                pkg_subtype_id=source.get("pkgSubtypeId", 0),
                is_flight_included=source.get("isFlightIncluded", "N"),
                product_id=source.get("productId", 0),
                holiday_plus_subtype=source.get("holidayPlusSubType", -1),
            )

            return ItemOut(
                id=hit["_id"],
                itinerary_data=savedItineraryData(
                    packageId=source["packageId"],
                    availableMonths=source["availableMonths"],
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
                    departureCities=[
                        DepartureCity(
                            cityName=city.get("cityName", ""),
                            cityCode=city.get("cityCode", ""),
                            ltItineraryCode=city.get("ltItineraryCode", ""),
                            holidayLtPricingId=city.get("holidayLtPricingId", ""),
                        )
                        for city in source.get("departureCities", [])
                    ],
                    packageItinerary=PackageItinerary(
                        summary=source.get("packageItinerary", {}).get("summary", ""),
                        itinerary=[
                            DayItinerary(
                                day=itinerary_item.get("day", 0),
                                description=itinerary_item.get("description", ""),
                                mealDescription=itinerary_item.get(
                                    "mealDescription", ""
                                ),
                                overnightStay=itinerary_item.get("overnightStay", ""),
                            )
                            for itinerary_item in source.get(
                                "packageItinerary", {}
                            ).get("itinerary", [])
                        ],
                    ),
                    hotels=source.get("hotels"),
                    hotels_list=source.get("hotels_list"),
                    continents=source.get("continents"),
                    packageTourType=source.get("packageTourType", []),
                    meals=source.get("meals"),
                    visa=source.get("visa"),
                    transfer=source.get("transfer"),
                    sightseeing=source.get("sightseeing"),
                    inclusions=source.get("inclusions"),
                    exclusions=source.get("exclusions"),
                    termsAndConditions=source.get("termsAndConditions"),
                    pkgSubtypeId=source.get("pkgSubtypeId"),
                    pkgSubtypeName=source.get("pkgSubtypeName", ""),
                    pkgTypeId=source.get("pkgTypeId"),
                    isFlightIncluded=source.get("isFlightIncluded"),
                    holidayPlusSubType=source.get("holidayPlusSubType"),
                    productId=source.get("productId"),
                    flightsAvailability=flights_availability,
                    constructed_thumbnailImage=(
                        f"{RESOURCES_URL}{source['packageId']}/{source.get('thumbnailImage', '')}"
                        if source.get("thumbnailImage")
                        else ""
                    ),
                    constructed_images=_generate_image_urls(
                        source["packageId"], source.get("images") or []
                    ),
                ),
                score=hit.get("_score", 0.0),
            )

        # Collect first page
        for hit in hits:
            results.append(to_item_out(hit))

        # Scroll through the rest
        while True:
            if not hits:
                break
            response = es.scroll(scroll_id=scroll_id, scroll="2m")
            scroll_id = response.get("_scroll_id")
            hits = response.get("hits", {}).get("hits", [])
            if not hits:
                break
            for hit in hits:
                results.append(to_item_out(hit))

        # Clear scroll context (best-effort)
        try:
            if scroll_id:
                es.clear_scroll(scroll_id=scroll_id)
        except Exception:
            pass

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


def get_cached_auth_token():
    """Return requestId, sessionId if still valid, else None."""
    now = time.time()
    if (
        TOKEN_CACHE["requestId"]
        and TOKEN_CACHE["sessionId"]
        and now < TOKEN_CACHE["expires_at"]
    ):
        return TOKEN_CACHE["requestId"], TOKEN_CACHE["sessionId"]
    return None, None


def store_auth_token(request_id: str, session_id: str):
    """Store the token in memory with an expiration time."""
    TOKEN_CACHE["requestId"] = request_id
    TOKEN_CACHE["sessionId"] = session_id
    TOKEN_CACHE["expires_at"] = time.time() + TOKEN_TTL


async def fetch_autosuggest_results(
    search_term: str, short_timeout: float = 10.0
) -> List[dict]:
    """
    Helper function to fetch AutoSuggest results with a short timeout
    and in-memory token caching to reduce overhead.
    """
    # 1) Check if we have a valid token in cache
    req_id, sess_id = get_cached_auth_token()
    if not req_id or not sess_id:
        # Retrieve new token
        try:
            fresh_req_id, fresh_sess_id = get_new_auth_token()
            store_auth_token(fresh_req_id, fresh_sess_id)
            req_id, sess_id = fresh_req_id, fresh_sess_id
        except Exception as e:
            logging.error(
                f"[fetch_autosuggest_results] Could not retrieve new token: {str(e)}"
            )
            raise HTTPException(
                status_code=500, detail="Failed to retrieve new token for AutoSuggest"
            )

    # 2) Make the call
    url = "https://services.thomascook.in/tcHolidayRS/autosuggest"
    params = {"searchAutoSuggest": search_term}
    headers = {"Requestid": req_id, "Sessionid": sess_id}

    try:
        logging.info(
            f"[fetch_autosuggest_results] Calling AutoSuggest with searchTerm='{search_term}'"
        )
        resp = requests.get(url, headers=headers, params=params, timeout=short_timeout)
        resp.raise_for_status()
        data = resp.json()

        if not isinstance(data, list):
            logging.error(
                "[fetch_autosuggest_results] Invalid response format (not a list)."
            )
            return []

        return data

    except requests.exceptions.RequestException as e:
        # This includes timeouts, connection errors, HTTP errors
        logging.error(
            f"[fetch_autosuggest_results] Error calling AutoSuggest: {str(e)}"
        )
        return []


@router.post("/v1/livepackagesv1")
async def get_packages(request: PackageSearchRequest) -> JSONResponse:
    start_time = datetime.now()
    logging.info(
        f"[livepackages] Received request at {start_time.isoformat()}: {request.dict()}"
    )

    search_term = request.search_term.strip().lower()
    departure_city = (request.departureCity or "").strip().lower()
    target_days = request.days if request.days > 0 else None
    target_budget = request.budget if request.budget > 0 else None
    month_of_travel = (request.monthOfTravel or "").strip().lower()
    theme = (request.theme or "").strip().lower()
    people_count = request.number_of_people if request.number_of_people > 0 else None

    try:
        autosuggest_results = await fetch_autosuggest_results(
            search_term, short_timeout=10.0
        )

        filtered_packages = [
            item
            for item in autosuggest_results
            if search_term
            in [
                (item.get("cityName") or "").lower().strip(),
                (item.get("countryName") or "").lower().strip(),
                (item.get("stateName") or "").lower().strip(),
                (item.get("continentName") or "").lower().strip(),
            ]
        ]

        package_ids = {
            pkg["packageId"]
            for item in filtered_packages
            for pkg in item.get("pkgnameIdMappingList", [])
        }
        logging.info(f"[livepackages] package_ids from AutoSuggest => {package_ids}")

        if not package_ids:
            return JSONResponse(
                status_code=404,
                content={
                    "code": 404,
                    "message": f"No matching packages found for '{search_term}'.",
                    "body": [],
                },
            )

        ensure_index_exists(TCIL_PACKAGE_INDEX)

        # ✅ Use asyncio.gather for fast parallel ES fetches
        tasks = [
            search_item_by_package_id_internal(
                {"packageId": pid}, generate_summary=True
            )
            for pid in package_ids
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        detailed_packages = [r for r in results if not isinstance(r, Exception)]

        all_available_months = set()
        for pkg_data in detailed_packages:
            if hasattr(pkg_data.itinerary_data, "availableMonths"):
                all_available_months.update(pkg_data.itinerary_data.availableMonths)

        if not detailed_packages:
            return JSONResponse(
                status_code=404,
                content={
                    "code": 404,
                    "message": f"No packages found for '{search_term}'.",
                    "body": [],
                },
            )

        # Filter by month_of_travel
        if month_of_travel:
            month_filtered = [
                pkg
                for pkg in detailed_packages
                if any(
                    month_of_travel == m.split("_")[0].lower()
                    or month_of_travel == m.lower()
                    for m in pkg.itinerary_data.availableMonths
                )
            ]
            if not month_filtered:
                formatted_months = format_and_sort_months(all_available_months)
                nice_month = month_of_travel.capitalize().replace("_", "-")
                return JSONResponse(
                    status_code=401,
                    content={
                        "code": 401,
                        "message": (
                            f"No active packages available for '{search_term}' in {nice_month}, "
                            f"but available in {', '.join(formatted_months)}"
                        ),
                        "body": [],
                    },
                )
            detailed_packages = month_filtered

        # People count filtering (Honeymoon exclusion)
        if people_count and people_count > 2:
            detailed_packages = [
                pkg
                for pkg in detailed_packages
                if not (
                    pkg.itinerary_data.packageTheme
                    and any(
                        "honeymoon" == t.lower()
                        for t in pkg.itinerary_data.packageTheme
                    )
                )
            ]

        # GIT > FIT ordering
        git_packages = [
            p for p in detailed_packages if p.itinerary_data.pkgSubtypeName == "GIT"
        ]
        fit_packages = [
            p for p in detailed_packages if p.itinerary_data.pkgSubtypeName == "FIT"
        ]
        combined_packages = git_packages + fit_packages

        if not combined_packages:
            return JSONResponse(
                status_code=404,
                content={
                    "code": 404,
                    "message": "No matching packages found.",
                    "body": [],
                },
            )

        # Apply filters
        final_list, matched_user_budget = apply_filters(
            packages=combined_packages,
            departure_city=departure_city,
            target_budget=target_budget,
            theme=theme,
            target_days=target_days,
        )

        if isinstance(final_list, JSONResponse):
            return final_list

        if not matched_user_budget:
            return JSONResponse(
                status_code=200,
                content={
                    "code": 200,
                    "message": "There are no packages matching the mentioned budget, but here are some packages from other budget ranges.",
                    "body": jsonable_encoder(final_list),
                },
            )

        if not final_list:
            all_departure_cities = {
                dep.cityName.lower()
                for pkg in combined_packages
                for dep in pkg.itinerary_data.departureCities
            }
            return JSONResponse(
                status_code=402,
                content={
                    "code": 402,
                    "message": f"No packages found from your base city '{departure_city}'. Available departure cities: {', '.join(sorted(all_departure_cities))}",
                    "body": [],
                },
            )

        elapsed = (datetime.now() - start_time).total_seconds()
        logging.info(
            f"[livepackages] Found {len(final_list)} packages. Total time: {elapsed}s"
        )

        return JSONResponse(
            status_code=200,
            content={
                "code": 200,
                "message": "Here are the available packages matching your travel details.",
                "body": jsonable_encoder(final_list),
            },
        )

    except Exception as e:
        logging.error(f"[livepackages] Internal Server Error => {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"code": 500, "message": "Internal server error", "body": []},
        )


@router.post("/v1/livepackages")
async def get_packages_v2(request: PackageSearchRequest) -> JSONResponse:
    """
    Handles live package queries and returns results based on the search criteria.
    Includes logic for handling AutoSuggest results, Elasticsearch fallback, and various
    filtering conditions (month, city, budget).
    """
    start_time = datetime.now()
    logging.info(
        f"[livepackagesv1] Request received: {request.dict()} at {start_time.isoformat()}"
    )

    # Parse and sanitize input parameters
    search_term_raw = request.search_term.strip()
    search_term = search_term_raw.lower()
    departure_city = (request.departureCity or "").strip().lower()
    target_days = request.days if request.days > 0 else None
    target_budget = request.budget if request.budget > 0 else None
    month_of_travel = (request.monthOfTravel or "").strip().lower()
    theme = (request.theme or "").strip().lower()
    people_count = request.number_of_people if request.number_of_people > 0 else None
    include_fare_calendar = request.fareCalendar if request.fareCalendar else False
    pkg_subtype_filter = (request.pkgSubtypeName or "").strip().upper()  # GIT or FIT

    fallback_used = False  # Indicates if the fallback logic is used
    all_available_months = set()  # Collect available months for messaging
    city_fail = False
    month_fail = False

    try:
        # Attempt to find packages using AutoSuggest API
        autosuggest_results = await fetch_autosuggest_results(search_term)
        filtered_packages = [
            item
            for item in autosuggest_results
            if search_term
            in [
                (item.get("cityName") or "").lower().strip(),
                (item.get("countryName") or "").lower().strip(),
                (item.get("stateName") or "").lower().strip(),
                (item.get("continentName") or "").lower().strip(),
            ]
        ]
        package_ids = {
            pkg["packageId"]
            for item in filtered_packages
            for pkg in item.get("pkgnameIdMappingList", [])
        }

        # Fallback to Elasticsearch if AutoSuggest does not return any results
        if not package_ids:
            fallback_used = True
            logging.info(
                "[livepackagesv1] No packages from AutoSuggest, using Elasticsearch fallback."
            )

            # Split by common delimiters (comma, 'and', 'or') to extract individual destinations
            destinations = re.split(r"[,\s]+(?:and|or)\s+|,\s*", search_term)
            destinations = [d.strip() for d in destinations if d.strip()]
            logging.info(
                f"[livepackagesv1] Extracted destinations for fallback: {destinations}"
            )

            # Build a query that searches for any of the destinations
            should_clauses = []
            for dest in destinations:
                # Clean each destination term
                cleaned_dest = re.sub(r"[^\w\s]", "", dest).strip()
                if cleaned_dest:
                    should_clauses.extend(
                        [
                            {"match_phrase": {"packageName": cleaned_dest}},
                            {"match": {"cities.cityName": cleaned_dest}},
                            {"match_phrase": {"packageSummary": cleaned_dest}},
                        ]
                    )

            if not should_clauses:
                # Fallback to original behavior if no valid destinations extracted
                cleaned_term = re.sub(r"\b(and|or)\b|[^\w\s]", "", search_term).strip()
                cleaned_term = re.sub(r"\s+", " ", cleaned_term)
                should_clauses = [
                    {"match_phrase": {"packageName": cleaned_term}},
                    {"match": {"cities.cityName": cleaned_term}},
                    {"match_phrase": {"packageSummary": cleaned_term}},
                ]

            fallback_body = {
                "query": {
                    "bool": {
                        "should": should_clauses,
                        "minimum_should_match": 1,
                    }
                },
                "size": 50,  # Increased to accommodate multiple destinations
            }

            ensure_index_exists(TCIL_PACKAGE_INDEX)
            try:
                es_result = es.search(index=TCIL_PACKAGE_INDEX, body=fallback_body)
                package_ids = {hit["_id"] for hit in es_result["hits"]["hits"]}
                logging.info(
                    f"[livepackagesv1] Fallback found {len(package_ids)} package IDs"
                )
            except Exception as es_error:
                logging.warning(
                    f"[livepackagesv1] Elasticsearch fallback failed: {es_error}. Retrying without nested cities query."
                )
                # Fallback without cities nested query if it fails
                simple_should = []
                for dest in destinations:
                    cleaned_dest = re.sub(r"[^\w\s]", "", dest).strip()
                    if cleaned_dest:
                        simple_should.extend(
                            [
                                {"match_phrase": {"packageName": cleaned_dest}},
                                {"match_phrase": {"packageSummary": cleaned_dest}},
                            ]
                        )

                if not simple_should:
                    cleaned_term = re.sub(
                        r"\b(and|or)\b|[^\w\s]", "", search_term
                    ).strip()
                    cleaned_term = re.sub(r"\s+", " ", cleaned_term)
                    simple_should = [
                        {"match_phrase": {"packageName": cleaned_term}},
                        {"match_phrase": {"packageSummary": cleaned_term}},
                    ]

                simple_fallback = {
                    "query": {
                        "bool": {
                            "should": simple_should,
                            "minimum_should_match": 1,
                        }
                    },
                    "size": 50,
                }
                es_result = es.search(index=TCIL_PACKAGE_INDEX, body=simple_fallback)
                package_ids = {hit["_id"] for hit in es_result["hits"]["hits"]}
                logging.info(
                    f"[livepackagesv1] Simple fallback found {len(package_ids)} package IDs"
                )

        # Return 404 if no packages found after both AutoSuggest and fallback
        if not package_ids:
            return JSONResponse(
                status_code=404,
                content={
                    "code": 404,
                    "message": f"No packages found for '{search_term_raw}'.",
                    "body": [],
                },
            )

        # Fetch detailed package data from Elasticsearch or other sources
        ensure_index_exists(TCIL_PACKAGE_INDEX)
        tasks = [
            search_item_by_package_id_internal(
                {"packageId": pid}, generate_summary=True
            )
            for pid in package_ids
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        detailed_packages = [r for r in results if not isinstance(r, Exception)]

        # Collect available months from the fetched packages
        for pkg in detailed_packages:
            if pkg.itinerary_data.availableMonths:
                all_available_months.update(pkg.itinerary_data.availableMonths)

        # Filter packages based on the number of people (exclude honeymoon packages for groups)
        if people_count and people_count > 2:
            detailed_packages = [
                p
                for p in detailed_packages
                if "honeymoon" not in [t.lower() for t in p.itinerary_data.packageTheme]
            ]

        # Filter packages by pkgSubtypeName (GIT or FIT)
        if pkg_subtype_filter and pkg_subtype_filter in ["GIT", "FIT"]:
            subtype_filtered = [
                p
                for p in detailed_packages
                if p.itinerary_data.pkgSubtypeName
                and p.itinerary_data.pkgSubtypeName.upper() == pkg_subtype_filter
            ]
            if subtype_filtered:
                detailed_packages = subtype_filtered
                logging.info(
                    f"[livepackagesv1] Filtered to {len(detailed_packages)} {pkg_subtype_filter} packages"
                )
            else:
                logging.warning(
                    f"[livepackagesv1] No {pkg_subtype_filter} packages found, showing all package types"
                )

        # Filter packages by theme (optional — if no match, skip the filter and return all)
        if theme:
            theme_filtered = [
                p
                for p in detailed_packages
                if any(t.lower() == theme for t in p.itinerary_data.packageTheme)
            ]
            if theme_filtered:
                detailed_packages = theme_filtered
                logging.info(
                    f"[livepackagesv1] Filtered to {len(detailed_packages)} packages matching theme '{theme}'"
                )
            else:
                # Theme not matched — log a warning but continue with all packages
                all_available_themes = sorted(
                    {
                        t
                        for pkg in detailed_packages
                        if pkg.itinerary_data.packageTheme
                        for t in pkg.itinerary_data.packageTheme
                    }
                )
                logging.warning(
                    f"[livepackagesv1] No packages found for theme '{theme}'. "
                    f"Ignoring theme filter. Available themes: {all_available_themes}"
                )

        # Store unfiltered results before applying month/city filters
        unfiltered_packages = detailed_packages[:]

        # Apply month filter
        if month_of_travel:
            month_filtered = [
                pkg
                for pkg in detailed_packages
                if any(
                    m.startswith(month_of_travel)
                    for m in (pkg.itinerary_data.availableMonths or [])
                )
            ]
            if not month_filtered:
                month_fail = True
            else:
                detailed_packages = month_filtered

        # Apply city filter with fuzzy matching
        if departure_city:
            all_departure_cities = {
                city.cityName.lower()
                for pkg in detailed_packages
                for city in pkg.itinerary_data.departureCities
            }
            closest_city = get_closest_match(
                departure_city, list(all_departure_cities), threshold=80
            )
            if not closest_city:
                city_fail = True
            else:
                departure_city = closest_city

        # Handle scenarios where month and/or city filters fail
        if month_fail and city_fail:
            final_candidates = unfiltered_packages
            # Apply pkgSubtypeName filter to unfiltered packages
            if pkg_subtype_filter and pkg_subtype_filter in ["GIT", "FIT"]:
                subtype_filtered = [
                    p
                    for p in final_candidates
                    if p.itinerary_data.pkgSubtypeName
                    and p.itinerary_data.pkgSubtypeName.upper() == pkg_subtype_filter
                ]
                if subtype_filtered:
                    final_candidates = subtype_filtered
            final_list, matched_user_budget = apply_filters(
                final_candidates, None, target_budget, theme, target_days
            )
            final_list = prioritize_bogo(final_list)[:15]
            code_val = 202 if not fallback_used else 212
            all_departure_cities = {
                city.cityName.lower()
                for pkg in unfiltered_packages
                for city in pkg.itinerary_data.departureCities
            }
            formatted_months = format_and_sort_months(all_available_months)
            return JSONResponse(
                status_code=200,
                content={
                    "code": code_val,
                    "message": f"No matching packages found for the specified month and departure city. Available months: **{', '.join(formatted_months)}**. Available cities: **{', '.join(sorted(all_departure_cities))}**.",
                    "body": jsonable_encoder(final_list),
                },
            )

        if month_fail:
            final_candidates_month = unfiltered_packages
            # Apply pkgSubtypeName filter
            if pkg_subtype_filter and pkg_subtype_filter in ["GIT", "FIT"]:
                subtype_filtered = [
                    p
                    for p in final_candidates_month
                    if p.itinerary_data.pkgSubtypeName
                    and p.itinerary_data.pkgSubtypeName.upper() == pkg_subtype_filter
                ]
                if subtype_filtered:
                    final_candidates_month = subtype_filtered
            final_list, matched_user_budget = apply_filters(
                final_candidates_month,
                departure_city,
                target_budget,
                theme,
                target_days,
            )
            final_list = prioritize_bogo(final_list)[:15]
            code_val = 203 if not fallback_used else 213
            formatted_months = format_and_sort_months(all_available_months)
            return JSONResponse(
                status_code=200,
                content={
                    "code": code_val,
                    "message": f"Here are the Packages.**No packages available for {month_of_travel}**, but available months are **{', '.join(formatted_months)}**.",
                    "body": jsonable_encoder(final_list),
                },
            )

        if city_fail:
            final_candidates_city = unfiltered_packages
            # Apply pkgSubtypeName filter
            if pkg_subtype_filter and pkg_subtype_filter in ["GIT", "FIT"]:
                subtype_filtered = [
                    p
                    for p in final_candidates_city
                    if p.itinerary_data.pkgSubtypeName
                    and p.itinerary_data.pkgSubtypeName.upper() == pkg_subtype_filter
                ]
                if subtype_filtered:
                    final_candidates_city = subtype_filtered
            final_list, matched_user_budget = apply_filters(
                final_candidates_city, None, target_budget, theme, target_days
            )
            final_list = prioritize_bogo(final_list)[:15]
            code_val = 204 if not fallback_used else 214
            all_departure_cities = {
                city.cityName.lower()
                for pkg in final_candidates_city
                for city in pkg.itinerary_data.departureCities
            }
            return JSONResponse(
                status_code=200,
                content={
                    "code": code_val,
                    "message": f"Here are the Packages.**No packages found for the specified departure city**. Available cities: **{', '.join(sorted(all_departure_cities))}**.",
                    "body": jsonable_encoder(final_list),
                },
            )

        # Apply budget filters and finalize response
        final_list, matched_user_budget = apply_filters(
            detailed_packages, departure_city, target_budget, theme, target_days
        )
        final_list = prioritize_bogo(final_list)[:15]

        # Fetch fare calendar if requested
        if include_fare_calendar:
            logging.info(
                f"[livepackages] Fetching fare calendar for {len(final_list)} packages"
            )

            # Get auth token once and reuse for all fare calendar calls
            from services import get_new_auth_token

            try:
                request_id, session_id = get_new_auth_token()
                logging.info(
                    f"[livepackages] Using shared auth token (requestId: {request_id[:10]}...) for {len(final_list)} packages"
                )
            except Exception as e:
                logging.error(
                    f"[livepackages] Failed to get auth token for fare calendar: {str(e)}"
                )
                request_id, session_id = None, None

            for pkg in final_list:
                try:
                    fare_calendar_data = fetch_fare_calendar_for_package(
                        package_id=pkg.itinerary_data.packageId,
                        pkg_subtype_id=pkg.itinerary_data.pkgSubtypeId or 0,
                        pkg_type_id=pkg.itinerary_data.pkgTypeId or 1,
                        departure_cities=pkg.itinerary_data.departureCities,
                        request_id=request_id,
                        session_id=session_id,
                        departure_city_filter=departure_city,  # Only fetch for specified city
                    )
                    if fare_calendar_data:
                        pkg.fareCalendar = fare_calendar_data
                except Exception as e:
                    logging.error(
                        f"[livepackages] Error fetching fare calendar for {pkg.itinerary_data.packageId}: {str(e)}"
                    )

        if not matched_user_budget:
            code_val = 201 if not fallback_used else 211
            return JSONResponse(
                status_code=200,
                content={
                    "code": code_val,
                    "message": (
                        "Here are the available packages.\n\n"
                        "**Note:** None of the packages exactly match your budget, "
                        "but we’ve included options across various price ranges for your consideration."
                    ),
                    "body": jsonable_encoder(final_list),
                },
            )

        code_val = 200 if not fallback_used else 210
        return JSONResponse(
            status_code=200,
            content={
                "code": code_val,
                "message": "Here are the Packages found matching your criteria.",
                "body": jsonable_encoder(final_list),
            },
        )

    except Exception as e:
        logging.error(f"[livepackagesv1] Internal Server Error: {e}")
        return JSONResponse(
            status_code=500,
            content={"code": 500, "message": "Internal server error", "body": []},
        )


def prioritize_bogo(packages: List[ItemOut]) -> List[ItemOut]:
    """
    Put packages with 'Buy 1 Get 1 Free' in the packageName at the front (case-insensitive),
    while preserving relative order within BOGO and non-BOGO groups.
    """

    def is_bogo(p: ItemOut) -> bool:
        name = (p.itinerary_data.packageName or "").lower()
        return "buy 1 get 1 free" in name

    bogo = [p for p in packages if is_bogo(p)]
    others = [p for p in packages if not is_bogo(p)]
    return bogo + others


def get_closest_match(input_city, available_cities, threshold=80):
    """
    Find the closest match to `input_city` from the list of `available_cities`
    using fuzzy matching.
    """
    input_city = input_city.lower().strip()
    available_cities = [c.lower().strip() for c in available_cities]

    result = process.extractOne(input_city, available_cities)
    if result:
        match, score, *_ = result  # Fix: Safely unpack values, ignoring extra ones

        if score >= threshold:  # If similarity score is above threshold (default 80)
            return match
    return None


def apply_filters(
    packages: List[Any],
    departure_city: Optional[str],
    target_budget: Optional[int],
    theme: Optional[str],  # unused, kept for compatibility
    target_days: Optional[int],
) -> Union[Tuple[List[Any], bool], JSONResponse]:
    """
    1) Fuzzy match the user’s departure_city among the packages’ departureCities.
    2) If found, filter to only those packages. If not found, return 402 + available cities.
    3) Apply tier-based sorting (S-Tier, D-Tier, price diff, days diff).
    4) Return (sorted_packages, matched_user_budget).
    """

    all_departure_cities = {
        city.cityName.lower()
        for pkg in packages
        for city in getattr(pkg.itinerary_data, "departureCities", [])
    }

    # (A) Fuzzy match departure city
    if departure_city and departure_city.strip():
        closest_city = get_closest_match(
            departure_city, list(all_departure_cities), threshold=80
        )
        if not closest_city:
            logging.warning(
                f"[apply_filters] No fuzzy match for city '{departure_city}' among {all_departure_cities}"
            )
            return JSONResponse(
                status_code=402,
                content={
                    "code": 402,
                    "message": (
                        f"No packages found from your base city **{departure_city}**. "
                        f"Available departure cities: **{', '.join(sorted(all_departure_cities))}**"
                    ),
                    "body": [],
                },
            )
        departure_city = closest_city
        logging.info(f"[apply_filters] Fuzzy matched departure city '{departure_city}'")

        relevant_packages = [
            pkg
            for pkg in packages
            if departure_city
            in [dc.cityName.lower() for dc in pkg.itinerary_data.departureCities]
        ]
    else:
        relevant_packages = packages

    # (B) Apply tiered budget & day sorting
    return filter_by_budget(
        target_budget=target_budget,
        detailed_packages=relevant_packages,
        requested_days=target_days,
    )


def filter_by_budget(
    target_budget: Optional[int],
    detailed_packages: List[Any],
    requested_days: Optional[int] = None,
) -> Tuple[List[Any], bool]:
    """
    1) Assign each package to S-Tier & D-Tier.
    2) Sort in order: S1[D1] → S1[D2] → S1[D3] → ...
    3) Logs tier-wise and SD-wise breakdowns.
    4) Returns (final_sorted, matched_user_budget).
    """

    if not detailed_packages:
        logging.warning("[filter_by_budget] No packages to filter.")
        return [], False

    annotated = []
    s_tier_counts = {1: 0, 2: 0, 3: 0}
    d_tier_counts = {1: 0, 2: 0, 3: 0}
    sd_tier_map = {}  # Key: 'S1D1', Value: list of package IDs

    logging.info(
        f"[filter_by_budget] Starting tier assignment for {len(detailed_packages)} packages"
    )

    for pkg in detailed_packages:
        p_price = pkg.itinerary_data.price or 0
        p_days = pkg.itinerary_data.days or 0
        package_id = pkg.itinerary_data.packageId

        s_tier = get_price_tier(p_price, target_budget)
        d_tier = get_days_tier(p_days, requested_days)
        price_diff = abs(p_price - (target_budget or 0))
        days_diff = abs(p_days - (requested_days or 0))

        # Count S and D tiers
        s_tier_counts[s_tier] += 1
        d_tier_counts[d_tier] += 1

        # Count SD tier
        sd_key = f"S{s_tier}D{d_tier}"
        if sd_key not in sd_tier_map:
            sd_tier_map[sd_key] = []
        sd_tier_map[sd_key].append(package_id)

        # Log individual package classification
        logging.info(
            f"[filter_by_budget] Package={package_id} | Price={p_price}, Days={p_days}, "
            f"S-Tier=S{s_tier}, D-Tier=D{d_tier}, PriceDiff={price_diff}, DaysDiff={days_diff}"
        )

        annotated.append(
            {
                "pkg": pkg,
                "S": s_tier,
                "D": d_tier,
                "price_diff": price_diff,
                "days_diff": days_diff,
            }
        )

    # Sorting logic: S, then D, then price diff, then days diff
    annotated.sort(key=lambda x: (x["S"], x["D"], x["price_diff"], x["days_diff"]))

    final_sorted = [x["pkg"] for x in annotated]
    matched_user_budget = any(x["S"] == 1 for x in annotated)

    # Tier count summaries
    logging.info("[filter_by_budget] Tier Summary:")
    logging.info(f"  - S1: {s_tier_counts[1]} packages")
    logging.info(f"  - S2: {s_tier_counts[2]} packages")
    logging.info(f"  - S3: {s_tier_counts[3]} packages")
    logging.info(f"  - D1: {d_tier_counts[1]} packages")
    logging.info(f"  - D2: {d_tier_counts[2]} packages")
    logging.info(f"  - D3: {d_tier_counts[3]} packages")

    # SD tier breakdown
    logging.info("[filter_by_budget] S-D Breakdown:")
    for sd_key, pkg_ids in sorted(sd_tier_map.items()):
        logging.info(f"  - {sd_key}: {len(pkg_ids)} package(s) => {pkg_ids}")

    # Final output
    logging.info(
        f"[filter_by_budget] Sorted {len(final_sorted)} packages. "
        f"MatchedUserBudget: {'YES' if matched_user_budget else 'NO'}"
    )

    return final_sorted, matched_user_budget


def is_geocode_cache_valid(search_term: str) -> bool:
    """
    Check if we have a valid geocode result in the cache for the given term.
    """
    if search_term not in GEOCODE_CACHE:
        return False
    cached_country, cached_ts = GEOCODE_CACHE[search_term]
    # If older than TTL, invalid
    return (time.time() - cached_ts) < GEOCODE_TTL_SECONDS


def store_geocode_in_cache(search_term: str, country_name: str):
    """Store the geocoded country in the in-memory cache with a timestamp."""
    GEOCODE_CACHE[search_term] = (country_name, time.time())


def load_destination_faq_from_json(destination_name: str) -> Dict[str, Any]:
    """
    Generic function to load FAQ data from JSON files for a given destination.

    Args:
        destination_name: Name of the destination (e.g., 'thailand', 'singapore')

    Returns:
        Dictionary containing FAQ data, or empty dict if file not found

    Usage:
        To add a new destination:
        1. Create a JSON file: data/{destination_name}_faq.json
        2. The function will automatically load it when called

    Example file structure:
        data/thailand_faq.json
        data/singapore_faq.json
        data/dubai_faq.json
    """
    try:
        # Get the directory where this script is located
        current_dir = Path(__file__).parent

        # Construct the path to the FAQ JSON file
        faq_file_path = current_dir / "data" / f"{destination_name.lower()}_faq.json"

        # Check if file exists
        if not faq_file_path.exists():
            logging.info(
                f"[load_destination_faq_from_json] No FAQ file found for '{destination_name}' at {faq_file_path}"
            )
            return {}

        # Load and return the JSON data
        with open(faq_file_path, "r", encoding="utf-8") as f:
            faq_data = json.load(f)
            logging.info(
                f"[load_destination_faq_from_json] Successfully loaded FAQ data for '{destination_name}' from {faq_file_path}"
            )
            return faq_data

    except json.JSONDecodeError as e:
        logging.error(
            f"[load_destination_faq_from_json] Invalid JSON format in FAQ file for '{destination_name}': {str(e)}"
        )
        return {}
    except Exception as e:
        logging.error(
            f"[load_destination_faq_from_json] Error loading FAQ file for '{destination_name}': {str(e)}"
        )
        return {}


def merge_faq_data(es_faq: Dict[str, Any], json_faq: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge FAQ data from Elasticsearch and JSON file.
    JSON data takes precedence over ES data for duplicate keys.

    Args:
        es_faq: FAQ data from Elasticsearch
        json_faq: FAQ data from JSON file

    Returns:
        Merged FAQ dictionary
    """
    if not es_faq and not json_faq:
        return {}

    # Start with ES data, then update with JSON data (JSON takes precedence)
    merged = dict(es_faq) if es_faq else {}
    if json_faq:
        merged.update(json_faq)
        logging.info(
            f"[merge_faq_data] Merged {len(json_faq)} FAQ entries from JSON with {len(es_faq or {})} from ES"
        )

    return merged


@router.post("/v1/get_destination_details")
async def get_destination_details(request: AutoBudgetRequest):
    """
    Fetches package IDs from the AutoSuggest API, retrieves package details from Elasticsearch (no PDP fallback),
    provides budget ranges, and fetches visa information if required.

    Production-readiness improvements:
      1) Short-lived cache for geocoding results (Nominatim) to reduce rate-limit issues.
      2) Return HTTP 400 if we cannot find any location for the search_term,
         so user knows to refine the query or correct spelling.
      3) Clearer logic if no packages found in ES or from AutoSuggest.
    """
    start_time = datetime.now()
    search_term = request.search_term.strip().lower()
    logging.info(f"[get_destination_details] Starting. searchTerm='{search_term}'")

    visa_information = []  # List of visa info strings to be returned
    country_name = None

    try:
        # ---------------------------
        # 1. Attempt to get country via Geopy (with caching)
        # ---------------------------
        if is_geocode_cache_valid(search_term):
            cached_country, _ = GEOCODE_CACHE[search_term]
            country_name = cached_country
            logging.info(
                f"[get_destination_details] Found '{search_term}' in geocode cache => '{country_name}'"
            )
        else:
            # Not in cache or cache expired => call Nominatim
            try:
                location = geolocator.geocode(
                    search_term, exactly_one=True, language="en"
                )
                logging.debug(
                    f"[get_destination_details] Geopy raw output for '{search_term}': {location}"
                )

                if location:
                    # Nominatim returns a string like "Paris, Île-de-France, France"
                    address_parts = location.address.split(",")
                    country_name = address_parts[-1].strip() if address_parts else None
                    logging.info(
                        f"[get_destination_details] Geocoded '{search_term}' => country='{country_name}'"
                    )

                    # Store in cache
                    if country_name:
                        store_geocode_in_cache(search_term, country_name)
                else:
                    logging.warning(
                        f"[get_destination_details] Nominatim returned no location for '{search_term}'"
                    )

            except GeocoderTimedOut:
                logging.error("[get_destination_details] Nominatim request timed out.")
                country_name = None

        # If we STILL have no country, consider returning HTTP 400 or 404
        if not country_name:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Unable to determine a country for '{search_term}'. "
                    "Please refine your query or try a more specific location."
                ),
            )

        # ---------------------------
        # 2. If country != 'india', fetch Visa info
        # ---------------------------
        if country_name.lower() != "india":
            logging.info(
                f"[get_destination_details] Attempting to fetch visa info for {country_name}..."
            )
            visa_search_result = await search_visa_faq(country_name)
            logging.debug(
                f"[get_destination_details] Visa search result for {country_name}: {visa_search_result}"
            )

            if visa_search_result and "results" in visa_search_result:
                # Collect all 'visa_info' fields if present
                for each_res in visa_search_result["results"]:
                    visa_text = each_res.get("visa_info", "")
                    if visa_text:
                        visa_information.append(visa_text)

        # ---------------------------
        # 3. Call AutoSuggest to get package suggestions
        # ---------------------------
        try:
            request_id, session_id = get_new_auth_token()
        except Exception as e:
            logging.error(f"[get_destination_details] Error retrieving token: {str(e)}")
            raise HTTPException(
                status_code=500, detail="Failed to retrieve token for AutoSuggest"
            )

        url = "https://services.thomascook.in/tcHolidayRS/autosuggest"
        params = {"searchAutoSuggest": search_term}
        headers = {"Requestid": request_id, "Sessionid": session_id}

        logging.info(f"[get_destination_details] Calling AutoSuggest with {params}")
        try:
            auto_resp = requests.get(
                url, params=params, headers=headers, timeout=15
            )  # Shorter timeout
            auto_resp.raise_for_status()
            autosuggest_data = auto_resp.json()
        except requests.exceptions.RequestException as e:
            logging.error(f"[get_destination_details] AutoSuggest error => {str(e)}")
            raise HTTPException(
                status_code=500,
                detail="Error retrieving suggestions from the AutoSuggest API. Try again later.",
            )

        if not isinstance(autosuggest_data, list):
            logging.error(
                f"[get_destination_details] Invalid response format from AutoSuggest. Got: {autosuggest_data}"
            )
            raise HTTPException(
                status_code=500, detail="Invalid response from AutoSuggest API."
            )

        logging.info(
            f"[get_destination_details] AutoSuggest returned {len(autosuggest_data)} items for '{search_term}'."
        )

        # ---------------------------
        # 4. Filter results by city/country/state/continent
        # ---------------------------
        filtered_results = [
            item
            for item in autosuggest_data
            if search_term
            in [
                (item.get("cityName") or "").lower().strip(),
                (item.get("countryName") or "").lower().strip(),
                (item.get("stateName") or "").lower().strip(),
                (item.get("continentName") or "").lower().strip(),
            ]
        ]
        logging.info(
            f"[get_destination_details] Filtered count => {len(filtered_results)}"
        )

        # ---------------------------
        # 5. Extract package IDs from those results
        # ---------------------------
        package_ids = {
            pkg["packageId"]
            for item in filtered_results
            for pkg in item.get("pkgnameIdMappingList", [])
        }
        logging.info(f"[get_destination_details] Extracted packageIds => {package_ids}")

        if not package_ids:
            # Return an HTTP 200 or 404 – your choice.
            # Currently we do a 200 with an empty body.
            return JSONResponse(
                status_code=200,
                content={
                    "message": "No matching packages found.",
                    "visa_information": visa_information,
                    "faq": {},
                },
            )

        # ---------------------------
        # 6. Fetch package details from Elasticsearch (No PDP fallback)
        # ---------------------------
        detailed_packages = []
        for pkg_id in package_ids:
            try:
                # Query ES directly; skip if not found
                # Use match query instead of term to handle analyzed fields
                search_body = {"query": {"match": {"packageId": pkg_id}}, "size": 1}
                es_resp = es.search(index=TCIL_PACKAGE_INDEX, body=search_body)
                hits = es_resp.get("hits", {}).get("hits", [])
                # Log only IDs so we don't dump entire ES response
                hit_ids = [h["_id"] for h in hits]
                logging.info(
                    f"[get_destination_details] For pkgId={pkg_id}, found hits => {hit_ids}"
                )

                if not hits:
                    logging.warning(
                        f"[get_destination_details] pkgId={pkg_id} not found in ES. Skipping."
                    )
                    continue

                source = hits[0]["_source"]
                doc_id = hits[0]["_id"]

                # Minimal object that mimics your existing ItemOut
                item_price = source.get("price") or source.get("minimumPrice")

                item_obj = ItemOut(
                    id=doc_id,
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
                        departureCities=[
                            DepartureCity(
                                cityName=city.get("cityName", ""),
                                cityCode=city.get("cityCode", ""),
                                ltItineraryCode=city.get("ltItineraryCode", ""),
                                holidayLtPricingId=city.get("holidayLtPricingId", ""),
                            )
                            for city in source.get("departureCities", [])
                        ],
                        packageItinerary=PackageItinerary(
                            summary=source.get("packageItinerary", {}).get(
                                "summary", ""
                            ),
                            itinerary=[],
                        ),
                        hotels=source.get("hotels"),
                        packageTourType=source.get("packageTourType", []),
                        meals=source.get("meals"),
                        visa=source.get("visa"),
                        transfer=source.get("transfer"),
                        sightseeing=source.get("sightseeing"),
                        inclusions=source.get("inclusions"),
                        exclusions=source.get("exclusions"),
                        termsAndConditions=source.get("termsAndConditions"),
                        pkgSubtypeId=source.get("pkgSubtypeId"),
                        pkgSubtypeName=source.get("pkgSubtypeName", ""),
                        pkgTypeId=source.get("pkgTypeId"),
                        isFlightIncluded=source.get("isFlightIncluded"),
                        holidayPlusSubType=source.get("holidayPlusSubType"),
                        productId=source.get("productId"),
                    ),
                    score=hits[0].get("_score", 1.0),
                )
                detailed_packages.append(item_obj)

            except Exception as e:
                logging.error(
                    f"[get_destination_details] Error retrieving pkg={pkg_id} from ES => {str(e)}"
                )
                continue

        # ---------------------------
        # 7. Fetch FAQ data from Elasticsearch and JSON files
        # ---------------------------
        es_faq_data = {}
        json_faq_data = {}

        # Fetch from Elasticsearch
        try:
            # Use the search_term and country_name for lookup
            # Try search_term first, then country_name
            es_faq_data = await get_destination_faq_internal(search_term)

            if not es_faq_data and country_name:
                es_faq_data = await get_destination_faq_internal(country_name)

            if es_faq_data:
                logging.info(f"[get_destination_details] Found FAQ data from ES")
            else:
                logging.info(
                    f"[get_destination_details] No FAQ data found in ES for '{search_term}' or '{country_name}'"
                )

        except Exception as e:
            logging.error(
                f"[get_destination_details] Error fetching FAQ from ES: {str(e)}"
            )

        # Fetch from JSON files (try both search_term and country_name)
        json_faq_data = load_destination_faq_from_json(search_term)
        if not json_faq_data and country_name and country_name.lower() != search_term:
            json_faq_data = load_destination_faq_from_json(country_name)

        # Merge FAQ data (JSON takes precedence)
        faq_data = merge_faq_data(es_faq_data, json_faq_data)

        if faq_data:
            logging.info(
                f"[get_destination_details] Final FAQ data contains {len(faq_data)} entries"
            )
        else:
            logging.info(
                f"[get_destination_details] No FAQ data available from any source"
            )

        # ---------------------------
        # 8. Return final results
        # ---------------------------
        elapsed_time = (datetime.now() - start_time).total_seconds()
        logging.info(f"[get_destination_details] Completed in {elapsed_time}s")

        return {
            "search_term": request.search_term,
            "country_detected": country_name,
            "visa_information": visa_information,
            "destination_details": faq_data,
        }

    except HTTPException:
        # If we intentionally raise an HTTPException above, let it propagate.
        raise
    except Exception as e:
        logging.error(f"[get_destination_details] Unexpected error => {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error: see logs.")


from fastapi import Body, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
import asyncio
import logging
import re
from difflib import SequenceMatcher

# --- Reuse existing helpers/clients/constants from your codebase ---
# - fetch_autosuggest_results
# - search_item_by_package_id_internal

# ---------- Pydantic Schemas ----------


class CombinedSearchRequest(BaseModel):
    search_term: str = Field(
        ..., description="Destination (city/state/country/continent)"
    )
    packageName: Optional[str] = Field(
        None, description="Package name (fuzzy matched within result set)"
    )
    month_of_travel: Optional[str] = Field(
        None, description="Month of travel (e.g., 'jan', 'january')"
    )


class MinimalPackageOut(BaseModel):
    packageName: str
    packageId: str
    availableMonths: List[str] = []
    inclusions: Optional[Any] = None
    exclusions: Optional[Any] = None
    sightseeing: Optional[Any] = None


# ---------- Local helpers ----------

_ws_re = re.compile(r"\s+", re.U)


def _norm(s: str) -> str:
    return _ws_re.sub(" ", (s or "").strip().lower())


def _tokenize(s: str) -> List[str]:
    return [t for t in re.split(r"[^a-z0-9]+", _norm(s)) if t]


def _seq_ratio(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio() * 100.0


def _partial_ratio(a: str, b: str) -> float:
    # Compare the shorter string against all windows of the longer string
    a, b = _norm(a), _norm(b)
    if not a or not b:
        return 0.0
    short, long = (a, b) if len(a) <= len(b) else (b, a)
    if short in long:
        return 100.0
    # quick windowed scan on token boundaries
    long_tokens = _tokenize(long)
    if not long_tokens:
        return _seq_ratio(short, long)
    # build windows roughly matching the length (in chars) of short
    target_len = max(1, len(short))
    best = 0.0
    acc = ""
    left = 0
    for right in range(len(long_tokens)):
        if acc:
            acc += " "
        acc += long_tokens[right]
        while len(acc) > target_len * 1.4 and left <= right:  # shrink if window too big
            # remove left token
            cut = acc.find(" ")
            acc = "" if cut == -1 else acc[cut + 1 :]
            left += 1
        best = max(best, _seq_ratio(short, acc))
    return best


def _token_set_ratio(a: str, b: str) -> float:
    # Similar to fuzzywuzzy's token_set_ratio
    ta, tb = set(_tokenize(a)), set(_tokenize(b))
    if not ta or not tb:
        return _seq_ratio(_norm(a), _norm(b))
    inter = ta & tb
    if not inter:
        return _seq_ratio(_norm(a), _norm(b))
    # Build strings from intersection and unique parts and compare
    inter_s = " ".join(sorted(inter))
    a_rem = " ".join(sorted(ta - inter))
    b_rem = " ".join(sorted(tb - inter))
    score_inter = _seq_ratio(inter_s, inter_s)  # 100 by construction
    score_a = _seq_ratio(inter_s, inter_s + (" " + a_rem if a_rem else ""))
    score_b = _seq_ratio(inter_s, inter_s + (" " + b_rem if b_rem else ""))
    return max(score_inter, score_a, score_b)


def _name_matches(pkg_name: str, query: str, threshold: float = 70.0) -> bool:
    # Fast paths
    pn = _norm(pkg_name)
    qn = _norm(query)
    if not qn:
        return True
    if qn in pn:
        return True
    # word-prefix (e.g., "anant" matches "anantara")
    if any(
        w.startswith(qn) or qn.startswith(w)
        for w in _tokenize(pn)
        for q in [qn]
        for wq in [q]
    ):
        pass  # handled by token logic, keep going

    # compute a composite score
    scores = [
        _seq_ratio(pn, qn),
        _partial_ratio(pn, qn),
        _token_set_ratio(pn, qn),
    ]
    best = max(scores)
    logging.debug(
        f"[search_combined] fuzzy scores for '{pkg_name}' vs '{query}': {scores} (best={best:.1f})"
    )
    # also consider token overlap directly
    ta, tb = set(_tokenize(pkg_name)), set(_tokenize(query))
    jacc = (len(ta & tb) / max(1, len(tb))) * 100.0
    best = max(best, jacc)

    return best >= threshold


def _month_matches(months: List[str], month_query: str) -> bool:
    if not month_query:
        return True
    mq = _norm(month_query)
    return any(_norm(m).startswith(mq) for m in (months or []))


# ---------- Endpoint ----------


@router.post(
    "/v1/search_and_validate_package_details", response_model=List[MinimalPackageOut]
)
async def search_combined(body: CombinedSearchRequest = Body(...)):
    """
    Combined search:
    1) Initial search via AutoSuggest using `search_term` (destination).
    2) Optional month filter using availableMonths prefix match (like /v1/livepackages).
    3) Fuzzy match by `packageName` within the set from (1)/(2) using partial+token match.
    4) Return only {packageName, packageId, availableMonths, inclusions, exclusions, sightseeing}.
    """
    start_time = datetime.now()
    logging.info(
        f"[search_combined] Request received: {body.dict()} at {start_time.isoformat()}"
    )

    search_term_raw = body.search_term or ""
    search_term = _norm(search_term_raw)
    if not search_term:
        raise HTTPException(status_code=400, detail="`search_term` is required.")

    month_query = body.month_of_travel
    pkgname_query = body.packageName

    try:
        # 1) AutoSuggest flow
        autosuggest_results = await fetch_autosuggest_results(search_term)

        filtered_dest_matches = [
            item
            for item in autosuggest_results
            if search_term
            in {
                _norm(item.get("cityName")),
                _norm(item.get("countryName")),
                _norm(item.get("stateName")),
                _norm(item.get("continentName")),
            }
        ]

        package_ids = {
            pkg.get("packageId")
            for item in filtered_dest_matches
            for pkg in (item.get("pkgnameIdMappingList") or [])
            if pkg.get("packageId")
        }

        if not package_ids:
            return []

        # 2) Get details
        tasks = [
            search_item_by_package_id_internal(
                {"packageId": pid}, generate_summary=False
            )
            for pid in package_ids
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        detailed_packages = [
            r for r in results if not isinstance(r, Exception) and r is not None
        ]

        # 3) Month filter (prefix)
        if month_query:
            detailed_packages = [
                p
                for p in detailed_packages
                if _month_matches(p.itinerary_data.availableMonths or [], month_query)
            ]
            if not detailed_packages:
                return []

        # 4) Fuzzy packageName filter (now robust to short queries like "Anantara")
        if pkgname_query:
            # Lower threshold slightly to allow brand-word matches; tune as needed
            threshold = 68.0
            pq = pkgname_query
            detailed_packages = [
                p
                for p in detailed_packages
                if _name_matches(
                    p.itinerary_data.packageName or "", pq, threshold=threshold
                )
            ]
            # If still empty, try a final graceful fallback: strict substring on tokens
            if not detailed_packages:
                qtokens = set(_tokenize(pq))
                detailed_packages = [
                    p
                    for p in results
                    if not isinstance(p, Exception)
                    and p is not None
                    and (qtokens & set(_tokenize(p.itinerary_data.packageName or "")))
                ]

        # 5) Shape minimal response
        final_list: List[MinimalPackageOut] = []
        for p in detailed_packages:
            data = p.itinerary_data
            final_list.append(
                MinimalPackageOut(
                    packageName=data.packageName,
                    packageId=str(data.packageId),
                    availableMonths=list(data.availableMonths or []),
                    inclusions=data.inclusions,
                    exclusions=data.exclusions,
                    sightseeing=data.sightseeing,
                )
            )

        final_list.sort(key=lambda x: _norm(x.packageName))
        return final_list

    except Exception as e:
        logging.error(f"[search_combined] Internal Server Error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
