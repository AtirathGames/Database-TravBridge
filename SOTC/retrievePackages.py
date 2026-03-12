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
    SOTC_PACKAGE_INDEX,
    SOTC_RAW_INDEX,
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
)
from fetchpackages import fetch_package_dynamically
from fetchvisainfo import search_visa_faq
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from rapidfuzz import process
import asyncio
from fetchpackages import session as api_session, COMMON_HEADERS

router = APIRouter()


@router.post("/sotc/SOTC_search_by_package_id", response_model=ItemOut)
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
        index = SOTC_PACKAGE_INDEX  # Default index
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
                    meals=package_data.get("meals", []),
                    visa=package_data.get("visa", []),
                    transfer=package_data.get("transfer", []),
                    sightseeing=package_data.get("sightseeing", []),
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
                meals=source.get("meals", []),
                visa=source.get("visa", []),
                transfer=source.get("transfer", []),
                sightseeing=source.get("sightseeing", []),
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


@router.post("/sotc/SOTC_search_by_package_name", response_model=List[ItemOut])
async def search_by_package_name(
    body: Dict[str, Union[str, Optional[str], Optional[bool]]] = Body(
        ...,
        example={
            "packageName": "Amazing Goa",
            "departureCity": "Mumbai",
            "fareCalendar": False,
        },
    )
):
    """
    Search for packages by the given text in the packageName field.
    Returns up to 6 relevant results.
    If fareCalendar=true, fetch and include fare calendar data for each package.
    Optional departureCity parameter to filter fare calendar for specific city only.
    """
    try:
        packageName = body.get("packageName")
        departure_city = body.get("departureCity")  # Optional field
        include_fare_calendar = body.get(
            "fareCalendar", False
        )  # Optional field, default False

        if not packageName:
            raise HTTPException(
                status_code=400, detail="packageName is required in the request body."
            )

        ensure_index_exists(SOTC_PACKAGE_INDEX)

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

        response = es.search(index=SOTC_PACKAGE_INDEX, body=search_body)
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
                    logging.info(f"Fare calendar fetched for {source['packageId']}")

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


@router.post("/sotc/SOTC_get_raw_response")
async def get_raw_response(
    body: Dict[str, str] = Body(..., example={"packageId": "12345"})
):
    """
    Retrieve the raw PDP API response (with HTML tags preserved) for a given packageId
    from the sotcdatav1_raw index.

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
            f"[SOTC_get_raw_response] Fetching raw response for packageId: {package_id}"
        )

        search_body = {"query": {"term": {"packageId": package_id}}, "size": 1}

        response = es.search(index=SOTC_RAW_INDEX, body=search_body)
        hits = response.get("hits", {}).get("hits", [])

        if not hits:
            raise HTTPException(
                status_code=404,
                detail=f"No raw response found for packageId: {package_id}",
            )

        source = hits[0]["_source"]
        logging.info(
            f"[SOTC_get_raw_response] Found raw response for packageId: {package_id}"
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
        logging.error(f"[SOTC_get_raw_response] Internal Server Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@router.get("/sotc/SOTC_get_all_BOGO_packages", response_model=List[ItemOut])
async def get_all_bogo_packages():
    """
    Retrieve ALL packages that have the exact phrase "Buy 1 Get 1 Free" in the packageName.
    Uses Elasticsearch scroll to return the full result set.
    """
    try:
        ensure_index_exists(SOTC_PACKAGE_INDEX)

        # Exact phrase match in packageName (case-insensitive by default analyzer)
        search_body = {
            "query": {"match_phrase": {"packageName": "Buy 1 Get 1 Free"}},
            "size": 500,  # batch size per scroll page
            "track_total_hits": True,
        }

        results: List[ItemOut] = []

        # Initial search with scroll
        response = es.search(index=SOTC_PACKAGE_INDEX, body=search_body, scroll="2m")

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


def _build_headers(request_id: str, session_id: str) -> dict:
    """
    Merge COMMON_HEADERS (cookie-aware, Accept: */*) with the request / session IDs
    so every downstream call carries the same header-&-cookie combo that Postman shows.
    """
    return {**COMMON_HEADERS, "requestid": request_id, "sessionid": session_id}


async def fetch_autosuggest_results(
    search_term: str, short_timeout: float = 10.0
) -> List[dict]:
    """
    Call the SOTC AutoSuggest endpoint with the **same session + header set**
    we use for SRP/PDP so the server recognises the cookie and doesn’t return 406.
    """
    # 1)  Get / refresh the token (cached in memory)
    req_id, sess_id = get_cached_auth_token()
    if not req_id or not sess_id:
        fresh_req_id, fresh_sess_id = get_new_auth_token()
        store_auth_token(fresh_req_id, fresh_sess_id)
        req_id, sess_id = fresh_req_id, fresh_sess_id

    # 2)  Make the call – note `api_session.get`, *not* `requests.get`
    url = "https://services.sotc.in/holidayRS/autosuggest"
    params = {"searchAutoSuggest": search_term}
    headers = _build_headers(req_id, sess_id)  # inherits Accept: */*

    try:
        resp = api_session.get(
            url, headers=headers, params=params, timeout=short_timeout
        )
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []

    except requests.RequestException as e:
        logging.error(f"[fetch_autosuggest_results] AutoSuggest error: {e}")
        return []


@router.post("/sotc/SOTC_livepackagesv1")
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
    include_fare_calendar = request.fareCalendar if request.fareCalendar else False
    pkg_subtype_filter = (request.pkgSubtypeName or "").strip().upper()  # GIT or FIT

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

        ensure_index_exists(SOTC_PACKAGE_INDEX)

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

        # Filter packages by pkgSubtypeName (GIT or FIT)
        if pkg_subtype_filter and pkg_subtype_filter in ["GIT", "FIT"]:
            subtype_filtered = [
                p
                for p in detailed_packages
                if p.itinerary_data.pkgSubtypeName == pkg_subtype_filter
            ]
            if subtype_filtered:
                detailed_packages = subtype_filtered
                logging.info(
                    f"[livepackages] Filtered to {len(detailed_packages)} {pkg_subtype_filter} packages"
                )
            else:
                logging.warning(
                    f"[livepackages] No {pkg_subtype_filter} packages found, keeping all package types"
                )

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

        # Fetch fare calendar if requested
        if include_fare_calendar:
            logging.info(
                f"[livepackages] Fetching fare calendar for {len(final_list)} packages"
            )

            # Get auth token once and reuse for all fare calendar calls
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


@router.post("/sotc/SOTC_livepackages")
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

            # Clean search term for exact match queries
            cleaned_term = re.sub(r"\b(and|or)\b|[^\w\s]", "", search_term).strip()
            cleaned_term = re.sub(r"\s+", " ", cleaned_term)
            logging.info(f"[livepackagesv2] Cleaned search term: '{cleaned_term}'")

            fallback_body = {
                "query": {
                    "bool": {
                        "should": [
                            {"match_phrase": {"packageName": cleaned_term}},
                            {"match": {"cities.cityName": cleaned_term}},
                            {"match_phrase": {"packageSummary": cleaned_term}},
                        ],
                        "minimum_should_match": 1,
                    }
                },
                "size": 15,
            }

            ensure_index_exists(SOTC_PACKAGE_INDEX)
            try:
                es_result = es.search(index=SOTC_PACKAGE_INDEX, body=fallback_body)
                package_ids = {hit["_id"] for hit in es_result["hits"]["hits"]}
            except Exception as es_error:
                logging.warning(
                    f"[livepackagesv2] Elasticsearch fallback failed: {es_error}. Retrying without nested cities query."
                )
                # Fallback without cities nested query if it fails
                simple_fallback = {
                    "query": {
                        "bool": {
                            "should": [
                                {"match_phrase": {"packageName": cleaned_term}},
                                {"match_phrase": {"packageSummary": cleaned_term}},
                            ],
                            "minimum_should_match": 1,
                        }
                    },
                    "size": 15,
                }
                es_result = es.search(index=SOTC_PACKAGE_INDEX, body=simple_fallback)
                package_ids = {hit["_id"] for hit in es_result["hits"]["hits"]}

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
        ensure_index_exists(SOTC_PACKAGE_INDEX)
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
                if p.itinerary_data.pkgSubtypeName == pkg_subtype_filter
            ]
            if subtype_filtered:
                detailed_packages = subtype_filtered
                logging.info(
                    f"[livepackagesv1] Filtered to {len(detailed_packages)} {pkg_subtype_filter} packages"
                )
            else:
                logging.warning(
                    f"[livepackagesv1] No {pkg_subtype_filter} packages found, keeping all package types"
                )

        # Filter packages by theme
        if theme:
            theme_filtered = [
                p
                for p in detailed_packages
                if any(t.lower() == theme for t in p.itinerary_data.packageTheme)
            ]
            if theme_filtered:
                detailed_packages = theme_filtered

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
            final_list, matched_user_budget = apply_filters(
                final_candidates, None, target_budget, theme, target_days
            )
            final_list = prioritize_bogo(final_list)
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
            final_list, matched_user_budget = apply_filters(
                unfiltered_packages, departure_city, target_budget, theme, target_days
            )
            final_list = prioritize_bogo(final_list)
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
            final_list, matched_user_budget = apply_filters(
                unfiltered_packages, None, target_budget, theme, target_days
            )
            final_list = prioritize_bogo(final_list)
            code_val = 204 if not fallback_used else 214
            all_departure_cities = {
                city.cityName.lower()
                for pkg in unfiltered_packages
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
        final_list = prioritize_bogo(final_list)

        # Fetch fare calendar if requested
        if include_fare_calendar:
            logging.info(
                f"[livepackagesv2] Fetching fare calendar for {len(final_list)} packages"
            )

            # Get auth token once and reuse for all fare calendar calls
            try:
                request_id, session_id = get_new_auth_token()
                logging.info(
                    f"[livepackagesv2] Using shared auth token (requestId: {request_id[:10]}...) for {len(final_list)} packages"
                )
            except Exception as e:
                logging.error(
                    f"[livepackagesv2] Failed to get auth token for fare calendar: {str(e)}"
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
                        f"[livepackagesv2] Error fetching fare calendar for {pkg.itinerary_data.packageId}: {str(e)}"
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


@router.post("/sotc/SOTC_get_destination_details")
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

        url = "https://services.sotc.in/holidayRS/autosuggest?searchAutoSuggest"
        params = {"searchAutoSuggest": search_term}
        headers = {"Requestid": request_id, "Sessionid": session_id}

        logging.info(f"[get_destination_details] Calling AutoSuggest with {params}")
        try:
            auto_resp = api_session.get(
                url,
                params=params,
                headers=_build_headers(request_id, session_id),
                timeout=15,
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
                    "available_budget_ranges": [],
                    "visa_information": visa_information,
                },
            )

        # ---------------------------
        # 6. Fetch package details from Elasticsearch (No PDP fallback)
        # ---------------------------
        detailed_packages = []
        for pkg_id in package_ids:
            try:
                # Query ES directly; skip if not found
                search_body = {"query": {"term": {"packageId": pkg_id}}, "size": 1}
                es_resp = es.search(index=SOTC_PACKAGE_INDEX, body=search_body)
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
        # 7. Extract prices & build budget ranges
        # ---------------------------
        prices = sorted(
            [
                p.itinerary_data.price
                for p in detailed_packages
                if p.itinerary_data.price
            ]
        )
        logging.info(f"[get_destination_details] Found prices => {prices}")

        if not prices:
            return JSONResponse(
                status_code=200,
                content={
                    "message": "No price data available for the packages (or no valid ES packages).",
                    "available_budget_ranges": [],
                    "visa_information": visa_information,
                },
            )

        budget_labels = []
        if any(p <= 30000 for p in prices):
            budget_labels.append("Less than ₹30,000")
        if any(30000 < p <= 100000 for p in prices):
            budget_labels.append("₹30,000 - ₹1 Lac")
        if any(100000 < p <= 200000 for p in prices):
            budget_labels.append("₹1 Lac - ₹2 Lac")
        if any(p > 200000 for p in prices):
            budget_labels.append("More than ₹2 Lac")

        # ---------------------------
        # 8. Return final results
        # ---------------------------
        elapsed_time = (datetime.now() - start_time).total_seconds()
        logging.info(f"[get_destination_details] Completed in {elapsed_time}s")

        return {
            "search_term": request.search_term,
            "country_detected": country_name,
            "available_budget_ranges": budget_labels,
            "visa_information": visa_information,
        }

    except HTTPException:
        # If we intentionally raise an HTTPException above, let it propagate.
        raise
    except Exception as e:
        logging.error(f"[get_destination_details] Unexpected error => {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error: see logs.")
