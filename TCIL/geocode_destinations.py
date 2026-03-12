"""
Geocode unique destinations and store them in Elasticsearch
This script extracts all unique cities from packages and geocodes them using geopy
"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, Set, Tuple, Optional
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
from elasticsearch import Elasticsearch, helpers
from destination_index_mapping import destination_index_mapping

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Elasticsearch connection
es = Elasticsearch(
    ['http://localhost:9200'],
    verify_certs=False,
    basic_auth=('elastic', 'iweXVQuayXSCP9PFkHcZ')
)

TCIL_PACKAGE_INDEX = "tcildatav1"
DESTINATIONS_INDEX = "all_unique_destinations"

# Initialize geolocator with a custom user agent
geolocator = Nominatim(user_agent="thomascook_destination_geocoder", timeout=10)


def ensure_destinations_index():
    """Create the destinations index if it doesn't exist"""
    try:
        if not es.indices.exists(index=DESTINATIONS_INDEX):
            logging.info(f"Creating index: {DESTINATIONS_INDEX}")
            es.indices.create(index=DESTINATIONS_INDEX, body=destination_index_mapping)
            logging.info(f"Index {DESTINATIONS_INDEX} created successfully")
        else:
            logging.info(f"Index {DESTINATIONS_INDEX} already exists")
    except Exception as e:
        logging.error(f"Error creating index: {e}")
        raise


async def geocode_city(city_name: str, max_retries: int = 3) -> Optional[Tuple[float, float, str, str]]:
    """
    Geocode a city name to get latitude, longitude, state, and country
    Returns: (lat, lon, state, country) or None if geocoding fails
    """
    for attempt in range(max_retries):
        try:
            logging.info(f"Geocoding: {city_name} (attempt {attempt + 1}/{max_retries})")
            
            # Add a small delay to respect rate limits
            if attempt > 0:
                await asyncio.sleep(2 ** attempt)
            
            location = await asyncio.to_thread(geolocator.geocode, city_name, exactly_one=True, addressdetails=True)
            
            if location:
                # Extract state and country from address components
                address = location.raw.get('address', {})
                
                # Try multiple fields for state
                state = (
                    address.get('state', '') or 
                    address.get('state_district', '') or 
                    address.get('region', '') or
                    address.get('county', '') or
                    ''
                )
                
                # Get country
                country = address.get('country', '')
                
                lat, lon = location.latitude, location.longitude
                logging.info(f"✅ Geocoded {city_name}: ({lat}, {lon}) - {state}, {country}")
                return (lat, lon, state, country)
            else:
                logging.warning(f"⚠️ No location found for: {city_name}")
                return None
                
        except GeocoderTimedOut:
            logging.warning(f"⏱️ Geocoding timeout for {city_name} (attempt {attempt + 1})")
            if attempt == max_retries - 1:
                return None
        except GeocoderServiceError as e:
            logging.error(f"❌ Geocoding service error for {city_name}: {e}")
            return None
        except Exception as e:
            logging.error(f"❌ Unexpected error geocoding {city_name}: {e}")
            return None
    
    return None


async def get_existing_geocoded_cities() -> Set[str]:
    """
    Get all city names that are already geocoded in the destinations index
    Returns: Set of city names (lowercase)
    """
    try:
        existing_cities = set()
        
        # Scroll through all documents in the destinations index
        query = {"query": {"match_all": {}}, "_source": ["cityName"], "size": 1000}
        
        response = es.search(index=DESTINATIONS_INDEX, body=query, scroll='2m')
        scroll_id = response['_scroll_id']
        hits = response['hits']['hits']
        
        while hits:
            for hit in hits:
                city_name = hit['_source'].get('cityName', '').lower()
                if city_name:
                    existing_cities.add(city_name)
            
            response = es.scroll(scroll_id=scroll_id, scroll='2m')
            scroll_id = response['_scroll_id']
            hits = response['hits']['hits']
        
        # Clear scroll
        es.clear_scroll(scroll_id=scroll_id)
        
        logging.info(f"Found {len(existing_cities)} existing geocoded cities in {DESTINATIONS_INDEX}")
        return existing_cities
        
    except Exception as e:
        if "index_not_found_exception" in str(e):
            logging.info(f"Index {DESTINATIONS_INDEX} not found, assuming no existing cities")
            return set()
        logging.error(f"Error fetching existing cities: {e}")
        return set()


async def extract_unique_cities() -> Dict[str, int]:
    """
    Extract all unique cities from packages in Elasticsearch
    Returns a dictionary with city names and their package counts
    """
    logging.info("Extracting unique cities from packages...")
    
    city_counts = {}
    
    try:
        # Scroll through all packages
        query = {
            "query": {"match_all": {}},
            "_source": ["cities", "packageId"]
        }
        
        response = es.search(
            index=TCIL_PACKAGE_INDEX,
            body=query,
            scroll='5m',
            size=1000
        )
        
        scroll_id = response['_scroll_id']
        hits = response['hits']['hits']
        
        while hits:
            for hit in hits:
                source = hit['_source']
                cities = source.get('cities', [])
                
                for city in cities:
                    city_name = city.get('cityName', '').strip()
                    if city_name:
                        city_counts[city_name] = city_counts.get(city_name, 0) + 1
            
            # Get next batch
            response = es.scroll(scroll_id=scroll_id, scroll='5m')
            scroll_id = response['_scroll_id']
            hits = response['hits']['hits']
        
        # Clear scroll
        es.clear_scroll(scroll_id=scroll_id)
        
        logging.info(f"✅ Found {len(city_counts)} unique cities")
        return city_counts
        
    except Exception as e:
        logging.error(f"❌ Error extracting cities: {e}")
        raise


async def geocode_and_store_destinations(incremental: bool = True) -> dict:
    """
    Main function to extract cities, geocode them, and store in Elasticsearch
    
    Args:
        incremental: If True, only geocode new cities not already in the index.
                    If False, geocode all cities (full refresh).
    
    Returns a summary of the operation
    """
    logging.info("=" * 80)
    logging.info(f"Starting destination geocoding process (incremental={incremental})")
    logging.info("=" * 80)
    
    # Ensure index exists
    ensure_destinations_index()
    
    # Extract unique cities from packages
    logging.info("Extracting unique cities from packages...")
    city_package_counts = await extract_unique_cities()
    logging.info(f"✅ Found {len(city_package_counts)} unique cities in packages")
    
    if not city_package_counts:
        return {"success": False, "message": "No cities found in packages"}
    
    # If incremental mode, filter out already geocoded cities
    cities_to_geocode = city_package_counts
    if incremental:
        existing_cities = await get_existing_geocoded_cities()
        cities_to_geocode = {
            city: count 
            for city, count in city_package_counts.items() 
            if city.lower() not in existing_cities
        }
        logging.info(
            f"📊 Incremental mode: {len(existing_cities)} cities already geocoded, "
            f"{len(cities_to_geocode)} new cities to process"
        )
    else:
        logging.info(f"📊 Full refresh mode: Processing all {len(cities_to_geocode)} cities")
    
    if not cities_to_geocode:
        logging.info("✅ All cities are already geocoded. Nothing to do.")
        return {
            "success": True,
            "message": "All destinations already geocoded",
            "total_cities": len(city_package_counts),
            "new_cities": 0,
            "geocoded": 0,
            "failed": 0
        }
    
    # Geocode cities with delays to respect rate limits
    geocoded_data = []
    failed_cities = []
    
    total_cities = len(cities_to_geocode)
    for idx, (city_name, package_count) in enumerate(cities_to_geocode.items(), 1):
        logging.info(f"\n[{idx}/{total_cities}] Processing: {city_name} ({package_count} packages)")
        
        result = await geocode_city(city_name)
        if result:
            lat, lon, state, country = result
            geocoded_data.append({
                "cityName": city_name,
                "location": {"lat": lat, "lon": lon},
                "stateName": state,
                "countryName": country,
                "packageCount": package_count,
                "lastUpdated": datetime.utcnow().isoformat()
            })
        else:
            failed_cities.append(city_name)
            logging.error(f"❌ Failed to geocode: {city_name}")
        
        # Add delay between geocoding requests (1.1 seconds to respect Nominatim's 1 req/sec limit)
        await asyncio.sleep(1.1)
    
    # Bulk index to Elasticsearch
    if geocoded_data:
        logging.info(f"\n📌 Bulk indexing {len(geocoded_data)} destinations to {DESTINATIONS_INDEX}")
        try:
            actions = [
                {
                    "_index": DESTINATIONS_INDEX,
                    "_id": data["cityName"],  # Use city name as document ID
                    "_source": data
                }
                for data in geocoded_data
            ]
            success, failed = helpers.bulk(es, actions, stats_only=True, raise_on_error=False)
            logging.info(f"✅ Successfully indexed {success} destinations")
            if failed:
                logging.warning(f"⚠️ Failed to index {failed} destinations")
        except Exception as e:
            logging.error(f"❌ Error during bulk indexing: {e}")
    
    # Summary
    logging.info("\n" + "=" * 80)
    logging.info("GEOCODING SUMMARY")
    logging.info("=" * 80)
    logging.info(f"Total cities in packages: {len(city_package_counts)}")
    logging.info(f"New cities to process: {total_cities}")
    logging.info(f"Successfully geocoded: {len(geocoded_data)}")
    logging.info(f"Failed: {len(failed_cities)}")
    if failed_cities:
        logging.info(f"\nFailed cities: {', '.join(failed_cities)}")
    
    return {
        "success": True,
        "total_cities": len(city_package_counts),
        "new_cities": total_cities,
        "geocoded": len(geocoded_data),
        "failed": len(failed_cities),
        "failed_cities": failed_cities
    }


async def find_nearest_destination(search_term: str, max_results: int = 5) -> list:
    """
    Find nearest destinations to a search term based on geo-proximity
    
    Args:
        search_term: The city/destination name to search for
        max_results: Maximum number of results to return
    
    Returns:
        List of nearest destinations with their details
    """
    try:
        # First, geocode the search term
        logging.info(f"Finding nearest destinations for: {search_term}")
        result = await geocode_city(search_term)
        
        if not result:
            logging.warning(f"Could not geocode search term: {search_term}")
            return []
        
        lat, lon, _, _ = result
        logging.info(f"Search term coordinates: ({lat}, {lon})")
        
        # Search for nearest destinations using geo_distance
        query = {
            "query": {
                "bool": {
                    "must": {
                        "match_all": {}
                    },
                    "filter": {
                        "geo_distance": {
                            "distance": "1000km",  # Search within 1000km radius
                            "location": {
                                "lat": lat,
                                "lon": lon
                            }
                        }
                    }
                }
            },
            "sort": [
                {
                    "_geo_distance": {
                        "location": {
                            "lat": lat,
                            "lon": lon
                        },
                        "order": "asc",
                        "unit": "km"
                    }
                }
            ],
            "size": max_results
        }
        
        response = es.search(index=DESTINATIONS_INDEX, body=query)
        
        results = []
        for hit in response['hits']['hits']:
            source = hit['_source']
            distance_km = hit['sort'][0] if 'sort' in hit else None
            
            results.append({
                "cityName": source['cityName'],
                "stateName": source.get('stateName', ''),
                "countryName": source.get('countryName', ''),
                "location": source['location'],
                "packageCount": source.get('packageCount', 0),
                "distanceKm": round(distance_km, 2) if distance_km else None
            })
        
        logging.info(f"✅ Found {len(results)} nearest destinations")
        return results
        
    except Exception as e:
        logging.error(f"❌ Error finding nearest destinations: {e}")
        return []


# Standalone execution for testing
if __name__ == "__main__":
    async def main():
        # Test geocoding and storing
        result = await geocode_and_store_destinations()
        print(f"\nGeocoding Result: {result}")
        
        # Test finding nearest destinations
        test_search = "spiti valley"
        print(f"\n\nTesting nearest destination search for: {test_search}")
        nearest = await find_nearest_destination(test_search, max_results=5)
        
        if nearest:
            print(f"\nNearest destinations to '{test_search}':")
            for idx, dest in enumerate(nearest, 1):
                print(f"{idx}. {dest['cityName']} ({dest['countryName']}) - {dest['distanceKm']} km away - {dest['packageCount']} packages")
        else:
            print("No nearby destinations found")
    
    asyncio.run(main())
