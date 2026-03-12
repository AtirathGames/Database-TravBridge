"""
Enhanced 210 fallback logic integration
This module provides the enhanced geo-proximity search for retrievePackages.py
Supports multiple destinations (e.g., "Georgia,Switzerland" or "Manali and Shimla")
"""

import logging
import re
from typing import List, Optional
from geocode_destinations import find_nearest_destination


async def enhanced_fallback_search(
    search_term: str,
    fetch_autosuggest_results_func,
    max_nearby_destinations: int = 3
) -> tuple[set, Optional[str]]:
    """
    Enhanced fallback search using geo-proximity when AutoSuggest returns no results.
    Supports multiple destinations separated by commas, 'and', or 'or'.
    
    Args:
        search_term: The original search term from the user (can be multiple destinations)
        fetch_autosuggest_results_func: Function to call AutoSuggest API
        max_nearby_destinations: Number of nearby destinations to try per search term
    
    Returns:
        (package_ids_set, matched_destinations_str) or (empty_set, None) if all fail
        matched_destinations_str is a comma-separated list like "Athens, Zurich"
    """
    logging.info(f"[enhanced_fallback] Starting geo-proximity search for: {search_term}")
    
    # Step 0: Split multiple destinations
    destinations = re.split(r'[,\s]+(?:and|or)\s+|,\s*', search_term)
    destinations = [d.strip() for d in destinations if d.strip()]
    
    if len(destinations) > 1:
        logging.info(f"[enhanced_fallback] Detected {len(destinations)} destinations: {destinations}")
    
    all_package_ids = set()
    matched_cities = []
    
    # Process each destination separately
    for dest_idx, single_destination in enumerate(destinations, 1):
        logging.info(f"[enhanced_fallback] Processing destination {dest_idx}/{len(destinations)}: {single_destination}")
        
        try:
            # Step 1: Find nearest destinations using geo-proximity for this specific destination
            nearest_destinations = await find_nearest_destination(
                single_destination,
                max_results=max_nearby_destinations
            )
            
            if not nearest_destinations:
                logging.warning(f"[enhanced_fallback] No nearby destinations found for: {single_destination}")
                continue
            
            logging.info(
                f"[enhanced_fallback] Found {len(nearest_destinations)} nearby destinations for {single_destination}"
            )
            
            # Step 2: Try each nearby destination with AutoSuggest
            for idx, dest in enumerate(nearest_destinations, 1):
                dest_name = dest['cityName']
                distance = dest['distanceKm']
                
                # Skip destinations beyond 150km
                if distance and distance > 100:
                    logging.info(
                        f"[enhanced_fallback] Skipping destination {idx}/{len(nearest_destinations)}: "
                        f"{dest_name} ({distance} km away - exceeds 100km limit)"
                    )
                    continue
                
                logging.info(
                    f"[enhanced_fallback] Trying destination {idx}/{len(nearest_destinations)}: "
                    f"{dest_name} ({distance} km away, {dest['packageCount']} packages)"
                )
                
                # Query AutoSuggest with this destination
                autosuggest_results = await fetch_autosuggest_results_func(dest_name.lower())
                
                if autosuggest_results:
                    # Filter to exact matches
                    filtered_packages = [
                        item
                        for item in autosuggest_results
                        if dest_name.lower()
                        in [
                            (item.get("cityName") or "").lower().strip(),
                            (item.get("countryName") or "").lower().strip(),
                            (item.get("stateName") or "").lower().strip(),
                        ]
                    ]
                    
                    package_ids = {
                        pkg["packageId"]
                        for item in filtered_packages
                        for pkg in item.get("pkgnameIdMappingList", [])
                    }
                    
                    if package_ids:
                        all_package_ids.update(package_ids)
                        matched_cities.append(dest_name)
                        logging.info(
                            f"[enhanced_fallback] ✅ Found {len(package_ids)} packages "
                            f"for {dest_name} (nearest to {single_destination})"
                        )
                        break  # Found packages for this destination, move to next
                    else:
                        logging.warning(
                            f"[enhanced_fallback] AutoSuggest returned results for {dest_name} "
                            f"but no matching packages found"
                        )
                else:
                    logging.warning(
                        f"[enhanced_fallback] AutoSuggest returned no results for {dest_name}"
                    )
            
        except Exception as e:
            logging.error(f"[enhanced_fallback] Error processing destination '{single_destination}': {e}")
            continue
    
    # Return aggregated results - keep same return format (set, Optional[str])
    if all_package_ids:
        matched_cities_str = ", ".join(matched_cities)
        logging.info(
            f"[enhanced_fallback] ✅ Total: {len(all_package_ids)} packages from "
            f"{len(matched_cities)} matched destinations: {matched_cities_str}"
        )
        return all_package_ids, matched_cities_str
    else:
        logging.warning(
            f"[enhanced_fallback] No packages found for any of the destinations: {destinations}"
        )
        return set(), None


def format_enhanced_fallback_message(
    original_search_term: str,
    matched_destination: str,
    distance_km: float
) -> str:
    """
    Create a user-friendly message explaining the geo-proximity match
    
    Args:
        original_search_term: What the user searched for
        matched_destination: The nearest destination we found packages for
        distance_km: Distance between search term and matched destination
    
    Returns:
        Formatted message string
    """
    return (
        f"We found packages for **{matched_destination}**, which is approximately "
        f"**{distance_km:.0f} km** from {original_search_term}. "
        f"Here are the available packages."
    )


# Example integration code for retrievePackages.py
"""
INTEGRATION EXAMPLE - Add to retrievePackages.py around line 861:

from enhanced_fallback_integration import enhanced_fallback_search, format_enhanced_fallback_message

# Inside get_packages_v2 function:
if not package_ids:
    fallback_used = True
    logging.info(
        "[livepackagesv1] No packages from AutoSuggest, trying enhanced geo-proximity fallback."
    )
    
    # NEW: Try geo-proximity search first
    package_ids, matched_destination = await enhanced_fallback_search(
        search_term=search_term,
        fetch_autosuggest_results_func=fetch_autosuggest_results,
        max_nearby_destinations=3
    )
    
    if package_ids:
        logging.info(
            f"[livepackagesv1] Enhanced fallback found {len(package_ids)} packages "
            f"via nearest destination: {matched_destination}"
        )
        # Continue with normal flow, package_ids is now populated
    else:
        # Fall back to existing ES text-based search
        logging.info(
            "[livepackagesv1] Enhanced fallback failed, using ES text search fallback."
        )
        
        # ... EXISTING ES FALLBACK CODE FROM LINE 866-945 ...
        destinations = re.split(r'[,\s]+(?:and|or)\s+|,\s*', search_term)
        # ... etc ...
"""
