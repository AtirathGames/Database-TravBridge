"""
Service to fetch destination FAQ information from Elasticsearch.
Similar pattern to fetchvisainfo.py
"""

from fastapi import APIRouter, HTTPException
from elasticsearch import Elasticsearch
from constants import es, DESTINATIONS_FAQ_INDEX
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter()


def ensure_faq_index_exists():
    """Ensure the destinations_faq index exists."""
    try:
        if not es.indices.exists(index=DESTINATIONS_FAQ_INDEX):
            mapping = {
                "mappings": {
                    "properties": {
                        "destination": {
                            "type": "text",
                            "fields": {"keyword": {"type": "keyword"}},
                        },
                        "question": {
                            "type": "text",
                            "fields": {"keyword": {"type": "keyword"}},
                        },
                        "answer": {
                            "type": "text"
                        },
                        "created_at": {"type": "date"},
                        "updated_at": {"type": "date"},
                    }
                },
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1,
                },
            }
            es.indices.create(index=DESTINATIONS_FAQ_INDEX, body=mapping)
            logger.info(f"Created index: {DESTINATIONS_FAQ_INDEX}")
    except Exception as e:
        logger.error(f"Error ensuring FAQ index exists: {str(e)}")


@router.get("/searchDestinationFAQ")
async def search_destination_faq(query: str):
    """
    Search for destination FAQ by destination name.
    Returns all FAQ questions and answers for the destination.
    
    Args:
        query: Destination name (e.g., "maldives", "kashmir")
    
    Returns:
        Dict with FAQ data grouped by question-answer pairs
    """
    try:
        ensure_faq_index_exists()

        # Query all FAQs for the destination
        search_body = {
            "query": {
                "bool": {
                    "should": [
                        # Exact match (highest priority)
                        {"term": {"destination.keyword": query.title()}},
                        # Case-insensitive match
                        {"match": {"destination": {"query": query, "boost": 2}}},
                        # Fuzzy match for typos
                        {
                            "match": {
                                "destination": {
                                    "query": query,
                                    "fuzziness": "AUTO",
                                    "boost": 1,
                                }
                            }
                        },
                    ],
                    "minimum_should_match": 1,
                }
            },
            "size": 100,  # Get all FAQs for the destination
        }

        response = es.search(index=DESTINATIONS_FAQ_INDEX, body=search_body)
        hits = response.get("hits", {}).get("hits", [])

        if not hits:
            logger.info(f"No FAQ found for destination: {query}")
            return {"destination": query, "faq_data": {}, "found": False}

        # Group FAQs by destination and build a dict
        faq_data = {}
        destination_name = hits[0]["_source"]["destination"]
        
        for hit in hits:
            source = hit["_source"]
            question = source.get("question", "")
            answer = source.get("answer", "")
            if question:
                faq_data[question] = answer

        logger.info(
            f"Found {len(faq_data)} FAQ entries for destination: {destination_name}"
        )

        return {
            "destination": destination_name,
            "faq_data": faq_data,
            "found": True,
            "total_faqs": len(faq_data),
        }

    except Exception as e:
        logger.error(f"Error searching destination FAQ: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


async def get_destination_faq_internal(destination: str) -> dict:
    """
    Internal function to get FAQ data (for use in other endpoints).
    Does not raise HTTPException, returns empty dict on error.
    
    Args:
        destination: Destination name
        
    Returns:
        Dict with FAQ data (question: answer pairs) or empty dict
    """
    try:
        ensure_faq_index_exists()

        search_body = {
            "query": {
                "bool": {
                    "should": [
                        {"term": {"destination.keyword": destination.title()}},
                        {"match": {"destination": {"query": destination, "boost": 2}}},
                        {
                            "match": {
                                "destination": {
                                    "query": destination,
                                    "fuzziness": "AUTO",
                                }
                            }
                        },
                    ],
                    "minimum_should_match": 1,
                }
            },
            "size": 100,  # Get all FAQs for the destination
        }

        response = es.search(index=DESTINATIONS_FAQ_INDEX, body=search_body)
        hits = response.get("hits", {}).get("hits", [])

        if hits:
            # Build FAQ dict from all matching documents
            faq_data = {}
            for hit in hits:
                source = hit["_source"]
                question = source.get("question", "")
                answer = source.get("answer", "")
                if question:
                    faq_data[question] = answer
            
            return faq_data

        return {}

    except Exception as e:
        logger.error(f"Error in get_destination_faq_internal: {str(e)}")
        return {}
