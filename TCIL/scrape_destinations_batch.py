"""
Destination FAQ Scraper - Batch Processing Module
Scrapes destination data from Thomas Cook website and indexes to Elasticsearch.
Designed to run as part of the nightly batch process.
"""

import json
import re
import time
import logging
import asyncio
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime
from elasticsearch import helpers
from bs4 import BeautifulSoup
import requests

from constants import es, DESTINATIONS_FAQ_INDEX

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

MAX_WORKERS = 5          # Concurrent scraping workers
REQUEST_DELAY = 0.5      # Delay between requests (seconds)
SCRAPE_TIMEOUT = 15      # Request timeout for scraping

BASE_URLS = [
    "https://www.thomascook.in/holidays/india-tour-packages/{}-tour-packages",
    "https://www.thomascook.in/holidays/international-tour-packages/{}-tour-packages"
]

# Path to destinations file (same as used in original Tcil_tool)
DESTINATIONS_FILE = "data/bd.json"

def load_destinations(filepath: str = DESTINATIONS_FILE) -> List[str]:
    """Load destinations from JSON file (same as original scraping process)."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            destinations = json.load(f)
            logger.info(f"Loaded {len(destinations)} destinations from {filepath}")
            return destinations
    except FileNotFoundError:
        logger.error(f"Destinations file not found: {filepath}")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing destinations file: {e}")
        return []

# ============================================================================
# TEXT UTILITIES
# ============================================================================

def clean_text(text: str) -> str:
    """Normalize whitespace in text."""
    if not text:
        return ""
    return re.sub(r'\s+', ' ', text).strip()

def get_element_text(element) -> str:
    """Extract clean text from a BeautifulSoup element."""
    if hasattr(element, 'get_text'):
        return clean_text(element.get_text())
    if isinstance(element, str):
        return clean_text(str(element))
    return ""

# ============================================================================
# CONTENT EXTRACTORS
# ============================================================================

def extract_content_until_heading(element, stop_tags: tuple = ('h2',)) -> str:
    """Get all text content after element until next heading."""
    parts = []
    sibling = element.next_sibling
    
    while sibling:
        if hasattr(sibling, 'name') and sibling.name in stop_tags:
            break
        text = get_element_text(sibling)
        if text:
            parts.append(text)
        sibling = sibling.next_sibling
    
    return ' '.join(parts)

def extract_modal_sections(soup: BeautifulSoup) -> Dict[str, str]:
    """Extract sections from <div id="modalTextReadMore">."""
    sections = {}
    modal_div = soup.find('div', {'id': 'modalTextReadMore'})
    
    if not modal_div:
        return sections
    
    # Extract introduction (content before first H2)
    intro_parts = []
    for child in modal_div.children:
        if hasattr(child, 'name') and child.name == 'h2':
            break
        text = get_element_text(child)
        if text:
            intro_parts.append(text)
    
    if intro_parts:
        sections['Introduction'] = ' '.join(intro_parts)
    
    # Extract H2 sections
    for h2 in modal_div.find_all('h2'):
        heading = get_element_text(h2)
        if heading:
            content = extract_content_until_heading(h2, stop_tags=('h2',))
            if content:
                sections[heading] = content
    
    return sections

def extract_faq_items(soup: BeautifulSoup) -> List[Dict[str, str]]:
    """Extract FAQ from <div class="srp_footer_content_no_more">."""
    faqs = []
    faq_div = soup.find('div', class_=lambda x: x and 'srp_footer_content_no_more' in x)
    
    if not faq_div:
        return faqs
    
    for h3 in faq_div.find_all('h3'):
        question = get_element_text(h3)
        if not question:
            continue
        
        # Find next P tag for answer
        answer = ""
        next_elem = h3.find_next_sibling()
        while next_elem:
            if hasattr(next_elem, 'name'):
                if next_elem.name == 'h3':
                    break
                if next_elem.name == 'p':
                    answer = get_element_text(next_elem)
                    break
            next_elem = next_elem.find_next_sibling()
        
        if question and answer:
            faqs.append({"question": question, "answer": answer})
    
    return faqs

# ============================================================================
# SCRAPING FUNCTIONS
# ============================================================================

def slugify(name: str) -> str:
    """Convert destination name to URL slug."""
    slug = name.lower().strip()
    slug = re.sub(r'[^a-z0-9\s-]', '', slug)
    slug = re.sub(r'[\s_]+', '-', slug)
    slug = re.sub(r'-+', '-', slug)
    return slug.strip('-')

def scrape_from_html(html: str) -> Dict[str, Any]:
    """Extract travel content from HTML string."""
    soup = BeautifulSoup(html, 'html.parser')
    sections = extract_modal_sections(soup)
    faqs = extract_faq_items(soup)
    
    result = {
        "table_of_content": list(sections.keys()),
        "faq": faqs
    }
    result.update(sections)
    
    return result

def scrape_destination_sync(destination: str) -> Tuple[str, Optional[Dict[str, Any]], str]:
    """
    Scrape a single destination, trying both URL patterns.
    Synchronous version for use in asyncio.to_thread.
    
    Returns:
        (destination_name, scraped_data or None, url_used or error_message)
    """
    slug = slugify(destination)
    
    for url_template in BASE_URLS:
        url = url_template.format(slug)
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            response = requests.get(url, headers=headers, timeout=SCRAPE_TIMEOUT)
            response.raise_for_status()
            
            data = scrape_from_html(response.text)
            
            # Check if we got meaningful content
            if data.get("table_of_content") or data.get("faq"):
                logger.info(f"✓ Scraped {destination} from {url}")
                return (destination, data, url)
                
        except Exception as e:
            logger.debug(f"Failed to scrape {destination} from {url}: {str(e)}")
            continue
        
        time.sleep(REQUEST_DELAY)
    
    logger.warning(f"✗ No valid page found for {destination}")
    return (destination, None, "No valid page found")

async def scrape_destination(destination: str) -> Tuple[str, Optional[Dict[str, Any]], str]:
    """Async wrapper for scraping."""
    return await asyncio.to_thread(scrape_destination_sync, destination)

# ============================================================================
# ELASTICSEARCH FUNCTIONS
# ============================================================================

def ensure_faq_index_exists():
    """Ensure the destinations_faq index exists with proper mapping."""
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

def prepare_es_documents(destination: str, faq_data: Dict[str, Any]) -> List[Dict]:
    """
    Convert scraped data to Elasticsearch documents.
    Each FAQ question-answer pair AND each section becomes a separate document.
    
    This includes:
    1. Sections from modalTextReadMore (H2 headings with content)
    2. FAQ items from srp_footer_content_no_more (H3 questions with answers)
    """
    actions = []
    current_time = datetime.now().isoformat()
    doc_index = 0
    
    # Index all sections (H2 content from modalTextReadMore)
    # These are the main content sections like "Introduction", "Best Time to Visit", etc.
    for section_key, section_value in faq_data.items():
        # Skip metadata fields
        if section_key in ["table_of_content", "faq"]:
            continue
        
        # Each section becomes a document
        if isinstance(section_value, str) and section_value.strip():
            doc_id = f"{slugify(destination)}_{doc_index}"
            action = {
                "_index": DESTINATIONS_FAQ_INDEX,
                "_id": doc_id,
                "_source": {
                    "destination": destination,
                    "question": section_key,
                    "answer": section_value,
                    "created_at": current_time,
                    "updated_at": current_time,
                },
            }
            actions.append(action)
            doc_index += 1
    
    # Index FAQ items (H3 questions from srp_footer_content_no_more)
    faqs = faq_data.get("faq", [])
    for faq_item in faqs:
        question = faq_item.get("question", "")
        answer = faq_item.get("answer", "")
        
        if not question or not answer:
            continue
        
        doc_id = f"{slugify(destination)}_{doc_index}"
        action = {
            "_index": DESTINATIONS_FAQ_INDEX,
            "_id": doc_id,
            "_source": {
                "destination": destination,
                "question": question,
                "answer": answer,
                "created_at": current_time,
                "updated_at": current_time,
            },
        }
        actions.append(action)
        doc_index += 1
    
    return actions

async def delete_destination_faqs(destination: str):
    """Delete all existing FAQs for a destination before updating."""
    try:
        query = {
            "query": {
                "term": {
                    "destination.keyword": destination
                }
            }
        }
        await asyncio.to_thread(
            es.delete_by_query,
            index=DESTINATIONS_FAQ_INDEX,
            body=query
        )
        logger.info(f"Deleted existing FAQs for {destination}")
    except Exception as e:
        logger.error(f"Error deleting FAQs for {destination}: {str(e)}")

# ============================================================================
# BATCH PROCESSING
# ============================================================================

async def scrape_and_index_all_destinations(destinations: List[str] = None) -> Dict[str, Any]:
    """
    Main function to scrape all destinations and index to Elasticsearch.
    
    Args:
        destinations: List of destination names (loads from bd.json if None)
        
    Returns:
        Dict with statistics about the scraping process
    """
    if destinations is None:
        destinations = load_destinations()
    
    if not destinations:
        logger.error("No destinations to scrape")
        return {
            "total": 0,
            "scraped": 0,
            "failed": 0,
            "faq_documents": 0,
            "failed_destinations": []
        }
    
    logger.info(f"Starting batch scrape for {len(destinations)} destinations")
    
    # Ensure index exists
    ensure_faq_index_exists()
    
    # Statistics
    stats = {
        "total": len(destinations),
        "scraped": 0,
        "failed": 0,
        "faq_documents": 0,
        "failed_destinations": []
    }
    
    # Create semaphore for concurrency control
    semaphore = asyncio.Semaphore(MAX_WORKERS)
    
    async def process_destination(dest: str):
        async with semaphore:
            try:
                # Scrape destination
                dest_name, data, url_or_error = await scrape_destination(dest)
                
                if data:
                    # Delete old FAQs
                    await delete_destination_faqs(dest_name)
                    
                    # Prepare ES documents
                    actions = prepare_es_documents(dest_name, data)
                    
                    if actions:
                        # Bulk index to ES
                        success, failed = await asyncio.to_thread(
                            helpers.bulk,
                            es,
                            actions,
                            raise_on_error=False
                        )
                        
                        stats["scraped"] += 1
                        stats["faq_documents"] += success
                        
                        logger.info(f"✓ Indexed {success} FAQs for {dest_name}")
                    else:
                        logger.warning(f"No FAQs found for {dest_name}")
                        stats["failed"] += 1
                        stats["failed_destinations"].append(dest_name)
                else:
                    stats["failed"] += 1
                    stats["failed_destinations"].append(dest_name)
                    
            except Exception as e:
                logger.error(f"Error processing {dest}: {str(e)}")
                stats["failed"] += 1
                stats["failed_destinations"].append(dest)
            
            # Rate limiting
            await asyncio.sleep(REQUEST_DELAY)
    
    # Process all destinations concurrently
    tasks = [process_destination(dest) for dest in destinations]
    await asyncio.gather(*tasks)
    
    logger.info(f"""
    ╔══════════════════════════════════════════════════════════╗
    ║           DESTINATION SCRAPING COMPLETED                 ║
    ╠══════════════════════════════════════════════════════════╣
    ║  Total Destinations: {stats['total']:>5}                            ║
    ║  Successfully Scraped: {stats['scraped']:>5}                        ║
    ║  Failed: {stats['failed']:>5}                                       ║
    ║  FAQ Documents Indexed: {stats['faq_documents']:>5}                 ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    return stats

# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

async def run_destination_scraping():
    """
    Main entry point for destination scraping.
    To be called from batchprocessing.py.
    """
    try:
        stats = await scrape_and_index_all_destinations()
        return stats
    except Exception as e:
        logger.error(f"Error in destination scraping: {str(e)}")
        raise

if __name__ == "__main__":
    # For testing purposes
    asyncio.run(run_destination_scraping())
