"""
Script to load destination FAQ data from JSON file into Elasticsearch.
Run this once to migrate data from JSON to ES index.
"""

import json
import logging
from elasticsearch import Elasticsearch, helpers
from constants import es, DESTINATIONS_FAQ_INDEX

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_faq_index():
    """Create the destinations_faq index with appropriate mapping."""
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

    try:
        if es.indices.exists(index=DESTINATIONS_FAQ_INDEX):
            logger.info(f"Index '{DESTINATIONS_FAQ_INDEX}' already exists.")
            return

        es.indices.create(index=DESTINATIONS_FAQ_INDEX, body=mapping)
        logger.info(f"Created index: {DESTINATIONS_FAQ_INDEX}")
    except Exception as e:
        logger.error(f"Error creating index: {str(e)}")
        raise


def load_faq_data():
    """Load FAQ data from JSON file into Elasticsearch."""
    faq_file = "/home/gcp-admin/thomascook-travelplanner/Elastic Search/data/all_destinations_faq.json"

    try:
        # Read JSON file
        with open(faq_file, "r", encoding="utf-8") as f:
            all_faqs = json.load(f)

        logger.info(f"Loaded {len(all_faqs)} destinations from JSON file")

        # Prepare bulk indexing actions
        # Each FAQ question-answer pair becomes a separate document
        actions = []
        total_faq_entries = 0
        
        for destination, faq_data in all_faqs.items():
            if isinstance(faq_data, dict):
                for question, answer in faq_data.items():
                    # Normalize answer to string regardless of input type
                    if isinstance(answer, str):
                        answer_text = answer
                    elif isinstance(answer, list):
                        # Convert list to newline-separated string
                        answer_text = "\n".join(str(item) for item in answer)
                    elif isinstance(answer, dict):
                        # Convert dict to JSON string
                        answer_text = json.dumps(answer, ensure_ascii=False)
                    elif answer is None:
                        answer_text = ""
                    else:
                        # Convert any other type to string
                        answer_text = str(answer)
                    
                    # Create unique ID: destination + question
                    doc_id = f"{destination.lower().replace(' ', '_')}_{len(actions)}"
                    
                    action = {
                        "_index": DESTINATIONS_FAQ_INDEX,
                        "_id": doc_id,
                        "_source": {
                            "destination": destination,
                            "question": question,
                            "answer": answer_text,
                            "created_at": "2025-12-23T00:00:00",
                            "updated_at": "2025-12-23T00:00:00",
                        },
                    }
                    actions.append(action)
                    total_faq_entries += 1

        logger.info(f"Prepared {total_faq_entries} FAQ entries for indexing")

        # Bulk index
        success, failed = helpers.bulk(es, actions, raise_on_error=False)
        logger.info(f"Successfully indexed: {success} documents")
        
        if failed:
            logger.warning(f"Failed to index: {len(failed)} documents")
            logger.warning("=" * 60)
            logger.warning("FAILED DOCUMENTS DETAILS:")
            logger.warning("=" * 60)
            
            for idx, fail_info in enumerate(failed[:10], 1):  # Show first 10 failures
                if isinstance(fail_info, dict):
                    error_info = fail_info.get('index', {}).get('error', {})
                    doc_id = fail_info.get('index', {}).get('_id', 'unknown')
                    error_type = error_info.get('type', 'unknown')
                    error_reason = error_info.get('reason', 'unknown')
                    
                    logger.warning(f"\n{idx}. Document ID: {doc_id}")
                    logger.warning(f"   Error Type: {error_type}")
                    logger.warning(f"   Error Reason: {error_reason}")
                else:
                    logger.warning(f"\n{idx}. {fail_info}")
            
            if len(failed) > 10:
                logger.warning(f"\n... and {len(failed) - 10} more failures")
            
            logger.warning("=" * 60)

        return success, failed

    except FileNotFoundError:
        logger.error(f"FAQ file not found: {faq_file}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error loading FAQ data: {str(e)}")
        raise


def main():
    """Main function to create index and load data."""
    logger.info("Starting FAQ data migration to Elasticsearch...")

    # Step 1: Create index
    create_faq_index()

    # Step 2: Load data
    success, failed = load_faq_data()

    logger.info("=" * 60)
    logger.info("Migration completed!")
    logger.info(f"Successfully indexed: {success} documents")
    logger.info(f"Failed: {len(failed) if failed else 0} documents")
    logger.info("=" * 60)
    
    # Step 3: Save failed documents to file for analysis
    if failed:
        failed_file = "failed_faq_migrations.json"
        try:
            with open(failed_file, 'w') as f:
                json.dump(failed, f, indent=2)
            logger.info(f"Failed documents details saved to: {failed_file}")
        except Exception as e:
            logger.error(f"Could not save failed documents: {str(e)}")


if __name__ == "__main__":
    main()
