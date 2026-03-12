from elasticsearch import Elasticsearch, helpers
import json

# Elasticsearch credentials
ES_HOST = "http://localhost:9200"
ES_USERNAME = "elastic"
ES_PASSWORD = "iweXVQuayXSCP9PFkHcZ"
INDEX_NAME = "tcildatav1"

# Load regions.json
with open("/home/gcp-admin/thomascook-travelplanner/Elastic Search/data/regions.json", "r", encoding="utf-8") as f:
    regions = json.load(f)

# Convert regions list into a dictionary for fast lookup
city_alias_mapping = {}
for region in regions:
    city_name = region.split(",")[0].strip()  # Extract city name (first part of the region string)
    city_alias_mapping[city_name.lower()] = region  # Store in dictionary for case-insensitive search

# Connect to Elasticsearch
es = Elasticsearch(ES_HOST, basic_auth=(ES_USERNAME, ES_PASSWORD))

# Fetch all documents from the index
query = {
    "query": {"match_all": {}},
    "_source": ["packageId", "cities"]  # Fetch only necessary fields
}

# Get all documents
response = es.search(index=INDEX_NAME, body=query, size=10000)  # Adjust size as needed

# Prepare bulk update operations
actions = []
for doc in response["hits"]["hits"]:
    doc_id = doc["_id"]
    cities = doc["_source"].get("cities", [])  # Get cities array

    updated_cities = []
    for city in cities:
        city_name = city.get("cityName", "").strip()
        alias_name = city_alias_mapping.get(city_name.lower(), "")  # Lookup alias name

        city["aliasCityName"] = alias_name  # Update aliasCityName
        updated_cities.append(city)

    # Prepare bulk update
    action = {
        "_op_type": "update",
        "_index": INDEX_NAME,
        "_id": doc_id,
        "doc": {"cities": updated_cities}  # Update cities field
    }
    actions.append(action)

# Execute bulk update
if actions:
    helpers.bulk(es, actions)
    print(f"Updated {len(actions)} documents successfully!")
else:
    print("No updates were needed.")
