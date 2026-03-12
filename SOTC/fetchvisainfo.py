from fastapi import FastAPI, HTTPException, Query, UploadFile, BackgroundTasks,Body,APIRouter
from elasticsearch import Elasticsearch, RequestError, TransportError, helpers
from services import ensure_index_exists,create_visa_faq_index
from constants import visa_data,VISA_INDEX,es
import json



router = APIRouter()


@router.post("/createVisaFAQ")
async def create_visa_faq():
    try:
        create_visa_faq_index()
        with open(visa_data, 'r') as f:
            restructured_data = json.load(f)
        actions = [
            {
                "_index": VISA_INDEX,
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

@router.get("/searchVisaFAQ")
async def search_visa_faq(query: str):
    try:
        create_visa_faq_index()
        response = es.search(
            index=VISA_INDEX,
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