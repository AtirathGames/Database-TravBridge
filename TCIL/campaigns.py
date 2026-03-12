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

from pydantic import BaseModel
from elasticsearch import Elasticsearch, RequestError, TransportError, helpers
from elasticsearch.exceptions import NotFoundError, ConflictError
import logging
import json, requests
import time
import re
from datetime import datetime
from models import CampaignCreateRequest, Field, ItemOut
from constants import (
    TCIL_CAMPAIGN_INDEX,
    es,
    TOKEN_TTL,
    GEOCODE_TTL_SECONDS,
    geolocator,
)
from services import (
    ensure_index_exists,search_item_by_package_id_internal
)
from NewESmapping import CAMPAIGN_INDEX_MAPPING


router = APIRouter()


def ensure_index_exists(index_name: str, mapping: Optional[dict] = None):
    """
    If index does not exist, create it (optionally with mapping).
    Existing indices are left untouched.
    """
    if es.indices.exists(index=index_name):
        return

    body = mapping or {}
    es.indices.create(index=index_name, body=body)


@router.post("/v1/create_campaign")
async def create_campaign(request: CampaignCreateRequest):
    """
    Create (or overwrite) a campaign document in ES.

    Example body:
    {
      "campaignId": "c00001",
      "packageIds": ["12345","2345","5678","5679"]
    }
    """
    try:
        # 1) make sure the index exists
        ensure_index_exists(TCIL_CAMPAIGN_INDEX, CAMPAIGN_INDEX_MAPPING)

        doc_id = request.campaignId
        doc_body = {
            "campaignId": request.campaignId,
            "packageIds": list(set(request.packageIds)),  # de-dupe
            "createdAt": datetime.utcnow().isoformat(),
        }

        # 2) upsert (create or replace)
        es.index(
            index=TCIL_CAMPAIGN_INDEX, id=doc_id, body=doc_body, refresh="wait_for"
        )

        return {"message": "Campaign stored successfully", "campaignId": doc_id}

    except Exception as e:
        logging.error(f"[create_campaign] error => {e}")
        raise HTTPException(status_code=500, detail="Failed to store campaign")


# ── models.py (add if you want typed responses; optional) ─────────────────────
class CampaignResponse(BaseModel):
    campaignId: str
    packageIds: List[str]
    createdAt: datetime


# ──────────────────────────────────────────────────────────────────────────────


# ── NEW ENDPOINT:  GET  /v1/campaign/{campaign_id}  ───────────────────────────
@router.get(
    "/v1/campaign/{campaign_id}",
    response_model=CampaignResponse,  # drop if you prefer plain dict
    responses={404: {"description": "Campaign not found"}},
)
async def get_campaign(campaign_id: str):
    """
    Fetch a campaign document by its ID.
    """
    ensure_index_exists(TCIL_CAMPAIGN_INDEX, CAMPAIGN_INDEX_MAPPING)

    try:
        res = es.get(index=TCIL_CAMPAIGN_INDEX, id=campaign_id)
        return res["_source"]  # FastAPI → dict → Pydantic → JSON

    except NotFoundError:
        raise HTTPException(status_code=404, detail="Campaign not found")
    except Exception as e:
        logging.error(f"[get_campaign] error => {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


# ── NEW ENDPOINT:  DELETE  /v1/campaign/{campaign_id} ─────────────────────────
@router.delete(
    "/v1/campaign/{campaign_id}",
    responses={
        200: {"description": "Campaign deleted"},
        404: {"description": "Campaign not found"},
    },
)
async def delete_campaign(campaign_id: str):
    """
    Delete a campaign document (idempotent: 404 if not present).
    """
    ensure_index_exists(TCIL_CAMPAIGN_INDEX, CAMPAIGN_INDEX_MAPPING)

    try:
        es.delete(index=TCIL_CAMPAIGN_INDEX, id=campaign_id, refresh="wait_for")
        return {"message": "Campaign deleted", "campaignId": campaign_id}

    except NotFoundError:
        raise HTTPException(status_code=404, detail="Campaign not found")
    except Exception as e:
        logging.error(f"[delete_campaign] error => {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# ── imports (top of retrivepackages.py) ───────────────────────────────────────
# Most of these are already present – just make sure asyncio & typing are, too.
import asyncio
from typing import Dict, Union, List, Optional
# ──────────────────────────────────────────────────────────────────────────────


# ── models.py (optional request model – keeps things tidy) ────────────────────
class CampaignPackagesRequest(BaseModel):
    campaignId: str = Field(..., example="c00001")
    generateSummary: Optional[bool] = Field(
        False,
        description="Pass true if you want fresh summaries for each package",
    )
# ──────────────────────────────────────────────────────────────────────────────


# ── NEW ENDPOINT:  POST  /v1/campaign_packages  ───────────────────────────────
@router.post(
    "/v1/campaign_packages",
    response_model=List[ItemOut],
    responses={
        404: {"description": "Campaign not found"},
        400: {"description": "Bad request"},
    },
)
async def get_campaign_packages(body: CampaignPackagesRequest):
    """
    Return full package details for every package ID stored in a campaign.

    **Request body**
    ```json
    {
      "campaignId": "c00001",
      "generateSummary": false   // optional
    }
    ```

    **Success response (200)**
    ```json
    [
      { ...ItemOut... },
      { ...ItemOut... }
    ]
    ```
    """
    campaign_id = body.campaignId.strip()
    generate_summary = bool(body.generateSummary)

    if not campaign_id:
        raise HTTPException(status_code=400, detail="campaignId cannot be empty")

    # Ensure the index exists (creates it silently if missing)
    ensure_index_exists(TCIL_CAMPAIGN_INDEX, CAMPAIGN_INDEX_MAPPING)

    try:
        # 1) Pull campaign doc
        res = es.get(index=TCIL_CAMPAIGN_INDEX, id=campaign_id)
        package_ids = res["_source"].get("packageIds") or []
    except NotFoundError:
        raise HTTPException(status_code=404, detail="Campaign not found")
    except Exception as e:
        logging.error(f"[get_campaign_packages] ES error => {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

    if not package_ids:
        return []  # Empty list if campaign has no packages

    # 2) Fetch each package concurrently
    tasks = [
        search_item_by_package_id_internal(
            {"packageId": pid}, generate_summary=generate_summary
        )
        for pid in package_ids
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # 3) Filter out any individual 404/other failures
    detailed_packages = [
        r for r in results if not isinstance(r, Exception)
    ]

    if not detailed_packages:
        # All lookups failed – surface a 404 so the client knows
        raise HTTPException(
            status_code=404,
            detail="No valid packages found for the provided campaign",
        )

    return detailed_packages
# ──────────────────────────────────────────────────────────────────────────────

@router.post(
    "/v1/list_campaigns",
    response_model=List[Dict[str, Union[str, List[str]]]],
    responses={
        200: {"description": "List of all available campaigns"},
        500: {"description": "Internal server error"}
    }
)
async def list_all_campaigns():
    """
    Returns a list of all campaigns with their `campaignId` and `packageIds`.
    """
    try:
        ensure_index_exists(TCIL_CAMPAIGN_INDEX, CAMPAIGN_INDEX_MAPPING)

        # Use ES scroll API for reliability if expecting more than 10,000 docs
        search_body = {
            "query": {"match_all": {}},
            "_source": ["campaignId", "packageIds"],
            "size": 1000  # Adjust based on expected load
        }

        response = es.search(index=TCIL_CAMPAIGN_INDEX, body=search_body)
        hits = response.get("hits", {}).get("hits", [])

        campaigns = [
            {
                "campaignId": hit["_source"].get("campaignId", hit["_id"]),
                "packageIds": hit["_source"].get("packageIds", [])
            }
            for hit in hits
        ]

        return campaigns

    except Exception as e:
        logging.error(f"[list_all_campaigns] Error => {str(e)}")
        raise HTTPException(status_code=500, detail="Unable to list campaigns")
