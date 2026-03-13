from collections import defaultdict
from io import BytesIO
import re
from fastapi import (
    FastAPI,
    HTTPException,
    Query,
    UploadFile,
    BackgroundTasks,
    Body,
    APIRouter,
)
import pandas as pd
from models import (
    Conversation,
    ConversationIdRequest,
    DeleteConversationRequest,
    UserIdRequest,
    UpdateChatNameRequest,
    FilterRequest,
    DateRangeRequest,BugReportRequest,BugReportFilterRequest
)
from typing import Dict, List, Optional, Union, Any
from constants import SOTC_CHAT_INDEX_NAME, es, SOTC_PACKAGE_INDEX,SOTC_BUG_INDEX_NAME
from services import ensure_index_exists
from datetime import datetime
from NewESmapping import conversation_index_mapping,bug_index_mapping
import logging
import json, requests
from dateutil.parser import parse as parse_date
from fastapi.responses import StreamingResponse


router = APIRouter()


def clean_date_fields(data):
    """
    Recursively clean empty string values from date fields.
    Converts empty strings to None, and removes None values.
    """
    if isinstance(data, dict):
        cleaned = {}
        for key, value in data.items():
            if isinstance(value, str) and value == "":
                # Skip empty strings for date-like fields
                continue
            elif isinstance(value, dict):
                cleaned_nested = clean_date_fields(value)
                if cleaned_nested:  # Only add if not empty
                    cleaned[key] = cleaned_nested
            elif isinstance(value, list):
                cleaned_list = []
                for item in value:
                    if isinstance(item, dict):
                        cleaned_item = clean_date_fields(item)
                        if cleaned_item:  # Only add if not empty
                            cleaned_list.append(cleaned_item)
                    else:
                        cleaned_list.append(item)
                if cleaned_list:
                    cleaned[key] = cleaned_list
            else:
                cleaned[key] = value
        return cleaned
    return data


@router.post("/sotc/SOTC_save_conversation")
def save_conversation(conversation_data: dict = Body(...)):
    """
    Save (index) the provided conversation into Elasticsearch.
    - Normalizes empty strings in date fields to None so ES date parsing doesn't fail.
    """
    try:
        # ✅ Convert empty string to `None` for top-level booking_date
        if conversation_data.get("booking_date") == "":
            conversation_data["booking_date"] = None

        # ✅ Ensure `saved_time` exists in `packages_saved`
        for pkg in conversation_data.get("packages_saved", []):
            if "saved_time" not in pkg or not pkg["saved_time"]:
                pkg["saved_time"] = (
                    datetime.utcnow().isoformat()
                )  # Set default time if missing

        # ✅ Clean handoff_history date fields (end_time, request_time) if they are empty strings
        handoff_history = conversation_data.get("handoff_history")
        if handoff_history:
            # handoff_history can be a dict or a list of dicts depending on how you send it
            def _clean_handoff_record(rec: dict):
                # Only convert date fields that are mapped as date in ES
                for date_field in ("end_time", "request_time"):
                    if date_field in rec and rec[date_field] == "":
                        rec[date_field] = None

            if isinstance(handoff_history, list):
                for rec in handoff_history:
                    if isinstance(rec, dict):
                        _clean_handoff_record(rec)
            elif isinstance(handoff_history, dict):
                _clean_handoff_record(handoff_history)

        # (Optional) ✅ You can similarly clean events.timestamp if needed in future
        # events = conversation_data.get("events")
        # if events:
        #     def _clean_event(ev: dict):
        #         if "timestamp" in ev and ev["timestamp"] == "":
        #             ev["timestamp"] = None
        #     if isinstance(events, list):
        #         for ev in events:
        #             if isinstance(ev, dict):
        #                 _clean_event(ev)
        #     elif isinstance(events, dict):
        #         _clean_event(events)

        # Ensure index exists
        if not es.indices.exists(index=SOTC_CHAT_INDEX_NAME):
            es.indices.create(
                index=SOTC_CHAT_INDEX_NAME, body=conversation_index_mapping
            )
            logging.info(f"Index {SOTC_CHAT_INDEX_NAME} created with mapping.")

        doc_id = conversation_data["conversationId"]
        response = es.index(
            index=SOTC_CHAT_INDEX_NAME,
            id=doc_id,
            document=conversation_data,
        )
        return {"status": "success", "result": response}

    except Exception as e:
        logging.error(f"Error saving conversation: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sotc/SOTC_get_conversation")
def get_conversation(body: ConversationIdRequest):
    """
    Retrieve the conversation document with the given conversationId.
    (Changed from GET to POST to accept a JSON body.)
    """
    try:
        if not es.indices.exists(index=SOTC_CHAT_INDEX_NAME):
            raise HTTPException(
                status_code=404, detail=f"Index '{SOTC_CHAT_INDEX_NAME}' not found."
            )

        response = es.get(index=SOTC_CHAT_INDEX_NAME, id=body.conversationId)
        if response.get("found"):
            return {"status": "success", "conversation": response["_source"]}
        else:
            return {
                "status": "not_found",
                "message": f"No conversation found for {body.conversationId}",
            }

    except Exception as e:
        logging.error(f"Error retrieving conversation {body.conversationId}: {str(e)}")
        # Check for a "not_found" phrase in the error to return a 404, if needed
        if "not_found" in str(e).lower():
            raise HTTPException(
                status_code=404, detail=f"Conversation {body.conversationId} not found."
            )
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sotc/SOTC_get_conversation_summaries")
def get_conversation_summaries(body: UserIdRequest):
    """
    Given a userId, return a list of documents with:
      - conversationId
      - chat_name
      - chat_started
      - chat_modified
      - packages_saved (number of saved packages in each conversation)
    sorted by the latest chat_modified in descending order.
    Only include conversations with "chat_status": "active".
    """
    try:
        ensure_index_exists(SOTC_CHAT_INDEX_NAME)

        search_body = {
            "size": 1000,
            "sort": [{"chat_modified": {"order": "desc", "missing": "_last"}}],
            "query": {
                "bool": {
                    "must": [
                        {"term": {"userId": body.userId}},
                        {"term": {"chat_status": "active"}},
                    ]
                }
            },
            "_source": [
                "conversationId",
                "chat_name",
                "chat_started",
                "chat_modified",
                "packages_saved",
            ],
        }

        response = es.search(index=SOTC_CHAT_INDEX_NAME, body=search_body)
        hits = response.get("hits", {}).get("hits", [])

        results = []
        for hit in hits:
            src = hit["_source"]
            item = {
                "conversationId": src.get("conversationId"),
                "chat_name": src.get("chat_name"),
                "chat_started": src.get("chat_started"),
                "chat_modified": src.get("chat_modified", None),
                "packages_saved": len(src.get("packages_saved", [])),
            }
            results.append(item)

        return {"status": "success", "total": len(results), "conversations": results}

    except Exception as e:
        logging.error(f"Error retrieving conversation summaries: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


def fetch_package_details_internally(package_id: str) -> Dict[str, Any]:
    """
    Fetch package details from Elasticsearch by packageId.
    """
    try:
        search_body = {"query": {"term": {"packageId": package_id}}, "size": 1}

        logging.info(
            f"Sending Elasticsearch Query: {json.dumps(search_body, indent=2)}"
        )

        response = es.search(index=SOTC_PACKAGE_INDEX, body=search_body)

        response_dict = response.body if hasattr(response, "body") else response
        logging.info(f"Elasticsearch Response: {json.dumps(response_dict, indent=2)}")

        hits = response_dict.get("hits", {}).get("hits", [])

        if not hits:
            logging.warning(f"Package ID {package_id} not found in Elasticsearch.")
            return {}

        package_data = hits[0]["_source"]

        return {
            "packageName": package_data.get("packageName", "Expired Package"),
            "days": package_data.get("days", ""),
            "cities": package_data.get("cities", []),
            "price": package_data.get("price", ""),
        }

    except Exception as e:
        logging.error(f"Error fetching package details for {package_id}: {str(e)}")
        return {}


@router.post("/sotc/SOTC_get_packages_saved")
def get_packages_saved(body: UserIdRequest):
    """
    Given a userId, return a unique list of saved packages
    from all documents with the same userId, sorted by latest "chat_modified".
    Only include conversations with "chat_status": "active".
    """
    ensure_index_exists(SOTC_CHAT_INDEX_NAME)

    search_body = {
        "size": 1000,
        "sort": [{"chat_modified": {"order": "desc", "missing": "_last"}}],
        "query": {
            "bool": {
                "must": [
                    {"term": {"userId": body.userId}},
                    {"term": {"chat_status": "active"}},
                ]
            }
        },
        "_source": ["packages_saved", "conversationId"],
    }

    response = es.search(index=SOTC_CHAT_INDEX_NAME, body=search_body)
    hits = response.get("hits", {}).get("hits", [])

    combined_results = []
    seen = set()

    for hit in hits:
        doc = hit["_source"]
        conv_id = doc["conversationId"]
        packages = doc.get("packages_saved", [])

        for pkg_obj in packages:
            pkg_id = pkg_obj["packageId"]
            saved_time = pkg_obj.get("saved_time", datetime.utcnow().isoformat())

            combo_key = (conv_id, pkg_id)
            if combo_key in seen:
                continue
            seen.add(combo_key)

            details = fetch_package_details_internally(pkg_id)

            package_info = {
                "packageID": pkg_id,
                "conversationId": conv_id,
                "saved_time": saved_time,
                "packageName": details.get("packageName", "Expired Package"),
                "Destination": list(
                    {
                        c["cityName"]
                        for c in details.get("cities", [])
                        if "cityName" in c
                    }
                ),
                "duration": details.get("days", ""),
                "estimatedcost": details.get("price", ""),
            }

            combined_results.append(package_info)

    return {"status": "success", "packages_saved": combined_results}


@router.post("/sotc/SOTC_update_chat_name")
def update_chat_name(body: UpdateChatNameRequest):
    """
    Update the chat name for a given conversationId.
    """
    try:
        if not es.indices.exists(index=SOTC_CHAT_INDEX_NAME):
            raise HTTPException(
                status_code=404, detail=f"Index '{SOTC_CHAT_INDEX_NAME}' not found."
            )

        # Check if the conversation exists
        response = es.get(index=SOTC_CHAT_INDEX_NAME, id=body.conversationId)
        if not response.get("found"):
            raise HTTPException(
                status_code=404, detail=f"Conversation {body.conversationId} not found."
            )

        # Update the chat name
        update_body = {
            "doc": {
                "chat_name": body.new_chat_name,
                "chat_modified": datetime.utcnow().isoformat(),  # Update modification timestamp
            }
        }
        es.update(index=SOTC_CHAT_INDEX_NAME, id=body.conversationId, body=update_body)

        return {"status": "success", "message": "Chat name updated successfully."}

    except Exception as e:
        logging.error(f"Error updating chat name: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sotc/SOTC_delete_conversation")
def delete_conversation(body: DeleteConversationRequest):
    """
    Soft delete a conversation document by conversationId.
    Instead of deleting the document, update the chat_status to "deleted".
    """
    try:
        if not es.indices.exists(index=SOTC_CHAT_INDEX_NAME):
            raise HTTPException(
                status_code=404, detail=f"Index '{SOTC_CHAT_INDEX_NAME}' not found."
            )

        # Check if the conversation exists
        response = es.get(index=SOTC_CHAT_INDEX_NAME, id=body.conversationId)
        if not response.get("found"):
            raise HTTPException(
                status_code=404, detail=f"Conversation {body.conversationId} not found."
            )

        # Update the chat_status to "deleted"
        update_body = {
            "doc": {
                "chat_status": "deleted",
                "chat_modified": datetime.utcnow().isoformat(),  # Update the modification timestamp
            }
        }
        es.update(index=SOTC_CHAT_INDEX_NAME, id=body.conversationId, body=update_body)

        return {
            "status": "success",
            "message": "Conversation marked as deleted successfully.",
        }

    except Exception as e:
        logging.error(f"Error marking conversation as deleted: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sotc/SOTC_get_user_conversations")
def get_user_conversations(body: UserIdRequest):
    """
    Retrieve all saved packageIds for the given userId, sorted by latest chat_modified descending.
    """
    try:
        if not es.indices.exists(index=SOTC_CHAT_INDEX_NAME):
            raise HTTPException(
                status_code=404, detail=f"Index '{SOTC_CHAT_INDEX_NAME}' not found."
            )

        search_body = {
            "size": 1000,
            "sort": [{"chat_modified": {"order": "desc", "missing": "_last"}}],
            "query": {"term": {"userId": body.userId}},
            "_source": ["conversationId", "packages_saved.packageId"],
        }

        response = es.search(index=SOTC_CHAT_INDEX_NAME, body=search_body)
        hits = response.get("hits", {}).get("hits", [])

        result = []
        for hit in hits:
            conv_id = hit["_source"].get("conversationId")
            packages = hit["_source"].get("packages_saved", [])
            pkg_ids = [pkg.get("packageId") for pkg in packages if "packageId" in pkg]

            result.append({"conversationId": conv_id, "packageIds": pkg_ids})

        return {"status": "success", "total": len(result), "conversations": result}

    except Exception as e:
        logging.error(
            f"Error retrieving conversations for userId={body.userId}: {str(e)}"
        )
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.post("/sotc/SOTC_get_all_conversations")
def get_all_conversations(filters: FilterRequest = Body(default={})):
    """
    Get conversation summaries with optional filters:
    - chat_started (from, to) — expands to full day if same.
    - booking_date (from, to) — expands to full day if same.
    - chat_channel (exact match).
    - opportunity_id: if provided (checkbox), returns docs that have an opportunity_id.
    - userId: dropdown with options "all", "registered" (userId contains "@"), and "guest" (userId starts with "guest_").
    - only_tool_conversations: if true, return docs where all conversation roles are 'tool'.
    - count (number of documents per page, default 10).
    - page (page number, default 1).
    """
    try:
        if not es.indices.exists(index=SOTC_CHAT_INDEX_NAME):
            raise HTTPException(
                status_code=404, detail=f"Index '{SOTC_CHAT_INDEX_NAME}' not found."
            )

        must_clauses = []

        # --- Chat Started Range ---
        if filters.chat_started_from or filters.chat_started_to:
            from_dt = filters.chat_started_from
            to_dt = filters.chat_started_to
            if (
                filters.chat_started_from
                and filters.chat_started_to
                and filters.chat_started_from == filters.chat_started_to
            ):
                from_dt = (
                    parse_date(filters.chat_started_from)
                    .replace(hour=0, minute=0, second=0, microsecond=0)
                    .isoformat()
                    + "Z"
                )
                to_dt = (
                    parse_date(filters.chat_started_to)
                    .replace(hour=23, minute=59, second=59, microsecond=999999)
                    .isoformat()
                    + "Z"
                )
            range_filter = {}
            if from_dt:
                range_filter["gte"] = from_dt
            if to_dt:
                range_filter["lte"] = to_dt
            must_clauses.append({"range": {"chat_started": range_filter}})

        # --- Booking Date Range ---
        if filters.booking_date_from or filters.booking_date_to:
            from_dt = filters.booking_date_from
            to_dt = filters.booking_date_to
            if (
                filters.booking_date_from
                and filters.booking_date_to
                and filters.booking_date_from == filters.booking_date_to
            ):
                from_dt = (
                    parse_date(filters.booking_date_from)
                    .replace(hour=0, minute=0, second=0, microsecond=0)
                    .isoformat()
                    + "Z"
                )
                to_dt = (
                    parse_date(filters.booking_date_to)
                    .replace(hour=23, minute=59, second=59, microsecond=999999)
                    .isoformat()
                    + "Z"
                )
            range_filter = {}
            if from_dt:
                range_filter["gte"] = from_dt
            if to_dt:
                range_filter["lte"] = to_dt
            must_clauses.append({"range": {"booking_date": range_filter}})

        # --- Chat Channel ---
        if filters.chat_channel:
            must_clauses.append({"term": {"chat_channel": filters.chat_channel}})

        # --- Opportunity ID ---
        if filters.opportunity_id:
            must_clauses.append(
                {
                    "bool": {
                        "must": [{"exists": {"field": "opportunity_id"}}],
                        "must_not": [{"term": {"opportunity_id": ""}}],
                    }
                }
            )

        # --- UserId ---
        if filters.userId and filters.userId.lower() != "all":
            if filters.userId.lower() == "registered":
                must_clauses.append({"wildcard": {"userId": "*@*"}})
            elif filters.userId.lower() == "guest":
                must_clauses.append({"prefix": {"userId": "guest_"}})

        # --- Only Tool Role Conversations ---
        if filters.only_tool_conversations:
            must_clauses.append(
                {
                    "nested": {
                        "path": "conversation",
                        "query": {"term": {"conversation.role": "tool"}},
                    }
                }
            )

        # Final query
        query = (
            {"match_all": {}} if not must_clauses else {"bool": {"must": must_clauses}}
        )

        # Pagination
        page = filters.page or 1
        per_page = filters.count or 10
        offset = (page - 1) * per_page

        # Search body
        search_body = {
            "_source": [
                "conversationId",
                "userId",
                "chat_name",
                "opportunity_id",
                "chat_modified",
            ],
            "from": offset,
            "size": per_page,
            "sort": [{"chat_modified": {"order": "desc", "missing": "_last"}}],
            "query": query,
        }

        response = es.search(index=SOTC_CHAT_INDEX_NAME, body=search_body)
        hits = response.get("hits", {}).get("hits", [])
        summaries = [hit["_source"] for hit in hits]

        return {
            "status": "success",
            "total": response.get("hits", {})
            .get("total", {})
            .get("value", len(summaries)),
            "page": page,
            "per_page": per_page,
            "conversations": summaries,
        }

    except Exception as e:
        logging.error(f"Error retrieving all conversations with filters: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ✅ Utility Functions
def extract_user_details(conversation):
    details = {"first_name": "", "last_name": "", "email": "", "mobile": ""}
    for msg in conversation:
        if msg["role"] == "user":
            content = msg["content"]
            name_match = re.search(
                r"(?i)name:\s*([\w\s]+?)(?:,?\s*(?:mobile|email|phone):|\s*$)", content
            )
            if name_match:
                name = name_match.group(1).strip()
                names = name.split()
                details["first_name"] = " ".join(names[:-1]) if len(names) > 1 else name
                details["last_name"] = names[-1] if len(names) > 1 else ""
            email_match = re.search(r"(?i)email:\s*([^\s]+@[^\s]+)", content)
            if email_match:
                details["email"] = email_match.group(1).strip()
            mobile_match = re.search(r"(?i)(mobile|phone):\s*(\d+)", content)
            if mobile_match:
                details["mobile"] = mobile_match.group(2).strip()
    return details


def has_tool_message(conversation):
    return "Yes" if any(msg.get("role") == "tool" for msg in conversation) else "No"


def get_last_assistant_args(conversation):
    assistant_msgs = [msg for msg in conversation if msg["role"] == "assistant"]
    if not assistant_msgs:
        return {
            "searchText": "",
            "dates_start": "",
            "dates_end": "",
            "number_of_people": "",
            "number_of_days": "",
            "budget": "",
            "hub": "",
            "phone": "",
        }
    last_msg = assistant_msgs[-1]
    try:
        content_json = json.loads(last_msg["content"])
        args = content_json.get("arguments", {})
        return {
            "searchText": (
                str(args.get("searchText", ""))
                if args.get("searchText") is not None
                else ""
            ),
            "dates_start": (
                str(args.get("dates", {}).get("start", ""))
                if args.get("dates", {}).get("start") is not None
                else ""
            ),
            "dates_end": (
                str(args.get("dates", {}).get("end", ""))
                if args.get("dates", {}).get("end") is not None
                else ""
            ),
            "number_of_people": (
                str(args.get("number_of_people", ""))
                if args.get("number_of_people") is not None
                else ""
            ),
            "number_of_days": (
                str(args.get("number_of_days", ""))
                if args.get("number_of_days") is not None
                else ""
            ),
            "budget": (
                str(args.get("budget", "")) if args.get("budget") is not None else ""
            ),
            "hub": str(args.get("hub", "")) if args.get("hub") is not None else "",
            "phone": (
                str(args.get("phone", "")) if args.get("phone") is not None else ""
            ),
        }
    except json.JSONDecodeError:
        return {
            "searchText": "",
            "dates_start": "",
            "dates_end": "",
            "number_of_people": "",
            "number_of_days": "",
            "budget": "",
            "hub": "",
            "phone": "",
        }


# ✅ API Route
@router.post("/sotc/SOTC_export_conversations")
def export_conversation_summary(request: DateRangeRequest):
    try:
        from_dt = datetime.strptime(request.from_date, "%Y-%m-%d")
        to_dt = datetime.strptime(request.to_date, "%Y-%m-%d")

        if from_dt > to_dt:
            raise HTTPException(
                status_code=400, detail="from_date cannot be after to_date."
            )

        from_date = from_dt.strftime("%Y-%m-%dT00:00:00Z")
        to_date = to_dt.strftime("%Y-%m-%dT23:59:59Z")

        query = {
            "bool": {
                "must": [
                    {"range": {"chat_started": {"gte": from_date, "lte": to_date}}},
                    {"term": {"chat_channel": "ChatBot"}},
                ]
            }
        }

        scroll = es.search(
            index=SOTC_CHAT_INDEX_NAME, body={"query": query}, scroll="2m", size=1000
        )
        sid = scroll["_scroll_id"]
        scroll_size = len(scroll["hits"]["hits"])
        results = scroll["hits"]["hits"]

        while scroll_size > 0:
            scroll = es.scroll(scroll_id=sid, scroll="2m")
            sid = scroll["_scroll_id"]
            scroll_size = len(scroll["hits"]["hits"])
            results.extend(scroll["hits"]["hits"])

        stats_by_date = defaultdict(
            lambda: {
                "total_convos": 0,
                "packages_shown": 0,
                "opportunities": 0,
                "empty_convos": 0,
            }
        )
        data_by_date = defaultdict(list)

        exact_assistant_content = '{"message": "Hi, I am Ezy, your personal Gen AI holiday assistant from SOTC. Where would you like to travel?", "name": "packageSearch", "arguments": {"searchText": "", "dates": {"start": "", "end": ""}, "number_of_people": null, "number_of_days": null, "budget": "", "hub": "", "first_name": "", "last_name": "", "email": "", "phone": ""}}'

        for hit in results:
            source = hit["_source"]
            date_key_summary = datetime.strptime(
                source["chat_started"][:10], "%Y-%m-%d"
            ).strftime("%d-%m-%y")
            stats_by_date[date_key_summary]["total_convos"] += 1
            if source.get("opportunity_id"):
                stats_by_date[date_key_summary]["opportunities"] += 1
            for msg in source.get("conversation", []):
                if msg.get("role") == "tool":
                    stats_by_date[date_key_summary]["packages_shown"] += 1
                    break

            date_key_detail = source["chat_started"][:10]
            user_details = extract_user_details(source["conversation"])
            assistant_args = get_last_assistant_args(source["conversation"])
            packages_shown = has_tool_message(source["conversation"])

            category = ""
            if (
                len(source["conversation"]) == 2
                and source["conversation"][0]["role"] == "user"
                and source["conversation"][0]["content"] == "Hello"
                and source["conversation"][1]["role"] == "assistant"
                and source["conversation"][1]["content"] == exact_assistant_content
            ):
                category = "EMPTY"
                stats_by_date[date_key_summary]["empty_convos"] += 1

            conversation_data = {
                "Conversation ID": source["conversationId"],
                "Opportunity ID": source.get("opportunity_id", ""),
                "Packages Shown": packages_shown,
                "Search Text": assistant_args["searchText"],
                "Start Date": assistant_args["dates_start"],
                "End Date": assistant_args["dates_end"],
                "Number of People": assistant_args["number_of_people"],
                "Number of Days": assistant_args["number_of_days"],
                "Budget": assistant_args["budget"],
                "Hub": assistant_args["hub"],
                "First Name": user_details["first_name"],
                "Last Name": user_details["last_name"],
                "Email": user_details["email"],
                "Mobile": user_details["mobile"],
                "Phone": assistant_args["phone"],
                "Category": category,
            }
            data_by_date[date_key_detail].append(conversation_data)

        summary_df = pd.DataFrame(
            [
                {
                    "Date": date,
                    "Total Convos": d["total_convos"],
                    "Packages Shown Count": d["packages_shown"],
                    "Opportunities": d["opportunities"],
                    "Empty Bucket": d["empty_convos"],
                }
                for date, d in sorted(stats_by_date.items())
            ]
        )

        summary_stats_df = pd.DataFrame(
            {
                "Metric": [
                    "Total Conversations",
                    "Total Packages Shown",
                    "Total Opportunities",
                    "Total Empty Conversations",
                ],
                "Count": [
                    summary_df["Total Convos"].sum(),
                    summary_df["Packages Shown Count"].sum(),
                    summary_df["Opportunities"].sum(),
                    summary_df["Empty Bucket"].sum(),
                ],
            }
        )

        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            summary_df.to_excel(
                writer, sheet_name="Conversation Summary", index=False, startrow=0
            )
            summary_stats_df.to_excel(
                writer,
                sheet_name="Conversation Summary",
                index=False,
                startrow=len(summary_df) + 2,
            )
            for date_key in sorted(data_by_date):
                df = pd.DataFrame(data_by_date[date_key])
                df.to_excel(writer, sheet_name=date_key, index=False)
        output.seek(0)

        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename=conversations_{request.from_date}_to_{request.to_date}.xlsx",
                "Content-Type": "application/octet-stream",
            },
        )

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error processing request: {str(e)}"
        )


# =========================================================================
# BUG REPORTING ENDPOINTS
# =========================================================================

def create_bug_index():
    """
    Create the bug reporting index if it doesn't already exist.
    Called during application startup.
    """
    try:
        if not es.indices.exists(index=SOTC_BUG_INDEX_NAME):
            es.indices.create(
                index=SOTC_BUG_INDEX_NAME,
                mappings=bug_index_mapping
            )
            logging.info(f"Bug index '{SOTC_BUG_INDEX_NAME}' created successfully.")
        else:
            logging.info(f"Bug index '{SOTC_BUG_INDEX_NAME}' already exists.")
    except Exception as e:
        logging.error(f"Error creating bug index: {str(e)}")


@router.post("/sotc/Sotc_report_bug")
def report_bug(bug_data: BugReportRequest):
    """
    Save a bug report to Elasticsearch.
    - Automatically sets timestamp
    - Auto-sets reported_at if issue exists
    """
    try:
        # Ensure bug index exists
        if not es.indices.exists(index=SOTC_BUG_INDEX_NAME):
            create_bug_index()

        # Convert request model to dict
        doc = bug_data.dict(exclude_unset=True)

        # ✅ Auto-set timestamp
        doc["timestamp"] = datetime.utcnow().isoformat()

        # ✅ Auto-set reported_at if issue exists
        if "issue" in doc and doc["issue"]:
            if "reported_at" not in doc["issue"] or not doc["issue"]["reported_at"]:
                doc["issue"]["reported_at"] = datetime.utcnow().isoformat()

        # Save to Elasticsearch
        response = es.index(
            index=SOTC_BUG_INDEX_NAME,
            document=doc
        )

        logging.info(f"Bug report saved successfully with ID: {response['_id']}")
        return {
            "status": "success",
            "message": "Bug report saved successfully",
            "document_id": response["_id"]
        }

    except Exception as e:
        logging.error(f"Error saving bug report: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sotc/Sotc_get_bug_logs")
def get_bug_logs(filters: BugReportFilterRequest = Body(default=BugReportFilterRequest())):
    """
    Retrieve bug logs with optional filters and merge results by conversationId.
    
    Filters supported:
    - agent_id: Filter by agent
    - conversation_id: Filter by conversation
    - issue_type: Filter by issue type
    - start_date / end_date: Date range filter (ISO format)
    - limit: Maximum results (default 100)
    """
    try:
        if not es.indices.exists(index=SOTC_BUG_INDEX_NAME):
            raise HTTPException(
                status_code=404,
                detail=f"Bug index '{SOTC_BUG_INDEX_NAME}' not found."
            )

        # Build filter clauses
        filter_clauses = []

        if filters.agent_id:
            filter_clauses.append({"term": {"agent_id": filters.agent_id}})

        if filters.conversation_id:
            filter_clauses.append({"term": {"conversationId": filters.conversation_id}})

        if filters.issue_type:
            filter_clauses.append({"term": {"issue.issue_type": filters.issue_type}})

        if filters.start_date and filters.end_date:
            filter_clauses.append({
                "range": {
                    "timestamp": {
                        "gte": filters.start_date,
                        "lte": filters.end_date
                    }
                }
            })

        # Build query
        query = {
            "bool": {"filter": filter_clauses}
        } if filter_clauses else {"match_all": {}}

        # Execute search
        search_body = {
            "query": query,
            "size": filters.limit or 100,
            "sort": [{"timestamp": {"order": "desc"}}]
        }

        response = es.search(index=SOTC_BUG_INDEX_NAME, body=search_body)
        docs = [hit["_source"] for hit in response["hits"]["hits"]]

        # ✅ Merge results by conversationId
        merged = _merge_bug_reports(docs)

        return {
            "status": "success",
            "total": len(docs),
            "merged_conversations": len(merged),
            "results": list(merged.values())
        }

    except Exception as e:
        logging.error(f"Error retrieving bug logs: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


def _merge_bug_reports(docs: List[Dict]) -> Dict[str, Dict]:
    """
    Merge bug report documents by conversationId.
    Aggregates issues, tool_usage, and card_usage across multiple documents.
    """
    merged = {}

    for doc in docs:
        cid = doc.get("conversationId", "unknown")

        if cid not in merged:
            merged[cid] = {
                "conversationId": cid,
                "agent_id": doc.get("agent_id"),
                "userId": doc.get("userId"),
                "customerId": doc.get("customerId"),
                "chat_name": doc.get("chat_name"),
                "chat_channel": doc.get("chat_channel"),
                "chat_status": doc.get("chat_status"),
                "issues": [],
                "tool_usage": [],
                "card_usage": {}
            }

        # ✅ Collect issues
        if "issue" in doc and doc["issue"]:
            merged[cid]["issues"].append(doc["issue"])

        # ✅ Collect tool usage
        if "tool_usage" in doc and doc["tool_usage"]:
            if isinstance(doc["tool_usage"], list):
                merged[cid]["tool_usage"].extend(doc["tool_usage"])
            else:
                merged[cid]["tool_usage"].append(doc["tool_usage"])

        # ✅ Merge card clicks
        if "card_usage" in doc and doc["card_usage"]:
            cards = doc["card_usage"] if isinstance(doc["card_usage"], list) else [doc["card_usage"]]
            for card in cards:
                card_name = card.get("card_name")
                if card_name:
                    if card_name not in merged[cid]["card_usage"]:
                        merged[cid]["card_usage"][card_name] = 0
                    merged[cid]["card_usage"][card_name] += card.get("click_count", 1)

    # Convert card_usage dict → list
    for cid in merged:
        merged[cid]["card_usage"] = [
            {"card_name": k, "click_count": v}
            for k, v in merged[cid]["card_usage"].items()
        ]

    return merged


@router.get("/sotc/Sotc_bug_logs_by_conversation/{conversation_id}")
def get_bug_logs_by_conversation(conversation_id: str):
    """
    Retrieve all bug reports for a specific conversation.
    """
    try:
        if not es.indices.exists(index=SOTC_BUG_INDEX_NAME):
            raise HTTPException(
                status_code=404,
                detail=f"Bug index '{SOTC_BUG_INDEX_NAME}' not found."
            )

        search_body = {
            "query": {"term": {"conversationId": conversation_id}},
            "size": 1000,
            "sort": [{"timestamp": {"order": "desc"}}]
        }

        response = es.search(index=SOTC_BUG_INDEX_NAME, body=search_body)
        docs = [hit["_source"] for hit in response["hits"]["hits"]]

        if not docs:
            return {
                "status": "not_found",
                "message": f"No bug reports found for conversation {conversation_id}",
                "results": []
            }

        # Merge results
        merged = _merge_bug_reports(docs)

        return {
            "status": "success",
            "total": len(docs),
            "conversation_id": conversation_id,
            "results": list(merged.values())
        }

    except Exception as e:
        logging.error(f"Error retrieving bug logs for conversation {conversation_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sotc/Sotc_bug_analytics")
def get_bug_analytics(filters: BugReportFilterRequest = Body(default=BugReportFilterRequest())):
    """
    Get aggregated bug analytics for a given date range.
    Returns:
    - Total issues by type
    - Most affected conversations
    - Tool usage patterns
    - Card interaction metrics
    """
    try:
        if not es.indices.exists(index=SOTC_BUG_INDEX_NAME):
            raise HTTPException(
                status_code=404,
                detail=f"Bug index '{SOTC_BUG_INDEX_NAME}' not found."
            )

        # Build aggregation query
        agg_query = {
            "query": {"match_all": {}},
            "size": 0,
            "aggs": {
                "issue_types": {
                    "terms": {
                        "field": "issue.issue_type",
                        "size": 100
                    }
                },
                "tool_names": {
                    "terms": {
                        "field": "issue.tool_name",
                        "size": 100
                    }
                },
                "conversations_with_issues": {
                    "terms": {
                        "field": "conversationId",
                        "size": 100
                    }
                }
            }
        }

        # Add date range if provided
        if filters.start_date and filters.end_date:
            agg_query["query"] = {
                "range": {
                    "timestamp": {
                        "gte": filters.start_date,
                        "lte": filters.end_date
                    }
                }
            }

        response = es.search(index=SOTC_BUG_INDEX_NAME, body=agg_query)
        aggregations = response.get("aggregations", {})

        return {
            "status": "success",
            "total_hits": response["hits"]["total"]["value"],
            "analytics": {
                "issue_types": [
                    {"type": bucket["key"], "count": bucket["doc_count"]}
                    for bucket in aggregations.get("issue_types", {}).get("buckets", [])
                ],
                "tools_affected": [
                    {"tool": bucket["key"], "count": bucket["doc_count"]}
                    for bucket in aggregations.get("tool_names", {}).get("buckets", [])
                ],
                "conversations_affected": [
                    {"conversation_id": bucket["key"], "issue_count": bucket["doc_count"]}
                    for bucket in aggregations.get("conversations_with_issues", {}).get("buckets", [])
                ]
            }
        }

    except Exception as e:
        logging.error(f"Error retrieving bug analytics: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
from collections import defaultdict
from io import BytesIO
import re
from fastapi import (
    FastAPI,
    HTTPException,
    Query,
    UploadFile,
    BackgroundTasks,
    Body,
    APIRouter,
)
import pandas as pd
from models import (
    Conversation,
    ConversationIdRequest,
    DeleteConversationRequest,
    UserIdRequest,
    UpdateChatNameRequest,
    FilterRequest,
    DateRangeRequest,
)
from typing import Dict, List, Optional, Union, Any
from constants import SOTC_CHAT_INDEX_NAME, es, SOTC_PACKAGE_INDEX
from services import ensure_index_exists
from datetime import datetime
from NewESmapping import conversation_index_mapping
import logging
import json, requests
from dateutil.parser import parse as parse_date
from fastapi.responses import StreamingResponse


router = APIRouter()


def clean_date_fields(data):
    """
    Recursively clean empty string values from date fields.
    Converts empty strings to None, and removes None values.
    """
    if isinstance(data, dict):
        cleaned = {}
        for key, value in data.items():
            if isinstance(value, str) and value == "":
                # Skip empty strings for date-like fields
                continue
            elif isinstance(value, dict):
                cleaned_nested = clean_date_fields(value)
                if cleaned_nested:  # Only add if not empty
                    cleaned[key] = cleaned_nested
            elif isinstance(value, list):
                cleaned_list = []
                for item in value:
                    if isinstance(item, dict):
                        cleaned_item = clean_date_fields(item)
                        if cleaned_item:  # Only add if not empty
                            cleaned_list.append(cleaned_item)
                    else:
                        cleaned_list.append(item)
                if cleaned_list:
                    cleaned[key] = cleaned_list
            else:
                cleaned[key] = value
        return cleaned
    return data


@router.post("/sotc/SOTC_save_conversation")
def save_conversation(conversation_data: dict = Body(...)):
    """
    Save (index) the provided conversation into Elasticsearch.
    - Normalizes empty strings in date fields to None so ES date parsing doesn't fail.
    """
    try:
        # ✅ Convert empty string to `None` for top-level booking_date
        if conversation_data.get("booking_date") == "":
            conversation_data["booking_date"] = None

        # ✅ Ensure `saved_time` exists in `packages_saved`
        for pkg in conversation_data.get("packages_saved", []):
            if "saved_time" not in pkg or not pkg["saved_time"]:
                pkg["saved_time"] = (
                    datetime.utcnow().isoformat()
                )  # Set default time if missing

        # ✅ Clean handoff_history date fields (end_time, request_time) if they are empty strings
        handoff_history = conversation_data.get("handoff_history")
        if handoff_history:
            # handoff_history can be a dict or a list of dicts depending on how you send it
            def _clean_handoff_record(rec: dict):
                # Only convert date fields that are mapped as date in ES
                for date_field in ("end_time", "request_time"):
                    if date_field in rec and rec[date_field] == "":
                        rec[date_field] = None

            if isinstance(handoff_history, list):
                for rec in handoff_history:
                    if isinstance(rec, dict):
                        _clean_handoff_record(rec)
            elif isinstance(handoff_history, dict):
                _clean_handoff_record(handoff_history)

        # (Optional) ✅ You can similarly clean events.timestamp if needed in future
        # events = conversation_data.get("events")
        # if events:
        #     def _clean_event(ev: dict):
        #         if "timestamp" in ev and ev["timestamp"] == "":
        #             ev["timestamp"] = None
        #     if isinstance(events, list):
        #         for ev in events:
        #             if isinstance(ev, dict):
        #                 _clean_event(ev)
        #     elif isinstance(events, dict):
        #         _clean_event(events)

        # Ensure index exists
        if not es.indices.exists(index=SOTC_CHAT_INDEX_NAME):
            es.indices.create(
                index=SOTC_CHAT_INDEX_NAME, body=conversation_index_mapping
            )
            logging.info(f"Index {SOTC_CHAT_INDEX_NAME} created with mapping.")

        doc_id = conversation_data["conversationId"]
        response = es.index(
            index=SOTC_CHAT_INDEX_NAME,
            id=doc_id,
            document=conversation_data,
        )
        return {"status": "success", "result": response}

    except Exception as e:
        logging.error(f"Error saving conversation: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sotc/SOTC_get_conversation")
def get_conversation(body: ConversationIdRequest):
    """
    Retrieve the conversation document with the given conversationId.
    (Changed from GET to POST to accept a JSON body.)
    """
    try:
        if not es.indices.exists(index=SOTC_CHAT_INDEX_NAME):
            raise HTTPException(
                status_code=404, detail=f"Index '{SOTC_CHAT_INDEX_NAME}' not found."
            )

        response = es.get(index=SOTC_CHAT_INDEX_NAME, id=body.conversationId)
        if response.get("found"):
            return {"status": "success", "conversation": response["_source"]}
        else:
            return {
                "status": "not_found",
                "message": f"No conversation found for {body.conversationId}",
            }

    except Exception as e:
        logging.error(f"Error retrieving conversation {body.conversationId}: {str(e)}")
        # Check for a "not_found" phrase in the error to return a 404, if needed
        if "not_found" in str(e).lower():
            raise HTTPException(
                status_code=404, detail=f"Conversation {body.conversationId} not found."
            )
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sotc/SOTC_get_conversation_summaries")
def get_conversation_summaries(body: UserIdRequest):
    """
    Given a userId, return a list of documents with:
      - conversationId
      - chat_name
      - chat_started
      - chat_modified
      - packages_saved (number of saved packages in each conversation)
    sorted by the latest chat_modified in descending order.
    Only include conversations with "chat_status": "active".
    """
    try:
        ensure_index_exists(SOTC_CHAT_INDEX_NAME)

        search_body = {
            "size": 1000,
            "sort": [{"chat_modified": {"order": "desc", "missing": "_last"}}],
            "query": {
                "bool": {
                    "must": [
                        {"term": {"userId": body.userId}},
                        {"term": {"chat_status": "active"}},
                    ]
                }
            },
            "_source": [
                "conversationId",
                "chat_name",
                "chat_started",
                "chat_modified",
                "packages_saved",
            ],
        }

        response = es.search(index=SOTC_CHAT_INDEX_NAME, body=search_body)
        hits = response.get("hits", {}).get("hits", [])

        results = []
        for hit in hits:
            src = hit["_source"]
            item = {
                "conversationId": src.get("conversationId"),
                "chat_name": src.get("chat_name"),
                "chat_started": src.get("chat_started"),
                "chat_modified": src.get("chat_modified", None),
                "packages_saved": len(src.get("packages_saved", [])),
            }
            results.append(item)

        return {"status": "success", "total": len(results), "conversations": results}

    except Exception as e:
        logging.error(f"Error retrieving conversation summaries: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


def fetch_package_details_internally(package_id: str) -> Dict[str, Any]:
    """
    Fetch package details from Elasticsearch by packageId.
    """
    try:
        search_body = {"query": {"term": {"packageId": package_id}}, "size": 1}

        logging.info(
            f"Sending Elasticsearch Query: {json.dumps(search_body, indent=2)}"
        )

        response = es.search(index=SOTC_PACKAGE_INDEX, body=search_body)

        response_dict = response.body if hasattr(response, "body") else response
        logging.info(f"Elasticsearch Response: {json.dumps(response_dict, indent=2)}")

        hits = response_dict.get("hits", {}).get("hits", [])

        if not hits:
            logging.warning(f"Package ID {package_id} not found in Elasticsearch.")
            return {}

        package_data = hits[0]["_source"]

        return {
            "packageName": package_data.get("packageName", "Expired Package"),
            "days": package_data.get("days", ""),
            "cities": package_data.get("cities", []),
            "price": package_data.get("price", ""),
        }

    except Exception as e:
        logging.error(f"Error fetching package details for {package_id}: {str(e)}")
        return {}


@router.post("/sotc/SOTC_get_packages_saved")
def get_packages_saved(body: UserIdRequest):
    """
    Given a userId, return a unique list of saved packages
    from all documents with the same userId, sorted by latest "chat_modified".
    Only include conversations with "chat_status": "active".
    """
    ensure_index_exists(SOTC_CHAT_INDEX_NAME)

    search_body = {
        "size": 1000,
        "sort": [{"chat_modified": {"order": "desc", "missing": "_last"}}],
        "query": {
            "bool": {
                "must": [
                    {"term": {"userId": body.userId}},
                    {"term": {"chat_status": "active"}},
                ]
            }
        },
        "_source": ["packages_saved", "conversationId"],
    }

    response = es.search(index=SOTC_CHAT_INDEX_NAME, body=search_body)
    hits = response.get("hits", {}).get("hits", [])

    combined_results = []
    seen = set()

    for hit in hits:
        doc = hit["_source"]
        conv_id = doc["conversationId"]
        packages = doc.get("packages_saved", [])

        for pkg_obj in packages:
            pkg_id = pkg_obj["packageId"]
            saved_time = pkg_obj.get("saved_time", datetime.utcnow().isoformat())

            combo_key = (conv_id, pkg_id)
            if combo_key in seen:
                continue
            seen.add(combo_key)

            details = fetch_package_details_internally(pkg_id)

            package_info = {
                "packageID": pkg_id,
                "conversationId": conv_id,
                "saved_time": saved_time,
                "packageName": details.get("packageName", "Expired Package"),
                "Destination": list(
                    {
                        c["cityName"]
                        for c in details.get("cities", [])
                        if "cityName" in c
                    }
                ),
                "duration": details.get("days", ""),
                "estimatedcost": details.get("price", ""),
            }

            combined_results.append(package_info)

    return {"status": "success", "packages_saved": combined_results}


@router.post("/sotc/SOTC_update_chat_name")
def update_chat_name(body: UpdateChatNameRequest):
    """
    Update the chat name for a given conversationId.
    """
    try:
        if not es.indices.exists(index=SOTC_CHAT_INDEX_NAME):
            raise HTTPException(
                status_code=404, detail=f"Index '{SOTC_CHAT_INDEX_NAME}' not found."
            )

        # Check if the conversation exists
        response = es.get(index=SOTC_CHAT_INDEX_NAME, id=body.conversationId)
        if not response.get("found"):
            raise HTTPException(
                status_code=404, detail=f"Conversation {body.conversationId} not found."
            )

        # Update the chat name
        update_body = {
            "doc": {
                "chat_name": body.new_chat_name,
                "chat_modified": datetime.utcnow().isoformat(),  # Update modification timestamp
            }
        }
        es.update(index=SOTC_CHAT_INDEX_NAME, id=body.conversationId, body=update_body)

        return {"status": "success", "message": "Chat name updated successfully."}

    except Exception as e:
        logging.error(f"Error updating chat name: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sotc/SOTC_delete_conversation")
def delete_conversation(body: DeleteConversationRequest):
    """
    Soft delete a conversation document by conversationId.
    Instead of deleting the document, update the chat_status to "deleted".
    """
    try:
        if not es.indices.exists(index=SOTC_CHAT_INDEX_NAME):
            raise HTTPException(
                status_code=404, detail=f"Index '{SOTC_CHAT_INDEX_NAME}' not found."
            )

        # Check if the conversation exists
        response = es.get(index=SOTC_CHAT_INDEX_NAME, id=body.conversationId)
        if not response.get("found"):
            raise HTTPException(
                status_code=404, detail=f"Conversation {body.conversationId} not found."
            )

        # Update the chat_status to "deleted"
        update_body = {
            "doc": {
                "chat_status": "deleted",
                "chat_modified": datetime.utcnow().isoformat(),  # Update the modification timestamp
            }
        }
        es.update(index=SOTC_CHAT_INDEX_NAME, id=body.conversationId, body=update_body)

        return {
            "status": "success",
            "message": "Conversation marked as deleted successfully.",
        }

    except Exception as e:
        logging.error(f"Error marking conversation as deleted: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/sotc/SOTC_get_user_conversations")
def get_user_conversations(body: UserIdRequest):
    """
    Retrieve all saved packageIds for the given userId, sorted by latest chat_modified descending.
    """
    try:
        if not es.indices.exists(index=SOTC_CHAT_INDEX_NAME):
            raise HTTPException(
                status_code=404, detail=f"Index '{SOTC_CHAT_INDEX_NAME}' not found."
            )

        search_body = {
            "size": 1000,
            "sort": [{"chat_modified": {"order": "desc", "missing": "_last"}}],
            "query": {"term": {"userId": body.userId}},
            "_source": ["conversationId", "packages_saved.packageId"],
        }

        response = es.search(index=SOTC_CHAT_INDEX_NAME, body=search_body)
        hits = response.get("hits", {}).get("hits", [])

        result = []
        for hit in hits:
            conv_id = hit["_source"].get("conversationId")
            packages = hit["_source"].get("packages_saved", [])
            pkg_ids = [pkg.get("packageId") for pkg in packages if "packageId" in pkg]

            result.append({"conversationId": conv_id, "packageIds": pkg_ids})

        return {"status": "success", "total": len(result), "conversations": result}

    except Exception as e:
        logging.error(
            f"Error retrieving conversations for userId={body.userId}: {str(e)}"
        )
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.post("/sotc/SOTC_get_all_conversations")
def get_all_conversations(filters: FilterRequest = Body(default={})):
    """
    Get conversation summaries with optional filters:
    - chat_started (from, to) — expands to full day if same.
    - booking_date (from, to) — expands to full day if same.
    - chat_channel (exact match).
    - opportunity_id: if provided (checkbox), returns docs that have an opportunity_id.
    - userId: dropdown with options "all", "registered" (userId contains "@"), and "guest" (userId starts with "guest_").
    - only_tool_conversations: if true, return docs where all conversation roles are 'tool'.
    - count (number of documents per page, default 10).
    - page (page number, default 1).
    """
    try:
        if not es.indices.exists(index=SOTC_CHAT_INDEX_NAME):
            raise HTTPException(
                status_code=404, detail=f"Index '{SOTC_CHAT_INDEX_NAME}' not found."
            )

        must_clauses = []

        # --- Chat Started Range ---
        if filters.chat_started_from or filters.chat_started_to:
            from_dt = filters.chat_started_from
            to_dt = filters.chat_started_to
            if (
                filters.chat_started_from
                and filters.chat_started_to
                and filters.chat_started_from == filters.chat_started_to
            ):
                from_dt = (
                    parse_date(filters.chat_started_from)
                    .replace(hour=0, minute=0, second=0, microsecond=0)
                    .isoformat()
                    + "Z"
                )
                to_dt = (
                    parse_date(filters.chat_started_to)
                    .replace(hour=23, minute=59, second=59, microsecond=999999)
                    .isoformat()
                    + "Z"
                )
            range_filter = {}
            if from_dt:
                range_filter["gte"] = from_dt
            if to_dt:
                range_filter["lte"] = to_dt
            must_clauses.append({"range": {"chat_started": range_filter}})

        # --- Booking Date Range ---
        if filters.booking_date_from or filters.booking_date_to:
            from_dt = filters.booking_date_from
            to_dt = filters.booking_date_to
            if (
                filters.booking_date_from
                and filters.booking_date_to
                and filters.booking_date_from == filters.booking_date_to
            ):
                from_dt = (
                    parse_date(filters.booking_date_from)
                    .replace(hour=0, minute=0, second=0, microsecond=0)
                    .isoformat()
                    + "Z"
                )
                to_dt = (
                    parse_date(filters.booking_date_to)
                    .replace(hour=23, minute=59, second=59, microsecond=999999)
                    .isoformat()
                    + "Z"
                )
            range_filter = {}
            if from_dt:
                range_filter["gte"] = from_dt
            if to_dt:
                range_filter["lte"] = to_dt
            must_clauses.append({"range": {"booking_date": range_filter}})

        # --- Chat Channel ---
        if filters.chat_channel:
            must_clauses.append({"term": {"chat_channel": filters.chat_channel}})

        # --- Opportunity ID ---
        if filters.opportunity_id:
            must_clauses.append(
                {
                    "bool": {
                        "must": [{"exists": {"field": "opportunity_id"}}],
                        "must_not": [{"term": {"opportunity_id": ""}}],
                    }
                }
            )

        # --- UserId ---
        if filters.userId and filters.userId.lower() != "all":
            if filters.userId.lower() == "registered":
                must_clauses.append({"wildcard": {"userId": "*@*"}})
            elif filters.userId.lower() == "guest":
                must_clauses.append({"prefix": {"userId": "guest_"}})

        # --- Only Tool Role Conversations ---
        if filters.only_tool_conversations:
            must_clauses.append(
                {
                    "nested": {
                        "path": "conversation",
                        "query": {"term": {"conversation.role": "tool"}},
                    }
                }
            )

        # Final query
        query = (
            {"match_all": {}} if not must_clauses else {"bool": {"must": must_clauses}}
        )

        # Pagination
        page = filters.page or 1
        per_page = filters.count or 10
        offset = (page - 1) * per_page

        # Search body
        search_body = {
            "_source": [
                "conversationId",
                "userId",
                "chat_name",
                "opportunity_id",
                "chat_modified",
            ],
            "from": offset,
            "size": per_page,
            "sort": [{"chat_modified": {"order": "desc", "missing": "_last"}}],
            "query": query,
        }

        response = es.search(index=SOTC_CHAT_INDEX_NAME, body=search_body)
        hits = response.get("hits", {}).get("hits", [])
        summaries = [hit["_source"] for hit in hits]

        return {
            "status": "success",
            "total": response.get("hits", {})
            .get("total", {})
            .get("value", len(summaries)),
            "page": page,
            "per_page": per_page,
            "conversations": summaries,
        }

    except Exception as e:
        logging.error(f"Error retrieving all conversations with filters: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# ✅ Utility Functions
def extract_user_details(conversation):
    details = {"first_name": "", "last_name": "", "email": "", "mobile": ""}
    for msg in conversation:
        if msg["role"] == "user":
            content = msg["content"]
            name_match = re.search(
                r"(?i)name:\s*([\w\s]+?)(?:,?\s*(?:mobile|email|phone):|\s*$)", content
            )
            if name_match:
                name = name_match.group(1).strip()
                names = name.split()
                details["first_name"] = " ".join(names[:-1]) if len(names) > 1 else name
                details["last_name"] = names[-1] if len(names) > 1 else ""
            email_match = re.search(r"(?i)email:\s*([^\s]+@[^\s]+)", content)
            if email_match:
                details["email"] = email_match.group(1).strip()
            mobile_match = re.search(r"(?i)(mobile|phone):\s*(\d+)", content)
            if mobile_match:
                details["mobile"] = mobile_match.group(2).strip()
    return details


def has_tool_message(conversation):
    return "Yes" if any(msg.get("role") == "tool" for msg in conversation) else "No"


def get_last_assistant_args(conversation):
    assistant_msgs = [msg for msg in conversation if msg["role"] == "assistant"]
    if not assistant_msgs:
        return {
            "searchText": "",
            "dates_start": "",
            "dates_end": "",
            "number_of_people": "",
            "number_of_days": "",
            "budget": "",
            "hub": "",
            "phone": "",
        }
    last_msg = assistant_msgs[-1]
    try:
        content_json = json.loads(last_msg["content"])
        args = content_json.get("arguments", {})
        return {
            "searchText": (
                str(args.get("searchText", ""))
                if args.get("searchText") is not None
                else ""
            ),
            "dates_start": (
                str(args.get("dates", {}).get("start", ""))
                if args.get("dates", {}).get("start") is not None
                else ""
            ),
            "dates_end": (
                str(args.get("dates", {}).get("end", ""))
                if args.get("dates", {}).get("end") is not None
                else ""
            ),
            "number_of_people": (
                str(args.get("number_of_people", ""))
                if args.get("number_of_people") is not None
                else ""
            ),
            "number_of_days": (
                str(args.get("number_of_days", ""))
                if args.get("number_of_days") is not None
                else ""
            ),
            "budget": (
                str(args.get("budget", "")) if args.get("budget") is not None else ""
            ),
            "hub": str(args.get("hub", "")) if args.get("hub") is not None else "",
            "phone": (
                str(args.get("phone", "")) if args.get("phone") is not None else ""
            ),
        }
    except json.JSONDecodeError:
        return {
            "searchText": "",
            "dates_start": "",
            "dates_end": "",
            "number_of_people": "",
            "number_of_days": "",
            "budget": "",
            "hub": "",
            "phone": "",
        }


# ✅ API Route
@router.post("/sotc/SOTC_export_conversations")
def export_conversation_summary(request: DateRangeRequest):
    try:
        from_dt = datetime.strptime(request.from_date, "%Y-%m-%d")
        to_dt = datetime.strptime(request.to_date, "%Y-%m-%d")

        if from_dt > to_dt:
            raise HTTPException(
                status_code=400, detail="from_date cannot be after to_date."
            )

        from_date = from_dt.strftime("%Y-%m-%dT00:00:00Z")
        to_date = to_dt.strftime("%Y-%m-%dT23:59:59Z")

        query = {
            "bool": {
                "must": [
                    {"range": {"chat_started": {"gte": from_date, "lte": to_date}}},
                    {"term": {"chat_channel": "ChatBot"}},
                ]
            }
        }

        scroll = es.search(
            index=SOTC_CHAT_INDEX_NAME, body={"query": query}, scroll="2m", size=1000
        )
        sid = scroll["_scroll_id"]
        scroll_size = len(scroll["hits"]["hits"])
        results = scroll["hits"]["hits"]

        while scroll_size > 0:
            scroll = es.scroll(scroll_id=sid, scroll="2m")
            sid = scroll["_scroll_id"]
            scroll_size = len(scroll["hits"]["hits"])
            results.extend(scroll["hits"]["hits"])

        stats_by_date = defaultdict(
            lambda: {
                "total_convos": 0,
                "packages_shown": 0,
                "opportunities": 0,
                "empty_convos": 0,
            }
        )
        data_by_date = defaultdict(list)

        exact_assistant_content = '{"message": "Hi, I am Ezy, your personal Gen AI holiday assistant from SOTC. Where would you like to travel?", "name": "packageSearch", "arguments": {"searchText": "", "dates": {"start": "", "end": ""}, "number_of_people": null, "number_of_days": null, "budget": "", "hub": "", "first_name": "", "last_name": "", "email": "", "phone": ""}}'

        for hit in results:
            source = hit["_source"]
            date_key_summary = datetime.strptime(
                source["chat_started"][:10], "%Y-%m-%d"
            ).strftime("%d-%m-%y")
            stats_by_date[date_key_summary]["total_convos"] += 1
            if source.get("opportunity_id"):
                stats_by_date[date_key_summary]["opportunities"] += 1
            for msg in source.get("conversation", []):
                if msg.get("role") == "tool":
                    stats_by_date[date_key_summary]["packages_shown"] += 1
                    break

            date_key_detail = source["chat_started"][:10]
            user_details = extract_user_details(source["conversation"])
            assistant_args = get_last_assistant_args(source["conversation"])
            packages_shown = has_tool_message(source["conversation"])

            category = ""
            if (
                len(source["conversation"]) == 2
                and source["conversation"][0]["role"] == "user"
                and source["conversation"][0]["content"] == "Hello"
                and source["conversation"][1]["role"] == "assistant"
                and source["conversation"][1]["content"] == exact_assistant_content
            ):
                category = "EMPTY"
                stats_by_date[date_key_summary]["empty_convos"] += 1

            conversation_data = {
                "Conversation ID": source["conversationId"],
                "Opportunity ID": source.get("opportunity_id", ""),
                "Packages Shown": packages_shown,
                "Search Text": assistant_args["searchText"],
                "Start Date": assistant_args["dates_start"],
                "End Date": assistant_args["dates_end"],
                "Number of People": assistant_args["number_of_people"],
                "Number of Days": assistant_args["number_of_days"],
                "Budget": assistant_args["budget"],
                "Hub": assistant_args["hub"],
                "First Name": user_details["first_name"],
                "Last Name": user_details["last_name"],
                "Email": user_details["email"],
                "Mobile": user_details["mobile"],
                "Phone": assistant_args["phone"],
                "Category": category,
            }
            data_by_date[date_key_detail].append(conversation_data)

        summary_df = pd.DataFrame(
            [
                {
                    "Date": date,
                    "Total Convos": d["total_convos"],
                    "Packages Shown Count": d["packages_shown"],
                    "Opportunities": d["opportunities"],
                    "Empty Bucket": d["empty_convos"],
                }
                for date, d in sorted(stats_by_date.items())
            ]
        )

        summary_stats_df = pd.DataFrame(
            {
                "Metric": [
                    "Total Conversations",
                    "Total Packages Shown",
                    "Total Opportunities",
                    "Total Empty Conversations",
                ],
                "Count": [
                    summary_df["Total Convos"].sum(),
                    summary_df["Packages Shown Count"].sum(),
                    summary_df["Opportunities"].sum(),
                    summary_df["Empty Bucket"].sum(),
                ],
            }
        )

        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            summary_df.to_excel(
                writer, sheet_name="Conversation Summary", index=False, startrow=0
            )
            summary_stats_df.to_excel(
                writer,
                sheet_name="Conversation Summary",
                index=False,
                startrow=len(summary_df) + 2,
            )
            for date_key in sorted(data_by_date):
                df = pd.DataFrame(data_by_date[date_key])
                df.to_excel(writer, sheet_name=date_key, index=False)
        output.seek(0)

        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename=conversations_{request.from_date}_to_{request.to_date}.xlsx",
                "Content-Type": "application/octet-stream",
            },
        )

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error processing request: {str(e)}"
        )
