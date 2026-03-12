from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from elasticsearch import Elasticsearch
import pandas as pd
import re
import json
from collections import defaultdict, Counter
from datetime import datetime
from io import BytesIO
from typing import Optional
import uvicorn
import base64

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://report.atirath.com",  # ✅ Your production frontend domain
        "http://localhost:3000",  # Optional: For local development
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


es = Elasticsearch(
    ["http://localhost:9200"], basic_auth=("elastic", "iweXVQuayXSCP9PFkHcZ")
)


class DateRangeRequest(BaseModel):
    from_date: str
    to_date: str
    index_name: str  # NEW FIELD


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


@app.post("/dashboard/conversation_stats")
async def export_conversations(request: DateRangeRequest):
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
            index=request.index_name, body={"query": query}, scroll="2m", size=1000
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
        all_search_texts = []

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

            # Collect search text for most searched destinations
            search_text_cleaned = assistant_args["searchText"].strip().lower()
            if search_text_cleaned:
                all_search_texts.append(search_text_cleaned)

            # Updated empty conversation logic
            category = ""
            if (
                len(source["conversation"]) == 2
                and source["conversation"][0].get("role") == "user"
                and source["conversation"][0].get("content", "").strip().lower() in {"hello", "hi"}
                and source["conversation"][1].get("role") == "assistant"
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

        summary_data = [
            {
                "date": date,
                "total": d["total_convos"],
                "packages": d["packages_shown"],
                "opportunities": d["opportunities"],
                "empty": d["empty_convos"],
            }
            for date, d in sorted(stats_by_date.items())
        ]

        summary_df = pd.DataFrame(summary_data)
        summary_stats_df = pd.DataFrame(
            {
                "Metric": [
                    "Total Conversations",
                    "Total Packages Shown",
                    "Total Opportunities",
                    "Total Empty Conversations",
                ],
                "Count": [
                    summary_df["total"].sum(),
                    summary_df["packages"].sum(),
                    summary_df["opportunities"].sum(),
                    summary_df["empty"].sum(),
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

        encoded_file = base64.b64encode(output.read()).decode("utf-8")

        # Get top 5 most searched destinations
        counter = Counter(all_search_texts)
        most_searched_destinations = [
            text.title() for text, _ in counter.most_common(5)
        ]

        return JSONResponse(
            content={
                "status": "success",
                "summary": summary_data,
                "excel_base64": encoded_file,
                "filename": f"conversations_{request.from_date}_to_{request.to_date}.xlsx",
                "most_searched_destinations": most_searched_destinations,
            }
        )

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error processing request: {str(e)}"
        )


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8003)
