from fastapi import HTTPException, APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from collections import defaultdict, Counter
from datetime import datetime
from io import BytesIO
from typing import Optional, List, Dict, Any, Tuple
import pandas as pd
import re
import json
import base64
import asyncio
import httpx

from constants import es

router = APIRouter()


class DateRangeRequest(BaseModel):
    from_date: str
    to_date: str
    index_name: str
    summary_only: Optional[bool] = False


# ----------------------- Helpers -----------------------

API_KEY = "D4FVHV9Fzm3wzH0Xq5pJPZ20SmDtbbzg9HC7e4I66FxNvgxvqYZK9rIpJzqEZIuz5kALAs"


def _agent_base_url(index_name: str) -> str:
    # Pick the correct base URL by index
    if index_name == "sotc_user_conversations":
        return "https://travbridge.atirath.com/sotc/langchain/agent/"
    return "https://travbridge.atirath.com/chat/langchain/agent/"


async def _get_agent_name(
    agent_id: str,
    base_url: str,
    client: httpx.AsyncClient,
    cache: Dict[str, str],
) -> str:
    """
    Resolve a single agent_id -> agent_name with caching.
    Falls back to the original ID if the fetch fails or name is missing.
    """
    if not agent_id:
        return ""
    if agent_id in cache:
        return cache[agent_id]

    try:
        resp = await client.get(
            f"{base_url}{agent_id}",
            headers={"X-API-Key": API_KEY},
            timeout=10.0,
        )
        if resp.status_code == 200:
            data = resp.json() or {}
            name = (data.get("agent") or {}).get("agent_name") or agent_id
            cache[agent_id] = name
            return name
    except Exception:
        pass

    cache[agent_id] = agent_id
    return agent_id


def extract_user_details(conversation):
    details = {"first_name": "", "last_name": "", "email": "", "mobile": ""}
    for msg in conversation or []:
        if not isinstance(msg, dict):
            continue
        if msg.get("role") == "user":
            content = msg.get("content", "") or ""
            name_match = re.search(
                r"(?i)name:\s*([\w\s]+?)(?:,?\s*(?:mobile|email|phone):|\s*$)", content
            )
            if name_match:
                name = name_match.group(1).strip()
                parts = name.split()
                details["first_name"] = " ".join(parts[:-1]) if len(parts) > 1 else name
                details["last_name"] = parts[-1] if len(parts) > 1 else ""
            email_match = re.search(r"(?i)email:\s*([^\s]+@[^\s]+)", content)
            if email_match:
                details["email"] = email_match.group(1).strip()
            mobile_match = re.search(r"(?i)(mobile|phone):\s*([+\-\s\d]+)", content)
            if mobile_match:
                details["mobile"] = mobile_match.group(2).strip()
    return details


def any_tool_message(conversation) -> bool:
    return isinstance(conversation, list) and any(
        isinstance(m, dict) and (m.get("role") == "tool" or m.get("role") == "function_call") for m in conversation
    )


def has_tool_message_label(conversation) -> str:
    return "Yes" if any_tool_message(conversation) else "No"


def count_user_messages(conversation) -> int:
    return sum(
        1
        for msg in (conversation or [])
        if isinstance(msg, dict) and msg.get("role") == "user"
    )


def get_last_assistant_args(conversation):
    assistant_msgs = [
        m
        for m in (conversation or [])
        if isinstance(m, dict) and m.get("role") == "assistant"
    ]
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
    try:
        content_json = json.loads(assistant_msgs[-1].get("content", "") or "{}")
        args = content_json.get("arguments", {}) or {}
        return {
            "searchText": str(args.get("searchText", "") or ""),
            "dates_start": str((args.get("dates") or {}).get("start", "") or ""),
            "dates_end": str((args.get("dates") or {}).get("end", "") or ""),
            "number_of_people": str(args.get("number_of_people", "") or ""),
            "number_of_days": str(args.get("number_of_days", "") or ""),
            "budget": str(args.get("budget", "") or ""),
            "hub": str(args.get("hub", "") or ""),
            "phone": str(args.get("phone", "") or ""),
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


# ---- Live agent helpers ----


def _bool_any(events: List[Dict[str, Any]], key: str) -> bool:
    return any((e.get("event_type") or "").lower() == key for e in (events or []))


def _has_event(events: List[Dict[str, Any]], *types) -> bool:
    wanted = {t.lower() for t in types}
    return any((e.get("event_type") or "").lower() in wanted for e in (events or []))


def _latest_handoff(handoff_history: List[Dict[str, Any]]) -> Dict[str, Any]:
    return (handoff_history or [])[-1] if handoff_history else {}


def _end_reason_is(last_handoff: Dict[str, Any], *values) -> bool:
    v = (last_handoff or {}).get("end_reason", "") or ""
    return v.lower() in {x.lower() for x in values}


def derive_live_status(source: Dict[str, Any]) -> str:
    """
    Determine the last known live chat status.

    Priority of detection:
      1. success              → agent accepted and chat connected
      2. agent_ignored        → agent assigned but never accepted agent_not_accepted
      3. agent_busy           → all logged-in agents busy
      4. not_available        → no agent available in pool
      5. requested_only       → user requested but no agent events
    """
    events = source.get("events") or []
    handoff_history = source.get("handoff_history") or []
    last = _latest_handoff(handoff_history)

    # --- flags from event list ---
    has_request = _has_event(events, "user_request_agent")
    has_assign = _has_event(events, "agent_assigned")
    has_accept = _has_event(events, "agent_accepted") or bool(last.get("accept_time"))
    has_busy = _has_event(events, "agent_busy")
    has_not_avail = _has_event(events, "agent_not_available")
    has_not_accept = _has_event(events, "agent_not_accepted")
    has_user_left = _has_event(events, "user_left")

    # 1️⃣ success → accepted by agent
    if has_accept:
        return "success"

    # 2️⃣ assigned but agent never accepted (ignored / timeout)
    if has_not_accept:
        return "agent_not_accepted"

    if has_assign and not has_accept and has_user_left:
        return "user_left_before_assign"

    # 3️⃣ agents busy
    if has_busy:
        return "agent_busy"

    # 4️⃣ no agents available
    if has_not_avail:
        return "not_available"

    # 5️⃣ user requested only
    if has_request:
        return "requested_only"

    # default
    return "requested_only"


def live_detail_row(
    source: Dict[str, Any],
    final_status: str,
    date_key_detail: str,
    attended_agent_names: List[str],
) -> Dict[str, Any]:
    last = _latest_handoff(source.get("handoff_history"))
    return {
        "Date": date_key_detail,
        "Conversation ID": source.get("conversationId", ""),
        "User Type": source.get("is_utm_user", ""),
        "Agent ID": ", ".join(source.get("attended_agent_list") or []),
        "Attended Agents": ", ".join(attended_agent_names),
        "Final Status": final_status,
        "Opportunity ID": source.get("opportunity_id", "") or "",
        # times from handoff
        "Request Time": last.get("request_time", ""),
        "Assign Time": last.get("assign_time", ""),
        "Accept Time": last.get("accept_time", ""),
        "End Time": last.get("end_time", ""),
        "Wait (sec)": last.get("wait_time_sec", ""),
        "Duration (sec)": last.get("duration_sec", ""),
        "End Reason": last.get("end_reason", ""),
    }


# ----------------------- Route -----------------------


@router.post("/v1/conversation_stats")
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
        sid = scroll.get("_scroll_id")
        results = scroll["hits"]["hits"]
        while True:
            scroll = es.scroll(scroll_id=sid, scroll="2m")
            hits = scroll["hits"]["hits"]
            if not hits:
                break
            results.extend(hits)
            sid = scroll.get("_scroll_id")

        # --- aggregation containers ---
        stats_by_date = defaultdict(
            lambda: {
                "total_convos": 0,
                "packages_shown": 0,
                "empty_convos": 0,
                "regular_total": 0,
                "regular_packages": 0,
                "regular_empty": 0,
                "utm_total": 0,
                "utm_packages": 0,
                "utm_empty": 0,
                "regular_single_user_msg": 0,
                "regular_double_user_msg": 0,
                "regular_triple_user_msg": 0,
                "utm_single_user_msg": 0,
                "utm_double_user_msg": 0,
                "utm_triple_user_msg": 0,
                # Live-agent counts
                "live_total": 0,
                "live_success": 0,
                "live_timeout": 0,
                "live_not_available": 0,
                # New flows
                "live_agent_not_accepted": 0,
                "live_user_left_after_assign": 0,
                "live_user_left_before_assign": 0,
                "live_agent_busy": 0,
            }
        )
        # Track unique opportunity IDs per date
        unique_opps_by_date = defaultdict(set)
        unique_regular_opps_by_date = defaultdict(set)
        unique_utm_opps_by_date = defaultdict(set)
        unique_live_opps_by_date = defaultdict(set)
        data_by_date = defaultdict(list)
        all_search_texts = []

        # live details
        live_details: List[Dict[str, Any]] = []

        # base URL + cache for agent names
        base_url = _agent_base_url(request.index_name)
        agent_name_cache: Dict[str, str] = {}

        # Use a single HTTP client; resolve agent names as needed
        async with httpx.AsyncClient() as client:
            for hit in results:
                source = hit.get("_source", {}) or {}
                chat_started = source.get("chat_started")
                if not chat_started:
                    continue

                try:
                    dt_obj = datetime.strptime(str(chat_started)[:10], "%Y-%m-%d")
                except Exception:
                    dt_obj = datetime.fromisoformat(str(chat_started).split("T")[0])
                date_key_summary = dt_obj.strftime("%d-%m-%y")
                date_key_detail = dt_obj.strftime("%Y-%m-%d")

                stats_by_date[date_key_summary]["total_convos"] += 1

                conversation = source.get("conversation") or []
                opp_id = source.get("opportunity_id")
                is_utm = source.get("is_utm_user")  # "regular" | "utm"
                roles = {
                    m.get("role")
                    for m in conversation
                    if isinstance(m, dict) and "role" in m
                }

                # package/opportunity/empty
                if any_tool_message(conversation):
                    stats_by_date[date_key_summary]["packages_shown"] += 1
                if opp_id:
                    unique_opps_by_date[date_key_summary].add(opp_id)
                is_empty = (len(conversation) == 2) and (roles == {"user", "assistant"})
                if is_empty:
                    stats_by_date[date_key_summary]["empty_convos"] += 1

                # segment metrics
                total_user_msgs = count_user_messages(conversation)
                user_msgs_after_opener = max(total_user_msgs - 1, 0)

                def bump_segment(prefix: str):
                    stats_by_date[date_key_summary][f"{prefix}_total"] += 1
                    if opp_id:
                        if prefix == "regular":
                            unique_regular_opps_by_date[date_key_summary].add(opp_id)
                        elif prefix == "utm":
                            unique_utm_opps_by_date[date_key_summary].add(opp_id)
                    if any_tool_message(conversation):
                        stats_by_date[date_key_summary][f"{prefix}_packages"] += 1
                    if is_empty:
                        stats_by_date[date_key_summary][f"{prefix}_empty"] += 1
                    if user_msgs_after_opener == 1:
                        stats_by_date[date_key_summary][
                            f"{prefix}_single_user_msg"
                        ] += 1
                    elif user_msgs_after_opener == 2:
                        stats_by_date[date_key_summary][
                            f"{prefix}_double_user_msg"
                        ] += 1
                    elif user_msgs_after_opener == 3:
                        stats_by_date[date_key_summary][
                            f"{prefix}_triple_user_msg"
                        ] += 1

                if is_utm == "regular":
                    bump_segment("regular")
                elif is_utm == "utm":
                    bump_segment("utm")

                # Live agent metrics
                if source.get("is_with_agent") is True:
                    stats_by_date[date_key_summary]["live_total"] += 1
                    final_status = derive_live_status(source)

                    if final_status == "success":
                        stats_by_date[date_key_summary]["live_success"] += 1
                    elif final_status == "timeout":
                        stats_by_date[date_key_summary]["live_timeout"] += 1
                    elif final_status == "not_available":
                        stats_by_date[date_key_summary]["live_not_available"] += 1
                    elif final_status == "agent_not_accepted":
                        stats_by_date[date_key_summary]["live_agent_not_accepted"] += 1
                    elif final_status == "user_left_after_assign":
                        stats_by_date[date_key_summary][
                            "live_user_left_after_assign"
                        ] += 1
                    elif final_status == "user_left_before_assign":
                        stats_by_date[date_key_summary][
                            "live_user_left_before_assign"
                        ] += 1
                    elif final_status == "agent_busy":
                        stats_by_date[date_key_summary]["live_agent_busy"] += 1

                    if opp_id:
                        unique_live_opps_by_date[date_key_summary].add(opp_id)

                    # resolve attended agent IDs -> names
                    attended_ids = source.get("attended_agent_list") or []
                    if attended_ids:
                        attended_names = await asyncio.gather(
                            *[
                                _get_agent_name(aid, base_url, client, agent_name_cache)
                                for aid in attended_ids
                            ]
                        )
                    else:
                        attended_names = []

                    # details row with names instead of IDs
                    live_details.append(
                        live_detail_row(
                            source,
                            final_status,
                            date_key_detail,
                            attended_agent_names=attended_names,
                        )
                    )

                # detail sheet row
                user_details = extract_user_details(conversation)
                assistant_args = get_last_assistant_args(conversation)
                if (assistant_args["searchText"] or "").strip():
                    all_search_texts.append(
                        assistant_args["searchText"].strip().lower()
                    )

                category = "EMPTY" if is_empty else ""
                data_by_date[date_key_detail].append(
                    {
                        "Conversation ID": source.get("conversationId", ""),
                        "Opportunity ID": opp_id or "",
                        "Packages Shown": has_tool_message_label(conversation),
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
                        "User Type": is_utm or "",
                        "Live Transfer?": (
                            "Yes" if source.get("is_with_agent") else "No"
                        ),
                    }
                )

        # ---- build response frames ----
        # Per-day summary with percentage metrics
        summary_data: List[Dict[str, Any]] = []
        for date, d in sorted(stats_by_date.items()):
            total = d["total_convos"] or 0
            empty = d["empty_convos"] or 0
            opp = len(unique_opps_by_date[date])

            non_empty = max(total - empty, 0)

            # 1) Empty Conversations %
            empty_pct = (empty / total * 100.0) if total else 0.0

            # 2) Conversations to leads %
            # (Non empty conversations / Total Opportunities) * 100
            conv_to_leads_pct = (non_empty / opp * 100.0) if opp else 0.0

            summary_data.append(
                {
                    "date": date,
                    "total": total,
                    "packages": d["packages_shown"],
                    "opportunities": opp,
                    "empty": empty,
                    "empty_pct": round(empty_pct, 2),
                    "conversations_to_leads_pct": round(conv_to_leads_pct, 2),
                }
            )

        segment_rows = [
            {
                "date": date,
                "regular_total": d["regular_total"],
                "regular_packages": d["regular_packages"],
                "regular_opportunities": len(unique_regular_opps_by_date[date]),
                "regular_empty": d["regular_empty"],
                "utm_total": d["utm_total"],
                "utm_packages": d["utm_packages"],
                "utm_opportunities": len(unique_utm_opps_by_date[date]),
                "utm_empty": d["utm_empty"],
            }
            for date, d in sorted(stats_by_date.items())
        ]
        live_rows = [
            {
                "date": date,
                "live_total": d["live_total"],
                "live_success": d["live_success"],
                "live_timeout": d["live_timeout"],
                "live_not_available": d["live_not_available"],
                "live_agent_not_accepted": d["live_agent_not_accepted"],
                "live_user_left_after_assign": d["live_user_left_after_assign"],
                "live_user_left_before_assign": d["live_user_left_before_assign"],
                "live_agent_busy": d["live_agent_busy"],
                "live_opportunities": len(unique_live_opps_by_date[date]),
            }
            for date, d in sorted(stats_by_date.items())
        ]

        summary_df = pd.DataFrame(summary_data)
        segment_df = pd.DataFrame(segment_rows)
        live_df = pd.DataFrame(live_rows)

        # --- Build the Excel-specific "Live Transfer Summary" view ---
        # Rename columns and remove the ones you don't want in the Excel sheet.
        rename_map = {
            "live_total": "Total",
            "live_success": "Sucess",  # (as requested)
            "live_not_available": "Agents Unavailable",
            "live_agent_not_accepted": "Agent Not Accepted",
            "live_user_left_before_assign": "Requested Only",
            "live_agent_busy": "Agents Busy",
        }
        # Start with a copy so original remains intact
        live_df_excel = live_df.copy()

        # Drop the undesired columns from the Excel version
        cols_to_drop = [
            "live_timeout",
            "live_user_left_after_assign",
            "live_opportunities",
        ]
        live_df_excel = live_df_excel.drop(
            columns=[c for c in cols_to_drop if c in live_df_excel.columns]
        )

        # Rename the remaining columns
        live_df_excel = live_df_excel.rename(columns=rename_map)

        # Reorder columns for readability (only include ones that exist)
        desired_order = [
            "date",
            "Total",
            "Sucess",
            "Agents Unavailable",
            "Agent Not Accepted",
            "Requested Only",
            "Agents Busy",
        ]
        live_df_excel = live_df_excel[
            [c for c in desired_order if c in live_df_excel.columns]
        ]

        live_details_df = pd.DataFrame(live_details)

        # --- Overall summary stats block (bottom of Conversation Summary sheet) ---
        if summary_df.empty:
            total_convos_sum = 0
            packages_sum = 0
            opportunities_sum = 0
            empty_sum = 0
        else:
            total_convos_sum = int(summary_df["total"].sum())
            packages_sum = int(summary_df["packages"].sum())
            opportunities_sum = int(summary_df["opportunities"].sum())
            empty_sum = int(summary_df["empty"].sum())

        non_empty_sum = max(total_convos_sum - empty_sum, 0)

        # Overall percentages
        overall_empty_pct = (
            empty_sum / total_convos_sum * 100.0 if total_convos_sum else 0.0
        )
        overall_conv_to_leads_pct = (
            non_empty_sum / opportunities_sum * 100.0 if opportunities_sum else 0.0
        )

        summary_stats_df = pd.DataFrame(
            {
                "Metric": [
                    "Total Conversations",
                    "Total Packages Shown",
                    "Total Opportunities",
                    "Total Empty Conversations",
                    "Empty Conversations (%)",
                    "Conversations to Leads (%)",
                ],
                "Count": [
                    total_convos_sum,
                    packages_sum,
                    opportunities_sum,
                    empty_sum,
                    round(overall_empty_pct, 2),
                    round(overall_conv_to_leads_pct, 2),
                ],
            }
        )

        detailed_buckets_df = pd.DataFrame(
            [
                {
                    "date": date,
                    "regular_single_user_msg": d["regular_single_user_msg"],
                    "regular_double_user_msg": d["regular_double_user_msg"],
                    "regular_triple_user_msg": d["regular_triple_user_msg"],
                    "utm_single_user_msg": d["utm_single_user_msg"],
                    "utm_double_user_msg": d["utm_double_user_msg"],
                    "utm_triple_user_msg": d["utm_triple_user_msg"],
                }
                for date, d in sorted(stats_by_date.items())
            ]
        )

        # ---- Excel (optional) ----
        encoded_file = None
        if not request.summary_only:
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
                segment_df.to_excel(writer, sheet_name="Segment Summary", index=False)
                detailed_buckets_df.to_excel(
                    writer, sheet_name="Detailed Stats", index=False
                )
                # Write the Excel-specific renamed/pruned version here:
                live_df_excel.to_excel(
                    writer, sheet_name="Live Transfer Summary", index=False
                )
                live_details_df.to_excel(
                    writer, sheet_name="Live Transfer Details", index=False
                )
                for date_key in sorted(data_by_date):
                    pd.DataFrame(data_by_date[date_key]).to_excel(
                        writer, sheet_name=date_key, index=False
                    )
            output.seek(0)
            encoded_file = base64.b64encode(output.read()).decode("utf-8")

        counter = Counter(all_search_texts)
        most_searched_destinations = [t.title() for t, _ in counter.most_common(5)]

        # overall live totals (unchanged; still based on the original live_df)
        live_totals = {
            "total": int(live_df["live_total"].sum()) if not live_df.empty else 0,
            "success": int(live_df["live_success"].sum()) if not live_df.empty else 0,
            "timeout": int(live_df["live_timeout"].sum()) if not live_df.empty else 0,
            "not_available": (
                int(live_df["live_not_available"].sum()) if not live_df.empty else 0
            ),
            "agent_not_accepted": (
                int(live_df["live_agent_not_accepted"].sum())
                if not live_df.empty
                else 0
            ),
            "user_left_after_assign": (
                int(live_df["live_user_left_after_assign"].sum())
                if not live_df.empty
                else 0
            ),
            "user_left_before_assign": (
                int(live_df["live_user_left_before_assign"].sum())
                if not live_df.empty
                else 0
            ),
            "agent_busy": (
                int(live_df["live_agent_busy"].sum()) if not live_df.empty else 0
            ),
            "opportunities": (
                int(live_df["live_opportunities"].sum()) if not live_df.empty else 0
            ),
        }

        resp = {
            "status": "success",
            "summary": summary_data,
            "segment_summary": segment_rows,
            "live_transfer": {
                "by_date": live_rows,
                "totals": live_totals,
                "details": live_details,  # list of objects for UI modal
            },
            "most_searched_destinations": most_searched_destinations,
        }
        if encoded_file:
            resp["excel_base64"] = encoded_file
            resp["filename"] = (
                f"conversations_{request.from_date}_to_{request.to_date}.xlsx"
            )

        return JSONResponse(content=resp)

    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error processing request: {str(e)}"
        )


@router.post("/v1/voicebot_conversation_stats")
async def export_voicebot_conversations(request: DateRangeRequest):
    try:
        # ---- validate + normalise dates ----
        from_dt = datetime.strptime(request.from_date, "%Y-%m-%d")
        to_dt = datetime.strptime(request.to_date, "%Y-%m-%d")
        if from_dt > to_dt:
            raise HTTPException(
                status_code=400, detail="from_date cannot be after to_date."
            )

        from_date = from_dt.strftime("%Y-%m-%dT00:00:00Z")
        to_date = to_dt.strftime("%Y-%m-%dT23:59:59Z")

        # ---- ElasticSearch query: VoiceBot only ----
        query = {
            "bool": {
                "must": [
                    {"range": {"chat_started": {"gte": from_date, "lte": to_date}}},
                    {"term": {"chat_channel": "VoiceBot"}},
                ]
            }
        }

        scroll = es.search(
            index=request.index_name,
            body={"query": query},
            scroll="2m",
            size=1000,
        )
        sid = scroll.get("_scroll_id")
        results = scroll["hits"]["hits"]

        while True:
            scroll = es.scroll(scroll_id=sid, scroll="2m")
            hits = scroll["hits"]["hits"]
            if not hits:
                break
            results.extend(hits)
            sid = scroll.get("_scroll_id")

        # ───────────────────── aggregation containers ─────────────────────
        # NOTE: only "regular" flow, no UTM and no live-agent stats
        stats_by_date = defaultdict(
            lambda: {
                "total_convos": 0,
                "packages_shown": 0,
                "empty_convos": 0,
                # regular flow only
                "regular_total": 0,
                "regular_packages": 0,
                "regular_empty": 0,
                # detailed buckets
                "regular_single_user_msg": 0,
                "regular_double_user_msg": 0,
                "regular_triple_user_msg": 0,
            }
        )

        # Track unique opportunity IDs per date
        unique_opps_by_date = defaultdict(set)
        unique_regular_opps_by_date = defaultdict(set)

        data_by_date = defaultdict(list)
        all_search_texts: List[str] = []

        # ───────────────────── iterate ES hits ─────────────────────
        for hit in results:
            source = hit.get("_source", {}) or {}
            chat_started = source.get("chat_started")
            if not chat_started:
                continue

            # date keys: dd-mm-yy for summary, YYYY-mm-dd for detail
            try:
                dt_obj = datetime.strptime(str(chat_started)[:10], "%Y-%m-%d")
            except Exception:
                dt_obj = datetime.fromisoformat(str(chat_started).split("T")[0])

            date_key_summary = dt_obj.strftime("%d-%m-%y")
            date_key_detail = dt_obj.strftime("%Y-%m-%d")

            stats_by_date[date_key_summary]["total_convos"] += 1

            conversation = source.get("conversation") or []
            opp_id = source.get("opportunity_id")

            roles = {
                m.get("role")
                for m in conversation
                if isinstance(m, dict) and "role" in m
            }

            # ---- package / opportunity / empty ----
            if any_tool_message(conversation):
                stats_by_date[date_key_summary]["packages_shown"] += 1
            if opp_id:
                unique_opps_by_date[date_key_summary].add(opp_id)

            is_empty = (len(conversation) == 2) and (roles == {"user", "assistant"})
            if is_empty:
                stats_by_date[date_key_summary]["empty_convos"] += 1

            # ---- segment metrics: regular only ----
            total_user_msgs = count_user_messages(conversation)
            user_msgs_after_opener = max(total_user_msgs - 1, 0)

            # treat everything as "regular" flow for VoiceBot
            prefix = "regular"
            stats_by_date[date_key_summary][f"{prefix}_total"] += 1
            if opp_id:
                unique_regular_opps_by_date[date_key_summary].add(opp_id)
            if any_tool_message(conversation):
                stats_by_date[date_key_summary][f"{prefix}_packages"] += 1
            if is_empty:
                stats_by_date[date_key_summary][f"{prefix}_empty"] += 1

            if user_msgs_after_opener == 1:
                stats_by_date[date_key_summary][
                    f"{prefix}_single_user_msg"
                ] += 1
            elif user_msgs_after_opener == 2:
                stats_by_date[date_key_summary][
                    f"{prefix}_double_user_msg"
                ] += 1
            elif user_msgs_after_opener == 3:
                stats_by_date[date_key_summary][
                    f"{prefix}_triple_user_msg"
                ] += 1

            # ---- detail row for per-day sheet ----
            user_details = extract_user_details(conversation)
            assistant_args = get_last_assistant_args(conversation)

            if (assistant_args["searchText"] or "").strip():
                all_search_texts.append(
                    assistant_args["searchText"].strip().lower()
                )

            category = "EMPTY" if is_empty else ""

            data_by_date[date_key_detail].append(
                {
                    "Conversation ID": source.get("conversationId", ""),
                    "Opportunity ID": opp_id or "",
                    "Packages Shown": has_tool_message_label(conversation),
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
                    # VoiceBot → treat as "regular" user type
                    "User Type": "regular",
                    "Live Transfer?": "No",
                }
            )

        # ───────────────────── build per-day summary ─────────────────────
        summary_data: List[Dict[str, Any]] = []
        for date, d in sorted(stats_by_date.items()):
            total = d["total_convos"] or 0
            empty = d["empty_convos"] or 0
            opp = len(unique_opps_by_date[date])

            non_empty = max(total - empty, 0)

            # 1) Empty Conversations %
            empty_pct = (empty / total * 100.0) if total else 0.0

            # 2) Conversations → Leads % = Opportunities / Non-empty
            conv_to_leads_pct = (
                (opp / non_empty * 100.0) if non_empty else 0.0
            )

            summary_data.append(
                {
                    "date": date,
                    "total": total,
                    "packages": d["packages_shown"],
                    "opportunities": opp,
                    "empty": empty,
                    "empty_pct": round(empty_pct, 2),
                    "conversations_to_leads_pct": round(conv_to_leads_pct, 2),
                }
            )

        # ───────────────────── segment summary (regular only) ─────────────────────
        segment_rows = [
            {
                "date": date,
                "regular_total": d["regular_total"],
                "regular_packages": d["regular_packages"],
                "regular_opportunities": len(unique_regular_opps_by_date[date]),
                "regular_empty": d["regular_empty"],
            }
            for date, d in sorted(stats_by_date.items())
        ]

        summary_df = pd.DataFrame(summary_data)
        segment_df = pd.DataFrame(segment_rows)

        # ───────────────────── overall stats block ─────────────────────
        if summary_df.empty:
            total_convos_sum = 0
            packages_sum = 0
            opportunities_sum = 0
            empty_sum = 0
        else:
            total_convos_sum = int(summary_df["total"].sum())
            packages_sum = int(summary_df["packages"].sum())
            opportunities_sum = int(summary_df["opportunities"].sum())
            empty_sum = int(summary_df["empty"].sum())

        non_empty_sum = max(total_convos_sum - empty_sum, 0)

        overall_empty_pct = (
            empty_sum / total_convos_sum * 100.0 if total_convos_sum else 0.0
        )
        overall_conv_to_leads_pct = (
            opportunities_sum / non_empty_sum * 100.0
            if non_empty_sum
            else 0.0
        )

        summary_stats_df = pd.DataFrame(
            {
                "Metric": [
                    "Total Conversations",
                    "Total Packages Shown",
                    "Total Opportunities",
                    "Total Empty Conversations",
                    "Empty Conversations (%)",
                    "Conversations to Leads (%)",
                ],
                "Count": [
                    total_convos_sum,
                    packages_sum,
                    opportunities_sum,
                    empty_sum,
                    round(overall_empty_pct, 2),
                    round(overall_conv_to_leads_pct, 2),
                ],
            }
        )

        detailed_buckets_df = pd.DataFrame(
            [
                {
                    "date": date,
                    "regular_single_user_msg": d["regular_single_user_msg"],
                    "regular_double_user_msg": d["regular_double_user_msg"],
                    "regular_triple_user_msg": d["regular_triple_user_msg"],
                }
                for date, d in sorted(stats_by_date.items())
            ]
        )

        # ───────────────────── Excel (optional) ─────────────────────
        encoded_file: Optional[str] = None
        if not request.summary_only:
            output = BytesIO()
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                summary_df.to_excel(
                    writer,
                    sheet_name="Conversation Summary",
                    index=False,
                    startrow=0,
                )
                summary_stats_df.to_excel(
                    writer,
                    sheet_name="Conversation Summary",
                    index=False,
                    startrow=len(summary_df) + 2,
                )
                segment_df.to_excel(
                    writer, sheet_name="Segment Summary", index=False
                )
                detailed_buckets_df.to_excel(
                    writer, sheet_name="Detailed Stats", index=False
                )

                # per-day detail sheets
                for date_key in sorted(data_by_date):
                    pd.DataFrame(data_by_date[date_key]).to_excel(
                        writer, sheet_name=date_key, index=False
                    )

            output.seek(0)
            encoded_file = base64.b64encode(output.read()).decode("utf-8")

        # ───────────────────── top destinations ─────────────────────
        counter = Counter(all_search_texts)
        most_searched_destinations = [t.title() for t, _ in counter.most_common(5)]

        # ───────────────────── response ─────────────────────
        resp: Dict[str, Any] = {
            "status": "success",
            "summary": summary_data,
            "segment_summary": segment_rows,
            "most_searched_destinations": most_searched_destinations,
        }
        if encoded_file:
            resp["excel_base64"] = encoded_file
            resp["filename"] = (
                f"voicebot_conversations_{request.from_date}_to_{request.to_date}.xlsx"
            )

        return JSONResponse(content=resp)

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error processing VoiceBot request: {str(e)}",
        )
