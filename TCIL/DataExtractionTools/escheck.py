from elasticsearch import Elasticsearch, helpers
import json
from collections import defaultdict
from datetime import datetime
import pandas as pd

# Map of agent_id -> agent_name
AGENT_MAP = {
    "USR-7829DE9E": "Asif Ansari",
    "USR-1407D930": "Babita Saha",
    "USR-995592F5": "Mihir Bhavsar",
    "USR-608736F7": "Usaid Sarang",
    "USR-53FC02E4": "Bharat Bhandari(TravBridgeTest)",
}

def clean_agent_id(value):
    """Normalize agent_id by stripping spaces and quotes."""
    if value is None:
        return None
    v = str(value).strip().strip('"').strip("'")
    return v

es = Elasticsearch(
    ['http://localhost:9200'],
    basic_auth=('elastic', 'iweXVQuayXSCP9PFkHcZ')
)

try:
    # Use scan to get ALL matching docs (not just 10k limit)
    results = helpers.scan(
        es,
        index="user_conversations",
        query={
            "query": {
                "bool": {
                    "must": [
                        {"exists": {"field": "agent_id"}},
                        {"wildcard": {"agent_id": "?*"}},   # non-empty
                        {"term": {"chat_channel": "ChatBot"}}
                    ]
                }
            }
        },
        _source=[
            "conversationId",
            "chat_started",
            "opportunity_id",
            "agent_id",
            "chat_channel",
            "session_status"   # included
        ]
    )

    grouped = defaultdict(list)
    flat_rows = []  # <-- rows for Excel
    count = 0

    for hit in results:
        source = hit["_source"]
        conv_id = source.get("conversationId")
        chat_started = source.get("chat_started")
        opp_id = source.get("opportunity_id")
        agent_id_raw = source.get("agent_id")
        chat_channel = source.get("chat_channel")
        session_status = source.get("session_status")

        agent_id = clean_agent_id(agent_id_raw)
        agent_name = AGENT_MAP.get(agent_id)

        if not chat_started or not agent_id or chat_channel != "ChatBot":
            continue

        # Extract just the date (YYYY-MM-DD)
        try:
            date_key = datetime.fromisoformat(chat_started).date().isoformat()
        except Exception:
            # fallback if chat_started has nanos or Z suffix
            date_key = str(chat_started).split("T")[0]

        # Keep original grouped JSON structure (optional)
        grouped[date_key].append({
            "conversationId": conv_id,
            "opportunity_id": opp_id,
            "agent_id": agent_id,
            "agent_name": agent_name,
            "chat_channel": chat_channel,
            "session_status": session_status
        })

        # Add a flat row for Excel
        flat_rows.append({
            "date": date_key,
            "conversationId": conv_id,
            "opportunity_id": opp_id,
            "agent_id": agent_id,
            "agent_name": agent_name,
            "chat_channel": chat_channel,
            "session_status": session_status
        })

        count += 1
        if count >= 10000:  # safety cap
            break

    # -------- Save JSON (kept from your flow; safe to remove if not needed) --------
    output = {date: grouped[date] for date in sorted(grouped.keys(), reverse=True)}
    with open("liveAgent_conversations.json", "w") as f:
        json.dump(output, f, indent=2)

    # -------- Save Excel --------
    # Create DataFrame and sort by date (newest first) then by conversationId just for stable ordering
    df = pd.DataFrame(flat_rows)
    if not df.empty:
        df.sort_values(by=["date", "conversationId"], ascending=[False, True], inplace=True)

        # Write to Excel with some handy niceties (header freeze, autofilter, column widths)
        excel_path = "liveAgent_conversations.xlsx"
        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            sheet_name = "Conversations"
            df.to_excel(writer, index=False, sheet_name=sheet_name)

            ws = writer.book[sheet_name]
            # Freeze top header row
            ws.freeze_panes = "A2"
            # Apply auto-filter on all columns
            ws.auto_filter.ref = ws.dimensions
            # Best-effort column width based on max content length
            for col_idx, col in enumerate(df.columns, start=1):
                series = df[col].astype(str)
                max_len = max([len(col)] + series.map(len).tolist())
                ws.column_dimensions[ws.cell(row=1, column=col_idx).column_letter].width = min(max(12, max_len + 2), 60)

        print(f"✅ Saved {count} ChatBot conversations to conversations.xlsx and conversations.json")
    else:
        print("⚠️ No matching rows to write. Check your query/filters.")

except Exception as e:
    print(f"❌ Error: {e}")
