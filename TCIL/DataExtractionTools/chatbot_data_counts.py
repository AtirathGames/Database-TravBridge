from elasticsearch import Elasticsearch
from elasticsearch.exceptions import AuthenticationException, ConnectionError
import pandas as pd
from collections import defaultdict
from datetime import datetime

# ✅ use http:// because ES is not running TLS
es = Elasticsearch(
    ['http://localhost:9200'],
    basic_auth=('elastic', 'iweXVQuayXSCP9PFkHcZ')
)

def count_user_messages(conversation):
    """Return number of messages whose role == 'user'."""
    if not isinstance(conversation, list):
        return 0
    return sum(1 for msg in conversation if isinstance(msg, dict) and msg.get("role") == "user")

def has_tool_message(conversation):
    """Return True if any message has role == 'tool'."""
    if not isinstance(conversation, list):
        return False
    return any(isinstance(msg, dict) and msg.get("role") == "tool" for msg in conversation)

try:
    # Query: fetch recent docs where chat_channel = ChatBot
    response = es.search(
        index="sotc_user_conversations",
        query={
            "bool": {
                "must": [
                    {"term": {"chat_channel": "ChatBot"}}
                ]
            }
        },
        # Pull the full conversation so we can count messages reliably
        _source=[
            "conversationId",
            "chat_started",
            "opportunity_id",
            "is_utm_user",
            "conversation"  # need full array to count user msgs, check empties, and detect tool msgs
        ],
        sort=[{"chat_started": {"order": "desc"}}],
        size=10000
    )

    grouped = defaultdict(lambda: {
        # existing metrics
        "total_conversations": 0,
        "regular_total": 0,
        "empty_regular_conversations": 0,
        "regular_with_opportunity": 0,
        "regular_single_user_msg": 0,  # exactly 1 user message AFTER the opener  => total user msgs == 2
        "regular_double_user_msg": 0,  # exactly 2 user messages AFTER the opener => total user msgs == 3
        "regular_triple_user_msg": 0,  # exactly 3 user messages AFTER the opener => total user msgs == 4
        "regular_packages_shown": 0,
        # "utm_total": 0,
        # "empty_utm_conversations": 0,
        # "utm_with_opportunity": 0,
        # "utm_single_user_msg": 0,
        # "utm_double_user_msg": 0,
        # "utm_triple_user_msg": 0,
        # "utm_packages_shown": 0,
    })

    for hit in response["hits"]["hits"]:
        source = hit.get("_source", {})
        chat_started = source.get("chat_started")
        opp_id = source.get("opportunity_id")
        is_utm = source.get("is_utm_user")
        conversation = source.get("conversation", [])

        if not chat_started:
            continue

        # Extract just the date (YYYY-MM-DD)
        try:
            date_key = datetime.fromisoformat(chat_started).date().isoformat()
        except Exception:
            date_key = str(chat_started).split("T")[0]

        grouped[date_key]["total_conversations"] += 1

        # Compute counts once
        total_user_msgs = count_user_messages(conversation)
        user_msgs_after_opener = max(total_user_msgs - 1, 0)
        any_tool = has_tool_message(conversation)

        # Build roles set once for the "empty" check
        roles = {msg.get("role") for msg in conversation if isinstance(msg, dict) and "role" in msg}

        def bump_user_msg_buckets(prefix: str):
            if user_msgs_after_opener == 1:
                grouped[date_key][f"{prefix}_single_user_msg"] += 1
            elif user_msgs_after_opener == 2:
                grouped[date_key][f"{prefix}_double_user_msg"] += 1
            elif user_msgs_after_opener == 3:
                grouped[date_key][f"{prefix}_triple_user_msg"] += 1

        if is_utm == "regular":
            grouped[date_key]["regular_total"] += 1
            if opp_id:
                grouped[date_key]["regular_with_opportunity"] += 1

            # empty conversation: exactly 2 messages, roles == {'user','assistant'}
            if len(conversation) == 2 and roles == {"user", "assistant"}:
                grouped[date_key]["empty_regular_conversations"] += 1

            # bump new user-message buckets (after-opener logic)
            bump_user_msg_buckets("regular")

            # packages shown: any 'tool' role present
            if any_tool:
                grouped[date_key]["regular_packages_shown"] += 1

        elif is_utm == "utm":
            grouped[date_key]["utm_total"] += 1
            if opp_id:
                grouped[date_key]["utm_with_opportunity"] += 1

            # empty conversation: exactly 2 messages, roles == {'user','assistant'}
            if len(conversation) == 2 and roles == {"user", "assistant"}:
                grouped[date_key]["empty_utm_conversations"] += 1

            # bump new user-message buckets (after-opener logic)
            bump_user_msg_buckets("utm")

            # packages shown: any 'tool' role present
            if any_tool:
                grouped[date_key]["utm_packages_shown"] += 1

    # Convert grouped dict into a DataFrame
    df = pd.DataFrame.from_dict(grouped, orient="index")
    df.index.name = "date"
    df = df.reset_index().sort_values(by="date", ascending=False)

    # Save to Excel
    output_file = "SOTC_conversations_count_8thOct.xlsx"
    df.to_excel(output_file, index=False, engine="openpyxl")

    print(f"✅ Saved counts (empty + after-opener single/double/triple + packages_shown) for {len(df)} dates to {output_file}")

except AuthenticationException:
    print("❌ Authentication failed: invalid username/password or API key.")
except ConnectionError as e:
    print(f"❌ Connection failed: {e}")
except Exception as e:
    print(f"❌ Error: {e}")
