import json
from datetime import datetime, timedelta
from collections import defaultdict

# Load your original JSON file
with open('/home/gcp-admin/thomascook-travelplanner/Elastic Search/cleaned_tool_conversations.json', 'r') as f:
    data = json.load(f)

# Function to convert UTC to IST
def convert_utc_to_ist(utc_str):
    utc_time = datetime.fromisoformat(utc_str)
    ist_time = utc_time + timedelta(hours=5, minutes=30)
    return ist_time

# Grouping by IST date
grouped = defaultdict(list)

for entry in data:
    ist_time = convert_utc_to_ist(entry['chat_modified'])
    ist_date_str = ist_time.strftime('%Y-%m-%d')
    
    # Update the entry with IST time
    new_entry = {
        "conversationId": entry["conversationId"],
        "chat_modified": ist_time.isoformat()
    }

    grouped[ist_date_str].append(new_entry)

# Write each group to a separate file
for date_str, entries in grouped.items():
    output_file = f"conversations_{date_str}.json"
    with open(output_file, 'w') as f:
        json.dump(entries, f, indent=2)

    print(f"Saved {len(entries)} entries to {output_file}")
