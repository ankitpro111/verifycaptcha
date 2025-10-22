import os
import time
import json
import requests
import re
import random
import sys

# ---------------- CONFIG ----------------
custom_city_list = ["hyderabad"]

GOOGLE_API_KEY =  "AIzaSyD3vK4DI_gafgooZPTx3Jvi035Kzy-jTpI" # my account key
# GOOGLE_API_KEY = "AIzaSyAsZMmtbNe0EWjhYhjUM3qA3Je7ZqQUpXE" #propertyangel key 
GOOGLE_CSE_ID = "15c61af869e6b445e"

REQUEST_DELAY = 2           # Delay between API calls (seconds)
CHECKPOINT_EVERY = 5          # Save progress every N processed rows
REQUESTS_BEFORE_SLEEP = 50    # Throttle every 50 API calls
SLEEP_SECONDS = 120           # Sleep duration (seconds)

# ---------------- HELPERS ----------------
def load_json(file_path):
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                print("file not found")
                return []
            
    return []

def save_json(file_path, data):
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=2)

def extract_unit_bhk(units):
    unit_type = (units.get('unit_types', '') or '').strip()
    if not unit_type or unit_type.lower() == 'plot':
        return ''
    match = re.match(r'^(\d+(\.\d+)?)', unit_type)
    return match.group(1) if match else ''

def google_search(query: str, num_results: int = 10):
    url = "https://www.googleapis.com/customsearch/v1"
    params = {
        'key': GOOGLE_API_KEY,
        'cx': GOOGLE_CSE_ID,
        'q': query,
        'num': min(num_results, 10)
    }
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        search_results = response.json()
        items = search_results.get('items', []) or []
        return [
            {
                'title': item.get('title', ''),
                'url': item.get('link', ''),
                'description': item.get('snippet', ''),
            }
            for item in items
        ]
    except Exception as e:
        print(f"   ❌ Search error: {e}")
        return []

# ---------------- MAIN ----------------
city_name = "bangalore"
input_file = f"{city_name}/projects_{city_name}.json"
output_file = f"{city_name}/query_{city_name}_housing.json"
# output_file = f"{city_name}/query_{city_name}_mb_new.json"
# output_file = f"{city_name}/query_{city_name}_nb.json"
# output_file = f"{city_name}/query_{city_name}_99.json"
print(os.path.exists(input_file))
data_record = load_json(input_file)
result_record = load_json(output_file) or []

print(len(data_record),len(result_record))

# ---- Build resume/dedup state from existing output ----
seen_queries = set()
seen_urls = set()
for row in result_record:
    # mark the project's own URL
    src = row.get('source_url')
    if src:
        seen_urls.add(src)
    # mark all queries and URLs from query_results
    for it in row.get('query_results', []):
        q = it.get('query')
        if q:
            seen_queries.add(q)
        u = it.get('url')
        if u:
            seen_urls.add(u)

processed_rows = 0
request_count = 0

# Build list of not-fetched rows
not_fetched_record = [item for item in data_record if item.get('source_url') not in seen_urls]

# Shuffle so we don’t get stuck on first 10 every run
random.shuffle(not_fetched_record)

# total_rows = min(1, len(not_fetched_record))
total_rows = len(not_fetched_record)#min(200, len(not_fetched_record))

print(f"Total Records Before: {len(result_record)}")
try:
    for idx, row in enumerate(not_fetched_record[:total_rows], start=1):
        print(f"\n🔍 Processing row {idx}/{total_rows} — project: {row.get('project', {}).get('title', 'N/A')}")
        title = row.get('project', {}).get('title', '')
        builder = row.get('project', {}).get('builder', '')
        location = row.get('project', {}).get('location', '')
        total_units = row.get('units', []) or []

        query_searches = set()
        for units in total_units:
            rooms = extract_unit_bhk(units)
            if not rooms:
                continue
            query_searches.add(f'site:housing.com "{title} " "{builder}"')
            # query_searches.add(f'site:magicbricks.com "{title}" "{location}"')
            # query_searches.add(f'site:nobroker.in "{title}" "{location}"')
            # query_searches.add(f'site:99acres.com "{title}" "{location}"')

        query_result = []
        for query_search in sorted(query_searches):
            if query_search in seen_queries:
                continue

            time.sleep(REQUEST_DELAY)

            # throttle after every REQUESTS_BEFORE_SLEEP calls
            if request_count and request_count % REQUESTS_BEFORE_SLEEP == 0:
                print(f"⏳ Hit {request_count} requests — sleeping {SLEEP_SECONDS}s to respect quotas...")
                time.sleep(SLEEP_SECONDS)

            results = google_search(query_search)
            # print(results)
            request_count += 1
            seen_queries.add(query_search)

            for item in results:
                u = item.get('url')
                if not u or u in seen_urls:
                    continue
                item['query'] = query_search
                query_result.append(item)
                seen_urls.add(u)

        # Always record the row (even with no results)
        row_copy = dict(row)
        row_copy['query_results'] = query_result
        row_copy['query_status'] = 'ok' if query_result else 'no_results'
        row_copy['processed_at'] = int(time.time())
        result_record.append(row_copy)

        # mark this row as processed for the current run
        src = row_copy.get('source_url')
        if src:
            seen_urls.add(src)

        processed_rows += 1
        if processed_rows % CHECKPOINT_EVERY == 0:
            save_json(output_file, result_record)

except KeyboardInterrupt:
    print("Keyboard Interrput error")
    save_json(output_file, result_record)
    sys.exit(0)    

# Final save
save_json(output_file, result_record)

print(f"Total Records After: {len(result_record)}")