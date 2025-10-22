import os
import time
import json
import requests
import re
import random
import sys
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
# ---------------- CONFIG ----------------

GOOGLE_API_KEY =  "AIzaSyBX4dwOqg5XtKVWNc76ciArIpt3oZzEUBA" # omkar key
# GOOGLE_API_KEY =  "AIzaSyD3vK4DI_gafgooZPTx3Jvi035Kzy-jTpI" # my account key
# GOOGLE_API_KEY = "AIzaSyCGZHPrbKhuIeDHJjN8u8hp4NCcPssK1yo" #sachin key
# GOOGLE_API_KEY = "AIzaSyAsZMmtbNe0EWjhYhjUM3qA3Je7ZqQUpXE" #propertyangel key 

GOOGLE_CSE_ID = "15c61af869e6b445e"

REQUEST_DELAY = 2           # Delay between API calls (seconds)
CHECKPOINT_EVERY = 5          # Save progress every N processed rows
REQUESTS_BEFORE_SLEEP = 50    # Throttle every 50 API calls
SLEEP_SECONDS = 60           # Sleep duration (seconds)
RECORD_LIMIT = 2500 #1975
# ---------------- HELPERS ----------------
def load_ndjson(file_path):
    """
    Load NDJSON file into a list of dicts.
    Returns [] if file doesn't exist.
    Skips invalid lines but logs them.
    """
    if not os.path.exists(file_path):
        print(f"⚠️ File not found: {file_path}")
        return []

    data_record = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue  # skip empty lines
            try:
                obj = json.loads(line)
                data_record.append(obj)
            except json.JSONDecodeError as e:
                print(f"❌ Invalid JSON at line {line_num}: {e}")

    print(f"✅ Loaded {len(data_record)} valid records from {file_path}")
    return data_record


# def save_json(file_path, data):
#     with open(file_path, 'w') as f:
#         json.dump(data, f, indent=2)


def save_json(file_path, data):
    """
    Save data in NDJSON format.
    Each item in 'data' should be a dict (JSON object).
    """
    with open(file_path, 'w', encoding='utf-8') as f:
        for item in data:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")


def fix_project_title(project_title: str, builder_name: str) -> str:
    # Get the first word of builder name
    first_word = builder_name.split()[0]

    # Check if project title already starts with it
    if not project_title.lower().startswith(first_word.lower()):
        # Prepend it
        project_title = f"{first_word} {project_title}"

    return project_title


def extract_unit_bhk(units):
    unit_type = (units.get('unit_types', '') or '').strip()
    if not unit_type or unit_type.lower() == 'plot':
        return ''
    match = re.match(r'^(\d+(\.\d+)?)', unit_type)
    return match.group(1) if match else ''


def google_search(query: str, num_results: int = 30):
    """
    Always make 3 API calls (10 results each) in parallel to fetch up to 30 results.
    Shows a tqdm progress bar.
    """
    url = "https://www.googleapis.com/customsearch/v1"
    results = []
    num_results = min(num_results, 30)

    steps = [1] #[1, 11, 21]  # always 3 calls for 30 results

    def fetch_batch(start):
        params = {
            'key': GOOGLE_API_KEY,
            'cx': GOOGLE_CSE_ID,
            'q': query,
            'num': 10,
            'start': start
        }
        try:
            response = requests.get(url, params=params, timeout=15)

            # 🚨 STOP if rate limited
            if response.status_code == 429:
                print("❌ Got 429 Too Many Requests. Stopping script immediately.")
                sys.exit(1)  # <-- This kills the script
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
            print(f"   ❌ Error fetching {start}-{start+9}: {e}")
            return []

    # Run 3 requests in parallel with tqdm progress bar
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(fetch_batch, s) for s in steps]
        for future in tqdm(as_completed(futures), total=len(steps), desc=f"Fetching '{query}'" ): #desc=f"Fetching '{query}'"
            results.extend(future.result())

    return results

# ---------------- MAIN ----------------
city_name = "mumbai"
# input_file = f"{city_name}/projects_{city_name}.json"
input_file = f"./{city_name}/projects_{city_name}_sy.ndjson"
output_file = f"./{city_name}/query_{city_name}_all_sources_sqy.ndjson"
unmatched_file = f"./{city_name}/unmatched_sy_with_cf_urls.json"

data_record = load_ndjson(input_file)
result_record = load_ndjson(output_file)

un_matched = {}
with open(unmatched_file,"r",encoding="utf-8") as f:
    un_matched = json.load(f)

un_matched_record = set(un_matched.get("unmatched_squareyard_urls", []))

data_record = [item for item in data_record if item.get('url') in un_matched_record]

print(f"✅ Loaded {len(data_record)} valid records from unmatched_squareyard_urls.json")

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

# seen_queries = set()
# seen_urls = set()

# for row in result_record:
#     src = row.get('source_url')

#     query_results = row.get('query_results', [])

#     if query_results:  # has query_results
#         if src:
#             seen_urls.add(src)

#         for it in query_results:
#             q = it.get('query')
#             if q:
#                 seen_queries.add(q)

#             u = it.get('url')
#             if u:
#                 seen_urls.add(u)
#     else:
#         # explicitly remove src if present and no query_results
#         if src and src in seen_urls:
#             seen_urls.remove(src)


processed_rows = 0
request_count = 0

# Build list of not-fetched rows
not_fetched_record = [item for item in data_record if item.get('url') not in seen_urls]


# Shuffle so we don’t get stuck on first 10 every run
random.shuffle(not_fetched_record)

# total_rows = min(1, len(not_fetched_record))
total_rows = min(RECORD_LIMIT, len(not_fetched_record))

print(f"Total Records Before: {len(result_record)}")
try:
    for idx, row in enumerate(not_fetched_record[:total_rows], start=1):
        project_seen_urls = set()   
        
        print(f"\n🔍 Processing row {idx}/{total_rows} — project: {row.get("project_name","")}")
        title = row.get("project_name","")        
        builder = row.get('developer','')        
        query_searches = set()
        
        if builder:
            query_text = f"{fix_project_title(title, builder)} {city_name}"
        else:
            query_text = f"{title} {city_name}"
        
        exclusions = [
            "instagram.com", "facebook.com", "sulekha.com", "Homes247.in",
            "youtube.com", "gharpe.com", "setllin.in", "justdial.com",
            "quikr.com", "proptiger.com", "commonfloor.com"
            # "pdf", "bank", "banks"
        ]
        # Build exclusion string dynamically
        # exclusion_str = " ".join(f"-{term}" for term in exclusions)

        # query_text = f"{query_text} {exclusion_str}"


        # query_text = f"{query_text} -instagram.com -facebook.com -sulekha.com -Homes247.in -youtube.com -gharpe.com -setllin.in -justdial.com -quikr.com -proptiger.com -squareyards.com"
        # -instagram.com -facebook.com -sulekha.com -Homes247.in -youtube.com -gharpe.com -setllin.in -justdial.com -quikr.com -proptiger.com 
        # squareyards.com # PropTiger, Quikr, sulekha, social media
        # query_searches.add(query_text)
        exclusion_str = "" #"-pdf -bank -banks -reviews -brochures -photos -videos -photo -video"
        query_text = f"{query_text} {exclusion_str}"


        # housing_inurl = "(inurl:  OR inurl:/buy/ OR inurl:/rent-)"
        # magicbricks_inurl = "(inurl:pppfs OR inurl:pppfr OR inurl:pdpid)"
        # acres99_inurls  = "(inurl:resale OR inurl:rent OR inurl:buy OR inurl:npxid)"
        # nobroker_inurls = "(inurl:sale OR inurl:rent OR inurl:prjt OR inurl:buy)"

        housing_inurl = "(inurl:projects/page)"
        magicbricks_inurl = "(inurl:pdpid)"
        acres99_inurls  = "(inurl:npxid)"
        nobroker_inurls = "(inurl:prjt)"

        query_searches.add(f'site:housing.com {query_text} {housing_inurl}')
        query_searches.add(f'site:magicbricks.com {query_text} {magicbricks_inurl}')
        query_searches.add(f'site:99acres.com {query_text} {acres99_inurls}') #""
        query_searches.add(f'site:nobroker.in {query_text} {nobroker_inurls}') #""


        # query_searches.add(f'site:squareyards.com {query_text}') #        

        query_result = []
        for query_search in sorted(query_searches):
            if query_search in seen_queries:
                continue
            time.sleep(REQUEST_DELAY)
            # throttle after every REQUESTS_BEFORE_SLEEP calls
            if request_count and request_count % REQUESTS_BEFORE_SLEEP == 0:
                print(f"⏳ Hit {request_count} requests — sleeping {SLEEP_SECONDS}s to respect quotas...")
                time.sleep(SLEEP_SECONDS)

            results = google_search(query_search, num_results=10)
            print(f"Query: {query_search.split(' ')[0]} Total Result: {len(results)}")

            request_count += 1
            seen_queries.add(query_search)

            # for item in results:
            #     u = item.get('url')
            #     if not u or u in seen_urls:
            #         continue
            #     item['query'] = query_search
            #     query_result.append(item)
            #     seen_urls.add(u)

            for item in results:
                u = item.get('url')
                if not u or u in project_seen_urls:
                    continue
                item['query'] = query_search
                query_result.append(item)
                project_seen_urls.add(u)   # dedup only within this project

        # Always record the row (even with no results)
        # row_copy = dict(row)
        row_copy = {}
        row_copy["project"] = row.get("project_name","")
        row_copy["source_url"] = row.get("url","")
        # row_copy["query"] = query_text
        row_copy['query_results'] = query_result
        # row_copy['query_stats'] = {
        #         "sum_total_results": sum(len(r) for r in all_raw_results),
        #         "unique_results": len(query_result)
        #     }
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