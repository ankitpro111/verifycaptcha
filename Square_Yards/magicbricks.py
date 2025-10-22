import os
import re
import json
import time
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from signal import signal, SIGINT

import requests
from bs4 import BeautifulSoup
import ijson

# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------
city_name = "bangalore"
# unique_urls_file = "./unique_urls.json"
unique_urls_file = f"./{city_name}/unique_urls_sqy_mb.json"
raw_data_file = f"./{city_name}/raw_{city_name}_mb.ndjson"

MAX_WORKERS = 10
MAX_RETRIES = 2
BACKOFF_BASE = 2.0

# 🔹 Global rate limit across threads (seconds between any two requests)
RATE_LIMIT_MIN_GAP = 1.2     # base gap
RATE_LIMIT_JITTER = 0.4      # added jitter to avoid patterns

# 🔹 Limit total tasks for testing (0 = no limit)
TASK_LIMIT = 5000

# -------------------------------------------------------------------
# Thread-shared state
# -------------------------------------------------------------------
lock = threading.Lock()
stop_event = threading.Event()

# global throttler state
rate_lock = threading.Lock()
_last_request_time = 0.0

# run counters
counters = {
    "total_unique": 0,
    "already_scraped": 0,
    "to_scrape": 0,
    "task_limited": 0,
    "processed": 0,
    "written": 0,
    "success": 0,
    "failure": 0,
    "skipped": 0,
}

# per-run dedupe (avoid writing same URL twice within this run)
saved_urls_session = set()

# -------------------------------------------------------------------
# Utils
# -------------------------------------------------------------------
def load_json_stream(filepath, prefix="item"):
    if not os.path.exists(filepath):
        return []
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return [obj for obj in ijson.items(f, prefix)]
    except Exception as e:
        print(f"⚠️ Failed to load {filepath}: {e}")
        return []

def append_json_line(filepath, record):
    try:
        dir_name = os.path.dirname(filepath)
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)
        with open(filepath, "a", encoding="utf-8") as f:
            safe_line = json.dumps(record, ensure_ascii=False)
            safe_line = safe_line.encode("utf-8", "surrogatepass").decode("utf-8", "ignore")
            f.write(safe_line + "\n")
    except Exception as e:
        print(f"❌ Failed to write {filepath}: {e}")

def load_existing_urls_from_ndjson(filepath):
    """
    Read previously scraped URLs from the main NDJSON file.
    """
    urls = set()
    if not os.path.exists(filepath):
        return urls
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    u = (obj.get("url") or "").strip()
                    if u:
                        urls.add(u)
                except Exception:
                    continue
    except Exception as e:
        print(f"⚠️ Failed to load existing from {filepath}: {e}")
    return urls

def fix_url(url: str) -> str:
    if not url:
        return ""
    return url if url.startswith("http") else f"https://www.magicbricks.com/{url.lstrip('/')}"

def throttle_global():
    """
    Enforce a global min gap between *any* two HTTP requests across threads.
    """
    global _last_request_time
    with rate_lock:
        now = time.time()
        min_gap = RATE_LIMIT_MIN_GAP + random.uniform(0, RATE_LIMIT_JITTER)
        wait = (_last_request_time + min_gap) - now
        if wait > 0:
            time.sleep(wait)
        _last_request_time = time.time()

# -------------------------------------------------------------------
# Load & prepare URLs (+ source_url map)
# -------------------------------------------------------------------
unique_items = load_json_stream(unique_urls_file, "item") or []

# Build:
# - url_source_map: fixed_url -> source_url
# - all_urls_fixed: set of fixed URLs
url_source_map = {}
all_urls_fixed = []

for item in unique_items:
    src = (item.get("source_url") or "").strip()
    for u in item.get("urls", []):
        raw = (u or "").strip()
        if not raw:
            continue
        fixed = fix_url(raw)
        url_source_map[fixed] = src
        all_urls_fixed.append(fixed)

# De-duplicate
all_urls_fixed = list(dict.fromkeys(all_urls_fixed))
counters["total_unique"] = len(all_urls_fixed)

# Get already-scraped URLs from NDJSON (so we skip them)
existing_urls = load_existing_urls_from_ndjson(raw_data_file)
counters["already_scraped"] = len(existing_urls)

# Build worklist (skip already scraped)
to_scrape_list = [u for u in all_urls_fixed if u not in existing_urls]
counters["to_scrape"] = len(to_scrape_list)

# Apply task limit (for testing)
if TASK_LIMIT > 0 and len(to_scrape_list) > TASK_LIMIT:
    to_scrape_list = to_scrape_list[:TASK_LIMIT]
    counters["task_limited"] = len(to_scrape_list)
else:
    counters["task_limited"] = counters["to_scrape"]

print(
    f"Total unique: {counters['total_unique']} | "
    f"Already scraped: {counters['already_scraped']} | "
    f"To scrape now: {counters['to_scrape']} | "
    f"Task limit: {counters['task_limited']}"
)

# -------------------------------------------------------------------
# Signal Handling
# -------------------------------------------------------------------
def handle_sigint(sig, frame):
    if not stop_event.is_set():
        print("\n🛑 Ctrl+C detected! Exiting safely...")
        stop_event.set()
    else:
        print("\n⚠️ Force exit now.")
        os._exit(1)

signal(SIGINT, handle_sigint)

# -------------------------------------------------------------------
# HTTP + Scraper
# -------------------------------------------------------------------
def fetch_page(url: str):
    if not url:
        return None
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    ]
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            headers = {
                "User-Agent": random.choice(user_agents),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive",
            }
            throttle_global()  # 🔹 enforce global delay between requests
            resp = requests.get(url, headers=headers, timeout=20)
            resp.raise_for_status()
            return resp
        except Exception:
            if attempt == MAX_RETRIES:
                return None
            wait = (BACKOFF_BASE ** attempt) + random.uniform(0, 1.0)
            time.sleep(wait)

def raw_scrapper(url: str, source_url: str, verbose: bool = False, idx: int = 1, total: int = 1):
    if verbose:
        print(f"[{idx}/{total}] Scraping {url}...")
    # small per-task jitter to avoid perfect sync (global throttle still applies)
    time.sleep(random.uniform(0.1, 0.3))

    resp = fetch_page(url)
    if not resp:
        return {"source_url": source_url, "url": url, "error": "fetch_failed"}

    soup = BeautifulSoup(resp.text, "html.parser")
    script_tag_text = None
    for script in soup.find_all("script"):
        s = script.string
        if s and "window.SERVER_PRELOADED_STATE_" in s:
            script_tag_text = s
            break

    if not script_tag_text:
        return {"source_url": source_url, "url": url, "error": "state_not_found"}

    match = re.search(r"window\.SERVER_PRELOADED_STATE_\s*=\s*(\{.*?\});", script_tag_text, re.DOTALL)
    if not match:
        return {"source_url": source_url, "url": url, "error": "json_not_found"}

    json_str = match.group(1)
    json_str = re.sub(r",\s*}", "}", json_str)
    json_str = re.sub(r",\s*]", "]", json_str)

    try:
        data = json.loads(json_str)
        if 'pppfr' in url or 'pppfs' in url:
            initial_data = {
                "searchResult": data.get("searchResult",[]),
                "searchAdditionalDataBean": data.get("searchAdditionalDataBean",{})
            }
        else:
            initial_data = data.get("projectPageSeoStaticData", {}).get("bhkDetailsDTO", {})
        return {"source_url": source_url, "url": url, "data": initial_data}
    except Exception as e:
        return {"source_url": source_url, "url": url, "error": f"json_parse_failed: {e}"}

def process_url(url: str, source_url: str, idx: int, total: int):
    try:
        return raw_scrapper(url, source_url, True, idx, total)
    except Exception as e:
        return {"source_url": source_url, "url": url, "error": f"exception: {e}"}

# -------------------------------------------------------------------
# Thread pool
# -------------------------------------------------------------------
try:
    total = len(to_scrape_list)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_url = {
            executor.submit(process_url, u, url_source_map.get(u, ""), i + 1, total): u
            for i, u in enumerate(to_scrape_list)
        }

        for _i, future in enumerate(as_completed(future_to_url), start=1):
            if stop_event.is_set():
                break

            url = future_to_url[future]
            try:
                record = future.result()
            except Exception as ex:
                record = {"source_url": url_source_map.get(url, ""), "url": url, "error": f"thread_exception: {ex}"}

            with lock:
                counters["processed"] += 1

                # count success/failure
                if record and not record.get("error"):
                    counters["success"] += 1
                else:
                    counters["failure"] += 1

                # write once per URL in this run (and only if not already scraped before)
                if (url not in saved_urls_session) and (url not in existing_urls):
                    append_json_line(raw_data_file, record)
                    saved_urls_session.add(url)
                    counters["written"] += 1
                else:
                    counters["skipped"] += 1

except Exception as e:
    print(f"⚠️ Top-level error: {e}")

# -------------------------------------------------------------------
# Summary
# -------------------------------------------------------------------
print("\n===== SUMMARY =====")
print(f"File: {raw_data_file}")
print(f"Total unique URLs: {counters['total_unique']}")
print(f"Already scraped (pre-run): {counters['already_scraped']}")
print(f"To scrape now (pre-limit): {counters['to_scrape']}")
print(f"Task limit applied: {counters['task_limited']}")
print(f"Processed this run: {counters['processed']}")
print(f"Written to file this run: {counters['written']}")
print(f"Success: {counters['success']} | Failure: {counters['failure']} | Skipped (dupe in run or already scraped): {counters['skipped']}")
print("====================\n")
