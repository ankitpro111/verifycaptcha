#!/usr/bin/env python3

import sys
import os
import time
import json
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import ijson
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import urllib3

# Disable SSL warnings for testing
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def load_json(file_path):
    """Stream-load large NDJSON or JSON array files."""
    if not os.path.exists(file_path):
        return []

    if file_path.endswith(".ndjson"):
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    yield json.loads(line)
    else:
        with open(file_path, "r", encoding="utf-8") as f:
            for item in ijson.items(f, "item"):
                yield item

def save_json(filepath, record):
    """Append a single record to NDJSON file."""
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

# Enhanced user agents with more recent versions
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
]

def create_session():
    """Create a session with enhanced bot evasion"""
    session = requests.Session()
    
    # Configure retry strategy - more conservative
    retry_strategy = Retry(
        total=2,
        status_forcelist=[500, 502, 503, 504],  # Don't retry 429
        backoff_factor=1,
        raise_on_status=False
    )
    
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=5,
        pool_maxsize=10,
        pool_block=False
    )
    
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

# Thread-local session storage
thread_local = threading.local()

def get_session():
    if not hasattr(thread_local, 'session'):
        thread_local.session = create_session()
    return thread_local.session

# ------------------------------------------
# ✅ Optimized fetch_listing_urls()
# ------------------------------------------
def fetch_listing_urls(url):
    """Fetch all listing search result pages and return all property items."""
    all_properties = []
    page = 1
    per_page = 25  # each page returns 25 items

    while True:
        full_url = f"{url}?page={page}" if page > 1 else url
        initial_data = extract_initial_data_from_url(full_url)
        if not initial_data:
            print(f"⚠️ No data returned for {full_url}")
            break

        page_data = initial_data.get("srp", {}).get("pageData", {})
        if not page_data:
            print(f"⚠️ No pageData found for {full_url}")
            break

        total_count = page_data.get("count", 0)
        properties = page_data.get("properties", [])

        if not properties:
            print(f"🚫 No properties found on page {page}. Stopping.")
            break

        print(f"✅ Extracted {len(properties)} properties from page {page}")
        all_properties.extend(properties)

        # Stop if we have all the properties
        if len(all_properties) >= total_count:
            break

        # Stop if this page returned fewer than 25 items (likely last page)
        if len(properties) < per_page:
            break

        page += 1

    print(f"🏁 Finished. Total properties collected: {len(all_properties)} / {total_count}")
    return all_properties
\

# ------------------------------------------
# ✅ Helper: extract window.__initialData__ from URL
# ------------------------------------------
def extract_initial_data_from_url(url):
    """Fetch page and extract JSON inside window.__initialData__."""
    response = fetch_page(url)
    if not response or not response.text or len(response.text) < 100:
        print(f"❌ Failed or too short response for {url}")
        return None

    soup = BeautifulSoup(response.text, 'html.parser')

    for script in soup.find_all("script"):
        if script.string and "window.__initialData__" in script.string:
            json_text = extract_balanced_json(script.string, "window.__initialData__")
            if json_text:
                try:
                    return json.loads(json_text)
                except json.JSONDecodeError as e:
                    print(f"❌ JSON decode error for {url}: {e}")
    print(f"⚠️ No initialData found in {url}")
    return None


def fetch_page(url, max_retries=2, base_delay=1):
    """Enhanced fetch with better bot evasion"""
    if not url:
        return None
    
    session = get_session()
    
    for attempt in range(max_retries):
        try:
            # Quick delay to avoid detection
            sleep_time = random.uniform(0.5, 2.0)
            print(f"Waiting for {sleep_time:.2f} seconds before fetching {url}")
            time.sleep(sleep_time)

            # Enhanced headers to mimic real browser
            headers = {
                'User-Agent': random.choice(USER_AGENTS),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Cache-Control': 'max-age=0',
                'DNT': '1',
            }

            # Try different approaches with fast timeouts
            approaches = [
                {"verify": True, "timeout": (3, 8)},
                {"verify": False, "timeout": (5, 12)},
                {"verify": False, "timeout": (8, 15), "stream": True},
            ]
            
            for i, approach in enumerate(approaches):
                try:
                    print(f"  Attempt {attempt+1}.{i+1}: Trying approach {i+1}")
                    
                    response = session.get(url, headers=headers, allow_redirects=True, **approach)
                    
                    if approach.get('stream'):
                        # If streaming, read the content
                        content = response.content.decode('utf-8', errors='ignore')
                        response._content = content.encode('utf-8')
                    
                    if response.status_code == 200:
                        # Check for captcha
                        if response.url.startswith("https://www.99acres.com/load/verifycaptcha"):
                            print(f"❌ Blocked by captcha at {response.url}.")
                            askForCaptcha.set()
                            return None

                        print(f"✅ Successfully fetched {url} (approach {i+1})")
                        return response
                    
                    elif response.status_code == 429:
                        print(f"⚠️ Rate limited (429) for {url}")
                        time.sleep(random.uniform(3, 6))
                        break  # Try next attempt
                    
                    else:
                        print(f"⚠️ Status {response.status_code} for {url}")
                        
                except requests.exceptions.Timeout:
                    print(f"  ⚠️ Timeout with approach {i+1}")
                    continue
                except requests.exceptions.ConnectionError as e:
                    print(f"  ⚠️ Connection error with approach {i+1}: {str(e)[:100]}")
                    continue
                except Exception as e:
                    print(f"  ⚠️ Error with approach {i+1}: {str(e)[:100]}")
                    continue
            
            # If all approaches failed, wait before next attempt
            if attempt < max_retries - 1:
                backoff_time = base_delay + random.uniform(1, 3)
                print(f"All approaches failed. Backing off for {backoff_time:.2f} seconds.")
                time.sleep(backoff_time)

        except Exception as e:
            print(f"❌ Unexpected error for {url}: {e}")
            return None

    print(f"❌ Max retries reached for {url}")
    return None

def extract_balanced_json(text, start_token):
    start = text.find(start_token)
    if start == -1:
        return None
    start = text.find("{", start)
    if start == -1:
        return None

    brace_count = 0
    end = start
    while end < len(text):
        if text[end] == "{":
            brace_count += 1
        elif text[end] == "}":
            brace_count -= 1
            if brace_count == 0:
                break
        end += 1

    return text[start:end + 1]

# ------------------------------------------
# ✅ Optimized raw_scrapper()
# ------------------------------------------

def raw_scrapper(url):
    """Scrapes a 99acres project page for both sale and rent listings (runs listing URLs in parallel)."""
    try:
        if stop_event.is_set():
            return None

        print(f"🔍 Scraping project: {url}")
        initial_data = extract_initial_data_from_url(url)
        if not initial_data:
            print(f"❌ No initialData for {url}")
            return {
                "url": url,
                "error": "No initialData"
            }

        project_state = initial_data.get("projectDetailState", {}).get("pageData", {})
        components = project_state.get("components", {})
        basic_details = project_state.get("basicDetails", {})

        listing = {"sale": [], "rent": []}

        # Extract canonical URLs
    
        resale_url = components.get("resaleProperties", {}).get("data", {}).get("metaTagInfo", {}).get("CANONICAL_URL","")
        rent_url = components.get("rentalProperties", {}).get("data", {}).get("metaTagInfo", {}).get("CANONICAL_URL", "")
    
        # Run both listing fetches in parallel
        urls_to_fetch = {
            "sale": resale_url,
            "rent": rent_url
        }
   
        futures = {}
        with ThreadPoolExecutor(max_workers=2) as executor:
            for key, sub_url in urls_to_fetch.items():
                if sub_url:
                    futures[executor.submit(fetch_listing_urls, sub_url)] = key

            for future in as_completed(futures):
                key = futures[future]
                try:
                    result = future.result(timeout=45)
                    listing[key] = result if result else []
                    print(f"✅ Completed fetching {key} listings for {url} ({len(listing[key])} items)")
                except Exception as e:
                    print(f"⚠️ Error fetching {key} listings for {url}: {e}")
                    listing[key] = []

        # Return final structured result
        if not basic_details and not components:
            return {
                "url": url,
                "error": "No project data"
            }

        return {
            "url": url,
            "raw_data": {
                "basicDetails": basic_details,
                "components": components
            },
            "listing": listing
        }

    except Exception as e:
        print(f"❌ Error in raw_scrapper({url}): {e}")
        return {
            "url": url,
            "error": str(e)
        }

# Setup
BASE_URL = "./"
city_name = "bangalore"

askForCaptcha = threading.Event()
stop_event = threading.Event()

unique_urls_file = f"{BASE_URL}{city_name}/unique_urls_sqy_99acres.json"
raw_output_file = f"{BASE_URL}{city_name}/raw_{city_name}_99acres.ndjson"

# Load existing data
seen_urls = set(e.get("url", "").lower() for e in load_json(raw_output_file) if e.get("url"))

unique_urls_data = list(load_json(unique_urls_file))
if not unique_urls_data:
    print(f"No data found in {unique_urls_file}")
    sys.exit(0)

list_source_urls = {}

for item in unique_urls_data:
    for u in item.get("urls",[]):
        if u not in list_source_urls:
            list_source_urls[u.lower()] = item.get("source_url")

            


unique_urls = [u for item in unique_urls_data for u in item.get("urls",[])]
processed_urls = [u for u in unique_urls if u not in seen_urls]

# processed_urls = ["https://www.99acres.com/grc-shreekrish-sarjapur-road-bangalore-east-npxid-r399508","https://www.99acres.com/brigade-cosmopolis-whitefield-bangalore-east-npxid-r21160","https://www.99acres.com/brigade-cornerstone-utopia-varthur-bangalore-east-npxid-r319371"]
processed_urls = ["https://www.99acres.com/brigade-cornerstone-utopia-varthur-bangalore-east-npxid-r319371"]
print(f"Total urls: {len(unique_urls)} | Seen urls: {len(seen_urls)} | Remaining: {len(processed_urls)}")

# Use all URLs now that it's working
print(f"🚀 Starting enhanced scraper with {len(processed_urls)} URLs to process...")

# Multi-threading settings
lock = threading.Lock()
save_every = 10
batch_buffer = []
processed_count = 0
failed_count = 0

try:
    # Use 2 workers for better speed
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_to_url = {executor.submit(raw_scrapper, url): url for url in processed_urls}

        for idx, future in enumerate(as_completed(future_to_url), start=1):
            if askForCaptcha.is_set():
                print("🚨 Captcha detected! Exiting all threads immediately.")
                sys.exit(0)

            result = future.result()
            if result:
                  # Add source_url if available
                url = future_to_url[future]
                result["source_url"] = list_source_urls.get(url.lower())
                with lock:
                    batch_buffer.append(result)
                    seen_urls.add(result["url"].lower())
                    processed_count += 1

                    if len(batch_buffer) >= save_every:
                        for rec in batch_buffer:
                            save_json(raw_output_file, rec)
                        batch_buffer = []
                        print(f"💾 Progress saved after {save_every} new records")
                    
                    if processed_count % 25 == 0:
                        success_rate = (processed_count / (processed_count + failed_count)) * 100 if (processed_count + failed_count) > 0 else 0
                        print(f"📊 Progress: {processed_count} successful, {failed_count} failed (Success rate: {success_rate:.1f}%)")
            else:
                with lock:
                    failed_count += 1

except KeyboardInterrupt:
    print("\n🛑 KeyboardInterrupt detected! Saving current progress...")
    stop_event.set()
    with lock:
        for rec in batch_buffer:
            save_json(raw_output_file, rec)
    print("💾 Progress saved. Exiting gracefully.")
    
    if hasattr(thread_local, 'session'):
        thread_local.session.close()
    
    os._exit(1)

except Exception as e:
    print(f"❌ Unexpected error: {e}")
    stop_event.set()

finally:
    with lock:
        for rec in batch_buffer:
            save_json(raw_output_file, rec)
    
    if hasattr(thread_local, 'session'):
        thread_local.session.close()
    
    success_rate = (processed_count / (processed_count + failed_count)) * 100 if (processed_count + failed_count) > 0 else 0
    print(f"✅ Enhanced scraping completed! Final stats: {processed_count} successful, {failed_count} failed (Success rate: {success_rate:.1f}%)")


    #https://www.99acres.com/brigade-cosmopolis-whitefield-bangalore-east-npxid-r21160
    #https://www.99acres.com/brigade-cornerstone-utopia-varthur-bangalore-east-npxid-r31937