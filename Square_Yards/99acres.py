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


def load_json(file_path):
    """
    Stream-load large NDJSON or JSON array files.
    Returns a generator of objects.
    """
    if not os.path.exists(file_path):
        return []

    if file_path.endswith(".ndjson"):
        # NDJSON reader
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    yield json.loads(line)
    else:
        # JSON array reader
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
# -----------------------Fetch ------------------------ #


USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/109.0.0.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Firefox/112.0.0.0',
]

# Create a session with connection pooling and retry strategy
def create_session():
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=5,  # Total number of retries
        status_forcelist=[429, 500, 502, 503, 504],  # HTTP status codes to retry
        backoff_factor=2,  # Exponential backoff factor
        raise_on_status=False  # Don't raise exception on retry-able status codes
    )
    
    # Mount adapter with retry strategy
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=10,  # Number of connection pools
        pool_maxsize=20,  # Max connections per pool
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

# A potentially more robust fetch_page with retries and exponential backoff
def fetch_page(url, max_retries=3, base_delay=1):
    if not url:
        return None
    
    session = get_session()
    
    for attempt in range(max_retries):
        try:
            # Wait a random amount of time before each attempt
            sleep_time = random.uniform(0.5, 2.0)  # Much shorter delay
            print(f"Waiting for {sleep_time:.2f} seconds before fetching {url}")
            time.sleep(sleep_time)

            # Fake a browser visit with random User-Agent
            headers = {
                'User-Agent': random.choice(USER_AGENTS),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'DNT': '1', # Do Not Track request
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache'
            }

            # Try with different approaches
            response = session.get(
                url, 
                headers=headers, 
                timeout=(5, 15),  # (connect_timeout, read_timeout)
                allow_redirects=True,
                stream=False,
                verify=True  # SSL verification
            )
            
            # Check for successful response
            if response.status_code == 200:
                # 🚨 Check captcha
                if response.url.startswith("https://www.99acres.com/load/verifycaptcha"):
                    print(f"❌ Blocked by captcha at {response.url}.")
                    askForCaptcha.set()  # 🔑 set the event
                    return None

                print(f"✅ Successfully fetched {url}")
                return response
            
            elif response.status_code in [429, 500, 502, 503, 504]:
                print(f"⚠️ Received status {response.status_code} for {url}. Attempt {attempt + 1}/{max_retries}.")
                
                # Exponential Backoff
                if attempt < max_retries - 1:
                    backoff_time = base_delay * (1.5 ** attempt) + random.uniform(0, 1)
                    print(f"Backing off for {backoff_time:.2f} seconds.")
                    time.sleep(backoff_time)
                else:
                    print(f"❌ Max retries reached for {url}. Status: {response.status_code}")
                    return None
            else:
                print(f"❌ Unrecoverable HTTP status {response.status_code} for {url}")
                return None

        except requests.exceptions.Timeout as e:
            print(f"⚠️ Timeout for {url}. Attempt {attempt + 1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                backoff_time = base_delay * (1.5 ** attempt) + random.uniform(0.5, 1.5)
                print(f"Backing off for {backoff_time:.2f} seconds due to timeout.")
                time.sleep(backoff_time)
            else:
                print(f"❌ Max retries reached for {url} due to timeout.")
                return None
                
        except requests.exceptions.ConnectionError as e:
            print(f"⚠️ Connection error for {url}. Attempt {attempt + 1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                backoff_time = base_delay * (1.5 ** attempt) + random.uniform(0.5, 1.5)
                print(f"Backing off for {backoff_time:.2f} seconds due to connection error.")
                time.sleep(backoff_time)
            else:
                print(f"❌ Max retries reached for {url} due to connection error.")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Request error for {url}: {e}")
            return None

    return None

def scraper(response):     
    return BeautifulSoup(response.text,'html.parser')

# ---------------------- Helpers ---------------------- #
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


def raw_scrapper(url):
    """Scrapes and returns {'url': url, 'raw_data': ...} or None."""    
    try:
        # Check stop event
        if stop_event.is_set():
            return None
        
        # Minimal per-thread delay
        time.sleep(random.uniform(0.2, 0.8))
        if stop_event.is_set():
            return None

        print(f"🔍 Scraping: {url}")
        response = fetch_page(url) 
        if not response:
            print(f"❌ Failed to fetch {url}")
            return None
            
        # Verify we got content
        if not response.text or len(response.text) < 100:
            print(f"❌ Empty or too short response for {url}")
            return None
            
        soup = scraper(response)

        initial_data = {}
        scripts_found = 0
        for script in soup.find_all("script"):
            scripts_found += 1
            if script.string and "window.__initialData__" in script.string:
                json_text = extract_balanced_json(script.string, "window.__initialData__")
                if json_text:
                    try:
                        initial_data = json.loads(json_text)
                        print(f"✅ Found initialData in {url}")
                        break
                    except Exception as e:
                        print(f"❌ JSON parse failed for {url}: {e}")
                        continue
        
        if not initial_data:
            print(f"⚠️ No initialData found in {url} (checked {scripts_found} scripts)")
            return None

        result = {
            "url": url, 
            "raw_data": {
                "basicDetails": initial_data.get("projectDetailState", {}).get("pageData", {}).get("basicDetails", {}),
                "components": initial_data.get("projectDetailState", {}).get("pageData", {}).get("components", {})
            }
        }
        
        # Verify we got meaningful data
        if not result["raw_data"]["basicDetails"] and not result["raw_data"]["components"]:
            print(f"⚠️ No meaningful data extracted from {url}")
            return None
            
        return result

    except Exception as e:
        print(f"❌ Error scraping {url}: {e}")
        return None


# ---------------------- Setup ---------------------- #
BASE_URL = "./"
city_name = "bangalore"

# Use Event for thread-safe flag
askForCaptcha = threading.Event()
stop_event = threading.Event()

unique_urls_file = f"{BASE_URL}{city_name}/unique_urls_sqy_99acres.json"  # list of {source_url, urls[]}
raw_output_file = f"{BASE_URL}{city_name}/raw_{city_name}_99acres.ndjson"

# Already scraped (seen) URLs
seen_urls = set(e.get("url", "").lower() for e in load_json(raw_output_file) if e.get("url"))

# Input URLs
unique_urls_data = list(load_json(unique_urls_file))
if not unique_urls_data:
    print(f"No data found in {unique_urls_file}")
    sys.exit(0)

unique_urls = [u for item in unique_urls_data for u in item.get("urls",[])]
print(len(unique_urls))
processed_urls = [u for u in unique_urls if u not in seen_urls]

print(f"Total urls: {len(unique_urls)} | Seen urls: {len(seen_urls)} | Remaining: {len(processed_urls)}")

# ---------------------- Multi-threading ---------------------- #
lock = threading.Lock()
save_every = 5  # Save more frequently
batch_buffer = []
processed_count = 0  # track successful scrapes
failed_count = 0  # track failed attempts

print(f"🚀 Starting scraper with {len(processed_urls)} URLs to process...")

try:
    # Use 2 workers for better throughput
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_to_url = {executor.submit(raw_scrapper, url): url for url in processed_urls}

        for idx, future in enumerate(as_completed(future_to_url), start=1):

            # ✅ If captcha triggered, exit immediately
            if askForCaptcha.is_set():
                print("🚨 Captcha detected! Exiting all threads immediately.")
                sys.exit(0)

            result = future.result()
            if result:
                with lock:
                    batch_buffer.append(result)
                    seen_urls.add(result["url"].lower())
                    processed_count += 1  # increment success count

                    if len(batch_buffer) >= save_every:
                        for rec in batch_buffer:
                            save_json(raw_output_file, rec)
                        batch_buffer = []
                        print(f"💾 Progress saved after {save_every} new records")
                    
                    # Print progress every 10 successful scrapes
                    if processed_count % 10 == 0:
                        success_rate = (processed_count / (processed_count + failed_count)) * 100 if (processed_count + failed_count) > 0 else 0
                        print(f"📊 Progress: {processed_count} successful, {failed_count} failed (Success rate: {success_rate:.1f}%)")
                    
                # ⏸ Pause after every 50 successful items
                if processed_count % 50 == 0:
                    print("⏸ Processed 50 items. Sleeping for 2 minutes...")
                    time.sleep(120)  # 2 minutes
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
    
    # Close all sessions
    if hasattr(thread_local, 'session'):
        thread_local.session.close()
    
    os._exit(1)

except Exception as e:
    print(f"❌ Unexpected error: {e}")
    stop_event.set()

finally:
    # Final save
    with lock:
        for rec in batch_buffer:
            save_json(raw_output_file, rec)
    
    # Close sessions
    if hasattr(thread_local, 'session'):
        thread_local.session.close()
    
    success_rate = (processed_count / (processed_count + failed_count)) * 100 if (processed_count + failed_count) > 0 else 0
    print(f"✅ Scraping completed! Final stats: {processed_count} successful, {failed_count} failed (Success rate: {success_rate:.1f}%)")
