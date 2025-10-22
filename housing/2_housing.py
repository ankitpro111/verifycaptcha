import os
import json
import logging
import time
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
import re



import requests
import ijson
from bs4 import BeautifulSoup

# -------------------------------------------------------------------
# Setup
# -------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/115.0 Safari/537.36'
})

lock = threading.Lock()  # for safe file writes

# -------------------------------------------------------------------
# File utils
# -------------------------------------------------------------------
def load_json(file_path):
    """
    Stream-load large NDJSON or JSON array files.
    Returns a generator of objects.
    """
    if not os.path.exists(file_path):
        return []

    if file_path.endswith(".ndjson"):
        # NDJSON reader
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                if line.strip():
                    yield json.loads(line)
    else:
        # JSON array reader
        with open(file_path, 'r', encoding='utf-8') as f:
            for item in ijson.items(f, 'item'):
                yield item


def append_json(file_path, record):
    """
    Append one record to NDJSON file (newline-delimited JSON).
    """
    try:
        dir_name = os.path.dirname(file_path)
        if dir_name:  # only create dirs if path has them
            os.makedirs(dir_name, exist_ok=True)
        with lock:
            with open(file_path, 'a', encoding='utf-8') as f:
                safe_line = json.dumps(record, ensure_ascii=False)

                # 🔹 Clean invalid surrogate characters before writing
                safe_line = safe_line.encode("utf-8", "surrogatepass").decode("utf-8", "ignore")

                f.write(safe_line + '\n')
    except Exception as e:
        logger.error(f"Error writing record to {file_path}: {e}")



# -------------------------------------------------------------------
# Fetch helpers
# -------------------------------------------------------------------
def fetch_page(url, retries=3):
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=20)
            if r.status_code == 200:
                return r
            elif r.status_code == 404:
                return None
        except requests.RequestException:
            time.sleep(1 + attempt)
    return None


def scraper(response):
    return BeautifulSoup(response.text, "html.parser") if response else None


# -------------------------------------------------------------------
# JSON extractors
# -------------------------------------------------------------------
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

    return text[start:end + 1] if brace_count == 0 else None



def extract_initial_state(soup):
    patterns = ["__INITIAL_STATE__", "__initialData__"]
    for script in soup.find_all("script"):
        if not script.string:
            continue
        content = script.string

        for pat in patterns:
            if pat not in content:
                continue

            # --- Case 1: JSON.parse("...") ---
            match = re.search(r'JSON\.parse\("(.+)"\)', content, re.DOTALL)
            if match:
                raw_str = match.group(1)
                try:
                    return json.loads(raw_str.encode().decode("unicode_escape"))
                except Exception as e:
                    print("Parse error in JSON.parse case:", e)
                    continue

            # --- Case 2: Direct assignment {...} ---
            match = re.search(rf'{re.escape(pat)}.*?=\s*({{.*}});?', content, re.DOTALL)
            if match:
                raw_str = match.group(1)
                try:
                    return json.loads(raw_str)
                except Exception as e:
                    print("Parse error in direct JSON case:", e)
                    continue

    return {}


# -------------------------------------------------------------------
# Scraper
# -------------------------------------------------------------------
def scrape_housing_record(url):
    resp = fetch_page(url)
    if not resp:
        return {"url": url, "error": "Failed or 404"}

    soup = scraper(resp)
    if not soup:
        return {"url": url, "error": "No soup parsed"}

    initial_data = extract_initial_state(soup)
    props = initial_data.get("meta", {}).get("projectProperties", {}).get("data", {})
    buy_url = props.get("buy", {}).get("canonical")
    rent_url = props.get("rent", {}).get("canonical")

    urls = []
    if buy_url:
        urls.append(f"https://housing.com/{buy_url}")
    if rent_url:
        urls.append(f"https://housing.com/{rent_url}")

    return {"url": url, "raw_data": initial_data, "urls": urls}


# -------------------------------------------------------------------
# Multi-thread runner
# -------------------------------------------------------------------
def run_scraper(processed_data, raw_data_file, existing_urls, workers=10):
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(scrape_housing_record, u): u for u in processed_data if u not in existing_urls}

        for i, f in enumerate(as_completed(futures), 1):
            url = futures[f]
            try:
                result = f.result()
            except Exception as e:
                logger.error(f"Error on {url}: {e}")
                continue

            # Save immediately (streaming)
            append_json(raw_data_file, result)

            existing_urls.add(url)

            if i % 50 == 0:
                logger.info(f"Progress: {i}/{len(futures)} completed, {len(existing_urls)} total saved")

            # Random jitter to avoid blocking
            time.sleep(random.uniform(0.5, 1.5))

    logger.info(f"✅ Finished. Total saved: {len(existing_urls)}")


# -------------------------------------------------------------------
# Entry
# -------------------------------------------------------------------
if __name__ == "__main__":
    BASE_URL = "../../"
    city_name = "bangalore"
    unique_urls_file = "./unique_urls.json"
    raw_data_file = f"raw_{city_name}_housing.ndjson"   # switched to NDJSON

    # Load input URLs
    unique_urls_data = list(load_json(unique_urls_file))

    # Load already-scraped URLs (from NDJSON)
    raw_data_record = list(load_json(raw_data_file))
    existing_urls = {e["url"] for e in raw_data_record}

    # Flatten new URLs
    processed_data = [url for item in unique_urls_data for url in item.get("urls", []) if url not in existing_urls]

    logger.info(f"Total to process: {len(processed_data)}, Already scraped: {len(existing_urls)}")

    run_scraper(processed_data, raw_data_file, existing_urls, workers=10)
