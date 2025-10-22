import sys
import os
import json
import re
import random
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import ijson
from bs4 import BeautifulSoup
import requests
# -------------------------------------------------------------------
# Setup
# -------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
lock = threading.Lock()

# -------------------------------------------------------------------
# Config
# -------------------------------------------------------------------
ROW_LIMIT = 1   # set e.g. 500 to scrape only first 500 URLs


# -------------------------------------------------------------------
# Scraper core
# -------------------------------------------------------------------

headers = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
}

# Define the function to extract the project ID from URL
def extract_project_id(url):
    """
    Extract the project ID from the URL.
    The ID is the string after 'prjt-' in the URL.
    """
    match = re.search(r'prjt-([a-f0-9]+)', url)
    if match:
        return match.group(1)
    return None

# Function to fetch project data using the extracted project ID
def get_project_info_from_url(url):
    """
    Extract project ID from the URL and fetch the project info from NoBroker API.
    Returns the data in the required response format.
    """
    project_id = extract_project_id(url)
    if not project_id:
        return {"data": {}, "error": "Project ID not found in URL"}

    api_url = f"https://www.nobroker.in/api/v1/building/{project_id}/other-info"
    headers = {
        'Cookie': 'mbTrackID=bc5f54bb540144f2abc87a435955648a; nbDevice=desktop; nbccc=140ef2f81c494b018354a6420573171b'
    }

    try:
        response = requests.get(api_url, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors
        data = response.json()
        return {"data": data}  # Return the data in the required format
    except requests.exceptions.RequestException as e:
        return {"data": {}, "error": f"API request failed: {e}"}

def scraper(response):
    return BeautifulSoup(response.text, "html.parser") if response else None
def raw_scrapper(url):
    """
    Extract nb.appState -> builderProject data from NoBroker project page
    """
    response = requests.get(url, headers=headers)
    soup = scraper(response)
    if not soup:
        return {"url": url, "error": "No soup parsed"}

    # Step 1: Find the script containing nb.appState
    script_content = None
    for script in soup.find_all("script"):
        if script.string and "nb.appState" in script.string:
            script_content = script.string
            break

    if not script_content:
        return {"url": url, "error": "nb.appState script not found"}

    # Step 2: Extract the JSON-like block
    match = re.search(r'nb\.appState\s*=\s*(\{.*\})', script_content, re.DOTALL)
    if not match:
        return {"url": url, "error": "Could not extract nb.appState JSON block"}

    raw_data = match.group(1)

    # Step 3: Balance braces
    brace_count = 0
    end_index = None
    for i, ch in enumerate(raw_data):
        if ch == '{':
            brace_count += 1
        elif ch == '}':
            brace_count -= 1
            if brace_count == 0:
                end_index = i + 1
                break
    if end_index:
        raw_data = raw_data[:end_index]

    try:
        app_state = json.loads(raw_data)
    except Exception as e:
        return {"url": url, "error": f"JSON decode failed: {e}"}

    builderProject = app_state.get('builderProject', {})
    initial_data = builderProject.get('builderOtherData', {}).get('builderOtherInfo', {})

    return {"url": url, "raw_data": initial_data}



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
# Multi-thread runner
# -------------------------------------------------------------------
def run_scraper(unique_urls, raw_output_file, seen_urls, workers=10):
    with ThreadPoolExecutor(max_workers=workers) as executor:
        # futures = {executor.submit(raw_scrapper, u): u for u in unique_urls if u not in seen_urls}
        futures = {executor.submit(get_project_info_from_url, u): u for u in unique_urls if u not in seen_urls}

        for i, f in enumerate(as_completed(futures), 1):
            url = futures[f]
            try:
                result = f.result()
            except Exception as e:
                logger.error(f"Error on {url}: {e}")
                continue

            # Save immediately (append to NDJSON)
            append_json(raw_output_file, result)

            seen_urls.add(url)

            if i % 50 == 0:
                logger.info(f"Progress: {i}/{len(futures)} completed, {len(seen_urls)} total saved")

            # jitter
            time.sleep(random.uniform(0.5, 1.5))

    logger.info(f"✅ Finished. Total saved: {len(seen_urls)}")


# -------------------------------------------------------------------
# Entry
# -------------------------------------------------------------------
if __name__ == "__main__":
    BASE_URL = f"./"
    city_name = "bangalore"

    # unique_urls_file = f"{BASE_URL}{city_name}/unique_urls_nb.json"  # list of {source_url, urls[]}
    unique_urls_file = f"{BASE_URL}{city_name}/unique_urls_sqy_nobroker.json"  # list of {source_url, urls[]}
    raw_output_file = f"{BASE_URL}{city_name}/raw_{city_name}_nb.ndjson"  # NDJSON

    # Load input (array of objects with source_url + urls[])
    unique_urls_data = list(load_json(unique_urls_file))
    unique_urls = []

    if not unique_urls_data:
        logger.warning(f"No data found in {unique_urls_file}")
        sys.exit(0)

    # Flatten all nested "urls" into one list
    for item in unique_urls_data:
        for u in item.get("urls", []):
            if u:
                unique_urls.append(u.lower())

    # Load already-scraped records
    raw_data_list = list(load_json(raw_output_file))
    seen_urls = {e.get("url").lower() for e in raw_data_list if e.get("url")}

    # Filter
    processed_urls = [u for u in unique_urls if u not in seen_urls]

    # Apply row limit
    if ROW_LIMIT is not None:
        processed_urls = processed_urls[:ROW_LIMIT]
        logger.info(f"ROW_LIMIT applied → {ROW_LIMIT} URLs will be processed")

    logger.info(f"Total: {len(unique_urls)}, Already scraped: {len(seen_urls)}, To process: {len(processed_urls)}")

    run_scraper(processed_urls, raw_output_file, seen_urls, workers=10)
