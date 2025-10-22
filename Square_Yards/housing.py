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


lock = threading.Lock()  # for safe file writes
list_source_urls = {}

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


def collect_existing_urls(raw_data_file):
    existing_urls = set()
    with open(raw_data_file, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue  # skip malformed lines

            url = obj.get("url")
            if url:
                existing_urls.add(url)

    return existing_urls

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

def fetch_url_listing1():
    url = "https://mightyzeus-mum.housing.com/api/gql/stale?apiName=ESSENTIAL_DETAILS&emittedFrom=client_buy_details&isBot=false&platform=desktop&source=web&source_name=AudienceWeb"
    
    query = """
    query(
        $hash: String!
        $service: String!
        $category: String!
        $pageInfo: PageInfoInput
    ) {
        searchResults(
            hash: $hash
            service: $service
            category: $category
            pageInfo: $pageInfo
        ) {
            canonical
            properties {
                title
                url
                displayPrice {
                    displayValue
                    value
                }
                builtUpArea {
                    value
                    unit
                }
                propertyType
                isActiveProperty
                sellers {
                    name
                    firmName
                    url
                    type
                    isPrime
                    isPaid
                    designation
                    isCertifiedAgent
                }
            }
        }
    }
    """
    
    variables = {
        "service": "buy",
        "hash": "AGqx4",
        "category": "residential",
        "pageInfo": {
            "page": 1,
            "size": 20
        }
    }
    
    payload = {
        "query": query,
        "variables": variables
    }

    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.post(url, headers=headers, data=json.dumps(payload))
    return response.json()


def fetch_paginated(service, count, raw_data, query, headers):
    listings = []
    page = 1

    # Construct dynamic URL with proper emittedFrom value
    url = (
        f"https://zeusptest-mum.housing.com/api/gql/stale"
        f"?apiName=SEARCH_RESULTS"
        f"&emittedFrom=client-{service}-SRP"
        f"&isBot=false&platform=desktop&source=web&source_name=AudienceWeb"
    )
    
    while len(listings) < count:
        variables = {
            "hash": raw_data.get("id"),
            "service": service,
            "category": raw_data.get("category"),
            "city": {"id": raw_data.get("userCity")},
            "pageInfo": {"page": page, "size": 30}
        }

        payload = {
            "query": query,
            "variables": variables
        }

        try:
            response = requests.post(url, headers=headers, data=json.dumps(payload))
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Request failed: {e}")
            break

        data = response.json().get("data", {}).get("searchResults", {}).get("properties", [])
        if not data:
            break  # No more data

        listings.extend(data)
        page += 1

    return listings[:count]

    # fragment PR on Property {
    #     features { label description id }
    #     coverImage { src alt videoUrl }
    #     title
    #     subtitle
    #     address { address url }
    #     price
    #     displayPrice { displayValue unit }
    #     url
    #     listingId
    #     propertyType
    #     builtUpArea { value unit }
    #     furnishingType
    #     sellers {
    #         ...BS
    #         phone { partialValue }
    #     }
    # }


def fetch_url_listing(raw_data):
    query = """
    fragment PR on Property {
        title
        address { address }
        price        
        url
        listingId
        propertyType
        builtUpArea { value unit }
        furnishingType
        sellers {
            ...BS         
        }
    }

    fragment SR on Property {
        ...PR
        certifiedDetails {
            isVerifiedProperty
            isCertifiedProperty
        }
    }

    fragment BS on User {
        name
        id
        firmName
        url
        type
        sellerBadge
    }

    query SearchResultsQuery(
        $hash: String!
        $service: String!
        $category: String!
        $city: CityInput!
        $pageInfo: PageInfoInput!
    ) {
        searchResults(
            hash: $hash
            service: $service
            category: $category
            city: $city
            pageInfo: $pageInfo
        ) {
            properties { ...SR }
            config {
                filters
                pageInfo {
                    totalCount
                    size
                    page
                }
                entities {
                    id
                    type
                }
            }
        }
    }
    """

    headers = {'Content-Type': 'application/json'}
    result = {"buy": [], "rent": []}

    # if raw_data.buyCount:
    #     result["buy"] = fetch_paginated("buy", raw_data.buyCount, raw_data, query, headers)

    # if raw_data.rentCount:
    #     result["rent"] = fetch_paginated("rent", raw_data.rentCount, raw_data, query, headers)


    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {}
        if raw_data.get("buyCount"):
            futures["buy"] = executor.submit(fetch_paginated, "buy", raw_data["buyCount"], raw_data, query, headers)
        if raw_data.get("rentCount"):
            futures["rent"] = executor.submit(fetch_paginated, "rent", raw_data["rentCount"], raw_data, query, headers)

        for key, future in futures.items():
            result[key] = future.result()  # Waits here for each thread to finish

    return result


# -------------------------------------------------------------------
# Fetch helpers
# -------------------------------------------------------------------
def fetch_page(url, retries=3):
    if not url:
        return None
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    ]

    for attempt in range(1, retries+1):
        try:
            headers = {
                "User-Agent": random.choice(user_agents),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive",
            }
            r = requests.get(url, headers=headers, timeout=20)            
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
    
    props_meta = initial_data.get("meta", {})
    cookies = initial_data.get("cookies",{})

    if "/buy/projects/page/" in url:
        project_props = props_meta.get("projectProperties")
        if project_props:  # only return if present

            hash_data = project_props.get("data",{})
            hash_code_url = hash_data.get("buy", {}).get("canonical", "") or hash_data.get("rent", {}).get("canonical", "")
            if hash_code_url:
                last_string = hash_code_url.split("-")[-1]
                
                if last_string:
                    cookies["id"] = last_string
                    cookies["buyCount"] = project_props.get("buyCount",0)
                    cookies["rentCount"] = project_props.get("rentCount",0)
                    rent_sale = fetch_url_listing(cookies)

            return {"url": url, "raw_data":initial_data, "listing": True, "data":rent_sale}
        else:
            return {"url": url, "raw_data": props_meta, "listing": False}
    else:
        return {"url": url, "raw_data": props_meta, "listing": False}


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

            # Add source_url if available
            result["source_url"] = list_source_urls.get(url)
                        
            # Save immediately (streaming)
            append_json(raw_data_file, result)

            existing_urls.add(url)

            if i % 50 == 0:
                logger.info(f"Progress: {i}/{len(futures)} completed, {len(existing_urls)} total saved")

            # Random jitter to avoid blocking
            time.sleep(random.uniform(2, 5))

    logger.info(f"✅ Finished. Total saved: {len(existing_urls)}")


# -------------------------------------------------------------------
# Entry
# -------------------------------------------------------------------
if __name__ == "__main__":
    BASE_URL = "./"
    city_name = "bangalore"
    unique_urls_file = f"./{city_name}/unique_urls_sqy_housing.json"
    raw_data_file = f"./{city_name}/raw_{city_name}_housing_listing.ndjson"   # switched to NDJSON

    # Load input URLs
    unique_urls_data = list(load_json(unique_urls_file))

    for item in unique_urls_data:
        for u in item.get("urls",[]):
            if u not in list_source_urls:
                list_source_urls[u] = item.get("source_url")
            

    # Load already-scraped URLs (from NDJSON)
    existing_urls = collect_existing_urls(raw_data_file)

    # raw_data_record = list(load_json(raw_data_file))
    # existing_urls = {e["url"] for e in raw_data_record}

    # Flatten new URLs
    processed_data = [url for item in unique_urls_data for url in item.get("urls", []) if url not in existing_urls][:1000]#[:1000]
    
    logger.info(f"Total to process: {len(processed_data)}, Already scraped: {len(existing_urls)}")

    run_scraper(processed_data, raw_data_file, existing_urls, workers=5)
