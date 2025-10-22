
import os
import json
import sys
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import utils.custom_method as cm


def parse_structured_data(structured_data):
    prepare_data = []

    if not structured_data:
        return prepare_data

    # Case 1: First element contains itemListElement
    first = structured_data[0]
    if isinstance(first, dict) and "itemListElement" in first:
        for item in first.get("itemListElement", []):
            if "url" in item:
                prepare_data.append({
                    "source": "housing",
                    "url": item["url"],
                    "price": 0
                })

    # Case 2: Other elements contain product/apartment details
    for block in structured_data[1:]:
        if not isinstance(block, dict):
            continue
        url = block.get("url")
        offer = block.get("offers", {})
        if url and offer:
            price = offer.get("price", 0)
            for record in prepare_data:
                if record["url"] == url:
                    record["price"] = price
                    break
            else:
                prepare_data.append({
                    "source": "housing",
                    "url": url,
                    "price": price
                })

    return prepare_data


def extract_initial_state(soup):
    patterns = ["__INITIAL_STATE__", "__initialData__"]
    for script in soup.find_all("script"):
        if not script.string:
            continue
        content = script.string

        for pat in patterns:
            if pat not in content:
                continue

            # Case 1: JSON.parse("...") ---
            match = re.search(r'JSON\.parse\("(.+)"\)', content, re.DOTALL)
            if match:
                raw_str = match.group(1)
                try:
                    return json.loads(raw_str.encode().decode("unicode_escape"))
                except Exception as e:
                    print("Parse error in JSON.parse case:", e)
                    continue

            # Case 2: Direct assignment {...} ---
            match = re.search(rf'{re.escape(pat)}.*?=\s*({{.*}});?', content, re.DOTALL)
            if match:
                raw_str = match.group(1)
                try:
                    return json.loads(raw_str)
                except Exception as e:
                    print("Parse error in direct JSON case:", e)
                    continue

    return {}


BASE_URL = "https://housing.com"
city_name = "bangalore"
input_file = f"./raw_{city_name}_housing_data.ndjson"
output_file = f"./raw_{city_name}_housing_data_property.ndjson"
cache_file = f"./raw_{city_name}_suburl_cache.ndjson"


# -----------------------------------------------------------
# Load existing cache into dict
# -----------------------------------------------------------
suburl_cache = {}
if os.path.exists(cache_file):
    with open(cache_file, "r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
                suburl_cache[rec["url"]] = rec["structuredData"]
            except Exception:
                continue


def save_to_cache(url, structured_data):
    """Save one sub-url structuredData into cache file"""
    with open(cache_file, "a", encoding="utf-8") as f:
        f.write(json.dumps({"url": url, "structuredData": structured_data}, ensure_ascii=False) + "\n")


# -----------------------------------------------------------
# Thread worker to fetch + parse a single URL (with cache)
# -----------------------------------------------------------
def process_sub_url(e):
    if e in suburl_cache:
        structured_data = suburl_cache[e]
        return parse_structured_data(structured_data)

    try:
        response = cm.fetch_page(e)
        soup = cm.scraper(response)
        result = extract_initial_state(soup)
        structured_data = result.get("searchResults", {}).get("structuredData", [])

        if structured_data:
            save_to_cache(e, structured_data)
            suburl_cache[e] = structured_data

        return parse_structured_data(structured_data)
    except Exception as ex:
        print(f"Error fetching {e}: {ex}")
        return []


# -----------------------------------------------------------
# Main loop
# -----------------------------------------------------------
with open(input_file, "r", encoding="utf-8") as f_in, open(output_file, "a", encoding="utf-8") as f_out:

    for idx, line in enumerate(f_in, start=1):
        try:
            items = json.loads(line)
        except json.JSONDecodeError:
            continue

        if not items:
            continue

        url = items.get("url", "")
        output_data = None

        # Case 1: Project page
        if f"{BASE_URL}/in/buy/projects/page/" in url:
            urls = items.get("urls", [])
            print(f"[PROJECT] {url} → {len(urls)} sub-urls")

            prepare_data_all = []
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_map = {executor.submit(process_sub_url, e): e for e in urls}
                for future in as_completed(future_map):
                    data = future.result()
                    if data:
                        prepare_data_all.extend(data)
                    time.sleep(0.2)

            if prepare_data_all:
                output_data = {url: prepare_data_all}

        # Case 2: Rent page
        elif f"{BASE_URL}/rent" in url:
            print(f"[RENT] {url}")
            result = items.get("raw_data", {})
            structured_data = result.get("searchResults", {}).get("structuredData", [])
            prepare_data = parse_structured_data(structured_data)
            if prepare_data:
                output_data = {url: prepare_data}

        # Case 3: Buy page
        elif f"{BASE_URL}/in/buy/" in url:
            print(f"[BUY] {url}")
            result = items.get("raw_data", {})
            structured_data = result.get("searchResults", {}).get("structuredData", [])
            prepare_data = parse_structured_data(structured_data)
            if prepare_data:
                output_data = {url: prepare_data}

        if output_data:
            f_out.write(json.dumps(output_data, ensure_ascii=False) + "\n")

        if idx % 1000 == 0:
            print(f"Processed {idx} lines")

































































# import os
# import json
# import sys
# import re

# sys.path.append(os.path.dirname(os.path.dirname(__file__)))
# import utils.custom_method as cm
# import time


# def parse_structured_data(structured_data):
#     prepare_data = []

#     if not structured_data:
#         return prepare_data

#     # Case 1: First element contains itemListElement
#     urls_from_list = set()
#     first = structured_data[0]
#     if isinstance(first, dict) and "itemListElement" in first:
#         for item in first.get("itemListElement", []):
#             if "url" in item:
#                 urls_from_list.add(item["url"])
#                 # Add a placeholder (price will be filled if found later)
#                 prepare_data.append({
#                     "source": "housing",
#                     "url": item["url"],
#                     "price": 0
#                 })

#     # Case 2: Other elements contain product/apartment details
#     for block in structured_data[1:]:
#         if not isinstance(block, dict):
#             continue
#         url = block.get("url")
#         offer = block.get("offers", {})
#         if url and offer:
#             price = offer.get("price", 0)
#             # Update price if url already exists in prepare_data
#             for record in prepare_data:
#                 if record["url"] == url:
#                     record["price"] = price
#                     break
#             else:
#                 # If not already in prepare_data, add fresh
#                 prepare_data.append({
#                     "source": "housing",
#                     "url": url,
#                     "price": price
#                 })

#     return prepare_data


# def extract_balanced_json(text, start_token):
#     start = text.find(start_token)
#     if start == -1:
#         return None
#     start = text.find("{", start)
#     if start == -1:
#         return None

#     brace_count = 0
#     end = start
#     while end < len(text):
#         if text[end] == "{":
#             brace_count += 1
#         elif text[end] == "}":
#             brace_count -= 1
#             if brace_count == 0:
#                 break
#         end += 1

#     return text[start:end + 1] if brace_count == 0 else None



# def extract_initial_state(soup):
#     patterns = ["__INITIAL_STATE__", "__initialData__"]
#     for script in soup.find_all("script"):
#         if not script.string:
#             continue
#         content = script.string

#         for pat in patterns:
#             if pat not in content:
#                 continue

#             # --- Case 1: JSON.parse("...") ---
#             match = re.search(r'JSON\.parse\("(.+)"\)', content, re.DOTALL)
#             if match:
#                 raw_str = match.group(1)
#                 try:
#                     return json.loads(raw_str.encode().decode("unicode_escape"))
#                 except Exception as e:
#                     print("Parse error in JSON.parse case:", e)
#                     continue

#             # --- Case 2: Direct assignment {...} ---
#             match = re.search(rf'{re.escape(pat)}.*?=\s*({{.*}});?', content, re.DOTALL)
#             if match:
#                 raw_str = match.group(1)
#                 try:
#                     return json.loads(raw_str)
#                 except Exception as e:
#                     print("Parse error in direct JSON case:", e)
#                     continue

#     return {}

# city_name = "bangalore"
# input_file = f"./raw_{city_name}_housing_data.ndjson"
# output_file = f"./raw_{city_name}_housing_data_property.ndjson"

# count = 0

# print(f"✅ {count} records count")

# BASE_URL = "https://housing.com"

# def save_record(file_path, record):
#     """Append a single record to NDJSON file"""
#     with open(file_path, "a", encoding="utf-8") as f_out:
#         f_out.write(json.dumps(record, ensure_ascii=False) + "\n")


# with open(input_file, "r", encoding="utf-8") as f_in, open(output_file, "a", encoding="utf-8") as f_out:

#     for idx, line in enumerate(f_in, start=1):
#         try:
#             items = json.loads(line)
#         except json.JSONDecodeError:
#             continue

#         if not items:
#             continue

#         url = items.get("url", "")
#         output_data = None  

#         # Case 1: Project page
#         if f"{BASE_URL}/in/buy/projects/page/" in url:
#             urls = items.get("urls", [])
#             print(f"[PROJECT] {url} → {len(urls)} sub-urls")
#             prepare_data_all = []
#             for e in urls:
#                 try:
#                     response = cm.fetch_page(e)
#                     soup = cm.scraper(response)
#                     result = extract_initial_state(soup)
#                     structured_data = result.get("searchResults", {}).get("structuredData", [])
#                     prepare_data = parse_structured_data(structured_data)
#                     if prepare_data:
#                         prepare_data_all.extend(prepare_data)
#                     time.sleep(2)
#                 except Exception as ex:
#                     print(f"Error fetching {e}: {ex}")
            
#             if prepare_data_all:
#                 output_data = {url: prepare_data_all}

#         # Case 2: Rent page
#         elif f"{BASE_URL}/rent" in url:
#             print(f"[RENT] {url}")
#             result = items.get("raw_data", {})
#             structured_data = result.get("searchResults", {}).get("structuredData", [])
#             prepare_data = parse_structured_data(structured_data)
#             if prepare_data:
#                 output_data = {url: prepare_data}

#         # Case 3: Buy page
#         elif f"{BASE_URL}/in/buy/" in url:
#             print(f"[BUY] {url}")
#             result = items.get("raw_data", {})
#             structured_data = result.get("searchResults", {}).get("structuredData", [])
#             prepare_data = parse_structured_data(structured_data)
#             if prepare_data:
#                 output_data = {url: prepare_data}

#         # Write to disk
#         if output_data:
#             f_out.write(json.dumps(output_data, ensure_ascii=False) + "\n")

#         if idx % 10000 == 0:
#             print(f"Processed {idx} lines")

# stats = {
#     "new_sub_urls": 0,
#     "total_sub_urls": 0,
#     "total_urls": 0,
#     "rent_urls": 0,
#     "sale_urls": 0,
#     "others": 0
# }

# with open(input_file, "r", encoding="utf-8") as f_in, open(output_file, "w", encoding="utf-8") as f_out:
#     for idx, line in enumerate(f_in, start=1):
#         try:
#             record = json.loads(line)
#         except json.JSONDecodeError:
#             continue

#         if not record:
#             continue

#         url = record.get("url", "")

#         # Case 1: Project page
#         if f"{BASE_URL}/in/buy/projects/page/" in url:
#             existing_urls = record.get("urls", [])

#             if not existing_urls:
#                 props = (
#                     record.get("raw_data", {})
#                     .get("meta", {})
#                     .get("projectProperties", {})
#                     .get("data", {})
#                 )
#                 new_urls = []
#                 buy_url = props.get("buy", {}).get("canonical")
#                 rent_url = props.get("rent", {}).get("canonical")

#                 if buy_url:
#                     new_urls.append(f"https://housing.com/{buy_url}")
#                 if rent_url:
#                     new_urls.append(f"https://housing.com/{rent_url}")

#                 if new_urls:
#                     record["urls"] = new_urls
#                     stats["new_sub_urls"] += len(new_urls)
#                     stats["total_sub_urls"] += len(new_urls)
#                 else:
#                     stats["total_sub_urls"] += len(existing_urls)
#             else:
#                 normalized_urls = []
#                 for e in existing_urls:
#                     normalized_urls.append(e.replace("https://housing.com//", "https://housing.com/"))

#                 record["urls"] = normalized_urls
#                 stats["total_sub_urls"] += len(existing_urls)

#             stats["total_urls"] += 1

#         # Case 2: Rent page
#         elif f"{BASE_URL}/rent" in url:
#             stats["rent_urls"] += 1

#         # Case 3: Buy page
#         elif f"{BASE_URL}/in/buy/" in url:
#             stats["sale_urls"] += 1

#         else:
#             stats["others"] += 1

#         # Write record if needed
#         f_out.write(json.dumps(record, ensure_ascii=False) + "\n")

#         if idx % 10000 == 0:
#             print(f"Processed {idx} lines")

# print(stats)
# stats = {
#     "c_meta":0,
#     "not_meta":0,
# }
# with open(input_file, "r", encoding="utf-8") as f_in:
#     for idx, line in enumerate(f_in, start=1):
#         try:
#             record = json.loads(line)
#         except json.JSONDecodeError:
#             continue

#         if not record:
#             continue
        
#         url = record.get("url", "")
#         if f"{BASE_URL}/in/buy/projects/page/" in url:  
#             props = (record.get("raw_data", {}).get("meta", {}))
#             if props:
#                 stats["c_meta"] +=1
#             else:
#                 stats["not_meta"]+1

                 

#         if idx % 10000 == 0:
#             print(f"Processed {idx} lines")

# print(stats)