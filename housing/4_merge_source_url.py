import os
import json
from collections import defaultdict
BASE_URL = "https://housing.com"
city_name = "bangalore"
unique_url_file = f"./unique_urls.json"
input_file = f"./raw_{city_name}_housing_data_property.ndjson"
output_file = f"./raw_{city_name}_housing_data_property_source.ndjson"

# Load unique URLs
with open(unique_url_file, 'r', encoding="utf-8") as f:
    unique_urls = json.load(f)

# Index unique_urls for faster lookup {url -> source_url}
url_to_source = {}
for e in unique_urls:
    source = e.get("source_url")
    for u in e.get("urls", []):
        url_to_source[u] = source

# Storage for merged data
merged_data = defaultdict(list)

# Read input and group by source_url
with open(input_file, "r", encoding="utf-8") as f_in:
    for idx, line in enumerate(f_in, start=1):
        try:
            items = json.loads(line)
        except json.JSONDecodeError:
            continue

        if not items:
            continue

        url = next(iter(items.keys()), None)
        if not url:
            continue

        find_source_url = url_to_source.get(url, "")
        if not find_source_url:
            continue

        # Append items under same source_url        
        values = list(items.values())

        for v in values:
            if isinstance(v, list):
                merged_data[find_source_url].extend(v)  # flatten nested list
            else:
                merged_data[find_source_url].append(v)  # add dict directly

query_project_file = f"../../{city_name}/query_{city_name}_housing.json"

# Load existing project data
with open(query_project_file, 'r', encoding="utf-8") as f:
    data_project = json.load(f)

for items in data_project:
    source_url = items.get("source_url", "")
    if not source_url:
        continue

    # Get merged items safely
    items_list = merged_data.get(source_url, [])

    # ---- Handle Sale ----
    listingsMapSale = items.get("listingsMapSale", {})
    # If it's a list, replace with dict
    if isinstance(listingsMapSale, list):
        listingsMapSale = {}
    listingsMapSale["housing"] = [
        e for e in items_list if "buy" in e.get("url", "")
    ]
    items["listingsMapSale"] = listingsMapSale  # assign back

    # ---- Handle Rent ----
    listingsMapRent = items.get("listingsMapRent", {})
    if isinstance(listingsMapRent, list):
        listingsMapRent = {}
    listingsMapRent["housing"] = [
        e for e in items_list if "rent" in e.get("url", "")
    ]
    items["listingsMapRent"] = listingsMapRent  # assign back

# Save updated project file
with open(query_project_file, "w", encoding="utf-8") as f:
    json.dump(data_project, f, indent=2, ensure_ascii=False)
