import os
import sys
import re
import json
from urllib.parse import urlparse
from difflib import SequenceMatcher

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import utils.custom_method as cm


# Words to ignore in comparison
IGNORE_WORDS = {"bangalore", "for", "sale", "rent", "buy", "sell", "povp"}

def clean_url_for_comparison(url):
    """Extract meaningful words from URL for matching"""
    path = urlparse(url).path.lower()
    path = re.sub(r'-?p(dp|ov)p[idf]?-.*$', '', path)  # remove random IDs
    tokens = re.split(r'[-/]', path)
    return [t for t in tokens if t and t not in IGNORE_WORDS]


def progressive_filter(urls, keyword_words):
    """
    Progressive filtering:
    - If only one keyword, filter based on it.
    - If multiple keywords, filter URLs containing all in sequence.
    """
    if not keyword_words:
        return urls

    filtered_urls = []

    # Join keywords as a sequence with hyphen (to match project slugs like "royal-east")
    sequence = "-".join(keyword_words).lower()

    for u in urls:
        u_lower = u.lower()
        if len(keyword_words) == 1:
            # Single keyword check
            if keyword_words[0].lower() in u_lower:
                filtered_urls.append(u)
        else:
            # Multiple keywords: must match sequence
            if sequence in u_lower:
                filtered_urls.append(u)

    return filtered_urls or []



BASE_URL = "./"
city_name = "bangalore"
input_file = f"{BASE_URL}{city_name}/sources/housing.ndjson"

# ---------------- Load NDJSON ----------------
input_record = []
with open(input_file, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            input_record.append(json.loads(line))
        except json.JSONDecodeError as e:
            print(f"⚠️ Skipping bad line: {e}")

# ---------------- Processing ----------------
city_list = [
    "delhi", "noida", "bangalore", "hyderabad", "chennai", "kolkata",
    "ahmedabad", "pune", "surat", "visakhapatnam", "indore", 
    "kochi", "nagpur", "lucknow", "jaipur","chandigarh", "mumbai"
]
city_list = [e for e in city_list if e != city_name]
unique_urls = []

counter = 0
for items in input_record:
    data_items = {}
    data_items["source_url"] = items.get("source_url")

    items_urls = [item.get("url").lower() for item in items.get("query_results", [])]

    parsed = urlparse(data_items["source_url"]).path
    parts = [p for p in parsed.split("/") if p]

    

    project_keyword = ""
    if len(parts) >= 3:
        project_keyword = parts[-3]   # second last segment
    else:
        project_keyword = parts[-1] if parts else ""

    # Normalize into /slug/
    project_keyword = f"/{project_keyword}/"

    # Split for keyword matching
    keyword_words = project_keyword.strip("/").split("-")

    
    city_in_url = []      # e.g. bangalore
    
    exclude_keywords = [
        "photos", "brochure", "floor-plan", "directions",
        "review", "video", "amenities", "news"
    ]
    exclude_cities = [e for e in city_list if e != city_in_url]

    base_filtered = [
        u for u in items_urls
        if u.startswith("https://housing.com/in/buy/projects/page/")
        and not any(e in u for e in exclude_keywords)
        and not any(e in u for e in exclude_cities)
    ]
    
    filtered_urls = progressive_filter(base_filtered, keyword_words)

    if filtered_urls:
        data_items["urls"] = filtered_urls
        unique_urls.append(data_items)

# ---------------- Save Final ----------------
cm.save_json(f"./{city_name}/unique_urls_sqy_housing1.json", unique_urls)

print(
    "Non Empty After Cleanups:", len(unique_urls),
    "urls:", len([e for item in unique_urls for e in item.get("urls", [])])
)
