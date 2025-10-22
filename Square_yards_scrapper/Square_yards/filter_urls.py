import os
import json
from urllib.parse import urlparse

city_name = "bangalore"
commonfloor_file = f"../{city_name}/projects_{city_name}.json"
squareYard_file = f"./projects_{city_name}_sy.ndjson"

# Load commonfloor data
with open(commonfloor_file, "r", encoding="utf-8") as f_in:
    commonfloor_record = json.load(f_in)

# Load squareyard data
squareYard_record = []
with open(squareYard_file, "r", encoding="utf-8") as f_in:
    for line in f_in:
        if line.strip():
            squareYard_record.append(json.loads(line))

def extract_cf_project_name(url: str) -> str:
    """Extract project slug from commonfloor URL."""
    try:
        path_parts = urlparse(url).path.strip("/").split("/")
        return path_parts[0].lower() if len(path_parts) > 0 else ""
    except Exception:
        return ""

def extract_sy_project_name(url: str) -> str:
    """Extract project slug from squareyard URL."""
    try:
        path_parts = urlparse(url).path.strip("/").split("/")
        # project slug always comes after city-residential-property
        return path_parts[1].lower() if len(path_parts) > 1 else ""
    except Exception:
        return ""

def is_match(sy_tokens, cf_tokens):
    """Match if all SY tokens appear at start of CF tokens (prefix match)."""
    if not sy_tokens or not cf_tokens:
        return False
    if len(sy_tokens) > len(cf_tokens):
        return False
    return cf_tokens[:len(sy_tokens)] == sy_tokens


# Build list of tokenized project names from CommonFloor
cf_projects = []
for cf in commonfloor_record:
    cf_url = cf.get("source_url", "")
    name = extract_cf_project_name(cf_url)
    if name:
        cf_projects.append((name.split("-"), cf_url))  # store tokens + url


# Collect unmatched SquareYard URLs
unmatched_sy_urls = []
for sy in squareYard_record:
    sy_url = sy.get("url", "")
    if not sy_url:
        continue

    sy_name = extract_sy_project_name(sy_url)
    if not sy_name:
        continue

    sy_tokens = sy_name.split("-")

    # Try to find a match in commonfloor
    matched = False
    for cf_tokens, cf_url in cf_projects:
        if is_match(sy_tokens, cf_tokens):
            matched = True
            break

    if not matched:
        unmatched_sy_urls.append(sy_url)

# Collect all commonfloor URLs for manual verification
cf_urls = [cf.get("source_url", "") for cf in commonfloor_record if cf.get("source_url")]

# Save output
output = {
    "unmatched_squareyard_urls": unmatched_sy_urls,
    "all_commonfloor_urls": cf_urls
}

output_file = f"./unmatched_sy_with_cf_urls.json"
with open(output_file, "w", encoding="utf-8") as f_out:
    json.dump(output, f_out, indent=2, ensure_ascii=False)

print(f"Unmatched SquareYard URLs: {len(unmatched_sy_urls)}")
print(f"CommonFloor URLs: {len(cf_urls)}")
print(f"Saved in {output_file}")
