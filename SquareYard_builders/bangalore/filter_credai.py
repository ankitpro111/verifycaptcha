import json
import os

def load_ndjson(file_input):
    """Load NDJSON file and return a list of dicts"""
    data = []
    if os.path.exists(file_input):
        with open(file_input, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    data.append(json.loads(line))
    return data

def load_json(file_input):
    """Load JSON file and return a dict/list"""
    if os.path.exists(file_input):
        with open(file_input, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

# Input files
file_1 = "builders_bangalore.ndjson"
file_2 = "credai.json"

# Load data
file1_data = load_ndjson(file_1)
file2_data = load_json(file_2)

# Normalize names for comparison
def normalize_name(name: str) -> str:
    return "".join(name.lower().split())  # lowercase + remove spaces

# Example: find matching records
matches = []
seen = set()
seen1 = set()

print(f" {len(file1_data)} {len(file2_data)}")
for item1 in file1_data:
    name1 = normalize_name(item1.get("name", ""))
    for item2 in file2_data:
        name2 = normalize_name(item2.get("builder", ""))  # use correct key
        # matches.append(f"{name1} in {name2}  = {name1 in name2 }")
        if name1 and name1 in name2:
            # avoid duplicates
            if name1 not in seen:
                # keep only item1 (your version)
                matches.append(item1)
                # OR merge dicts -> matches.append({**item1, **item2})
                seen.add(name1)
                seen1.add(name2)

# Save to NDJSON output
output_file_ndjson = "merged_credai.ndjson"
with open(output_file_ndjson, "w", encoding="utf-8") as f:
    for row in matches:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")

print(f"✅ Saved {len(matches)} records to {output_file_ndjson}")

print(seen1)
# === Find non-matching items from file2 ===
unmatched = []
for item2 in file2_data:
    name2 = normalize_name(item2.get("builder", ""))
    # print(f"{name2} {name2 not in seen1}")
    if name2 not in seen1:
        unmatched.append(item2)

# Save unmatched
unmatched_file = "unmatched_credai.ndjson"
with open(unmatched_file, "w", encoding="utf-8") as f:
    for row in unmatched:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")

print(f"✅ Saved {len(unmatched)} unmatched records from file2 to {unmatched_file}")


