import json
import os

# ---------------- CONFIG ----------------
city_name = "bangalore"
INPUT_FILE = f"./{city_name}/query_{city_name}_all_sources_sqy.ndjson"
OUTPUT_DIR = f"./{city_name}/sources"
SOURCES = {
    "housing.com": "housing",
    "magicbricks.com": "magicbricks",
    "nobroker.in": "nobroker",
    "99acres.com": "99acres",
    "squareyards.com": "squareyards"
}

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Load input NDJSON (one JSON object per line)
    data = []
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    data.append(json.loads(line))
                except json.JSONDecodeError as e:
                    print(f"⚠️ Skipping invalid line: {e}")

    # Prepare buckets
    source_buckets = {short: [] for short in SOURCES.values()}
    leftover = []
    print(f"Total Projects: {len(data)}")

    for record in data:
        source_results = {short: [] for short in SOURCES.values()}
        other_results = []

        for result in record.get("query_results", []):
            url = result.get("url", "")
            matched = False
            for domain, short_name in SOURCES.items():
                if domain in url:
                    source_results[short_name].append(result)
                    matched = True
                    break
            if not matched:
                other_results.append(result)

        # Save filtered records per source
        for short_name, results in source_results.items():
            if results:
                new_record = dict(record)  # shallow copy
                new_record["query_results"] = results
                source_buckets[short_name].append(new_record)

        # Leftovers
        if other_results:
            new_record = dict(record)
            new_record["query_results"] = other_results
            leftover.append(new_record)

    # Save each bucket in NDJSON format
    for short_name, records in source_buckets.items():
        out_file = os.path.join(OUTPUT_DIR, f"{short_name}.ndjson")
        with open(out_file, "w", encoding="utf-8") as f:
            for r in records:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        print(f"✅ Saved {len(records)} records to {out_file}")

    # Save leftover
    leftover_file = os.path.join(OUTPUT_DIR, "leftover.ndjson")
    with open(leftover_file, "w", encoding="utf-8") as f:
        for r in leftover:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"✅ Saved {len(leftover)} leftover records to {leftover_file}")

if __name__ == "__main__":
    main()
