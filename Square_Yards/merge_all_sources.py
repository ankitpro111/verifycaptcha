import json
import os

city_name = "bangalore"

input_file = f"./{city_name}/projects_{city_name}_sy_response.ndjson"
listing_file1 = f"./{city_name}/raw_{city_name}_housing_listing.ndjson"
listing_file2 = f"./{city_name}/raw_{city_name}_mb_listing.ndjson"
listing_file3 = f"./{city_name}/raw_{city_name}_nb_listing.ndjson"
output_file = f"./{city_name}/final_{city_name}_sy_response1.ndjson"

# ----------------------------------------
# Load all listing sources into lookup dicts
# ----------------------------------------

def load_housing_lookup(path):
    lookup = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line.strip())
            source_url = obj.get("source_url")
            if source_url and obj.get("listing"):
                lookup[source_url] = obj
    return lookup

def load_magicbricks_lookup(path):
    lookup = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line.strip())
            source_url = obj.get("source_url")
            if source_url and obj.get("listing"):
                lookup[source_url] = obj
    return lookup

def load_nobroker_lookup(path):
    lookup = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line.strip())
            source_url = obj.get("source_url")
            if source_url and obj.get("data"):
                lookup[source_url] = obj
    return lookup


housing_lookup = load_housing_lookup(listing_file1)
magicbricks_lookup = load_magicbricks_lookup(listing_file2)
nobroker_lookup = load_nobroker_lookup(listing_file3)

# ----------------------------------------
# Merge all listings into a single output
# ----------------------------------------

with open(input_file, "r", encoding="utf-8") as f1, open(output_file, "w", encoding="utf-8") as out:
    for i, line in enumerate(f1, 1):
        try:
            obj = json.loads(line.strip())
            source_url = obj.get("source_url")

            if not source_url or obj.get("error") or not obj.get("units"):
                continue

            # Normalize unit text
            obj["units"] = [
                {
                    **item,
                    "text": " ".join(item.get("text", "").split())
                } for item in obj.get("units", [])
            ]

            # Normalize listingsMapRent/Sale to dicts
            if isinstance(obj.get("listingsMapRent"), list):
                obj["listingsMapRent"] = {"squareyard": obj["listingsMapRent"]}
            if isinstance(obj.get("listingsMapSale"), list):
                obj["listingsMapSale"] = {"squareyard": obj["listingsMapSale"]}
            obj.setdefault("listingsMapRent", {})
            obj.setdefault("listingsMapSale", {})

            # ------------------ HOUSING ------------------
            if source_url in housing_lookup:
                raw = housing_lookup[source_url].get("listing", {})
                rent = raw.get("rent", [])
                buy = raw.get("buy", [])
                obj["listingsMapRent"]["housing"] = [
                    {
                        "title": u.get("title", ""),
                        "unitType": "apartment" if "flat" in u.get("title", "").lower() else "",
                        "size": u.get("builtUpArea", {}).get("value", ""),
                        "price": u.get("price", 0),
                        "type": u.get("propertyType", ""),
                        "posted_date": "",
                        "posted_by": u.get("sellers", [{}])[0].get("type", "") if u.get("sellers") else "",
                        "url": f"https://housing.com{u.get('url')}" if u.get("url") else ""
                    } for u in rent
                ]
                obj["listingsMapSale"]["housing"] = [
                    {
                        "title": u.get("title", ""),
                        "unitType": "apartment" if "flat" in u.get("title", "").lower() else "",
                        "size": u.get("builtUpArea", {}).get("value", ""),
                        "price": u.get("price", 0),
                        "type": u.get("propertyType", ""),
                        "posted_date": "",
                        "posted_by": u.get("sellers", [{}])[0].get("type", "") if u.get("sellers") else "",
                        "url": f"https://housing.com{u.get('url')}" if u.get("url") else ""
                    } for u in buy
                ]

            # ------------------ MAGICBRICKS ------------------
            if source_url in magicbricks_lookup:
                raw = magicbricks_lookup[source_url].get("listing", {})
                obj["listingsMapRent"]["magicbricks"] = raw.get("rent", [])
                obj["listingsMapSale"]["magicbricks"] = raw.get("buy", [])

            # ------------------ NOBROKER ------------------
            if source_url in nobroker_lookup:
                raw = nobroker_lookup[source_url].get("data", {})
                rent = raw.get("rentProperties", [])
                buy = raw.get("resaleProperties", [])
                obj["listingsMapRent"]["nobroker"] = [
                    {
                        "title": u.get("title"),
                        "unitType": "Apartment",
                        "size": u.get("propertySize", 0),
                        "price": u.get("rent", 0),
                        "type": u.get("propertyType", "").lower(),
                        "posted_date": u.get("creationDate"),
                        "posted_by": "",
                        "url": f"https://nobroker.in{u.get('detailUrl')}" if u.get("detailUrl") else ""
                    } for u in rent
                ]
                obj["listingsMapSale"]["nobroker"] = [
                    {
                        "title": u.get("title"),
                        "unitType": "Apartment",
                        "size": u.get("propertySize", 0),
                        "price": u.get("rent", 0),
                        "type": u.get("propertyType", "").lower(),
                        "posted_date": u.get("creationDate"),
                        "posted_by": "",
                        "url": f"https://nobroker.in{u.get('detailUrl')}" if u.get("detailUrl") else ""
                    } for u in buy
                ]

            # ------------------ Final Counts ------------------
            obj["noListingsRent"] = sum(len(v) for v in obj["listingsMapRent"].values() if isinstance(v, list))
            obj["noListingsSale"] = sum(len(v) for v in obj["listingsMapSale"].values() if isinstance(v, list))

            out.write(json.dumps(obj) + "\n")

        except json.JSONDecodeError as e:
            print(f"[Line {i}] JSON decode error: {e}")
        except Exception as e:
            print(f"[Line {i}] Error: {e}")

print(f"✅ Final merged file written to: {output_file}")
