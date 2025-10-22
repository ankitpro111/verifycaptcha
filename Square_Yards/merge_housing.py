import json

city_name = "bangalore"

input_file1 = f"./{city_name}/final_{city_name}_sy_response.ndjson"
input_file2 = f"./{city_name}/raw_{city_name}_housing.ndjson"
output_file = f"./{city_name}/merged_{city_name}_sy_response.ndjson"

sy_unit_type = [
  "Apartment",
  "Office Space",
  "Penthouse",
  "Plot",
  "Row House",
  "Studio",
  "Villa",
  "Villament"
]
sy_unit_type = [u.lower() for u in sy_unit_type]

# Step 1: Load input_file2 into a dict for fast lookup (only if listing == True)
lookup = {}
with open(input_file2, "r", encoding="utf-8") as f2:
    for line in f2:
        obj = json.loads(line.strip())
        source_url = obj.get("source_url")
        if source_url and obj.get("listing") is True:   # ✅ only take if listing == True
            lookup[source_url] = obj


# Step 2: Process input_file1 and update fields based on lookup
with open(input_file1, "r", encoding="utf-8") as f1, open(output_file, "w", encoding="utf-8") as out:
    for i, line in enumerate(f1, 1):  # i = line number
        try:
            line = line.strip()
            obj = json.loads(line)
            
            source_url = obj.get("source_url")            

            if not source_url or source_url not in lookup or obj.get("error") or not obj.get("units"): 
                # out.write(json.dumps(obj) + "\n")
                continue
            
            # Debug print for inspection
            if not source_url:
                print(f"[Line {i}] Missing source_url: {line[:100]}...")  # show first 100 chars
            elif source_url not in lookup:
                print(f"[Line {i}] source_url not in lookup: {source_url}")

            units = obj.get("units",[])
            if units:
                obj["units"] = [
                    {
                        **item,
                        "text": " ".join(item.get("text","").split())
                    } for item in units
                ]

            # price_range = obj.get("project",{}).get("price_range","")
            # if price_range:
            #     obj["project"]["price_range"] = " ".join(price_range.split())

            # size_range = obj.get("project",{}).get("size_range","")
            # if size_range:
            #     obj["project"]["size_range"] = " ".join(size_range.split())
                

            # Ensure listingsMapRent/Sale are dicts
            # Normalize listingsMapRent
            if isinstance(obj.get("listingsMapRent"), list):
                obj["listingsMapRent"] = {
                    "squareyard": obj.get("listingsMapRent", [])
                }

            # Normalize listingsMapSale
            if isinstance(obj.get("listingsMapSale"), list):
                obj["listingsMapSale"] = {
                    "squareyard": obj.get("listingsMapSale", [])
                }

            # Append data if available
            raw_data = lookup.get(source_url, {})
            listing = raw_data.get("data", {})
            rent = listing.get("rent")
            buy = listing.get("buy")
            if rent:
                obj["listingsMapRent"]["housing"] = [{
                    "title":u.get("title",""),
                    "unitType": "apartment" if "flat" in u.get("title").lower() else "", 
                    "size":u.get("builtUpArea","").get("value",""),
                    "price":u.get("price",0),
                    "type":u.get("propertyType",""),
                    "posted_date":"",
                    "posted_by": u.get("sellers", [{}])[0].get("type", "") if u.get("sellers") else "",
                    "url": f"https://housing.com{u.get("url")}" if u.get("url") else ""
                } for u in rent
                ]
            if buy:
                obj["listingsMapSale"]["housing"] = [{
                    "title":u.get("title",""),
                    "unitType": "apartment" if "flat" in u.get("title").lower() else "", 
                    "size":u.get("builtUpArea","").get("value",""),
                    "price":u.get("price",0),
                    "type":u.get("propertyType",""),
                    "posted_date":"",
                    "posted_by": u.get("sellers", [{}])[0].get("type", "") if u.get("sellers") else "",
                    "url": f"https://housing.com{u.get("url")}" if u.get("url") else ""
                } for u in buy
                ]

            # Update counts
            obj["noListingsRent"] = sum(len(v) for v in obj["listingsMapRent"].values() if isinstance(v, list))
            obj["noListingsSale"] = sum(len(v) for v in obj["listingsMapSale"].values() if isinstance(v, list))

            out.write(json.dumps(obj) + "\n")
        except json.JSONDecodeError as e:
            print(f"[Line {i}] JSON decode error: {e}")
            print(f"Line content: {line[:200]}...")  # first 200 chars
        except Exception as e:
            print(f"[Line {i}] Error: {e}")
            print(f"Object: {obj}")

print(f"✅ Merged file written to {output_file}")
