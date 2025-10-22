import json
import re
import os

# ✅ Set your city name here
city_name = "bangalore"

units_file = f"./{city_name}/units_{city_name}_data1.json"
# project_file = f"./{city_name}/merged_{city_name}_sy_response.ndjson"
project_file = f"./{city_name}/final_{city_name}_merged.ndjson"


# Storage
data = []
data_based_on_unit = []
empty_project = []

def parse_price(value):
    """Convert string price ('1.21 Cr', '99 L') or int to numeric ₹."""
    if isinstance(value, (int, float)):
        return int(value)

    if not isinstance(value, str):
        return None

    value = value.strip().replace(",", "")

    if "Cr" in value:
        num = float(re.findall(r"[\d.]+", value)[0])
        return int(num * 10000000)   # 1 Cr = 1,00,00,000

    if "L" in value:
        num = float(re.findall(r"[\d.]+", value)[0])
        return int(num * 100000)     # 1 L = 1,00,000

    if value.isdigit():
        return int(value)

    return None


# Area extraction helper
def extract_range(text):
    if text in ["NA", "N/A", "-", ""]:
        return "", ""
    range_match = re.match(r"([\d,.]+)\s*-\s*([\d,.]+)\s*(\w+)?", text)
    single_match = re.match(r"([\d,.]+)\s*(\w+)?", text)
    if range_match:
        return range_match.group(1).replace(",", ""), range_match.group(2).replace(",", "")
    elif single_match:
        val = single_match.group(1).replace(",", "")
        return val, val
    return "", ""

# Price extraction helper
def extract_price(text):
    if text in ["NA", "N/A", "-", ""]:
        return "", ""
    prices = re.findall(r"([\d.]+)\s*([A-Za-z]+)", text)
    if len(prices) == 2:
        return f"{prices[0][0]} {prices[0][1]}", f"{prices[1][0]} {prices[1][1]}"
    elif len(prices) == 1:
        return f"{prices[0][0]} {prices[0][1]}", f"{prices[0][0]} {prices[0][1]}"
    return "", ""


def safe_price(value):
    """Convert price to int safely, return None if not valid."""
    try:
        return int(str(value).replace(",", "").strip())
    except (ValueError, TypeError):
        return None
    

# Load project data
with open(project_file, "r", encoding="utf-8") as f:
    for line in f:
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue  # skip malformed lines
        nolisitngsMapSale = obj.get("noListingsSale")
        nolistingsMapRent = obj.get("noListingsRent")
        if nolistingsMapRent and nolistingsMapRent:
            data.append(obj)
        

print("📦 Total Projects:", len(data))

for entry in data:
    units = entry.get("units", [])
    if not units:
        empty_project.append(entry)
        continue

    for _index, unit in enumerate(units):

        # Listing filter
        if unit.get("unit") and 'Apartment' == unit.get("unit") and unit.get("bhk"):
        
            # Flatten all sale listings (across housing, magicbricks, etc.)
            all_sale = [l for listings in entry.get("listingsMapSale", {}).values() for l in listings]

            # listMapSale = [item for item in all_sale if item.get("bhk") == unit.get("bhk")]

            for item in all_sale:
                bhk_value = unit.get("bhk")
                title_value = item.get("title")
                print("BHK:", bhk_value, "Title:", title_value)
                
                if bhk_value and str(bhk_value) in (title_value or ""):
                    # your logic here
                    pass

            listMapSale = [
                {
                    **item,  # keep all original keys
                    "price": parse_price(item.get("price"))  # overwrite price with numeric
                }
                for item in all_sale
                if unit.get("bhk") and str(unit.get("bhk")) in (item.get("title") or "")
            ]
            # Flatten all rent listings
            all_rent = [ l for listings in entry.get("listingsMapRent", {}).values() for l in listings]

            # listMapRent = [item for item in all_rent if item.get("bhk") == unit.get("bhk")]
            listMapRent = [
                    {
                        **item,  # keep all original keys
                        "price": parse_price(item.get("price"))  # overwrite price with numeric
                    }
                    for item in all_rent
                    if unit.get("bhk") and str(unit.get("bhk")) in (item.get("title") or "")
                ]

        else:
            listMapSale = entry.get("listingsMapSale", {})
            listMapRent = entry.get("listingsMapRent", {})
                

    
        # Flatten rent listings
        if isinstance(listMapSale, dict):
            sale_flat = [e for listings in listMapSale.values() for e in listings]
        else:
            sale_flat = listMapSale
    
        # Flatten rent listings
        if isinstance(listMapRent, dict):
            rent_flat = [e for listings in listMapRent.values() for e in listings]
        else:
            rent_flat = listMapRent


        # --- Rent ---
        rent_prices = [parse_price(e.get("price")) for e in rent_flat if e.get("price") is not None]
        rent_prices = [p for p in rent_prices if p is not None]

        if rent_prices:
            rent_average = round(sum(rent_prices) / len(rent_prices))  # rounded integer
            rent_min = min(rent_prices)
            rent_max = max(rent_prices)
        else:
            rent_average = rent_min = rent_max = None

        # --- Sale ---
        sale_prices = [parse_price(e.get("price")) for e in sale_flat if e.get("price") is not None]
        sale_prices = [p for p in sale_prices if p is not None]

        if sale_prices:
            sale_average = round(sum(sale_prices) / len(sale_prices))  # rounded integer
            sale_min = min(sale_prices)
            sale_max = max(sale_prices)
        else:
            sale_average = sale_min = sale_max = None
        

        superBuiltMin_val = unit.get("size",None)
        superBuiltMax_val = unit.get("size",None)
        carpetMin_val = unit.get("carpet",None)
        carpetMax_val = unit.get("carpet",None) 
    

        if superBuiltMin_val and superBuiltMax_val:
            averge_super_built_up = (superBuiltMin_val + superBuiltMax_val) / 2
        else:
            averge_super_built_up = None

        if carpetMin_val and carpetMax_val:
            averge_carpet = (carpetMin_val + carpetMax_val) / 2
        else:
            averge_carpet = None



        
        if rent_average and averge_carpet:
            avg_rent_carpet = round(rent_average / averge_carpet,2)
        else:
            avg_rent_carpet = None

        if rent_average and averge_super_built_up:
            avg_rent_superbuiltup = round(rent_average / averge_super_built_up,2)
        else:
            avg_rent_superbuiltup = None


        if sale_average and averge_carpet:
            avg_price_carpet = round(sale_average / averge_carpet, 2)
        else:
            avg_price_carpet = None

        if sale_average and averge_super_built_up:
            avg_price_sba = round(sale_average / averge_super_built_up, 2)
        else:
            avg_price_sba = None

        project_name = entry.get("project", {}).get("name", "")
        # if not project_name:
        #     description = entry.get("detail", {}).get("description", "")
        #     if description:
        #         # Take the first part before a comma, and strip "Introducing" + whitespace
        #         project_name = description.split(",")[0].replace("Introducing", "").strip()

        unit_data = {
            "unitType": unit.get("unit",""),
            "size": {
                "superBuiltMin": superBuiltMin_val,
                "superBuiltMax": superBuiltMax_val,
                "carpetMin": carpetMin_val,
                "carpetMax": carpetMax_val,
                "superBuiltAvg":averge_super_built_up, #superbuitMin + superBuiltMax)/2
                "carpetAvg":averge_carpet #(carpetbuitMin + carpetBuiltMax)/2
            },
            "price": {
                "priceMin": sale_min,
                "priceMax": sale_max,
                "avgPrice": sale_average, # Add of price of all sale listing/no.of sale listing
                "avgPriceSftCarpet": avg_price_carpet, #avrPrice /CarpetAvg
                "avgPriceSftSBA": avg_price_sba, #avgPrice /SuperAvg
            },
            "rent": {
                "rentMin": rent_min,
                "rentMax": rent_max,
                "avgRent": rent_average, #Add of price of all rent lsiting/ no of rent listing
                "avgrentSftCarpet": avg_rent_carpet, #avergeRent /superbuiltAvg
                "avgrentSftSBA": avg_rent_superbuiltup #averegeRent/capetAvg
            },
            "builder": entry.get("project", {}).get("developer", ""),
            "propType": entry.get("detail", {}).get("Property Type", ""),
            "projectName": project_name,
            "city": entry.get("project", {}).get("location", "").split(",")[:1],
            "location": entry.get("project", "").get("location",""),
            "address": entry.get("address", ""),
            "status": entry.get("project", {}).get("status", ""),
            "completionDate": entry.get("project", {}).get("completion_date", ""),
            "totalUnits": entry.get("detail", {}).get("Total Units", ""),
            "totalProjectArea": entry.get("project", {}).get("total_area", ""),
            "GoogleLocation": entry.get("location_url", ""),
            # "Amenities": entry.get("amenities", []),
            # "NearbyPlaces": entry.get("near_by_places", []),
            "noListingsSale": len(listMapSale),
            "noListingsRent": len(listMapRent),
            "listingsMapSale": listMapSale,
            "listingsMapRent": listMapRent,
            "onFirebase": False,
            "unique_unit": f"{entry.get("source_url").split("/")[-2]}_{unit.get("unit")}_{unit.get("size")}_{unit.get("price")}"
        }

        data_based_on_unit.append(unit_data)

# 🔁 Merge with existing data if available
if os.path.exists(units_file):
    try:
        with open(units_file, "r", encoding="utf-8") as f:
            existing_data = json.load(f)
            if not isinstance(existing_data, list):
                existing_data = []
    except Exception as e:
        print("⚠️ Error reading existing units file:", e)
        existing_data = []
else:
    existing_data = []


print(f"Total Units: {len(data_based_on_unit)}")
with open(units_file, "w", encoding="utf-8") as f:
    json.dump(data_based_on_unit, f, ensure_ascii=False, indent=2)