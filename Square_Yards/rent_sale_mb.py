import requests
import time
import random
import json

city_name = "bangalore"
raw_data_file = f"./{city_name}/raw_{city_name}_mb.ndjson"
raw_data_listing = f"./{city_name}/raw_{city_name}_mb_listing.ndjson"

def save_json(output_file, data):
    """Save extracted data to NDJSON file (append mode)."""
    with open(output_file, "a", encoding="utf-8") as f:
        if isinstance(data, list):
            for record in data:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        else:
            f.write(json.dumps(data, ensure_ascii=False) + "\n")

seen_url = set()

with open(raw_data_listing, "r", encoding="utf-8") as f:
    for line in f:
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue  # skip malformed lines

        url = obj.get("url")
        if url:
            seen_url.add(url)


def sale_rent_listing(category="R", data=None):
    if data is None:
        data = {}

    page = 1
    prepare_record = []

    while True:
        url = (
            f"https://www.magicbricks.com/mbsrp/propertySearch.html?"
            f"category={category}&city={data.get('city')}&page={page}"
            f"&psmid={data.get('psmid')}&multiLang=en&intent-text=project"
        )

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }

        try:
            response = requests.get(url, headers=headers, timeout=15)

            if response.status_code != 200:
                print(f"❌ HTTP {response.status_code} for page {page}")
                break

            try:
                result = response.json()
            except ValueError:
                print(f"❌ Not JSON on page {page}, got:\n{response.text[:200]}")
                break

            resultList = result.get("resultList", [])
            if not resultList:
                break

            for u in resultList:
                prepare_record.append({
                    "title": u.get("scdimgalt", ""),
                    "unitType": u.get("propTypeD"),
                    "size": u.get("coveredArea", ""),
                    "price": u.get("price", 0),
                    "type": (u.get("transactionTypeD") or "").lower(),
                    "posted_date": u.get("postDateT"),
                    "posted_by": u.get("sellers", [{}])[0].get("type", "") if u.get("sellers") else "",
                    "url": f"https://magicbricks.com/{u.get('url')}" if u.get("url") else ""
                })

            page += 1

            # 👇 Add randomized delay (1–3 seconds) between page requests
            sleep_time = random.uniform(1, 3)
            print(f"⏳ Sleeping {sleep_time:.2f}s before next request...")
            time.sleep(sleep_time)

        # except requests.RequestException as e:
        #     print(f"❌ Request failed on page {page}: {e}")
        #     break

        except requests.RequestException as e:
            return {"error": str(e)}


    return prepare_record


count_index = 0
sleep_time = 5 * 60  # 10 minutes

with open(raw_data_file, "r", encoding="utf-8") as f:
    for line in f:
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue  # skip malformed lines
                        
        url = obj.get("url")
        
        if city_name not in url:
            continue
        if url in seen_url:
            continue
        rentProperty = int(obj.get("data", {}).get("rentCountProperty") or 0)
        saleProperty = int(obj.get("data", {}).get("sellCountProperty") or 0)
        rentRedirectUrl = obj.get("data",{}).get("bhkProjectDetailsMap",{}).get("ALL",{}).get("rentRedirectUrl","")
        saleRedirectUrl = obj.get("data",{}).get("bhkProjectDetailsMap",{}).get("ALL",{}).get("saleRedirectUrl","")
        city = obj.get("data",{}).get("bhkProjectDetailsMap",{}).get("ALL",{}).get("city","")
        psmid = obj.get("data",{}).get("bhkProjectDetailsMap",{}).get("ALL",{}).get("psmid","")    

        if not city and not psmid:
            continue

        config = {
            "city":city,
            "psmid":psmid,            
        }


        # Always initialize listing with empty dicts
        data = {
            "listing": {
                "rent": [],
                "buy": []
            }
        }

        # # Fill rent/buy if URLs exist        
        if rentRedirectUrl or saleRedirectUrl:

            data["source_url"] = obj.get("source_url")
            data["url"]= url

            rent_result = sale_rent_listing("R", config) if rentRedirectUrl else []
            sale_result = sale_rent_listing("S", config) if saleRedirectUrl else []

            # If error occurred, it will be a dict with "error"
            if isinstance(rent_result, dict) and "error" in rent_result:
                data["listing"]["rent_error"] = rent_result["error"]
            else:
                data["listing"]["rent"] = rent_result

            if isinstance(sale_result, dict) and "error" in sale_result:
                data["listing"]["buy_error"] = sale_result["error"]
            else:
                data["listing"]["buy"] = sale_result

            save_json(raw_data_listing, data)
            seen_url.add(url)

            count_index += 1  # increment first

            if count_index % 50 == 0:                
                print(f"⏳ Sleeping for {sleep_time / 60:.0f} minutes to avoid rate limit...")
                time.sleep(sleep_time)
            
            

#/mbsrp/propertySearch.html?category=R&city=3327&page=2&psmid=5006210&multiLang=en&intent-text=project
#/mbsrp/propertySearch.html?category=S&city=3327&page=2&psmid=5178801&multiLang=en&intent-text=project
#/mbsrp/propertySearch.html?category=S&city=3327&page=1&psmid=5042692&multiLang=en&intent-text=project