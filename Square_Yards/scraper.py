from bs4 import BeautifulSoup
import re
import sys
import os
import json
import time
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import requests
import time

def scraper(response):     
    return BeautifulSoup(response.text,'html.parser')

def fetch_page(url):
    if not url:
        return None    
    try:         
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            # "Accept-Encoding": "gzip, deflate, br", 
        }
        response = requests.get(url,headers=headers)
        response.raise_for_status()
        # response.encoding = "utf-8"
        return response
    except Exception as e:
        print(f"⚠️ Error fetching {url}: {e}")
    return None

def parse_price(price_str):
    price_str = price_str.replace("₹", "").replace(",", "").strip()
    match = re.match(r"([\d\.]+)\s*([A-Za-z]+)", price_str)
    if not match:
        return None
    
    value, unit = match.groups()
    value = float(value)
    
    if unit.lower().startswith("cr"):
        return int(value * 1e7)  # 1 Cr = 10,000,000
    elif unit.lower().startswith("lac"):
        return int(value * 1e5)  # 1 Lac = 100,000
    elif unit.lower().startswith("k"):
        return int(value * 1e3)  # 1 K = 1,000
    else:
        return int(value)
    
def parse_unit_type(unit_type_str):
    """Extract BHK and unit (Apartment/Villa/Plot/etc.)"""
    bhk_match = re.search(r"(\d+)\s*BHK", unit_type_str, re.IGNORECASE)
    bhk = int(bhk_match.group(1)) if bhk_match else None
    
    # Remove BHK part to get clean unit type
    unit_clean = re.sub(r"\d+\s*BHK", "", unit_type_str, flags=re.IGNORECASE).strip()
    
    return bhk, unit_clean

def scrape_listings(url):
    """Scrape structured listing details from a SquareYards page (ignores similar properties)."""
    if not url:
        return []

    try:
        print(f"Scraping sub_urls: {url}")
        time.sleep(random.uniform(1, 2))  # Throttle request

        response = fetch_page(url)
        if not response:
            return []

        soup = scraper(response)
        results = []

        container = soup.find("div", class_="list-view-box")
        if not container:
            return results

        for article in container.find_all("article", recursive=False):
            div = article.find("div", class_="favorite-btn shortlistcontainerlink1")
            if not div:
                continue

            # Agent name
            agent_tag = article.find("strong", class_="agent-name")
            agent_name = agent_tag.get_text(strip=True) if agent_tag else None

            # Unit type (e.g. 3 BHK)
            unit_type = div.get("data-unittype")

            # Size (e.g. 1785 Sq.Ft.)
            size = div.get("data-area")

            # Price (e.g. ₹ 1.92 Cr or 19200000)
            price = div.get("data-price")  # numeric
            if not price:
                price = div.get("data-totalprice")

            # Type (rent/sale)
            listing_type = div.get("data-type") or article.get("data-type")

            # Posted date: Try finding any <span> or <div> that has "Posted on" or "Posted X days ago"
            posted_date = None
            posted_tag = article.find(lambda tag: tag.name in ["span", "div"] and "Posted" in tag.get_text())
            if posted_tag:
                posted_date = posted_tag.get_text(strip=True)

            results.append({
                "unit_type": unit_type,
                "size": size,
                "price": price,
                "type": listing_type,
                "posted_date": posted_date,
                "posted_by": agent_name,
                "url": div.get("data-url"),
                "name": div.get("data-name"),
                "sub_locality": div.get("data-sublocalityname"),
            })

        return results

    except Exception as e:
        print(f"❌ Error scraping sub-listings {url}: {e}")
        return []

def fetch_rera_overview(rera_id, delay=0.5):
    """
    Fetch RERA overview using SquareYards loadreradetail API with throttling.
    """
    url = "https://www.squareyards.com/loadreradetail"

    try:
        time.sleep(delay)  # Throttle request
        res = requests.post(url, data={"reraId": rera_id}, timeout=10)
        if res.status_code == 200:
            data = res.json()
            detail = data.get("projectDetail", {})
            if detail:
                return {
                    "project_name": detail.get("projectName", "").strip(),
                    "project_type": detail.get("projectType", "").strip(),
                    "project_status": detail.get("projectStatus", "").strip(),
                    "proposed_start_date": parse_project_date(detail.get("proposedStartDate", "")),
                    "proposed_completion_date": parse_project_date(detail.get("proposedDateOfCompletion", ""))
                }
    except Exception as e:
        print(f"⚠️ Error fetching RERA overview for {rera_id}: {e}")

    return {}


def parse_project_date(date_str):
    try:
        return datetime.strptime(date_str.strip(), "%d-%m-%Y").date().isoformat()
    except:
        return date_str.strip()

def extract_multiple_rera_details(soup):
    rera_details = []
    rera_ids = []

    rera_section = soup.find('section', id='reraDetails')
    if not rera_section:
        return [], []

    accordion_items = rera_section.find_all('article', class_='accordion-item')

    for item in accordion_items:
        rera_data = {
            "rera_id": "",
            "phase_name": "",  # ✅ ADD THIS
            "overview": {}
        }

        header = item.find('div', class_='accordion-header')
        if header and header.has_attr('data-reraid'):
            rera_id = header['data-reraid']
            rera_data["rera_id"] = rera_id
            rera_ids.append(rera_id)

            # ✅ Extract phase name from <span>
            span = header.find('span')
            if span:
                rera_data["phase_name"] = span.get_text(strip=True)

            # ✅ Fetch overview from API
            rera_data["overview"] = fetch_rera_overview(rera_id, delay=random.uniform(0.6, 1.2))

        rera_details.append(rera_data)

    return rera_details, rera_ids


def extract_squareyards_data(soup):
    """
    Extract SquareYards project data from HTML content
    """    
    data_response = {
        "project": {},
        "detail": {},
        "units": [],
        "amenities": [],
        "location_url": "",
        "address": "",
        "near_by_places": [],
        "listingsMapSale": [],
        "listingsMapRent": [],
        "noListingsSale": 0,
        "noListingsRent": 0
    }

      # --- Extract Project Name and Location ---
    project_name = ""
    location = ""

    project_name_el = soup.find('h1')
    if project_name_el:
        # First extract location from span
        location_box = project_name_el.find('span', class_='loction-box')
        if location_box:
            location = location_box.get_text(strip=True)
            location_box.extract()  # Remove the span from the h1 so we can get only project name

        # Now get only the text content of h1 (excluding location span)
        project_name = project_name_el.get_text(strip=True)

    data_response["project"]["name"] = project_name
    data_response["project"]["location"] = location

    

    # --- Project Prices ---
    price_box = soup.find('strong', class_='price-box')    
    data_response["project"]["price_range"] = price_box.get_text(" ", strip=True) if price_box else ""

    # # --- Price per Sqft ---
    # per_sqft = soup.find('span', class_='per-sqft')
    # data_response["project"]["price_per_sqft"] = per_sqft.get_text(strip=True) if per_sqft else ""

    # --- Project Status ---
    status = ""
    status_el = soup.find('em', class_='icon-project-status')
    if status_el:
        strong = status_el.find_next('strong')
        if strong:
            status = strong.get_text(strip=True)
    data_response["project"]["status"] = status

    # --- Unit Config ---
    unit_config = ""
    unit_config_el = soup.find('em', class_='icon-building')
    if unit_config_el:
        strong = unit_config_el.find_next('strong')
        if strong and "BHK" in strong.get_text():
            unit_config = strong.get_text(strip=True)
    data_response["project"]["unit_types"] = unit_config

    # --- Size (Min & Max Sq.Ft) ---
    size_range = {"min": "", "max": ""}
    size_el = soup.find('em', class_='icon-unit-size')
    if size_el:
        strong = size_el.find_next('strong')
        if strong:
            size_text = strong.get_text(" ", strip=True)
            match = re.findall(r'(\d+)', size_text)
            if len(match) >= 2:
                size_range["min"] = match[0]
                size_range["max"] = match[1]
    data_response["project"]["size_range"] = size_range

    # --- Number of Units ---
    total_units = ""
    unit_count_el = soup.find_all('em', class_='icon-building')
    if len(unit_count_el) >= 2:  # 1st = config, 2nd = number of units
        strong = unit_count_el[1].find_next('strong')
        if strong:
            total_units = strong.get_text(strip=True)
    data_response["project"]["total_units"] = total_units

    # --- Total Area ---
    total_area = ""
    area_el = soup.find('em', class_='icon-unit-total')
    if area_el:
        strong = area_el.find_next('strong')
        if strong:
            total_area = strong.get_text(strip=True)
    data_response["project"]["total_area"] = total_area

    # --- Developer Info ---
    developer = ""
    developer_logo = soup.find('img', class_='developer-logo')
    if developer_logo and developer_logo.get('alt'):
        developer = developer_logo['alt']
    data_response["project"]["developer"] = developer
    
    # Extract project details from about section
    # about_section = soup.find('section', id='aboutProject')
    # if about_section:
    #     content_box = about_section.find('div', class_='content-box')
    #     if content_box:
    #         paragraphs = content_box.find_all('p')
    #         description_parts = []
    #         for p in paragraphs:
    #             description_parts.append(p.get_text().strip())
    #         data_response["detail"]["description"] = ' '.join(description_parts)
    
    # Extract units from price list
    
    price_table = soup.find('section', id='priceList')
    if price_table:
        tbody = price_table.find('tbody')
        if tbody:
            rows = tbody.find_all('tr', class_='extra-unit-row active')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) >= 2:
                    unit_type_str = cells[0].find('span').get_text(strip=True) if cells[0].find('span') else ""
                    size_text = cells[0].find('strong').get_text(strip=True) if cells[0].find('strong') else ""
                    
                    # Extract size as integer
                    size_match = re.search(r"(\d+)", size_text.replace(",", ""))
                    unit_size = int(size_match.group(1)) if size_match else None

                    # Extract bhk + unit
                    bhk, unit_name = parse_unit_type(unit_type_str)

                    # Extract price
                    price_info = cells[1].get_text(strip=True)
                    price_value = parse_price(price_info)

                    # Build text
                    text_info = f"{unit_type_str} {unit_size} Sq. Ft. - {price_info}".strip()
                    text_info = ' '.join(text_info.split())
                    unit_data = {
                        "bhk": bhk,            # integer or null
                        "unit": unit_name,     # Apartment / Villa / Plot / etc.
                        "size": unit_size,     # integer size
                        "price": price_value,  # numeric price
                        "text": text_info      # full human-readable text
                    }
                    data_response["units"].append(unit_data)
    
    # Extract amenities
    amenities_section = soup.find('section', id='amenities')
    if amenities_section:
        amenity_items = amenities_section.find_all('li')
        for item in amenity_items:
            span = item.find('span')
            if span:
                amenity_name = span.get_text().strip()
                data_response["amenities"].append(amenity_name)
    
    # Extract coordinates for location
    lat_input = soup.find('input', {'id': 'hd_plat'})
    lng_input = soup.find('input', {'id': 'hd_plang'})
    if lat_input and lng_input:
        lat = lat_input.get('value')
        lng = lng_input.get('value')
        data_response["location_url"] = f"https://maps.google.com/?q={lat},{lng}"
    
    # Extract nearby places from landmarks data in script
    landmarks_script = None
    scripts = soup.find_all('script')
    for script in scripts:
        if script.string and 'landmarks' in script.string:
            landmarks_script = script.string
            break
    
    if landmarks_script:
        # Try to extract landmarks data
        try:
            # Find the landmarks object in the script
            start = landmarks_script.find('const landmarks = ')
            if start != -1:
                start += len('const landmarks = ')
                # Find the end of the object
                brace_count = 0
                end = start
                for i, char in enumerate(landmarks_script[start:], start):
                    if char == '{':
                        brace_count += 1
                    elif char == '}':
                        brace_count -= 1
                        if brace_count == 0:
                            end = i + 1
                            break
                
                landmarks_str = landmarks_script[start:end]
                landmarks_data = eval(landmarks_str)  # Note: eval is dangerous, use json.loads in production
                
                for category, places in landmarks_data.items():
                    for place in places[:3]:  # Get top 3 places per category
                        place_data = {
                            "name": place.get("landmarkname", ""),
                            "category": category,
                            "distance": f"{place.get('distance', [0])[0]:.2f} KM" if place.get('distance') else "N/A"
                        }
                        data_response["near_by_places"].append(place_data)
        except:
            pass
    
    rent_sale_list = soup.find("section", id="propertiesTopSellers")

    sale_url = None
    rent_url = None

    if rent_sale_list:
        for element in rent_sale_list.find_all("a"):
            event_name = element.get("data-eventname")
            href = element.get("href")

            if event_name == "Resale_dse_url" and href and not sale_url:
                sale_url = href
            elif event_name == "Rental_Dse_url" and href and not rent_url:
                rent_url = href

            # If both found, stop searching
            if sale_url and rent_url:
                break
    
    listings_rents = scrape_listings(rent_url)
    listings_sales = scrape_listings(sale_url)
    

    data_response["listingsMapRent"] = {
        "squareyard":listings_rents
    } 
    data_response["noListingsRent"] = len(listings_rents)
    data_response["listingsMapSale"] = {
        "squareyard":listings_sales
    } 
    data_response["noListingsSale"] = len(listings_sales)
    
    
    # Additional project details
    data_response["detail"]["why_consider"] = []

    # Find all "why-consider" boxes
    why_consider_boxes = soup.find_all('div', class_='why-consider-box')
    for box in why_consider_boxes:
        # Check if the list is nested inside a div
        list_container = box.find('div', class_='why-consider-list')
        if list_container:
            ul = list_container.find('ul')
            if ul:
                items = ul.find_all('li')
                for item in items:
                    data_response["detail"]["why_consider"].append(item.get_text(strip=True))
        else:
            # Sometimes the <ul> itself has the class directly
            ul = box.find('ul', class_='why-consider-list')
            if ul:
                items = ul.find_all('li')
                for item in items:
                    data_response["detail"]["why_consider"].append(item.get_text(strip=True))
    
    # RERA details
    rera_details, rera_ids = extract_multiple_rera_details(soup)

    data_response["detail"]["rera_details"] = rera_details
    # data_response["detail"]["rera"] = rera_ids

    
    return data_response

def save_json(output_file, data):
    """Save extracted data to NDJSON file (append mode)."""
    with open(output_file, "a", encoding="utf-8") as f:
        if isinstance(data, list):
            for record in data:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        else:
            f.write(json.dumps(data, ensure_ascii=False) + "\n")

def load_ndjson(file_path):
    """Load NDJSON into a list, skipping records without units_list or invalid JSON."""
    data = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue  # skip empty lines
            try:
                obj = json.loads(line)
                # keep only if units_list exists and is non-empty
                # if not obj.get("units_list"):
                #     continue
                if obj:
                    data.append(obj)
            except json.JSONDecodeError:
                # skip invalid JSON lines
                continue
    return data

def load_seen_urls(output_file):
    """Load already scraped project URLs from NDJSON file to skip duplicates."""
    seen = set()
    if os.path.exists(output_file):
        with open(output_file, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    obj = json.loads(line.strip())

                    # validate project name
                    project_name = obj.get("project", {}).get("name")
                    if not project_name:
                        continue

                    # # validate at least one unit with text
                    units = obj.get("units")
                    if units and not units[0].get("text"):
                        continue

                    if obj.get("source_url"):
                        seen.add(obj["source_url"])
                except:
                    continue
    return seen

def scrape_url(idx, total, url, output_file, seen_urls, lock):
    """Scrape one URL with error handling, deduplication, delay and logging."""
    if url in seen_urls:
        return None

    try:
        time.sleep(random.uniform(1, 3))

        response = fetch_page(url)
        if not response:
            error_data = {
                "source_url": url,
                "error": "No response or 404"
            }
            with lock:
                save_json(output_file, error_data)
                seen_urls.add(url)
            print(f"[{idx}/{total}] ⚠️ No response: {url}")
            return None

        soup = scraper(response)
        extracted_data = extract_squareyards_data(soup)
        extracted_data["source_url"] = url

        with lock:
            save_json(output_file, extracted_data)
            seen_urls.add(url)

        print(f"[{idx}/{total}] ✅ Scraped: {url}")
        return url

    except Exception as e:
        error_data = {
            "source_url": url,
            "error": str(e)
        }
        with lock:
            save_json(output_file, error_data)
            seen_urls.add(url)
        print(f"[{idx}/{total}] ❌ Exception: {url} | {e}")
        return None

def main():
    MIN_ROWS = 10000
    city_name = "bangalore" #mumbai
    os.makedirs(city_name, exist_ok=True)

    input_file = f"./{city_name}/projects_{city_name}_sy.ndjson"
    output_file = f"{city_name}/projects_{city_name}_sy_response.ndjson"

    input_file_data = load_ndjson(input_file)
    input_file_urls = [u.get("url") for u in input_file_data if u.get("url")]

    # Load already scraped URLs
    seen_urls = load_seen_urls(output_file)
    
    not_fetched_record = [u for u in input_file_urls if u not in seen_urls]

    # Limit scraping to at most 800 URLs 
    # To restart top 2903
    total_rows = min(MIN_ROWS, len(not_fetched_record))
    urls_to_scrape = not_fetched_record[:total_rows]
    # urls_to_scrape = ["https://www.squareyards.com/bangalore-residential-property/godrej-tiara/339146/project","https://www.squareyards.com/bangalore-residential-property/bhartiya-garden-estate/340403/project"]
    
    print(f"🔎 Total URLs: {len(input_file_urls)} | Already scraped: {len(seen_urls)} | Pending: {len(not_fetched_record)} | This run: {total_rows}")

    lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(scrape_url, idx, total_rows, url, output_file, seen_urls, lock)
            for idx, url in enumerate(urls_to_scrape, start=1)
        ]

        # Ensure logs come in as tasks finish
        for future in as_completed(futures):
            future.result()


if __name__ == "__main__":
    main()