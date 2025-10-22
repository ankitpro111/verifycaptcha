from bs4 import BeautifulSoup
import sys
import os
import json
import time
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import requests
sys.path.append(os.path.dirname(os.path.dirname(__file__)))


headers = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
}


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
    """Scrape ONLY top sub-listings (sale/rent) from a given SquareYards URL, ignoring 'similar properties'."""
    if not url:
        return []

    try:
        print(f"Scrapping sub_urls: {url}")
        # Delay for rate-limiting (small randomized pause)
        time.sleep(random.uniform(1, 2))
        
        response = requests.get(url, headers=headers)
        if not response:
            return []

        soup = BeautifulSoup(response.text, "html.parser")

        results = []
        # Find the container
        container = soup.find("div", class_="list-view-box")
        if not container:
            return results

        # Select only <article> tags directly inside list-view-box,
        # but stop before the "match-lisiting" div
        for article in container.find_all("article", recursive=False):
            div = article.find("div", class_="favorite-btn shortlistcontainerlink1")
            if not div:
                continue
            results.append({
                "price": div.get("data-price"),
                "url": div.get("data-url"),
                "name": div.get("data-name"),
                "area": div.get("data-area"),
                "unit_type": div.get("data-unittype"),
                "sub_locality": div.get("data-sublocalityname"),
            })

        return results

    except Exception as e:
        print(f"❌ Error scraping sub-listings {url}: {e}")
        return []


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

    # # --- Why Consider Points ---
    # why_points = []
    # why_box = soup.find('div', class_='why-consider-box')
    # if why_box:
    #     for li in why_box.find_all('li'):
    #         point = li.get_text(strip=True)
    #         if point:
    #             why_points.append(point)
    # data_response["project"]["why_consider"] = why_points
    
    # Extract project details from about section
    about_section = soup.find('section', id='aboutProject')
    if about_section:
        content_box = about_section.find('div', class_='content-box')
        if content_box:
            paragraphs = content_box.find_all('p')
            description_parts = []
            for p in paragraphs:
                description_parts.append(p.get_text().strip())
            data_response["detail"]["description"] = ' '.join(description_parts)
    
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
    

    data_response["listingsMapRent"] = listings_rents 
    data_response["noListingsRent"] = len(listings_rents)
    data_response["listingsMapSale"] = listings_sales
    data_response["noListingsSale"] = len(listings_sales)
    
    
    # Additional project details
    data_response["detail"]["why_consider"] = []
    why_consider = soup.find('ul', class_='why-consider-list')
    if why_consider:
        items = why_consider.find_all('li')
        for item in items:
            data_response["detail"]["why_consider"].append(item.get_text().strip())
    
    # RERA details
    rera_section = soup.find('section', id='reraDetails')
    if rera_section:
        rera_content = rera_section.find('ul')
        if rera_content:
            rera_li = rera_content.find('li')
            if rera_li:
                data_response["detail"]["rera"] = rera_li.get_text().strip()
    
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
    data = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                data.append(json.loads(line))
    return data

def load_seen_urls(output_file):
    """Load already scraped project URLs from NDJSON file to skip duplicates."""
    seen = set()
    if os.path.exists(output_file):
        with open(output_file, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    obj = json.loads(line.strip())
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

        response = requests.get(url, headers=headers)
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

        soup = BeautifulSoup(response.text, "html.parser")
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


# def scrape_url(idx, total, url, output_file, seen_urls, lock):
#     """Scrape one URL with error handling, deduplication, delay and logging."""
#     if url in seen_urls:
#         return None

#     try:
#         # Delay to avoid rate limit
#         time.sleep(random.uniform(1, 3))

#         response = cm.fetch_page(url)
#         if not response:
#             return None

#         soup = cm.scraper(response)
#         extracted_data = extract_squareyards_data(soup)

#         # Add source URL
#         extracted_data["source_url"] = url

#         # Save with thread-safe lock
#         with lock:
#             save_json(output_file, extracted_data)
#             seen_urls.add(url)

#         print(f"[{idx}/{total}] ✅ Scraped: {url}")
#         return url

#     except Exception as e:
#         print(f"[{idx}/{total}] ❌ Error scraping {url}: {e}")
#         return None


def main():
    MIN_ROWS = 5000
    city_name = "bangalore" #mumbai
    os.makedirs(city_name, exist_ok=True)

    input_file = f"./{city_name}/projects_{city_name}_sy_credai.ndjson"
    output_file = f"{city_name}/projects_{city_name}_sy_credai_list.ndjson"

    input_file_data = load_ndjson(input_file)
    input_file_urls = [u.get("url") for u in input_file_data if u.get("url")]

    # Load already scraped URLs
    seen_urls = load_seen_urls(output_file)
    not_fetched_record = [u for u in input_file_urls if u not in seen_urls]

    # Limit scraping to at most 800 URLs 
    # To restart top 2903
    total_rows = min(MIN_ROWS, len(not_fetched_record))
    urls_to_scrape = not_fetched_record[:total_rows]
    # urls_to_scrape = ["https://www.squareyards.com/bangalore-residential-property/raheja-residency/1753/project"]

    print(f"🔎 Total URLs: {len(input_file_urls)} | Already scraped: {len(seen_urls)} | Pending: {len(not_fetched_record)} | This run: {total_rows}")

    lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(scrape_url, idx, total_rows, url, output_file, seen_urls, lock)
            for idx, url in enumerate(urls_to_scrape, start=1)
        ]

        # Ensure logs come in as tasks finish
        for future in as_completed(futures):
            future.result()


if __name__ == "__main__":
    main()
