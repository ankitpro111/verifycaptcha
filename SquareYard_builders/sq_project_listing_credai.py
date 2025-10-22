import requests
import sys
import os
import time
import json
import random

from bs4 import BeautifulSoup

city_name = "bangalore"
input_file = f"./{city_name}/matched_credai.ndjson"
output_file = f"./{city_name}/projects_{city_name}_sy_credai.ndjson"
progress_file = f"./{city_name}/progress_{city_name}_sy_credai.json"

headers = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
}

def load_ndjson(file_path):
    results = []
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():  # skip empty lines
                try:
                    data = json.loads(line)
                    # Take only {"projects": "urls"} if keys exist
                    if "projects" in data and "name" in data:
                        results.append({
                            "urls": data["name"],
                            "projects": data["projects"],
                            
                        })
                except json.JSONDecodeError as e:
                    print(f"Skipping invalid line: {line.strip()} ({e})")
    return results    

def extract_projects(soup):
    """Extract project details from npTile elements inside newDseDynamicBody"""
    projects = []

    container = soup.find("div", class_="newDseDynamicBody")
    if not container:
        return projects

    tiles_box = container.find("div", class_="npTilesBox")
    if not tiles_box:
        return projects

    for tile in tiles_box.find_all("div", class_="npTile", recursive=False):
        try:
            # project link & name
            project_link_tag = tile.select_one(".npProjectName a.projectDetailUrl")
            url = project_link_tag["href"].strip() if project_link_tag else None
            project_name = project_link_tag.get("title") if project_link_tag else None

            # city & location
            city_span = project_link_tag.select_one(".npProjectCity") if project_link_tag else None
            city = city_span.get_text(strip=True) if city_span else None

            # price range
            price_box = tile.select_one(".npPriceBox")
            price = price_box.get_text(strip=True) if price_box else None

            # developer & units
            fav_btn = tile.select_one(".npFavBtn")
            developer = fav_btn.get("data-developer") if fav_btn else None
            units = fav_btn.get("data-units") if fav_btn else None

            # description
            desc_box = tile.select_one(".npDescBox")
            description = desc_box.get_text(" ", strip=True) if desc_box else None

            # tags
            tags = [li.get_text(strip=True) for li in tile.select(".npPropertyTagList li")]

            # unit listing details from table
            units_list = []
            table_rows = tile.select("div.pTableBox table tbody tr")
            for tr in table_rows:
                cols = tr.find_all("td")
                if len(cols) == 3:
                    units_list.append({
                        "unit": cols[0].get_text(strip=True),
                        "size": cols[1].get_text(strip=True),
                        "price": cols[2].get_text(strip=True),
                    })

            projects.append({
                "url": url,
                "project_name": project_name,
                "city": city,
                "price_range": price,
                "developer": developer,
                "units_text": units,
                "units_list": units_list,
                "description": description,
                "tags": tags
            })
        except Exception as e:
            print("Error parsing tile:", e)

    return projects


def extract_total_pages(soup):
    """Extract total number of pages from npPaginationBox"""
    pagination_box = soup.find("div", class_="npPaginationBox")
    if not pagination_box:
        return 1

    pages = []
    for li in pagination_box.select("ul.npPaginationList li a"):
        href = li.get("href", "")
        if "page=" in href:
            try:
                page_num = int(href.split("page=")[-1])
                pages.append(page_num)
            except:
                continue

    return max(pages) if pages else 1


def load_progress():
    """Load last scraped page number"""
    if os.path.exists(progress_file):
        try:
            with open(progress_file, "r", encoding="utf-8") as f:
                return json.load(f)                
        except:
            return {}
    return {}

def save_progress(progress_page, current_url, page, total_pages=None):
    """Update and save last scraped page number + completion flag"""
    is_completed = "True" if (total_pages and page >= total_pages) else "False"
    progress_page[current_url] = {
        "last_Index": page,
        "isCompleted": is_completed
    }
    with open(progress_file, "w", encoding="utf-8") as f:
        json.dump(progress_page, f, indent=2)




def save_projects(current_url, projects, page_number):
    """Append new projects to NDJSON file with pagination_url info"""
    with open(output_file, "a", encoding="utf-8") as f:
        for proj in projects:
            proj["pagination_url"] = f"{current_url}?page={page_number}"
            f.write(json.dumps(proj, ensure_ascii=False) + "\n")

def main():
    list_urls = load_ndjson(input_file)

    # load all saved progress once
    progress_page = load_progress()

    for item in list_urls[:150]:
        current_url = item["projects"]
        print(f"\n=== Processing URL: {current_url} ===")

        # get progress record for this URL
        url_progress = progress_page.get(current_url, {"last_Index": 0, "isCompleted": "False"})
        last_done_page = url_progress.get("last_Index", 0)
        is_completed = str(url_progress.get("isCompleted", "False")).lower() == "true"

        # skip if already completed
        if is_completed:
            print(f"✅ Skipping {current_url} → already completed")
            continue

        try:
            # fetch first page
            response = requests.get(current_url, headers=headers)
            soup = BeautifulSoup(response.text, "html.parser")

            total_pages = extract_total_pages(soup)
            print(f"Total pages found: {total_pages}")
            print(f"Last completed page: {last_done_page}")

            last_success = last_done_page

            # if first run, process page 1 immediately
            if last_done_page < 1:
                projects = extract_projects(soup)
                save_projects(current_url,projects, 1)
                last_success = 1
                print(f"Saved {len(projects)} projects from page 1")
                time.sleep(random.uniform(2, 5))

            # resume from next page
            for page in range(max(2, last_done_page + 1), total_pages + 1):
                page_url = f"{current_url}?page={page}"
                print(f"Processing page {page}/{total_pages} → {page_url}")

                response = requests.get(page_url, headers=headers)
                soup = BeautifulSoup(response.text, "html.parser")

                projects = extract_projects(soup)
                save_projects(current_url,projects, page)
                last_success = page  # update only in memory

                print(f"Saved {len(projects)} projects from page {page}")
                time.sleep(random.uniform(2, 5))

        except KeyboardInterrupt:
            print("Stopped manually. Progress saved.")
            save_progress(progress_page, current_url, last_success, total_pages)
            break
        except Exception as e:
            print(f"Error on {current_url}, page {last_success+1}: {e}")
            save_progress(progress_page, current_url, last_success, total_pages)
        finally:
            # save only once (on exit or completion of this URL)
            save_progress(progress_page, current_url, last_success, total_pages)
            url_state = progress_page[current_url]
            print(
                f"Progress saved → {current_url} → "
                f"Last completed page: {url_state['last_Index']}, "
                f"isCompleted: {url_state['isCompleted']}"
            )


if __name__ == "__main__":
    main()