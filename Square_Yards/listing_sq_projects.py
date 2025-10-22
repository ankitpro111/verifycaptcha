import sys
import os
import time
import json
import random

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import utils.custom_method as cm

# Bangalore
# Mumbai
# Hyderabad
# Delhi
# Noida
# Gurugram(Gurgaon)
# Chennai
# Kolkata
# Pune
# Lucknow
# Ahmedabad
# Surat
# Indore
# Chandigarh
# Kochi
# Nagpur
# Jaipur


# Visakhapatnam

city_name = "kolkata" 
#bangalore, chennai, delhi, gurgaon, hyderabad, kolkata, lucknow, noida, pune, ahmedabad, surat, indore, chandigarh, kochi, nagpur, jaipur, vizag(Visakhapatnam)
#    vizag(Visakhapatnam)
current_url = f"https://www.squareyards.com/new-projects-in-{city_name}"

output_file = f"projects_{city_name}_sy.ndjson"
progress_file = f"progress_{city_name}_sy.json"


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
                data = json.load(f)
                return data.get("last_page", 0)
        except:
            return 0
    return 0


def save_progress(page):
    """Save last scraped page number"""
    with open(progress_file, "w", encoding="utf-8") as f:
        json.dump({"last_page": page}, f)


def save_projects(projects, page_number):
    """Append new projects to NDJSON file with pagination_url info"""
    with open(output_file, "a", encoding="utf-8") as f:
        for proj in projects:
            proj["pagination_url"] = f"{current_url}?page={page_number}"
            f.write(json.dumps(proj, ensure_ascii=False) + "\n")


def main():
    # fetch first page once
    response = cm.fetch_page(current_url)
    soup = cm.scraper(response)

    total_pages = extract_total_pages(soup)
    print(f"Total pages found: {total_pages}")

    # load progress
    last_done_page = load_progress()
    print(f"Last completed page: {last_done_page}")

    # if first run, process page 1 immediately
    if last_done_page < 1:
        projects = extract_projects(soup)
        save_projects(projects, 1)
        save_progress(1)
        print(f"Saved {len(projects)} projects from page 1")
        time.sleep(random.uniform(2, 5))

    # resume from next page
    for page in range(max(2, last_done_page + 1), total_pages + 1):
        page_url = f"{current_url}?page={page}"
        print(f"Processing page {page}/{total_pages} → {page_url}")

        try:
            response = cm.fetch_page(page_url)
            soup = cm.scraper(response)
            projects = extract_projects(soup)

            save_projects(projects, page)
            save_progress(page)

            print(f"Saved {len(projects)} projects from page {page}")
            time.sleep(random.uniform(2, 5))

        except KeyboardInterrupt:
            print("Stopped manually. Progress saved.")
            break
        except Exception as e:
            print(f"Error on page {page}: {e}")
            break


if __name__ == "__main__":
    main()



# import sys
# import os

# sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# import utils.custom_method as cm

# current_url = "https://www.squareyards.com/new-projects-in-bangalore"
# output_file_name = "project_listing_response"
# response = cm.fetch_page(current_url)
# soup = cm.scraper(response)


# cm.save_html(f"{output_file_name}.html",soup)