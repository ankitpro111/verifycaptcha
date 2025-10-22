import requests
from bs4 import BeautifulSoup
import time
import json
import os
import random

city_name = "bangalore"
os.makedirs(city_name, exist_ok=True)

BASE_URL = f"https://www.squareyards.com/real-estate-builders-in-{city_name}?page="

OUTPUT_FILE = f"./{city_name}/builders_{city_name}.ndjson"
STATE_FILE = f"./{city_name}/scraper_state.json"

headers = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
}


def load_state():
    """Load scraper state: seen URLs + last scraped page."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            try:
                state = json.load(f)
                return set(state.get("seen", [])), state.get("last_page", 0)
            except json.JSONDecodeError:
                return set(), 0
    return set(), 0


def save_state(seen, last_page):
    """Save scraper state."""
    state = {"seen": list(seen), "last_page": last_page}
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def get_total_pages():
    res = requests.get(BASE_URL + "1", headers=headers)
    soup = BeautifulSoup(res.text, "html.parser")

    pagination = soup.select_one(".paginationBox")
    if not pagination:
        return 1

    try:
        last_li = pagination.find_all("li")[-1]
        last_a = last_li.find("a")
        if last_a and last_a.has_attr("href"):
            href = last_a["href"]
            if "page=" in href:
                return int(href.split("page=")[-1])
        return 1
    except Exception as e:
        print("⚠️ Pagination parse error:", e)
        return 1


def parse_builder(tile):
    name = tile.select_one(".tileTitle a span").get_text(strip=True) if tile.select_one(".tileTitle a span") else None
    builder_url = tile.select_one(".tileTitle a")["href"] if tile.select_one(".tileTitle a") else None
    location = tile.select_one(".tileLocation").get_text(strip=True) if tile.select_one(".tileLocation") else None

    stats = {}
    for stat in tile.select(".tileTotalBox .tileTotalLi"):
        key = stat.select_one(".tileTotalTitle").get_text(strip=True)
        value = stat.select_one(".tileTotalValue").get_text(strip=True)
        stats[key] = value

    categories = [cat.get_text(strip=True) for cat in tile.select(".tileTabBox .tileTabBtn")]

    projectList = None
    btn_box = tile.select_one(".tileBtnBox a")
    if btn_box and btn_box.has_attr("href"):
        projectList = btn_box["href"]

    return {
        "name": name,
        "url": builder_url,
        "location": location,
        "stats": stats,
        "categories": categories,
        "projects": projectList,
    }


def scrape():
    total_pages = get_total_pages()
    seen, last_page = load_state()

    print(f"Found {total_pages} pages")
    print(f"Resuming from page {last_page + 1}, already {len(seen)} builders scraped")

    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
        for page in range(last_page + 1, total_pages + 1):
            print(f"\n🔎 Scraping page {page}/{total_pages}")
            res = requests.get(BASE_URL + str(page), headers=headers)
            soup = BeautifulSoup(res.text, "html.parser")

            new_count = 0
            for tile in soup.select(".tileWrap .tileBox"):
                builder = parse_builder(tile)
                if not builder.get("url"):
                    continue
                if builder["url"] in seen:
                    continue
                f.write(json.dumps(builder, ensure_ascii=False) + "\n")
                seen.add(builder["url"])
                new_count += 1

            save_state(seen, page)
            print(f"✅ Page {page}: added {new_count} new builders, total unique = {len(seen)}")

            time.sleep(random.uniform(1, 3))


if __name__ == "__main__":
    scrape()
    print(f"🎉 Finished. Unique builders stored in {OUTPUT_FILE}")
