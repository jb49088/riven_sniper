import datetime
import sqlite3

import bs4
import requests

listings_schema = """
CREATE TABLE listings (
    id TEXT PRIMARY KEY,
    seller TEXT NOT NULL,
    source TEXT NOT NULL,
    weapon TEXT NOT NULL,
    stat1 TEXT,
    stat2 TEXT,
    stat3 TEXT,
    stat4 TEXT,
    price INTEGER NOT NULL,
    scraped_at TIMESTAMP
)
"""


def scrape_riven_market():
    """Scrape first page of riven.market and return normalized data"""

    url = "https://riven.market/_modules/riven/showrivens.php"

    params = {
        "platform": "ALL",
        "limit": 200,
        "recency": -1,
        "veiled": "false",
        "onlinefirst": "false",
        "polarity": "all",
        "rank": "all",
        "mastery": 16,
        "weapon": "Any",
        "stats": "Any",
        "neg": "all",
        "price": 99999,
        "rerolls": -1,
        "sort": "time",
        "direction": "ASC",
        "page": 1,
        "time": int(datetime.datetime.now().timestamp() * 1000),
    }

    # Fetch page
    r = requests.get(url, params=params)
    r.raise_for_status()
    soup = bs4.BeautifulSoup(r.text, "html.parser")

    # Parse rivens
    rivens = []
    for element in soup.select("div.riven"):
        # Get seller name
        seller_div = element.select_one("div.attribute.seller")
        if not seller_div:
            continue

        seller_name = seller_div.text.strip().split("\n")[0].strip()

        # Create normalized entry
        riven = {
            "id": f"rm_{element['id']}",
            "seller": seller_name,
            "source": "riven.market",
            "weapon": element["data-weapon"].lower().replace(" ", "_"),
            "stat1": element["data-stat1"],
            "stat2": element["data-stat2"],
            "stat3": element["data-stat3"],
            "stat4": element["data-stat4"],
            "price": int(element["data-price"]),
            "scraped_at": datetime.datetime.now().isoformat(),
        }
        rivens.append(riven)

    return rivens


def scrape_warframe_market():
    """Scrape recent riven listings from warframe.market API and return normalized data"""

    url = "https://api.warframe.market/v1/auctions"

    params = {
        "type": "riven",
        "sort": "created_desc",  # Newest first
    }

    headers = {
        "platform": "pc",
        "language": "en",
    }

    # Fetch data
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    data = response.json()

    auctions = data.get("payload", {}).get("auctions", [])

    # Parse and normalize auctions
    rivens = []
    for auction in auctions:
        # Only include direct sell listings
        if not auction.get("is_direct_sell", False):
            continue

        item = auction.get("item", {})
        attributes = item.get("attributes", [])

        # Separate positive and negative stats
        positives = []
        negative = None

        for attr in attributes:
            if attr.get("positive", True):
                positives.append(attr.get("url_name", ""))
            else:
                negative = attr.get("url_name", "")

        # Sort positives
        positives = positives[:3]  # Take up to 3 positives

        # Create normalized entry
        riven = {
            "id": f"wf_{auction['id']}",
            "seller": auction.get("owner", {}).get("ingame_name", ""),
            "source": "warframe.market",
            "weapon": item.get("weapon_url_name", ""),
            "stat1": positives[0] if len(positives) > 0 else "",
            "stat2": positives[1] if len(positives) > 1 else "",
            "stat3": positives[2] if len(positives) > 2 else "",
            "stat4": negative if negative else "",
            "price": auction.get("buyout_price", 0),
            "scraped_at": datetime.datetime.now().isoformat(),
        }
        rivens.append(riven)

    return rivens


def update_listings_with_new():
    """Scrape both sites and append only new listings to database"""

    conn = sqlite3.connect("market.db")
    cursor = conn.cursor()

    # Ensure table exists
    cursor.execute(
        listings_schema.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
    )

    # Get existing IDs for quick lookup
    cursor.execute("SELECT id FROM listings")
    existing_ids = {row[0] for row in cursor.fetchall()}

    new_count = 0

    # Scrape riven.market
    print("Scraping riven.market...")
    rm_listings = scrape_riven_market()
    for listing in rm_listings:
        if listing["id"] not in existing_ids:
            cursor.execute(
                """
                INSERT OR REPLACE INTO listings VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    listing["id"],
                    listing["seller"],
                    listing["source"],
                    listing["weapon"],
                    listing["stat1"],
                    listing["stat2"],
                    listing["stat3"],
                    listing["stat4"],
                    listing["price"],
                    listing["scraped_at"],
                ),
            )
            existing_ids.add(listing["id"])
            new_count += 1

    # Scrape warframe.market
    print("Scraping warframe.market...")
    wm_listings = scrape_warframe_market()
    for listing in wm_listings:
        if listing["id"] not in existing_ids:
            cursor.execute(
                """
                INSERT OR REPLACE INTO listings VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    listing["id"],
                    listing["seller"],
                    listing["source"],
                    listing["weapon"],
                    listing["stat1"],
                    listing["stat2"],
                    listing["stat3"],
                    listing["stat4"],
                    listing["price"],
                    listing["scraped_at"],
                ),
            )
            existing_ids.add(listing["id"])
            new_count += 1

    conn.commit()
    conn.close()

    print(f"\nAdded {new_count} new listings")

    return new_count
