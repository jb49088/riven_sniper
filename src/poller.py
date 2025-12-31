# TODO: drop redundant cache busting for riven.market

import datetime
import logging
import sqlite3
from pathlib import Path
from typing import Any

import bs4
import requests

from config import DATABASE
from normalizer import normalize


def init_database(database: Path) -> tuple[Path, sqlite3.Connection, sqlite3.Cursor]:
    """Initialize database with listings table and indexes."""
    conn = sqlite3.connect(database)
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS listings (
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
    )

    # Create index for fast lookups
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_listings_lookup
        ON listings(weapon, stat1, stat2, stat3, stat4, price)
        """
    )

    conn.commit()

    return database, conn, cursor


def get_headers() -> dict[str, str]:
    """Get Firefox browser headers."""
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:145.0) Gecko/20100101 Firefox/145.0",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


def get_riven_market_url() -> str:
    """Return the riven.market API URL."""
    return "https://riven.market/_modules/riven/showrivens.php"


def get_riven_market_params() -> dict[str, str | int]:
    """Return query parameters for riven.market API."""
    return {
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


def fetch_riven_market_html() -> bs4.BeautifulSoup:
    """Fetch and parse first page of riven.market HTML listing data."""
    url = get_riven_market_url()
    params = get_riven_market_params()
    headers = get_headers()

    # Update time for cache busting
    params["time"] = int(datetime.datetime.now().timestamp() * 1000)

    r = requests.get(url, params=params, headers=headers, timeout=10)
    r.raise_for_status()

    return bs4.BeautifulSoup(r.text, "html.parser")


def extract_riven_market_listings(
    soup: bs4.BeautifulSoup,
) -> list[dict[str, str | int]]:
    """Extract and return riven.market listings."""
    listings = []

    for listing in soup.select("div.riven"):
        # Get seller name
        seller_div = listing.select_one("div.attribute.seller")
        if not seller_div:
            continue

        seller_name = seller_div.text.strip().split("\n")[0].strip()

        # Build riven dictionary
        riven = {
            "id": f"rm_{listing['id']}",
            "seller": seller_name,
            "source": "riven.market",
            "weapon": listing["data-weapon"],
            "stat1": listing["data-stat1"],
            "stat2": listing["data-stat2"],
            "stat3": listing["data-stat3"],
            "stat4": listing["data-stat4"],
            "price": int(str(listing["data-price"])),
            "scraped_at": datetime.datetime.now().isoformat(),
        }
        listings.append(riven)

    return listings


def poll_riven_market() -> list[dict[str, str | int]]:
    """Poll first page of riven.market."""
    soup = fetch_riven_market_html()
    listings = extract_riven_market_listings(soup)

    return listings


def get_warframe_market_url() -> str:
    """Return the warframe.market API URL."""
    return "https://api.warframe.market/v1/auctions"


def get_warframe_market_params() -> dict[str, str]:
    """Return default parameters for warframe.market API."""
    return {
        "type": "riven",
        "sort": "created_desc",
    }


def fetch_warframe_market_json() -> list[dict[str, Any]]:
    """Fetch and parse most recent warframe.market JSON listing data."""
    url = get_warframe_market_url()
    params = get_warframe_market_params()
    headers = get_headers()

    r = requests.get(url, params=params, headers=headers, timeout=10)
    r.raise_for_status()
    data = r.json()

    return data.get("payload", {}).get("auctions", [])


def extract_warframe_market_listings(
    data: list[dict[str, Any]],
) -> list[dict[str, str | int]]:
    """Extract and return warframe.market listings."""
    listings = []

    for listing in data:
        # Only include direct sell listings
        if not listing.get("is_direct_sell", False):
            continue

        # Skip non-riven items (lich/sister listings)
        if listing.get("item", {}).get("type") != "riven":
            continue

        item = listing.get("item", {})
        attributes = item.get("attributes", [])

        # Separate positive and negative stats
        positives = []
        negative = None

        for attribute in attributes:
            if attribute.get("positive", True):
                positives.append(attribute.get("url_name", ""))
            else:
                negative = attribute.get("url_name", "")

        # Build riven dictionary
        riven = {
            "id": f"wm_{listing['id']}",
            "seller": listing.get("owner", {}).get("ingame_name", ""),
            "source": "warframe.market",
            "weapon": item.get("weapon_url_name", ""),
            "stat1": positives[0] if len(positives) > 0 else "",
            "stat2": positives[1] if len(positives) > 1 else "",
            "stat3": positives[2] if len(positives) > 2 else "",
            "stat4": negative if negative else "",
            "price": listing.get("buyout_price", 0),
            "scraped_at": datetime.datetime.now().isoformat(),
        }
        listings.append(riven)

    return listings


def poll_warframe_market() -> list[dict[str, str | int]]:
    """Poll recent listings from warframe.market."""
    data = fetch_warframe_market_json()
    listings = extract_warframe_market_listings(data)

    return listings


def insert_listing(
    listing: dict[str, str | int], existing_ids: set[str], cursor: sqlite3.Cursor
) -> None:
    """Normalize and insert a listing into the listings table."""
    if listing["id"] not in existing_ids:
        # Normalize weapon name and stats using source-specific mapping
        normalized = normalize(
            str(listing["weapon"]),
            str(listing["stat1"]),
            str(listing["stat2"]),
            str(listing["stat3"]),
            str(listing["stat4"]),
            str(listing["source"]),
        )

        # Skip if normalization failed (invalid/unmapped stats)
        if normalized is None:
            logging.warning(
                f"Skipping listing {listing['id']} ({listing['weapon']}) - unmapped stats: {listing['stat1']}, {listing['stat2']}, {listing['stat3']}, {listing['stat4']}"
            )
            return

        weapon, stat1, stat2, stat3, stat4 = normalized

        cursor.execute(
            """
            INSERT OR REPLACE INTO listings
            VALUES (?,?,?,?,?,?,?,?,?,?)
            """,
            (
                listing["id"],
                listing["seller"],
                listing["source"],
                weapon,
                stat1,
                stat2,
                stat3,
                stat4,
                listing["price"],
                listing["scraped_at"],
            ),
        )
        existing_ids.add(str(listing["id"]))


def poll() -> None:
    """Poll riven.market and warframe.market for new listings.

    Fetches listings from both sources and inserts new entries into the database.
    """
    db_path, conn, cursor = init_database(DATABASE)

    # Get existing IDs for quick lookup
    cursor.execute("SELECT id FROM listings")
    existing_ids = {row[0] for row in cursor.fetchall()}

    initial_count = len(existing_ids)

    logging.info("Polling riven.market...")
    try:
        for listing in poll_riven_market():
            insert_listing(listing, existing_ids, cursor)
    except Exception as e:
        logging.error(f"Failed to poll riven.market: {e}")

    logging.info("Polling warframe.market...")
    try:
        for listing in poll_warframe_market():
            insert_listing(listing, existing_ids, cursor)
    except Exception as e:
        logging.error(f"Failed to poll warframe.market: {e}")

    conn.commit()
    conn.close()

    new_count = len(existing_ids) - initial_count
    logging.info(f"Added {new_count} new listings to {db_path}")


if __name__ == "__main__":
    try:
        poll()
    except KeyboardInterrupt:
        print("Poller interrupted")
