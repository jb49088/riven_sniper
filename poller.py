import datetime

import requests

from snapshot import (
    fetch_page,
    get_riven_market_params,
    get_riven_market_url,
    init_database,
    parse_rivens,
)


def poll_riven_market():
    """Poll first page of riven.market and return normalized data."""

    url = get_riven_market_url()
    params = get_riven_market_params()

    soup = fetch_page(url, params)
    rivens = parse_rivens(soup)

    return rivens


def poll_warframe_market():
    """Poll recent riven listings from warframe.market API and return normalized data."""

    url = "https://api.warframe.market/v1/auctions"

    params = {
        "type": "riven",
        "sort": "created_desc",
    }

    # Fetch data
    r = requests.get(url, params=params)
    r.raise_for_status()
    data = r.json()

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

        # Create normalized entry
        riven = {
            "id": f"wm_{auction['id']}",
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


def insert_listing(listing, existing_ids, cursor):
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


def main():
    """Poll both sites and append only new listings to listings."""

    db_path, conn, cursor = init_database("market.db")

    # Get existing IDs for quick lookup
    cursor.execute("SELECT id FROM listings")
    existing_ids = {row[0] for row in cursor.fetchall()}

    initial_count = len(existing_ids)

    print("Polling riven.market...")
    for listing in poll_riven_market():
        insert_listing(listing, existing_ids, cursor)

    print("Polling warframe.market...")
    for listing in poll_warframe_market():
        insert_listing(listing, existing_ids, cursor)

    conn.commit()
    conn.close()

    new_count = len(existing_ids) - initial_count
    print(f"\nAdded {new_count} new listings")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Poller interrupted")
