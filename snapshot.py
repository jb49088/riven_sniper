import datetime
import sqlite3

import bs4
import requests


def scrape_riven_market_full():
    """One-time full scrape of riven.market for historical data"""
    url = "https://riven.market/_modules/riven/showrivens.php"

    # Database setup with single table
    db_path = "market.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
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
    """)

    # Clear existing data for fresh scrape
    cursor.execute("DELETE FROM listings")
    conn.commit()

    # Scraping parameters
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

    # Get total count
    print("Fetching total count...")
    soup = bs4.BeautifulSoup(requests.get(url, params=params).text, "html.parser")

    pagination_div = soup.select_one("div.pagination")
    if pagination_div:
        total_rivens = int(pagination_div.select("b")[-1].text)
        total_pages = (total_rivens + params["limit"] - 1) // params["limit"]
    else:
        total_rivens = 0
        total_pages = 1

    print(f"Found {total_rivens} rivens total ({total_pages} pages)")

    # Scrape all pages
    page = 1
    total_scraped = 0
    start_time = datetime.datetime.now()

    while page <= total_pages:
        try:
            # Update page and timestamp
            params["page"] = page
            params["time"] = int(datetime.datetime.now().timestamp() * 1000)

            # Fetch page
            response = requests.get(url, params=params)
            response.raise_for_status()
            soup = bs4.BeautifulSoup(response.text, "html.parser")

            # Parse rivens from page
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

            # Insert batch into database
            if rivens:
                cursor.executemany(
                    """
                    INSERT OR REPLACE INTO listings
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                    """,
                    [
                        (
                            r["id"],
                            r["seller"],
                            r["source"],
                            r["weapon"],
                            r["stat1"],
                            r["stat2"],
                            r["stat3"],
                            r["stat4"],
                            r["price"],
                            r["scraped_at"],
                        )
                        for r in rivens
                    ],
                )
                conn.commit()

                total_scraped += len(rivens)
                print(
                    f"Page {page}/{total_pages}: {len(rivens)} rivens (Total: {total_scraped})"
                )
            else:
                print(f"Page {page}/{total_pages}: No rivens found")

            page += 1

        except Exception as e:
            print(f"Error on page {page}: {e}")
            break

    # Final stats
    conn.close()
    end_time = datetime.datetime.now()
    duration = end_time - start_time

    print("\nScrape complete!")
    print(f"Total rivens scraped: {total_scraped}")
    print(f"Duration: {duration}")
    print(f"Database saved to: {db_path}")
