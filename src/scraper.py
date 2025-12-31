import datetime
import logging

from config import DATABASE
from normalizer import normalize
from poller import (
    extract_riven_market_listings,
    fetch_riven_market_html,
    get_riven_market_params,
    init_database,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
    ],
)


def get_total_count(params):
    """Extract total riven and page count."""
    soup = fetch_riven_market_html()
    pagination_div = soup.select_one("div.pagination")
    if not pagination_div:
        return 0, 1

    total_rivens = int(pagination_div.select("b")[-1].text)
    total_pages = (total_rivens + params["limit"] - 1) // params["limit"]

    return total_rivens, total_pages


def insert_batch(cursor, conn, rivens):
    """Insert a batch of listings into the database."""
    normalized_rivens = []
    skipped_count = 0

    for r in rivens:
        # Normalize weapon name and stats using source-specific mapping
        normalized = normalize(
            r["weapon"], r["stat1"], r["stat2"], r["stat3"], r["stat4"], r["source"]
        )

        # Skip if normalization failed (invalid/unmapped stats)
        if normalized is None:
            skipped_count += 1
            logging.warning(
                f"Skipping listing {r['id']} ({r['weapon']})- unmapped stats: {r['stat1']}, {r['stat2']}, {r['stat3']}, {r['stat4']}"
            )
            continue

        weapon, stat1, stat2, stat3, stat4 = normalized

        normalized_rivens.append(
            (
                r["id"],
                r["seller"],
                r["source"],
                weapon,
                stat1,
                stat2,
                stat3,
                stat4,
                r["price"],
                r["scraped_at"],
            )
        )

    cursor.executemany(
        """
        INSERT OR REPLACE INTO listings
        VALUES (?,?,?,?,?,?,?,?,?,?)
        """,
        normalized_rivens,
    )
    conn.commit()

    if skipped_count > 0:
        logging.info(f"Skipped {skipped_count} listings with unmapped stats")


def display_stats(start_time, total_scraped, db_path):
    """Display runtime statistics."""
    end_time = datetime.datetime.now()
    duration = end_time - start_time

    logging.info("Scrape complete!")
    logging.info(f"Total rivens scraped: {total_scraped}")
    logging.info(f"Duration: {duration}")
    logging.info(f"Listings table saved to: {db_path}")


def scrape():
    """One-time full scrape of riven.market for historical data."""
    db_path, conn, cursor = init_database(DATABASE)

    params = get_riven_market_params()

    logging.info("Fetching total count...")
    total_rivens, total_pages = get_total_count(params)
    logging.info(f"Found {total_rivens} rivens total ({total_pages} pages)")

    page = 1
    total_scraped = 0
    start_time = datetime.datetime.now()

    while page <= total_pages:
        try:
            params["page"] = page
            soup = fetch_riven_market_html()
            rivens = extract_riven_market_listings(soup)

            if rivens:
                insert_batch(cursor, conn, rivens)
                total_scraped += len(rivens)
                logging.info(
                    f"Page {page}/{total_pages}: {len(rivens)} rivens (Total: {total_scraped})"
                )
            else:
                logging.info(f"Page {page}/{total_pages}: No rivens found")

            page += 1

        except Exception as e:
            logging.error(f"Error on page {page}: {e}")
            break

    conn.close()

    display_stats(start_time, total_scraped, db_path)


if __name__ == "__main__":
    try:
        scrape()
    except KeyboardInterrupt:
        logging.info("Scraper interrupted")
