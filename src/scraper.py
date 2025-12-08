import datetime
import logging

from config import DATABASE
from poller import (
    fetch_riven_market_page,
    get_headers,
    get_riven_market_params,
    get_riven_market_url,
    init_database,
    parse_riven_market_rivens,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
    ],
)


def get_total_count(url, params, headers):
    """Extract total riven and page count."""
    soup = fetch_riven_market_page(url, params, headers)

    pagination_div = soup.select_one("div.pagination")

    if not pagination_div:
        return 0, 1

    total_rivens = int(pagination_div.select("b")[-1].text)
    total_pages = (total_rivens + params["limit"] - 1) // params["limit"]

    return total_rivens, total_pages


def insert_batch(cursor, conn, rivens):
    """Insert a batch of listings into the database."""
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


def display_stats(start_time, total_scraped, db_path):
    """Display runtime statistics."""
    end_time = datetime.datetime.now()
    duration = end_time - start_time

    logging.info("Scrape complete!")
    logging.info(f"Total rivens scraped: {total_scraped}")
    logging.info(f"Duration: {duration}")
    logging.info(f"Listings table saved to: {db_path}")


def scraper():
    """One-time full scrape of riven.market for historical data."""
    url = get_riven_market_url()
    params = get_riven_market_params()
    headers = get_headers()
    db_path, conn, cursor = init_database(DATABASE)

    logging.info("Fetching total count...")
    total_rivens, total_pages = get_total_count(url, params, headers)
    logging.info(f"Found {total_rivens} rivens total ({total_pages} pages)")

    page = 1
    total_scraped = 0
    start_time = datetime.datetime.now()

    while page <= total_pages:
        try:
            params["page"] = page
            soup = fetch_riven_market_page(url, params, headers)
            rivens = parse_riven_market_rivens(soup)

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
        scraper()
    except KeyboardInterrupt:
        logging.info("Scraper interrupted")
