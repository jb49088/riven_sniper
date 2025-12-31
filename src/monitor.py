import datetime
import logging
import os
import sqlite3
import time
from pathlib import Path
from typing import TypedDict

import requests
from dotenv import load_dotenv

from config import DATABASE, DEAL_THRESHOLD

load_dotenv()


class Deal(TypedDict):
    id: str
    weapon: str
    stats: list[str]
    price: int
    median_price: float
    discount_percentage: float
    seller: str
    source: str
    scraped_at: str
    sample_count: int
    sample_count_percentile: float


def init_alerted_table(database: Path) -> None:
    """Initialize database with alerted_listings table."""
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS alerted_listings (
            listing_id TEXT PRIMARY KEY
        )
    """)
    conn.commit()
    conn.close()


def find_deals(database: Path, threshold: float) -> list[Deal]:
    """Find new listings that are below the median price threshold."""
    conn = sqlite3.connect(database)
    cursor = conn.cursor()

    deals = []

    # Get already alerted listings
    cursor.execute("SELECT listing_id FROM alerted_listings")
    alerted = {row[0] for row in cursor.fetchall()}

    cursor.execute("SELECT * FROM godrolls")
    current_godrolls = cursor.fetchall()

    for godroll in current_godrolls:
        (
            weapon,
            stat1,
            stat2,
            stat3,
            stat4,
            median_price,
            sample_count,
            sample_count_percentile,
        ) = godroll

        cursor.execute(
            """
            SELECT id, price, seller, source, scraped_at
            FROM listings
            WHERE weapon = ? AND stat1 = ? AND stat2 = ? AND stat3 = ? AND stat4 = ?
            AND price <= ?
            AND price > 0
            ORDER BY scraped_at DESC
            LIMIT 10
            """,
            (weapon, stat1, stat2, stat3, stat4, median_price * threshold),
        )

        cheap_listings: list[tuple[str, int, str, str, str]] = cursor.fetchall()

        if cheap_listings:
            for listing in cheap_listings:
                listing_id, price, seller, source, scraped_at = listing

                # Skip if already alerted
                if listing_id in alerted:
                    continue

                discount_percentage = ((median_price - price) / median_price) * 100

                deals.append(
                    {
                        "id": listing_id,
                        "weapon": weapon,
                        "stats": [stat1, stat2, stat3, stat4],
                        "price": price,
                        "median_price": median_price,
                        "discount_percentage": discount_percentage,
                        "seller": seller,
                        "source": source,
                        "scraped_at": scraped_at,
                        "sample_count": sample_count,
                        "sample_count_percentile": sample_count_percentile,
                    }
                )

                # Mark as alerted
                cursor.execute(
                    "INSERT OR IGNORE INTO alerted_listings (listing_id) VALUES (?)",
                    (listing_id,),
                )

    conn.commit()
    conn.close()

    return deals


def format_riven_stats(stats: list[str]) -> str:
    """Format riven stats with correct signs."""
    positives = [s for s in stats[:-1] if s]
    negative = stats[-1]

    inverted_stats = {"reload_speed", "recoil"}

    formatted = []

    for stat in positives:
        sign = "-" if stat in inverted_stats else "+"
        formatted.append(f"{sign}{stat}")

    if negative:
        sign = "+" if negative in inverted_stats else "-"
        formatted.append(f"{sign}{negative}")

    return " ".join(formatted).replace("_", " ").title()


def send_alert(deal: Deal) -> None:
    """Format and send alert for a good deal."""
    weapon = deal["weapon"].replace("_", " ").title()
    stats = format_riven_stats(deal["stats"])
    dt = datetime.datetime.fromisoformat(deal["scraped_at"])
    formatted_time = dt.strftime("%Y-%m-%d %H:%M:%S")

    message = f"""
    Weapon: {weapon}
    Stats: {stats}
    Price: {deal["price"]}p
    Median: {deal["median_price"]}p
    Discount: {deal["discount_percentage"]:.1f}%
    Sample: {deal["sample_count"]} listings (top {deal["sample_count_percentile"]:.0f}%)
    Seller: {deal["seller"]}
    Source: {deal["source"]}
    Scraped: {formatted_time}
    """.strip()

    logging.info(f"DEAL FOUND:\n{message}\n")
    push_notification(message)


def push_notification(message: str) -> None:
    """Push notification to Pushover."""
    application_key = os.getenv("PUSHOVER_APPLICATION_KEY")
    user_key = os.getenv("PUSHOVER_USER_KEY")

    if not user_key:
        logging.error("PUSHOVER_USER_KEY not set")
        return

    if not application_key:
        logging.error("PUSHOVER_APPLICATION_KEY not set")
        return

    try:
        r = requests.post(
            "https://api.pushover.net/1/messages.json",
            data={
                "token": application_key,
                "user": user_key,
                "message": message,
            },
        )
        r.raise_for_status()
        logging.info("Pushover notification sent successfully")
    except Exception as e:
        logging.error(f"Failed to send Pushover notification: {e}")


def monitor(database: Path = DATABASE, threshold: float = DEAL_THRESHOLD) -> None:
    """Monitor and alert on deals."""
    init_alerted_table(database)

    logging.info(f"Starting monitor with threshold={threshold}")
    deals = find_deals(database, threshold)

    for deal in deals:
        send_alert(deal)
        time.sleep(1)

    logging.info(f"Monitor complete. Found {len(deals)} deals")


if __name__ == "__main__":
    try:
        monitor()
    except KeyboardInterrupt:
        print("Monitor interrupted")
