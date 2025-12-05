import logging
import sqlite3
import statistics
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/aggregator.log"),
        logging.StreamHandler(),
    ],
)


def init_database(database):
    """Setup the database with a godrolls table."""

    conn = sqlite3.connect(database)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS godrolls")
    cursor.execute(
        """
        CREATE TABLE godrolls (
            weapon TEXT,
            stat1 TEXT,
            stat2 TEXT,
            stat3 TEXT,
            stat4 TEXT,
            median_price REAL,
            sample_count INTEGER,
            sample_percentile REAL,
            PRIMARY KEY (weapon, stat1, stat2, stat3, stat4)
        )
        """
    )

    return conn, cursor


def normalize_riven_stats(stat1, stat2, stat3, stat4):
    """Normalize riven stats by sorting positives."""

    positives = [s for s in [stat1, stat2, stat3] if s]
    positives.sort()
    while len(positives) < 3:
        positives.append("")
    return tuple(positives + [stat4])


def build_profiles_from_listings(cursor):
    """Build price lists for each unique riven profile"""

    profiles = defaultdict(list)
    for row in cursor.fetchall():
        weapon, stat1, stat2, stat3, stat4, price = row

        normalized = normalize_riven_stats(stat1, stat2, stat3, stat4)

        key = (weapon, *normalized)
        profiles[key].append(price)

    return profiles


def aggregate_profiles(profiles):
    """Build aggregated list with median price and sample count."""
    return [
        (*key, statistics.median(prices), len(prices))
        for key, prices in profiles.items()
    ]


def group_by_weapon(aggregated):
    """Group profiles by weapon."""
    weapon_profiles = defaultdict(list)
    for profile in aggregated:
        weapon = profile[0]
        weapon_profiles[weapon].append(profile)

    return weapon_profiles


def calculate_percentiles(weapon_rolls):
    """Calculate sample count percentiles for weapon rolls."""

    sample_counts = [p[6] for p in weapon_rolls]  # p[6] is sample_count
    sorted_counts = sorted(sample_counts)

    profiles_with_percentiles = []
    for profile in weapon_rolls:
        sample_count = profile[6]
        rank = sorted_counts.index(sample_count)
        percentile = (rank / len(sample_counts)) * 100
        profiles_with_percentiles.append(profile + (percentile,))

    return profiles_with_percentiles


def get_top_rolls(profiles_with_percentiles):
    top_rolls = [r for r in profiles_with_percentiles if r[7] >= 80]
    top_rolls.sort(key=lambda x: x[5], reverse=True)  # x[5] is median_price
    return top_rolls[:5]


def display_stats(cursor):
    cursor.execute("SELECT COUNT(*) FROM godrolls")
    total = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(DISTINCT weapon) FROM godrolls")
    weapons = cursor.fetchone()[0]

    logging.info(f"Godrolls created: {total} top rolls across {weapons} weapons")


def aggregator():
    """Aggregate listings into godrolls table."""

    conn, cursor = init_database("market.db")

    cursor.execute(
        """
        SELECT weapon, stat1, stat2, stat3, stat4, price
        FROM listings
        WHERE price > 0 AND price < 50000
        """
    )

    # Build and aggregate profiles
    profiles = build_profiles_from_listings(cursor)
    aggregated = aggregate_profiles(profiles)
    weapon_profiles = group_by_weapon(aggregated)

    # Calculate godrolls
    godrolls = []
    for weapon_rolls in weapon_profiles.values():
        profiles_with_percentiles = calculate_percentiles(weapon_rolls)
        godrolls.extend(get_top_rolls(profiles_with_percentiles))

    # Insert into godrolls table
    cursor.executemany(
        """
        INSERT INTO godrolls VALUES (?,?,?,?,?,?,?,?)
        """,
        [(p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7]) for p in godrolls],
    )

    conn.commit()

    display_stats(cursor)

    conn.close()


if __name__ == "__main__":
    try:
        aggregator()
    except KeyboardInterrupt:
        print("Aggregator interrupted")
