import logging
import sqlite3
import statistics
from collections import defaultdict
from pathlib import Path

from config import DATABASE, GODROLL_COUNT, MAX_PRICE, SAMPLE_THRESHOLD


def init_database(database: Path) -> tuple[sqlite3.Connection, sqlite3.Cursor]:
    """Initialize database with godrolls table."""
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


def build_profiles_from_listings(
    cursor: sqlite3.Cursor,
) -> dict[tuple[str, str, str, str, str], list[int]]:
    """Build price lists for each unique riven profile."""
    profiles = defaultdict(list)
    for row in cursor.fetchall():
        weapon, stat1, stat2, stat3, stat4, price = row
        key = (weapon, stat1, stat2, stat3, stat4)
        profiles[key].append(price)
    return profiles


def aggregate_profiles(
    profiles: dict[tuple[str, str, str, str, str], list[int]],
) -> list[tuple[*tuple[str, ...], float, int]]:
    """Build aggregated list with median price and sample count."""
    return [
        (*key, statistics.median(prices), len(prices))
        for key, prices in profiles.items()
    ]


def group_profiles_by_weapon(
    aggregated: list[tuple[*tuple[str, ...], float, int]],
) -> dict[str, list[tuple[str, str, str, str, str, float, int]]]:
    """Build profile lists for each unique weapon."""
    weapon_profiles = defaultdict(list)
    for profile in aggregated:
        weapon = profile[0]
        weapon_profiles[weapon].append(profile)
    return weapon_profiles


def calculate_percentiles(
    weapon_rolls: list[tuple[str, str, str, str, str, float, int]],
) -> list[tuple[str, str, str, str, str, float, int, float]]:
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


def determine_godrolls(
    profiles_with_percentiles: list[tuple[str, str, str, str, str, float, int, float]],
) -> list[tuple[str, str, str, str, str, float, int, float]]:
    """Return top weapon rolls by median price above sample threshold."""
    top_rolls = [r for r in profiles_with_percentiles if r[7] >= SAMPLE_THRESHOLD]
    top_rolls.sort(key=lambda x: x[5], reverse=True)  # x[5] is median_price
    return top_rolls[:GODROLL_COUNT]


def display_stats(cursor: sqlite3.Cursor) -> None:
    cursor.execute("SELECT COUNT(*) FROM godrolls")
    total = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(DISTINCT weapon) FROM godrolls")
    weapons = cursor.fetchone()[0]
    logging.info(f"Godrolls created: {total} top rolls across {weapons} weapons")


def aggregate() -> None:
    """Aggregate listings into godrolls table."""
    conn, cursor = init_database(DATABASE)

    # Deduplicate by keeping the lowest price for each
    cursor.execute(
        """
        SELECT weapon, stat1, stat2, stat3, stat4, MIN(price) as price
        FROM listings
        WHERE price > 0 AND price < ?
        GROUP BY seller, weapon, stat1, stat2, stat3, stat4
        """,
        (MAX_PRICE,),
    )

    # Build and aggregate profiles
    profiles = build_profiles_from_listings(cursor)
    aggregated_profiles = aggregate_profiles(profiles)
    aggregated_weapon_profiles = group_profiles_by_weapon(aggregated_profiles)

    # Calculate godrolls
    godrolls = []
    for weapon_rolls in aggregated_weapon_profiles.values():
        profiles_with_percentiles = calculate_percentiles(weapon_rolls)
        godrolls.extend(determine_godrolls(profiles_with_percentiles))

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
        aggregate()
    except KeyboardInterrupt:
        print("Aggregator interrupted")
