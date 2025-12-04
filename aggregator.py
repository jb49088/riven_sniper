import sqlite3
import statistics
from collections import defaultdict


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


def build_godrolls():
    """Aggregate listings into godrolls table."""

    conn, cursor = init_database("market.db")

    # Get all listings
    cursor.execute(
        """
        SELECT weapon, stat1, stat2, stat3, stat4, price
        FROM listings
        WHERE price > 0 AND price < 50000
        """
    )

    # Build price lists for each unique riven profile
    profiles = defaultdict(list)
    for row in cursor.fetchall():
        weapon, stat1, stat2, stat3, stat4, price = row

        # Normalize stats: sort positives (1-3)
        positives = [s for s in [stat1, stat2, stat3] if s]
        positives.sort()
        while len(positives) < 3:
            positives.append("")
        normalized = tuple(positives + [stat4])

        key = (weapon, *normalized)
        profiles[key].append(price)

    # Build aggregated list with median price and sample count for each profile
    aggregated = [
        (*key, statistics.median(prices), len(prices))
        for key, prices in profiles.items()
    ]

    # Build profile lists for each weapon
    weapon_profiles = defaultdict(list)
    for profile in aggregated:
        weapon = profile[0]
        weapon_profiles[weapon].append(profile)

    # Build godroll list with median price filtering and sample count percentiles
    godrolls = []
    for weapon_rolls in weapon_profiles.values():
        sample_counts = [p[6] for p in weapon_rolls]  # p[6] is sample_count

        # Calculate percentiles
        profiles_with_percentiles = []
        for profile in weapon_rolls:
            sample_count = profile[6]
            rank = sorted(sample_counts).index(sample_count)
            percentile = (rank / len(sample_counts)) * 100
            profiles_with_percentiles.append(profile + (percentile,))

        # Filter and get top 5 for this weapon
        top_rolls = [r for r in profiles_with_percentiles if r[7] >= 80]
        top_rolls.sort(key=lambda x: x[5], reverse=True)  # x[5] is median_price
        godrolls.extend(top_rolls[:5])

    # Insert into godrolls table
    cursor.executemany(
        """
        INSERT INTO godrolls VALUES (?,?,?,?,?,?,?,?)
        """,
        [(p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7]) for p in godrolls],
    )

    conn.commit()

    # Stats
    cursor.execute("SELECT COUNT(*) FROM godrolls")
    total = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(DISTINCT weapon) FROM godrolls")
    weapons = cursor.fetchone()[0]

    print(f"Godrolls created: {total} top rolls across {weapons} weapons")

    conn.close()


build_godrolls()
