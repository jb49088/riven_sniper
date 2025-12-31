from pathlib import Path

# Database
DATABASE = Path("data/market.db")

# Aggregation - filtering and godroll selection
MAX_PRICE = 50000  # Filter out unrealistic/troll listings above this price
SAMPLE_THRESHOLD = 80  # Only consider rolls in top 80th percentile by sample size
GODROLL_COUNT = 5  # Top N godrolls to track per weapon

# Monitoring - deal detection
DEAL_THRESHOLD = 0.60  # Alert on listings <= 60% of median price
POLL_INTERVAL = 10  # Seconds between polling
POLL_JITTER = 2  # ± Seconds randomization
