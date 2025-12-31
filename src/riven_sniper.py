# ================================================================================
# =                                 RIVEN_SNIPER                                 =
# ================================================================================

# TODO: investigate read timed out error.

import datetime
import logging
import random
import time
from pathlib import Path
from typing import Never

from aggregator import aggregate
from config import POLL_INTERVAL, POLL_JITTER
from monitor import monitor
from poller import poll

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/riven_sniper.log"),
        logging.StreamHandler(),
    ],
)


def should_aggregate() -> bool:
    """Check if aggregator hasn't run today yet."""
    marker_file = Path("logs/.last_aggregate")
    today = datetime.date.today().isoformat()

    # Check if we already aggregated today
    if marker_file.exists():
        last_run = marker_file.read_text().strip()
        if last_run == today:
            return False

    # Only aggregate during 4am hour
    now = datetime.datetime.now()
    if now.hour == 4:
        marker_file.write_text(today)
        return True

    return False


def run_pipeline() -> None:
    """Run one iteration of the pipeline."""
    try:
        poll()
    except Exception as e:
        logging.error(f"Poller failed: {e}")
        return

    if should_aggregate():
        try:
            aggregate()
        except Exception as e:
            logging.error(f"Aggregator failed: {e}")

    try:
        monitor()
    except Exception as e:
        logging.error(f"Monitor failed: {e}")


def riven_sniper() -> Never:
    """Main entry point for riven_sniper."""
    logging.info(
        f"Starting riven_sniper (poll interval: {POLL_INTERVAL}s ± {POLL_JITTER}s)"
    )
    logging.info("Press Ctrl+C to stop")

    poll_count = 0

    while True:
        poll_count += 1
        start_time = time.time()

        logging.info(f"================ Poll #{poll_count} ================")

        try:
            run_pipeline()
        except Exception as e:
            logging.error(f"Pipeline error: {e}")

        elapsed = time.time() - start_time
        jitter = random.uniform(-POLL_JITTER, POLL_JITTER)
        sleep_time = max(0, POLL_INTERVAL + jitter - elapsed)

        if sleep_time > 0:
            next_time = datetime.datetime.now() + datetime.timedelta(seconds=sleep_time)
            logging.info(
                f"Poll complete in {elapsed:.1f}s. Next poll at {next_time.strftime('%H:%M:%S')}"
            )
            time.sleep(sleep_time)
        else:
            logging.warning(
                f"Poll took {elapsed:.1f}s (exceeds {POLL_INTERVAL}s interval)"
            )


if __name__ == "__main__":
    try:
        riven_sniper()
    except KeyboardInterrupt:
        logging.info("Riven sniper stopped")
    except Exception as e:
        logging.error(f"Fatal error: {e}")
