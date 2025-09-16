#!/usr/bin/env python3
"""
Entry point for syncing PostHog click data to a local MySQL database.

This script orchestrates the following steps:

1. Load environment variables from a .env file via ``python-dotenv`` (done in
   imported modules).
2. Accept optional command‑line arguments for the date range to sync.  If
   unspecified, the script defaults to syncing the previous day's data (the
   last 24 hours ending at the current UTC time).  Dates should be provided
   in ISO 8601 format without a timezone (e.g. ``2025-09-15T00:00:00``).
3. Fetch events from PostHog using the ``$autocapture`` event name, which
   includes clicks as part of PostHog's autocapture functionality【373885857529805†L101-L104】.
4. Insert the events into the local database, skipping duplicates via a
   unique constraint.

Example usage::

    python main.py --start 2025-09-15T00:00:00 --end 2025-09-16T00:00:00

Environment variables must define database credentials (see ``sqlalchemy_setup``)
and PostHog API settings (see ``utils``).
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta
import logging

from utils import sync_posthog_events

# Configure basic logging.  In production you may want to send logs to a file.
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def parse_args() -> argparse.Namespace:
    """Parse command‑line arguments for the sync script."""
    parser = argparse.ArgumentParser(description="Sync PostHog click (autocapture) data to a local database")
    parser.add_argument(
        "--start",
        help="Start timestamp (inclusive) in ISO 8601 format (e.g. 2025-09-15T00:00:00). Defaults to 24 hours ago.",
        dest="start",
    )
    parser.add_argument(
        "--end",
        help="End timestamp (exclusive) in ISO 8601 format (e.g. 2025-09-16T00:00:00). Defaults to now.",
        dest="end",
    )
    parser.add_argument(
        "--event-name",
        default="$autocapture",
        help="Event name to filter on (default: $autocapture)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Number of events to request per API page (default: 100)",
    )
    return parser.parse_args()


def iso_to_datetime(value: str) -> datetime:
    """Convert an ISO 8601 string (without timezone) into a datetime object."""
    return datetime.fromisoformat(value)


def main() -> None:
    args = parse_args()
    now = datetime.now()
    if args.end:
        end = iso_to_datetime(args.end)
    else:
        end = now
    if args.start:
        start = iso_to_datetime(args.start)
    else:
        start = end - timedelta(days=1)

    logging.info(f"Syncing PostHog events from {start.isoformat()} to {end.isoformat()} with event name '{args.event_name}'")
    inserted = sync_posthog_events(start=start, end=end, event_name=args.event_name, limit=args.limit)
    logging.info(f"Inserted {inserted} events into the database")


if __name__ == "__main__":
    main()


