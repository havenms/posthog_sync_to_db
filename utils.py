"""
Utility functions for syncing PostHog data into a local MySQL database.

This module contains helper functions to authenticate against the PostHog API,
fetch events within a specified date range, and insert them into the local
database using SQLAlchemy.  It relies on a few environment variables for
configuration:

* ``POSTHOG_API_KEY`` – Your personal API key for PostHog (required).
* ``POSTHOG_PROJECT_ID`` – The project ID to query (required).
* ``POSTHOG_BASE_HOST`` – Base URL for PostHog's private endpoints.  For US
  Cloud this should be ``https://us.posthog.com``, for EU Cloud it is
  ``https://eu.posthog.com``.  Self‑hosted instances should use the
  appropriate domain.  Defaults to ``https://us.posthog.com``.

In addition, environment variables for the database connection are used by
``sqlalchemy_setup.py``.

PostHog automatically captures clicks via its autocapture functionality.  As
noted in the documentation, autocapture collects pageviews, pageleaves, and
clicks for various HTML elements【373885857529805†L101-L104】.  These events are
stored internally with the special name ``$autocapture`` and include an
``elements_chain`` property【400729838930256†L96-L104】.  The ``fetch_events``
function below filters on that event name by default.

The events API accepts ``after`` and ``before`` query parameters to filter
events by timestamp range, along with an ``event`` parameter to filter by
event name【769991882066228†L90-L114】.  Pagination is handled via the ``next``
attribute in the API response.
"""

from __future__ import annotations

import os
import json
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

import requests

from sqlalchemy.orm import Session

from sqlalchemy_setup import ClickEvent, SessionLocal

# ---------------------------------------------------------------------------
# Configuration helpers
#
# Read PostHog API configuration from environment variables.  If any required
# values are missing, raise an informative error.  The base URL defaults to
# the US Cloud private endpoint.
# ---------------------------------------------------------------------------

def _get_posthog_config() -> Dict[str, str]:
    """Retrieve PostHog API configuration from environment variables.

    Returns a dictionary with keys ``api_key``, ``project_id``, and
    ``base_url``.  Raises ``EnvironmentError`` if required variables are
    missing.
    """
    api_key = os.getenv("POSTHOG_API_KEY")
    project_id = os.getenv("POSTHOG_PROJECT_ID")
    base_url = os.getenv("POSTHOG_BASE_HOST", "https://us.posthog.com")
    if not api_key or not project_id:
        raise EnvironmentError(
            "POSTHOG_API_KEY and POSTHOG_PROJECT_ID must be set in the environment."
        )
    return {
        "api_key": api_key,
        "project_id": project_id,
        "base_url": base_url.rstrip("/"),
    }

# ---------------------------------------------------------------------------
# API interaction
#
# Functions to fetch events from PostHog using the private GET endpoint.
# ``fetch_events`` handles pagination automatically.  The default event name
# filter is ``$autocapture`` to capture click data.  ``after`` and ``before``
# parameters should be ISO‑8601 date strings (e.g. ``2025-09-15T00:00:00Z``).
# ---------------------------------------------------------------------------

def fetch_events(
    after: str,
    before: str,
    event_name: str = "$autocapture",
    limit: int = 100,
    session: Optional[requests.Session] = None,
) -> List[Dict[str, Any]]:
    """Fetch events from PostHog within the given date range.

    Parameters
    ----------
    after : str
        ISO‑8601 timestamp (inclusive) to start collecting events from.  Only
        events occurring after this time will be returned.
    before : str
        ISO‑8601 timestamp (exclusive) to stop collecting events.  Only events
        before this time will be returned.
    event_name : str, optional
        Name of the event to filter on.  Defaults to ``"$autocapture"`` to
        capture click events.
    limit : int, optional
        Maximum number of events per page.  Defaults to 100.  Values above
        1000 may be rejected by the API.
    session : requests.Session, optional
        Optional session object for connection pooling.  If not provided, a
        new session will be created and closed automatically.

    Returns
    -------
    List[Dict[str, Any]]
        A list of raw event dictionaries returned by the API.
    """
    cfg = _get_posthog_config()
    url = f"{cfg['base_url']}/api/projects/{cfg['project_id']}/events/"
    headers = {"Authorization": f"Bearer {cfg['api_key']}", "Content-Type": "application/json"}
    params: Dict[str, Any] = {
        "after": after,
        "before": before,
        "event": event_name,
        "limit": limit,
    }

    events: List[Dict[str, Any]] = []
    # Use provided session or create a new one for the duration of this call.
    own_session = False
    if session is None:
        session = requests.Session()
        own_session = True

    try:
        next_url: Optional[str] = url
        while next_url:
            # When iterating to the next page, we should not reapply the original
            # params because the ``next`` URL contains query parameters already.
            if next_url == url:
                response = session.get(next_url, headers=headers, params=params, timeout=30)
            else:
                response = session.get(next_url, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            batch = data.get("results", [])
            events.extend(batch)
            # The ``next`` field is a fully qualified URL for the next page, or None
            next_url = data.get("next")
    finally:
        if own_session:
            session.close()
    return events

# ---------------------------------------------------------------------------
# Data transformation
#
# Convert raw event dictionaries into instances of ``ClickEvent``.  This
# helper extracts commonly used fields and stores the remainder of the JSON
# as the ``raw_data`` column.  Datetime parsing handles the ``Z`` suffix for
# UTC.
# ---------------------------------------------------------------------------

def _parse_iso_datetime(dt_str: str) -> datetime:
    """Parse an ISO‑8601 timestamp into a ``datetime`` object.

    This helper accepts timestamps with or without a trailing 'Z'.  If no
    timezone information is present, the timestamp is assumed to be UTC.
    """
    if dt_str.endswith("Z"):
        dt_str = dt_str[:-1]
    # Attempt parsing microseconds if present
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.fromisoformat(dt_str)
        except ValueError:
            continue
    # Fallback: rely on fromisoformat directly
    return datetime.fromisoformat(dt_str)

def transform_event(event: Dict[str, Any]) -> ClickEvent:
    """Transform a raw PostHog event dictionary into a ``ClickEvent`` instance.

    This function extracts useful fields for click analysis from the event.
    It looks up properties like the current URL, pathname, browser, OS and
    geoip country.  It also inspects the first element in the ``elements``
    list to determine the tag name and href of the clicked element.  The
    complete JSON payload is preserved in ``raw_data`` for later reference.
    """
    # Timestamp
    ts_str: Optional[str] = event.get("timestamp")
    timestamp = _parse_iso_datetime(ts_str) if ts_str else datetime.utcnow()
    # Base fields
    event_id = event.get("id", "")
    distinct_id = event.get("distinct_id", "")
    props = event.get("properties", {}) or {}
    elements = event.get("elements", []) or []
    # Extract first element details if present
    first_el: Dict[str, Any] = elements[0] if elements else {}
    # Build ClickEvent
    return ClickEvent(
        id=event_id,
        distinct_id=distinct_id,
        timestamp=timestamp,
        current_url=props.get("$current_url"),
        pathname=props.get("$pathname"),
        event_type=props.get("$event_type"),
        element_text=props.get("$el_text") or first_el.get("text"),
        element_tag=first_el.get("tag_name"),
        element_href=first_el.get("href"),
        browser=props.get("$browser"),
        os=props.get("$os"),
        country_code=props.get("$geoip_country_code"),
        referrer          = props.get("$referrer"),
        referring_domain  = props.get("$referring_domain"),
        city_name         = props.get("$geoip_city_name"),
        region_name       = props.get("$geoip_subdivision_1_name"),
        country_name      = props.get("$geoip_country_name"),
        postal_code       = props.get("$geoip_postal_code"),
        latitude          = props.get("$geoip_latitude"),
        longitude         = props.get("$geoip_longitude"),
        device_type       = props.get("$device_type"),
        os_version        = props.get("$os_version"),
        browser_version   = props.get("$browser_version"),
        viewport_width    = props.get("$viewport_width"),
        viewport_height   = props.get("$viewport_height"),
        session_id        = props.get("$session_id"),
        raw_data          = json.dumps(event),
    )

# ---------------------------------------------------------------------------
# Database insertion
#
# Insert transformed events into the database.  Deduplication is handled using
# the unique constraint on ``event_uuid``.  Any duplicates are silently
# skipped.  Sessions are scoped to this call to avoid leaking open
# connections.
# ---------------------------------------------------------------------------

def store_events(events: Iterable[Dict[str, Any]]) -> int:
    """Store raw event dictionaries in the database.

    For each raw event, this function transforms it into a ``ClickEvent`` and
    attempts to insert it.  Since the ``id`` of ``ClickEvent`` is the
    primary key, attempting to add a duplicate will result in a constraint
    violation.  To avoid exceptions, the function checks for existing
    primary keys before adding new records.  Returns the number of inserted
    records.
    """
    inserted_count = 0
    db: Session = SessionLocal()
    try:
        for evt in events:
            model = transform_event(evt)
            # Skip if primary key already exists
            exists = db.get(ClickEvent, model.id)
            if exists:
                continue
            db.add(model)
            inserted_count += 1
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
    return inserted_count

# ---------------------------------------------------------------------------
# High‑level sync
#
# This helper ties together fetching and storing.  It accepts Python ``datetime``
# objects for the date range and converts them to ISO strings.  Returns the
# number of events stored.
# ---------------------------------------------------------------------------

def sync_posthog_events(
    start: datetime,
    end: datetime,
    event_name: str = "$autocapture",
    limit: int = 100,
) -> int:
    """Fetch and store PostHog events between ``start`` and ``end``.

    Parameters
    ----------
    start : datetime
        Start timestamp (inclusive).
    end : datetime
        End timestamp (exclusive).
    event_name : str, optional
        Event name to filter on.  Defaults to ``"$autocapture"`` to sync
        click events.
    limit : int, optional
        Page size for API requests.  Defaults to 100.

    Returns
    -------
    int
        Number of events inserted into the database.
    """
    # Convert datetimes to ISO strings with 'Z' to denote UTC.  Use
    # ``isoformat()`` and append 'Z' if naive.
    def to_iso(dt: datetime) -> str:
        iso = dt.replace(tzinfo=None).isoformat()
        return iso + "Z"

    events = fetch_events(after=to_iso(start), before=to_iso(end), event_name=event_name, limit=limit)
    return store_events(events)