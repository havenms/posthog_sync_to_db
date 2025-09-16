"""
SQLAlchemy setup and model definitions for syncing PostHog click data.

This module centralizes the SQLAlchemy engine, session factory, and table
definitions used by the sync script.  It reads database connection details
from environment variables via a `.env` file.  The models defined here
represent the structure of the local tables into which PostHog event data
will be inserted.

Environment variables required:

* ``DB_HOST`` – Hostname of the MySQL server.
* ``DB_PORT`` – Port number of the MySQL server.
* ``DB_USER`` – Username for the database connection.
* ``DB_PASS`` – Password for the database connection.
* ``DB_NAME`` – Name of the database where tables should be created.

Optional environment variables are described in ``utils.py`` for the
PostHog API configuration.

See the `Events API Reference` from PostHog for query parameters like
``after`` and ``event`` that will be used when requesting events【769991882066228†L90-L114】.
Additionally, PostHog automatically captures click events as part of
its autocapture functionality【373885857529805†L101-L104】, and those events have
the name ``$autocapture``【400729838930256†L96-L104】, which our sync script
filters on when fetching data.
"""

from __future__ import annotations


import os
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Text,
    DateTime,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base, sessionmaker
from dotenv import load_dotenv

# Load environment variables from a .env file if present.  This call is
# idempotent, so importing this module multiple times will not reprocess the
# file unnecessarily.
load_dotenv()

# ---------------------------------------------------------------------------
# Engine & Session
#
# Compose the SQLAlchemy engine URL using MySQL + PyMySQL driver.  If any
# required database environment variables are missing, ``create_engine`` will
# raise an exception.
# ---------------------------------------------------------------------------
_db_user = os.getenv("DB_USER")
_db_pass = os.getenv("DB_PASS")
_db_host = os.getenv("DB_HOST")
_db_port = os.getenv("DB_PORT")
_db_name = os.getenv("DB_NAME")

if not all([_db_user, _db_pass, _db_host, _db_port, _db_name]):
    raise EnvironmentError(
        "Database credentials are not fully specified in environment variables. "
        "Please set DB_USER, DB_PASS, DB_HOST, DB_PORT, and DB_NAME."
    )

engine = create_engine(
    f"mysql+pymysql://{_db_user}:{_db_pass}@{_db_host}:{_db_port}/{_db_name}",
    pool_pre_ping=True,
)
Base = declarative_base()
SessionLocal = sessionmaker(bind=engine)

# ---------------------------------------------------------------------------
# Models
#
# The ``PosthogEvent`` table stores raw event data fetched from PostHog.  Each
# row corresponds to a single event.  We use a UUID from PostHog as the
# ``event_uuid`` to guarantee uniqueness and apply a unique constraint to
# prevent duplicate inserts.  A JSON representation of the full event payload
# is stored in ``raw_data`` as a fallback, along with individual columns
# extracted for common queries.
# ---------------------------------------------------------------------------

class ClickEvent(Base):
    """
    Table structure for storing click (autocapture) events from PostHog.

    Instead of storing the entire JSON blob in a single column, this model
    materializes frequently used click‑related fields into their own columns.
    This makes it easy to query click events in Metabase and other BI tools.

    The ``id`` column uses the PostHog event UUID directly as the primary key.
    The ``raw_data`` column preserves the complete JSON payload for reference.
    """

    __tablename__ = "click_events"
    # Use the PostHog event UUID as the primary key for easy deduplication
    id = Column(String(50), primary_key=True)
    distinct_id = Column(String(255), nullable=False, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    current_url = Column(String(2000))
    pathname = Column(String(1000))
    event_type = Column(String(100))
    element_text = Column(String(500))
    element_tag = Column(String(100))
    element_href = Column(String(2000))
    browser = Column(String(100))
    os = Column(String(100))
    country_code = Column(String(10))
    referrer            = Column(String(2000))
    referring_domain    = Column(String(255))
    city_name       = Column(String(100))
    region_name     = Column(String(100))
    country_name    = Column(String(100))
    postal_code     = Column(String(50))
    latitude        = Column(String(20))
    longitude       = Column(String(20))
    device_type     = Column(String(50))
    os_version      = Column(String(100))
    browser_version = Column(String(50))
    viewport_width  = Column(Integer)
    viewport_height = Column(Integer)
    session_id      = Column(String(100), index=True)


    # Preserve the full JSON for fallback or advanced analysis
    raw_data = Column(Text, nullable=False)

    def __repr__(self) -> str:
        return (
            f"<ClickEvent(id='{self.id}', distinct_id='{self.distinct_id}', "
            f"timestamp='{self.timestamp}', element_tag='{self.element_tag}', "
            f"element_text='{self.element_text}')>"
        )

# Create tables if they do not exist.  This call will create the table
# definitions in the configured database upon module import.
Base.metadata.create_all(engine)

