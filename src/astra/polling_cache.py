"""In-memory append-only cache for device polling data.

Stores processed (pivoted, 60s-grouped, JSON-safe) polling results keyed by
``(device_type, day)`` and serves both full and incremental (``since``) requests
without hitting the database on every call.

Concurrency is handled via one ``asyncio.Lock`` per cache key so that
simultaneous requests from multiple users never trigger duplicate DB queries.
"""

from __future__ import annotations

import asyncio
import datetime
import logging
import sqlite3
from datetime import UTC

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Minimum seconds between DB refreshes for the same cache key.
_REFRESH_COOLDOWN = 5


class PollingCacheEntry:
    """A single cache entry for one ``(device_type, day)`` key.

    Attributes:
        data: List of row-dicts (60s-grouped records) – the ``"data"`` field
            returned by the endpoint.
        latest: Dict of latest per-column values.
        safety_limits: Computed once from observatory config (``None`` for
            non-ObservingConditions device types).
        last_db_datetime: ISO-format string of the newest ``datetime`` value
            currently stored in *data*.  Used as the ``since`` boundary for
            incremental DB fetches.
        last_refresh: When the DB was last queried (wall-clock).
        lock: Per-entry asyncio lock.
    """

    __slots__ = (
        "data",
        "latest",
        "safety_limits",
        "last_db_datetime",
        "last_refresh",
        "lock",
    )

    def __init__(self) -> None:
        self.data: list[dict] = []
        self.latest: dict = {}
        self.safety_limits: dict | None = None
        self.last_db_datetime: str | None = None
        self.last_refresh: datetime.datetime | None = None
        self.lock = asyncio.Lock()


# ---------------------------------------------------------------------------
# Internal helpers – data fetch & transform
# ---------------------------------------------------------------------------


def _query_polling_df(
    db: sqlite3.Connection,
    device_type: str,
    *,
    day: float | None = None,
    since: str | None = None,
    obs_config: dict | None = None,
) -> pd.DataFrame:
    """Run the raw polling SQL queries and return a combined DataFrame.

    Exactly one of *day* or *since* must be provided:
    - *day*: fetch rows from the last ``day`` days.
    - *since*: fetch rows newer than the given datetime string.

    For ``ObservingConditions`` the SafetyMonitor, WeatherSafe and Dome
    (ShutterStatus) rows are also fetched and concatenated.
    """
    if since is not None:
        time_clause = f"datetime > '{since}'"
    else:
        time_clause = f"datetime > datetime('now', '-{day} day')"

    q = f"SELECT * FROM polling WHERE device_type = '{device_type}' AND {time_clause}"
    df = pd.read_sql_query(q, db)

    if device_type == "ObservingConditions" and obs_config is not None:
        if "SafetyMonitor" in obs_config:
            q_safe = f"SELECT * FROM polling WHERE device_type = 'SafetyMonitor' AND {time_clause}"
            q_ws = f"SELECT * FROM polling WHERE device_type = 'WeatherSafe' AND {time_clause}"
            df_safe = pd.read_sql_query(q_safe, db)
            df_ws = pd.read_sql_query(q_ws, db)
            if not df_safe.empty:
                df = pd.concat([df, df_safe], ignore_index=True)
            if not df_ws.empty:
                df = pd.concat([df, df_ws], ignore_index=True)

        if "Dome" in obs_config:
            q_dome = (
                f"SELECT * FROM polling WHERE device_type = 'Dome' "
                f"AND device_command = 'ShutterStatus' AND {time_clause}"
            )
            df_dome = pd.read_sql_query(q_dome, db)
            if not df_dome.empty:
                df = pd.concat([df, df_dome], ignore_index=True)

    return df


def _process_df(df: pd.DataFrame) -> tuple[list[dict], dict]:
    """Pivot, group, and convert *df* into ``(rows, latest)``.

    Returns the same shape the frontend expects for the ``"data"`` and
    ``"latest"`` keys.
    """
    if df.empty:
        return [], {}

    # Pivot: datetime → index, device_command → columns
    df = df.pivot(index="datetime", columns="device_command", values="device_value")

    if "ShutterStatus" in df.columns:
        df = df.rename(columns={"ShutterStatus": "Dome_Open"})

    df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    df = df.replace({"True": 1, "False": 0, True: 1, False: 0})
    df = df.apply(pd.to_numeric, errors="coerce")

    # Latest values (before grouping to preserve precision)
    latest: dict = {}
    for col in df.columns:
        series = df[col].dropna()
        latest[col] = series.iloc[-1] if not series.empty else None

    if "SkyTemperature" in latest and "Temperature" in latest:
        if latest["SkyTemperature"] is not None and latest["Temperature"] is not None:
            latest["RelativeSkyTemp"] = latest["SkyTemperature"] - latest["Temperature"]

    # Group by 60s
    df_grouped = df.groupby(pd.Grouper(freq="60s")).mean()
    df_grouped = df_grouped.dropna()

    # Invert Dome_Open (1=open, 0=closed)
    if "Dome_Open" in df_grouped.columns:
        df_grouped["Dome_Open"] = df_grouped["Dome_Open"].apply(
            lambda x: 0 if x == 1 else 1
        )

    # Derived column
    if "SkyTemperature" in df_grouped.columns and "Temperature" in df_grouped.columns:
        df_grouped["RelativeSkyTemp"] = (
            df_grouped["SkyTemperature"] - df_grouped["Temperature"]
        )

    rows = df_grouped.reset_index().to_dict(orient="records")
    return rows, latest


def _to_json_safe(value):  # type: ignore[no-untyped-def]
    """Recursively convert numpy/pandas scalars to native Python types."""
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _to_json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_to_json_safe(v) for v in value]
    return value


def _compute_safety_limits(obs_config: dict) -> dict:
    """Compute weather safety limits from the observatory config (once)."""
    closing_limits = obs_config["ObservingConditions"][0]["closing_limits"]
    safety_limits: dict = {}

    for key in closing_limits:
        upper_val = float("inf")
        lower_val = float("-inf")
        for item in closing_limits[key]:
            if item.get("upper", float("inf")) < upper_val:
                upper_val = item["upper"]
            if item.get("lower", float("-inf")) > lower_val:
                lower_val = item["lower"]

        safety_limits[key] = {
            "upper": upper_val if upper_val != float("inf") else None,
            "lower": lower_val if lower_val != float("-inf") else None,
        }

    return safety_limits


# ---------------------------------------------------------------------------
# Merge helpers
# ---------------------------------------------------------------------------


def _merge_rows(
    existing: list[dict],
    new_rows: list[dict],
    day: float,
) -> list[dict]:
    """Append *new_rows* to *existing*, de-duplicate the overlap row, and
    trim records older than *day* days from the head.
    """
    if not new_rows:
        return existing

    if existing and new_rows:
        # The 60s binning may produce an overlapping boundary row.  If the
        # last existing row has the same datetime as the first new row,
        # replace it (the newer version is more complete).
        last_dt = existing[-1].get("datetime")
        first_new_dt = new_rows[0].get("datetime")
        if last_dt is not None and first_new_dt is not None and last_dt == first_new_dt:
            existing[-1] = new_rows[0]
            new_rows = new_rows[1:]

    merged = existing + new_rows

    # Trim old data beyond the day window
    cutoff = datetime.datetime.now(UTC) - datetime.timedelta(days=day)
    trimmed = [row for row in merged if _parse_row_dt(row.get("datetime")) >= cutoff]

    return trimmed


def _parse_row_dt(dt_value) -> datetime.datetime:  # type: ignore[no-untyped-def]
    """Parse the ``datetime`` field from a row dict into an aware datetime.

    Handles ISO strings (with or without ``T``), ``pd.Timestamp``, and native
    ``datetime.datetime`` objects.  Returns ``datetime.min`` (UTC) for
    unparseable values so they sort to the front and get trimmed.
    """
    if dt_value is None:
        return datetime.datetime.min.replace(tzinfo=UTC)
    if isinstance(dt_value, datetime.datetime):
        if dt_value.tzinfo is None:
            return dt_value.replace(tzinfo=UTC)
        return dt_value
    if isinstance(dt_value, pd.Timestamp):
        dt = dt_value.to_pydatetime()
        if dt.tzinfo is None:
            return dt.replace(tzinfo=UTC)
        return dt
    # string
    try:
        s = str(dt_value).replace("T", " ")
        dt = datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=UTC)
    except Exception:
        return datetime.datetime.min.replace(tzinfo=UTC)


# ---------------------------------------------------------------------------
# Public API – used by the endpoint
# ---------------------------------------------------------------------------

# Global registry of cache entries, keyed by (device_type, day).
_CACHES: dict[tuple[str, float], PollingCacheEntry] = {}
# A global lock used only for creating new cache entries (not for reads).
_REGISTRY_LOCK = asyncio.Lock()


async def get_or_create_entry(device_type: str, day: float) -> PollingCacheEntry:
    """Return the ``PollingCacheEntry`` for the given key, creating it if
    necessary in a thread-safe manner."""
    key = (device_type, day)
    if key in _CACHES:
        return _CACHES[key]

    async with _REGISTRY_LOCK:
        # Double-check after acquiring.
        if key not in _CACHES:
            _CACHES[key] = PollingCacheEntry()
        return _CACHES[key]


async def refresh_cache(
    entry: PollingCacheEntry,
    device_type: str,
    day: float,
    db_factory,
    obs_config: dict,
) -> None:
    """Ensure *entry* is up-to-date by incrementally fetching new rows from
    the database.

    Must be called **while holding ``entry.lock``**.

    If the entry is empty (cold start) a full query is executed.  Otherwise
    only rows newer than ``entry.last_db_datetime`` are fetched, processed
    and merged.

    A cooldown of ``_REFRESH_COOLDOWN`` seconds prevents excessive DB hits
    when many clients call in quick succession.
    """
    now = datetime.datetime.now(UTC)

    # Cooldown – skip if refreshed very recently
    if (
        entry.last_refresh is not None
        and (now - entry.last_refresh).total_seconds() < _REFRESH_COOLDOWN
        and entry.data  # must have data already
    ):
        return

    db = db_factory()
    try:
        if entry.data:
            # Incremental fetch
            df = _query_polling_df(
                db,
                device_type,
                since=entry.last_db_datetime,
                obs_config=obs_config,
            )
        else:
            # Cold start – full fetch
            df = _query_polling_df(db, device_type, day=day, obs_config=obs_config)
    finally:
        db.close()

    if df.empty and entry.data:
        # Nothing new – just update refresh timestamp
        entry.last_refresh = now
        return

    new_rows, new_latest = _process_df(df)
    new_rows = _to_json_safe(new_rows)
    new_latest = _to_json_safe(new_latest)

    if entry.data:
        entry.data = _merge_rows(entry.data, new_rows, day)
    else:
        entry.data = new_rows

    # Update latest (new query always has the freshest values)
    if new_latest:
        entry.latest = new_latest

    # Update bookmark
    if entry.data:
        entry.last_db_datetime = entry.data[-1].get("datetime")

    # Compute safety limits once
    if (
        entry.safety_limits is None
        and device_type == "ObservingConditions"
        and "ObservingConditions" in obs_config
    ):
        try:
            entry.safety_limits = _compute_safety_limits(obs_config)
        except Exception:
            logger.warning("Failed to compute safety limits", exc_info=True)

    entry.last_refresh = now


def slice_since(data: list[dict], since: str) -> list[dict]:
    """Return the subset of *data* rows whose ``datetime`` > *since*."""
    cutoff = _parse_row_dt(since)
    return [row for row in data if _parse_row_dt(row.get("datetime")) > cutoff]
