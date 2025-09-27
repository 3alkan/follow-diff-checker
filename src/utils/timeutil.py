from __future__ import annotations
"""Time utilities with preference for Türkiye (Europe/Istanbul) timezone."""

import datetime
import time
from typing import Optional, Union

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


def get_istanbul_tz() -> Optional[datetime.tzinfo]:
    """Return Europe/Istanbul tzinfo if available, else None.

    On systems without IANA tz database, returns None (callers should fallback to localtime).
    """
    if ZoneInfo is None:
        return None
    try:
        return ZoneInfo("Europe/Istanbul")
    except Exception:
        return None


def tr_time_tuple(secs: float) -> time.struct_time:
    """Return time tuple in Türkiye timezone for logging formatter."""
    tz = get_istanbul_tz()
    if tz is not None:
        return datetime.datetime.fromtimestamp(secs, tz).timetuple()
    return time.localtime(secs)


def format_ts_tr(ts: Union[int, float, str, None], fmt: str = "%d.%m.%Y %H:%M:%S") -> str:
    """Format a Unix timestamp into Türkiye local time (Europe/Istanbul) string.

    Accepts int/float/str; returns empty string if not parseable.
    """
    if ts is None:
        return ""
    try:
        if isinstance(ts, str):
            ts = int(float(ts)) if ts.strip() else 0
        elif isinstance(ts, float):
            ts = int(ts)
        tz = get_istanbul_tz()
        if tz is not None:
            dt = datetime.datetime.fromtimestamp(int(ts), tz)
        else:
            dt = datetime.datetime.fromtimestamp(int(ts))
        return dt.strftime(fmt)
    except Exception:
        return ""


def now_tr(fmt: str = "%d.%m.%Y %H:%M:%S") -> str:
    """Current time formatted for Türkiye timezone."""
    ts = int(datetime.datetime.now().timestamp())
    return format_ts_tr(ts, fmt)
