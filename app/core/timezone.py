"""
Pakistan Standard Time (PKT) utility — UTC+5, no DST.
Use now_pkt() everywhere instead of datetime.utcnow().
"""
from datetime import datetime, timezone, timedelta

PKT = timezone(timedelta(hours=5))


def now_pkt() -> datetime:
    """Return the current Pakistan Standard Time as a naive datetime (for DB storage)."""
    return datetime.now(PKT).replace(tzinfo=None)
