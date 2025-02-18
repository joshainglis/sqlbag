from datetime import datetime, timedelta, tzinfo
from typing import Any, Dict, Generator

import pendulum
from dateutil.relativedelta import relativedelta
from pendulum import DateTime, Date, Time
from psycopg2.extensions import AsIs, new_type, register_adapter, register_type

ZERO = timedelta(0)
HOUR = timedelta(hours=1)


# A UTC class.
class UTC(tzinfo):
    """UTC"""

    def utcoffset(self, dt: datetime) -> timedelta:
        return ZERO

    def tzname(self, dt: datetime) -> str:
        return "UTC"

    def dst(self, dt: datetime) -> timedelta:
        return ZERO


utc = UTC()


def vanilla(pendulum_dt: DateTime) -> datetime:
    """Converts a Pendulum DateTime to a standard library datetime in UTC."""
    x = pendulum_dt.in_timezone("UTC")
    return datetime(
        x.year, x.month, x.day, x.hour, x.minute, x.second, x.microsecond, tzinfo=utc
    )


def naive(pendulum_dt: DateTime) -> datetime:
    """Converts a Pendulum DateTime to a naive datetime (timezone-unaware)."""
    return pendulum_dt.naive()


def utcnow() -> DateTime:
    """Returns the current time in UTC as a Pendulum DateTime."""
    return pendulum.now("UTC")


def localnow() -> DateTime:
    """Returns the current local time as a Pendulum DateTime."""
    return pendulum.now()


def parse_time_of_day(x: str) -> Time:
    """Parses a string into a Pendulum Time object."""
    return pendulum.parse(x).time()


def combine_date_and_time(date: Date, time: Time, timezone: str = "UTC") -> DateTime:
    """Combines a date and time into a Pendulum DateTime with the specified timezone."""
    naive_dt = datetime.combine(date, time)
    return pendulum.instance(naive_dt, tz=timezone)


OID_TIMESTAMP = 1114
OID_TIMESTAMPTZ = 1184
OID_DATE = 1082
OID_TIME = 1083
OID_INTERVAL = 1186


def tokens_iter(s: str) -> Generator[Dict[str, Any], None, None]:
    """
    Iterates through tokens in a PostgreSQL interval string.  Handles times
    and year/month/day values.
    """
    tokens = s.split()

    while tokens:
        if ":" in tokens[0]:
            x, tokens = tokens[0], tokens[1:]
            t = pendulum.parse(x, strict=False).time()

            yield {
                "hours": int(x.startswith("-")) * -t.hour or t.hour,
                "minutes": t.minute,
                "seconds": t.second,
                "microseconds": t.microsecond,
            }
        else:
            x, tokens = tokens[:2], tokens[2:]
            x[1] = x[1].replace("mons", "months")
            yield {x[1]: int(x[0])}


def parse_interval_values(s: str) -> Dict[str, int]:
    """Parses a PostgreSQL interval string into a dictionary."""
    values: Dict[str, int] = {}
    for value_dict in tokens_iter(s):
        values.update(value_dict)

    for k in list(values.keys()):
        if not k.endswith("s"):
            values[k + "s"] = values.pop(k)
    return values


def format_relativedelta(rd: relativedelta) -> str:
    """Formats a relativedelta object as a PostgreSQL interval string."""
    RELATIVEDELTA_FIELDS = [
        "years",
        "months",
        "days",
        "hours",
        "minutes",
        "seconds",
        "microseconds",
    ]

    fields = [(k, getattr(rd, k)) for k in RELATIVEDELTA_FIELDS if getattr(rd, k)]
    return " ".join(f"{v} {k}" for k, v in fields)


class sqlbagrelativedelta(relativedelta):
    """Custom relativedelta class with string representation for SQL."""

    def __str__(self) -> str:
        return format_relativedelta(self)

    def __repr__(self) -> str:
        return f"sqlbagrelativedelta({format_relativedelta(self)})"


def cast_timestamp(value: str, cur: Any) -> datetime | None:
    """Casts a PostgreSQL timestamp string to a naive datetime."""
    if value is None:
        return None
    return pendulum.parse(value).naive()


def cast_timestamptz(value: str, cur: Any) -> DateTime | None:
    """Casts a PostgreSQL timestamptz string to a Pendulum DateTime in UTC."""
    if value is None:
        return None
    return pendulum.parse(value).in_timezone("UTC")


def cast_time(value: str, cur: Any) -> Time | None:
    """Casts a PostgreSQL time string to a Pendulum Time."""
    if value is None:
        return None
    return pendulum.parse(value).time()


def cast_date(value: str, cur: Any) -> Date | None:
    """Casts a PostgreSQL date string to a Pendulum Date."""
    if value is None:
        return None
    return pendulum.parse(value).date()


def cast_interval(value: str, cur: Any) -> sqlbagrelativedelta | None:
    """Casts a PostgreSQL interval string to a sqlbagrelativedelta."""
    if value is None:
        return None
    values = parse_interval_values(value)
    return sqlbagrelativedelta(**values)


def adapt_datetime(dt: datetime) -> AsIs:
    """Adapts a datetime object for PostgreSQL."""
    # Handle both Pendulum DateTime and standard library datetime
    if isinstance(dt, DateTime):
        in_utc = dt.in_timezone("UTC")
    else:
        # Assume naive datetime is in UTC if no timezone is specified.
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=utc)
        in_utc = pendulum.instance(dt).in_timezone("UTC")
    return AsIs(f"'{in_utc.isoformat()}'")


def adapt_relativedelta(rd: relativedelta) -> AsIs:
    """Adapts a relativedelta object for PostgreSQL."""
    return AsIs(f"'{format_relativedelta(rd)}'")


def register_cast(oid: int, typename: str, method: Any) -> None:
    """Registers a type cast with psycopg2."""
    new_t = new_type((oid,), typename, method)
    register_type(new_t)


def use_pendulum_for_time_types() -> None:
    register_cast(OID_TIMESTAMP, "TIMESTAMP", cast_timestamp)
    register_cast(OID_TIMESTAMPTZ, "TIMESTAMPTZ", cast_timestamptz)
    register_cast(OID_DATE, "DATE", cast_date)
    register_cast(OID_TIME, "TIME", cast_time)
    register_cast(OID_INTERVAL, "INTERVAL", cast_interval)

    register_adapter(datetime, adapt_datetime)
    register_adapter(DateTime, adapt_datetime)
    register_adapter(relativedelta, adapt_relativedelta)
    register_adapter(sqlbagrelativedelta, adapt_relativedelta)
