from datetime import datetime, timedelta

import pendulum
from dateutil.relativedelta import relativedelta
from sqlalchemy import text

from sqlbag import S, raw_connection
from sqlbag.pg import (
    use_pendulum_for_time_types,
)
from sqlbag.pg.datetimes import (
    UTC,
    ZERO,
    combine_date_and_time,
    localnow,
    naive,
    parse_interval_values,
    parse_time_of_day,
    sqlbagrelativedelta,
    utcnow,
    vanilla,
)

USERNAME = "testonly_sqlbag_user"
PW = "duck"

HOUR = timedelta(hours=1)


def test_parse_interval():
    TEST_CASES = [
        "1 years 2 mons",
        "3 days 04:05:06",
        "-1 year -2 mons +3 days -04:05:06.2",
        "1 day",
    ]

    ANSWERS = [
        dict(years=1, months=2),
        dict(days=3, hours=4, minutes=5, seconds=6, microseconds=0),
        dict(
            years=-1,
            months=-2,
            days=3,
            hours=-4,
            minutes=5,
            seconds=6,
            microseconds=200000,
        ),
        dict(days=1),
    ]

    for case, answer in zip(TEST_CASES, ANSWERS):
        assert parse_interval_values(case) == answer


def test_datetime_primitives():
    dt = datetime.now()

    utc = UTC()
    assert utc.utcoffset(dt) == ZERO
    assert utc.utcoffset(None) == ZERO

    assert utc.tzname(dt) == "UTC"

    assert utc.dst(dt) == ZERO
    assert utc.dst(None) == ZERO

    p = pendulum.instance(dt)
    n = naive(p)
    assert n == dt
    assert type(n) is type(p)  # use pendulum naive type

    p2 = utcnow()

    assert p2.tz == p2.in_timezone("UTC").tz

    p3 = localnow()

    v = vanilla(p3)
    assert pendulum.instance(v) == p3

    tod = parse_time_of_day("2015-01-01 12:34:56")
    assert str(tod) == "12:34:56"

    d = pendulum.Date(2017, 1, 1)
    dt = combine_date_and_time(d, tod)
    assert str(dt) == "2017-01-01 12:34:56+00:00"

    sbrd = sqlbagrelativedelta(days=5, weeks=6, months=7)
    assert str(sbrd) == "7 months 47 days"


def test_pendulum_for_time_types(db):
    t = pendulum.parse("2017-12-31 23:34:45", tz="Australia/Melbourne")
    i = relativedelta(days=1, seconds=200, microseconds=99)

    with S(db) as s:
        c = raw_connection(s)
        cu = c.cursor()

        cu.execute(
            """
            select
                null::timestamp,
                null::timestamptz,
                null::date,
                null::time,
                null::interval
        """
        )

        use_pendulum_for_time_types()

        s.execute(
            text(
                """
            CREATE TEMPORARY TABLE dt(
                ts TIMESTAMP,
                tstz TIMESTAMPTZ,
                d DATE,
                ti TIME,
                i INTERVAL)
        """
            )
        )

        s.execute(
            text(
                """
            INSERT INTO dt(ts, tstz, d, ti, i)
            VALUES
            (:ts,
            :tstz,
            :d,
            :ti,
            :i)
        """
            ),
            {
                "ts": vanilla(t),
                "tstz": t.in_timezone("Australia/Sydney"),
                "d": t.date(),
                "ti": t.time(),
                "i": i,
            },
        )

        out = list(s.execute(text("""SELECT * FROM dt""")))[0]

        assert out.ts == naive(t.in_tz("UTC"))
        assert out.tstz == t.in_timezone("UTC")
        assert out.d == t.date()
        assert out.ti == t.time()
        assert out.i == i

        result = s.execute(
            text(
                """
            select
                null::timestamp,
                null::timestamptz,
                null::date,
                null::time,
                null::interval
        """
            )
        )

        out = list(result)[0]
        assert list(out) == [None, None, None, None, None]
