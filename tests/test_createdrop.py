from sqlalchemy import text

from sqlbag import (
    S,
    create_database,
    database_exists,
    drop_database,
    temporary_database,
)
from sqlbag.createdrop import build_url


def exists(db_url):
    e = database_exists(db_url)
    e2 = database_exists(db_url, test_can_select=True)
    assert e == e2
    return e


def test_createdrop(tmpdir, request):
    # Get parameters from pytest configuration or environment
    # Default to testing all databases if not specified
    test_postgresql = request.config.getoption("--test-postgresql", default=True)
    test_mysql = request.config.getoption("--test-mysql", default=False)

    sqlite_path = str(tmpdir / "testonly.db")

    urls = []

    if test_postgresql:
        urls.append(build_url("postgresql", database="sqlbag_testonly"))

    if test_mysql:
        urls.append(build_url("mysql", database="sqlbag_testonly"))

    for db_url in urls:
        drop_database(db_url)
        assert not drop_database(db_url)
        assert not exists(db_url)
        assert create_database(db_url)
        assert exists(db_url)

        if db_url.startswith("postgres"):
            assert create_database(db_url, template="template1", wipe_if_existing=True)
        else:
            assert create_database(db_url, wipe_if_existing=True)
        assert exists(db_url)
        assert drop_database(db_url)
        assert not exists(db_url)

    db_url = "sqlite://"  # in-memory special case

    assert exists(db_url)
    assert not create_database(db_url)
    assert exists(db_url)
    assert not drop_database(db_url)
    assert exists(db_url)

    db_url = "sqlite:///" + sqlite_path

    assert not database_exists(db_url)
    # selecting works because sqlite auto-creates
    assert database_exists(db_url, test_can_select=True)
    drop_database(db_url)
    create_database(db_url)
    assert exists(db_url)

    drop_database(db_url)
    assert not database_exists(db_url)
    assert database_exists(db_url, test_can_select=True)

    with temporary_database("sqlite") as dburi:
        with S(dburi) as s:
            s.execute(text("select 1"))
