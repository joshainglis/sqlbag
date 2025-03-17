import pytest
from sqlbag import temporary_database


def pytest_addoption(parser):
    parser.addoption(
        "--test-postgresql",
        action="store_true",
        default=True,
        help="Run PostgreSQL tests",
    )
    parser.addoption(
        "--test-mysql", action="store_true", default=False, help="Run MySQL tests"
    )


@pytest.fixture(scope="module")
def db():
    with temporary_database("postgresql") as dburi:
        yield dburi


@pytest.fixture(scope="module")
def mysqldb():
    with temporary_database("mysql") as dburi:
        yield dburi
