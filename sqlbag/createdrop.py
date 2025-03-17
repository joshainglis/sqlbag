import getpass
import os
import random
import string
import tempfile
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.exc import InternalError, OperationalError, ProgrammingError

from .misc import quoted_identifier
from .sqla import (
    admin_db_connection,
    connection_from_s_or_c,
    make_url,
    kill_other_connections,
)


def database_exists(db_url, test_can_select=False):
    url = make_url(db_url)
    name = url.database
    db_type = url.get_dialect().name

    if not test_can_select:
        if db_type == "sqlite":
            return name is None or name == ":memory:" or os.path.exists(name)
        elif db_type in ["postgresql", "mysql"]:
            with admin_db_connection(url) as s:
                return _database_exists(s, name)
    return can_select(url)


def can_select(url):
    txt = "select 1"
    e = create_engine(url)

    try:
        with e.connect() as conn:
            conn.execute(text(txt))
        return True
    except (ProgrammingError, OperationalError, InternalError):
        return False
    finally:
        e.dispose()


def _database_exists(session_or_connection, name):
    c = connection_from_s_or_c(session_or_connection)
    url = make_url(c.engine.url)
    dbtype = url.get_dialect().name

    if dbtype == "postgresql":
        EXISTENCE = """
            SELECT 1
            FROM pg_catalog.pg_database
            WHERE datname = :dbname
        """
        # Use a tuple for the parameters.  This is the crucial fix.
        result = c.execute(text(EXISTENCE), {"dbname": name}).scalar()
        return bool(result)

    elif dbtype == "mysql":
        EXISTENCE = """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name = :dbname
        """
        # Use a tuple for the parameters.
        result = c.execute(text(EXISTENCE), {"dbname": name}).scalar()
        return bool(result)


def create_database(db_url, template=None, wipe_if_existing=False):
    target_url = make_url(db_url)
    dbtype = target_url.get_dialect().name

    if wipe_if_existing:
        drop_database(db_url)

    if database_exists(target_url):
        return False
    else:
        if dbtype == "sqlite":
            engine = create_engine(target_url)
            engine.connect().close()
            engine.dispose()
            return True

        with admin_db_connection(target_url) as c:
            if template:
                t = "template {}".format(quoted_identifier(template))
            else:
                t = ""

            c.execute(
                text(
                    """
                create database {} {};
            """.format(quoted_identifier(target_url.database), t)
                )
            )
        return True


def drop_database(db_url):
    url = make_url(db_url)

    dbtype = url.get_dialect().name
    name = url.database

    if database_exists(url):
        if dbtype == "sqlite":
            if name and name != ":memory:":
                try:
                    os.remove(name)
                except FileNotFoundError:
                    pass
                return True
            else:
                return False
        else:
            with admin_db_connection(url) as c:
                if dbtype == "postgresql":
                    REVOKE = "revoke connect on database {} from public"
                    revoke = REVOKE.format(quoted_identifier(name))
                    c.execute(text(revoke))

                kill_other_connections(c, name, hardkill=True)

                c.execute(
                    text(
                        """
                    drop database if exists {};
                """.format(quoted_identifier(name))
                    )
                )
            return True
    else:
        return False


def _current_username():
    return getpass.getuser()


def temporary_name(prefix="sqlbag_tmp_"):
    random_letters = [random.choice(string.ascii_lowercase) for _ in range(10)]
    rnd = "".join(random_letters)
    tempname = prefix + rnd
    return tempname


def build_url(
    dialect="postgresql",
    host=None,
    port=None,
    username=None,
    password=None,
    database=None,
):
    def userpass(u, p):
        if u and p:
            return f"{u}:{p}@"
        elif u:
            return f"{u}@"

    if dialect == "postgresql":
        host = host or os.getenv("PGHOST", "localhost")
        port = port or os.getenv("PGPORT", "5432")
        username = username or os.getenv("PGUSER") or _current_username() or "postgres"
        database = database or "postgres"
        if host.startswith("/"):
            url = f"postgresql://{userpass(username, password)}/{database}?host={host}&port={port}"
        else:
            url = f"postgresql://{userpass(username, password)}{host}:{port}/{database}"
    elif dialect == "mysql":
        host = host or os.getenv("MYSQL_UNIX_PORT", "localhost")
        port = port or "3306"
        username = username or "root"
        if host.startswith("/"):
            url = f"mysql+pymysql://{userpass(username, password)}/{database}?unix_socket={host}"
        else:
            url = f"mysql+pymysql://{userpass(username, password)}{host}:{port}/{database}"
    elif dialect == "sqlite":
        url = f"sqlite:///{database}"
    else:
        raise ValueError("Unsupported dialect: {}".format(dialect))
    return url


@contextmanager
def temporary_database(dialect="postgresql", do_not_delete=False):
    """
    Args:
        dialect(str): Type of database to create (either 'postgresql', 'mysql', or 'sqlite').
        do_not_delete: Do not delete the database as this method usually would.
        host: Manually specify the host

    Creates a temporary database for the duration of the context manager scope.  Cleans it up when finished unless
    do_not_delete is specified.

    PostgreSQL, MySQL/MariaDB (requires pymysql), and SQLite are supported.
    """
    if dialect == "sqlite":
        with tempfile.NamedTemporaryFile(
            delete=not do_not_delete, delete_on_close=False
        ) as tmp:
            db_name = tmp.name
            url = build_url(dialect=dialect, database=db_name)
            yield url

    else:
        url = build_url(dialect=dialect, database=temporary_name())
        try:
            create_database(url)
            yield url
        finally:
            if not do_not_delete:
                drop_database(url)
