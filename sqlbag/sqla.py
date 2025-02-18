"""Miscellaneous helpful stuff for working with the SQLAlchemy core."""

import copy
import getpass
from contextlib import contextmanager
from typing import Any, Generator, Union

import sqlalchemy
from packaging import version
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.engine.url import make_url, URL
from sqlalchemy.exc import OperationalError, ProgrammingError, InternalError
from sqlalchemy.orm import scoped_session, sessionmaker, Session
from sqlalchemy.pool import NullPool

from .util_mysql import MYSQL_KILLQUERY_FORMAT as MYSQL_KILL
from .util_pg import PSQL_KILLQUERY_FORMAT_INCLUDING_DROPPED as PG_KILL

DB_ERROR_TUPLE = (
    OperationalError,
    InternalError,
    ProgrammingError,
)

SQLA14 = version.parse(sqlalchemy.__version__) >= version.parse("1.4.0b1")


try:
    import secrets

    scopefunc = secrets.token_hex
except ImportError:
    import random

    scopefunc = random.random


def copy_url(db_url: Union[str, URL]) -> URL:
    """
    Args:
        db_url: Already existing SQLAlchemy :class:`URL`, or URL string.
    Returns:
        A brand new SQLAlchemy :class:`URL`.

    Make a copy of a SQLAlchemy :class:`URL`.
    """
    return copy.copy(make_url(db_url))


def alter_url(db_url: Union[str, URL], **kwargs: Any) -> URL:
    """
    Args:
        db_url: Already existing SQLAlchemy :class:`URL`, or URL string.
        **kwargs: Attributes to modify
    Returns:
        A brand new SQLAlchemy :class:`URL`.

    Return a copy of a SQLALchemy :class:`URL` with some modifications.
    """
    db_url = make_url(db_url)
    if SQLA14:
        return db_url.set(**kwargs)
    else:
        new_url = copy.copy(db_url)
        for k, v in kwargs.items():
            setattr(new_url, k, v)
        return new_url


def connection_from_s_or_c(s_or_c: Union[Session, Connection]) -> Connection:
    """
    Args:
        s_or_c: Either an SQLAlchemy ORM :class:`Session`, or a core :class:`Connection`.

    Returns:
        Connection: An SQLAlchemy Core connection.

    Handles both Sessions and Connections.
    """
    if isinstance(s_or_c, Session):
        return s_or_c.connection()
    elif isinstance(s_or_c, Connection):
        return s_or_c
    else:
        raise TypeError("Expected Session or Connection, got: {}".format(type(s_or_c)))


def get_raw_autocommit_connection(
    url_or_engine_or_connection: Union[str, Engine, Connection],
) -> Any:
    """
    Args:
        url_or_engine_or_connection: A URL string, SQLAlchemy Engine, or DBAPI connection.

    Returns:
        A DBAPI connection in autocommit mode.
    """
    if isinstance(url_or_engine_or_connection, str):
        url = make_url(url_or_engine_or_connection)
        if url.drivername.startswith("postgresql"):
            import psycopg2

            conn = psycopg2.connect(url_or_engine_or_connection)
        elif url.drivername.startswith("mysql"):
            import pymysql

            conn = pymysql.connect(
                host=url.host,
                user=url.username,
                password=url.password,
                database=url.database,
                port=url.port or 3306,
            )  # default port if none provided
        else:
            raise NotImplementedError(
                "Only PostgreSQL and MySQL are supported with string URLs"
            )
        conn.autocommit = True
        return conn

    elif isinstance(url_or_engine_or_connection, Engine):
        with url_or_engine_or_connection.connect() as sqla_connection:
            sqla_connection = sqla_connection.execution_options(
                isolation_level="AUTOCOMMIT"
            )
            sqla_connection.detach()
            return sqla_connection.connection.dbapi_connection

    elif hasattr(url_or_engine_or_connection, "protocol_version"):
        url_or_engine_or_connection.autocommit = True
        return url_or_engine_or_connection
    else:
        raise ValueError("Must pass a URL, Engine, or DBAPI connection object")


def session(*args: Any, **kwargs: Any) -> Session:
    """
    Args:
        *args: Positional arguments for create_engine.
        **kwargs: Keyword arguments for create_engine.

    Returns:
        Session: A new SQLAlchemy :class:`Session`.

    Creates a database session.  Uses a simple sessionmaker, *not* a scoped_session.
    """
    engine = create_engine(*args, **kwargs)
    Session = sessionmaker(bind=engine)
    return Session()


@contextmanager
def S(*args: Any, **kwargs: Any) -> Generator[Session, None, None]:
    """Context manager for creating and using sessions.

    with S('postgresql:///databasename') as s:
        s.execute(text('select 1;'))

    Commits on close, rolls back on exception.
    """
    engine = create_engine(*args, **kwargs)
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
        engine.dispose()


def get_scoped_session_maker(*args: Any, **kwargs: Any) -> scoped_session:
    """Creates a scoped session maker."""
    engine = create_engine(*args, **kwargs)
    return scoped_session(sessionmaker(bind=engine), scopefunc=scopefunc)


def raw_connection(s_or_c_or_rawc: Union[Session, Connection, Any]) -> Any:
    """
    Args:
        s_or_c_or_rawc: SQLAlchemy Session, Connection, or DBAPI connection.

    Returns:
        connection: Raw DBAPI connection.
    """
    try:
        conn = connection_from_s_or_c(s_or_c_or_rawc).connection
        if hasattr(conn, "dbapi_connection"):
            return conn.dbapi_connection
    except TypeError:
        return s_or_c_or_rawc


def raw_execute(s_or_c: Union[Session, Connection], statements: str) -> None:
    """
    Args:
        s_or_c: SQLAlchemy Session or Connection
        statements: raw SQL string
    Executes a raw SQL statement using the underlying DBAPI connection.
    """
    raw_conn = raw_connection(s_or_c)
    with raw_conn.cursor() as cursor:
        cursor.execute(statements)


@contextmanager
def C(*args: Any, **kwargs: Any) -> Generator[Connection, None, None]:
    """
    Context manager for direct engine connections.

    with C('postgresql:///databasename') as c:
        c.execute(text('select 1;'))
    """
    engine = create_engine(*args, **kwargs)
    connection = engine.connect()
    transaction = connection.begin()
    try:
        yield connection
        transaction.commit()
    except Exception:
        transaction.rollback()
        raise
    finally:
        connection.close()
        engine.dispose()  # Clean up the engine


@contextmanager
def admin_db_connection(db_url: Union[str, URL]) -> Generator[Connection, None, None]:
    """Context manager for administrative database connections."""

    url = make_url(db_url)
    dbtype = url.get_dialect().name
    new_url = copy.copy(url)

    if dbtype == "postgresql":
        new_url = alter_url(new_url, database="postgres")
        if not new_url.username:
            new_url = alter_url(new_url, username=getpass.getuser())

    elif dbtype == "mysql":
        new_url = alter_url(new_url, database="")

    elif dbtype == "sqlite":
        pass
    else:
        raise NotImplementedError(f"Admin connections not supported for {dbtype}")

    engine = create_engine(new_url, poolclass=NullPool)
    if dbtype == "postgresql":
        with engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as connection:
            yield connection
    elif dbtype == "mysql":
        with engine.connect() as connection:
            connection.execute(text("SET sql_mode = 'ANSI';"))
            yield connection
    else:  # sqlite
        with engine.connect() as connection:
            yield connection
    engine.dispose()


def _killquery(dbtype: str, dbname: str = None, hardkill: bool = False) -> str:
    where = []

    if dbtype == "postgresql":
        sql = PG_KILL
        if not hardkill:
            where.append("psa.state = 'idle'")
        if dbname:
            where.append("datname = :databasename")
    elif dbtype == "mysql":
        sql = MYSQL_KILL
        if not hardkill:
            where.append("COMMAND = 'Sleep'")
        if dbname:
            where.append("DB = :databasename")
    else:
        raise NotImplementedError

    where_clause = " and ".join(where)
    if where_clause:
        sql += " and {}".format(where_clause)
    return sql


def kill_other_connections(
    s_or_c: Union[Session, Connection], dbname: str = None, hardkill: bool = False
) -> None:
    """Kills other connections to a database (or entire server)."""

    c = connection_from_s_or_c(s_or_c)
    dbtype = c.engine.dialect.name
    killquery = _killquery(dbtype, dbname=dbname, hardkill=hardkill)

    if dbtype == "mysql":
        result = c.execute(text(killquery), {"databasename": dbname} if dbname else {})
        for row in result:
            try:
                c.execute(text(f"kill connection {row.process_id}"))
            except DB_ERROR_TUPLE as e:
                if isinstance(e.orig, Exception) and "Unknown thread id" in str(e.orig):
                    pass
                else:
                    raise
    elif dbtype == "postgresql":
        c.execute(text(killquery), {"databasename": dbname} if dbname else {})
    else:
        raise NotImplementedError(f"kill_other_connections not supported for {dbtype}")


def table_exists(
    session_or_connection: Union[Session, Connection],
    tablename: str,
    schemaname: str = None,
) -> bool:
    """Checks if a table exists."""
    c = connection_from_s_or_c(session_or_connection)
    return c.engine.dialect.has_table(c, tablename, schema=schemaname)


def get_dbtype(session_or_connection: Union[Session, Connection]) -> str:
    """Gets the database type (e.g., 'postgresql', 'mysql')."""
    c = connection_from_s_or_c(session_or_connection)
    return c.engine.dialect.name


def sql_to_print(sql: str, params: Any = None) -> str:
    """Formats SQL for printing (for debugging)."""
    if params:
        if isinstance(params, (list, tuple)):
            result = sql.format(*params)
        elif isinstance(params, dict):
            result = sql.format(**params)
        else:
            result = sql
    else:
        result = sql
    return result


def execute_sql(
    session_or_connection: Union[Session, Connection],
    sql: str,
    params: Any = None,
    dryrun: bool = False,
    quiet: bool = False,
) -> Any:
    if not quiet:
        sql_statement = sql_to_print(sql, params)
        print(sql_statement)

    if dryrun:
        return None

    c = connection_from_s_or_c(session_or_connection)

    try:
        if params:
            result = c.execute(text(sql), params)
        else:
            result = c.execute(text(sql))
        return result
    except DB_ERROR_TUPLE as e:
        print(e)
        raise


def execute_fetchall(
    session_or_connection: Union[Session, Connection],
    sql: str,
    params: Any = None,
    dryrun: bool = False,
    quiet: bool = False,
) -> list:
    """Executes SQL and fetches all results."""
    result = execute_sql(session_or_connection, sql, params, dryrun, quiet)
    return result.fetchall() if result else []


def execute_fetchone(
    session_or_connection: Union[Session, Connection],
    sql: str,
    params: Any = None,
    dryrun: bool = False,
    quiet: bool = False,
) -> Any:
    """Executes SQL and fetches one result."""
    result = execute_sql(session_or_connection, sql, params, dryrun, quiet)
    return result.fetchone() if result else None


def execute_returns_result(sql: str) -> bool:
    """Checks if a SQL statement is expected to return results."""
    lsql = sql.strip().lower()
    return any(lsql.startswith(x) for x in ["select", "explain", "returning", "pragma"])
