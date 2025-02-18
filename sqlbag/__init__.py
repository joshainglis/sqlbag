"""sqlbag is a bunch of handy SQL things.

This is a whole bunch of useful boilerplate and helper methods, for working
with SQL databases, particularly PostgreSQL.

"""

from .createdrop import (
    database_exists,
    create_database,
    drop_database,
    temporary_database,
    can_select,
)
from .misc import (
    quoted_identifier,
    load_sql_from_folder,
    load_sql_from_file,
    sql_from_file,
    sql_from_folder,
    sql_from_folder_iter,
)
from .sqla import (
    S,
    raw_execute,
    admin_db_connection,
    _killquery,
    kill_other_connections,
    session,
    DB_ERROR_TUPLE,
    raw_connection,
    get_raw_autocommit_connection,
    copy_url,
    alter_url,
    connection_from_s_or_c,
    C,
)
from .sqla_orm import (
    row2dict,
    Base,
    metadata_from_session,
    sqlachanges,
    get_properties,
)

try:
    from . import pg
except ImportError:
    pg = None

__all__ = [
    "Base",
    "C",
    "DB_ERROR_TUPLE",
    "S",
    "_killquery",
    "admin_db_connection",
    "alter_url",
    "can_select",
    "connection_from_s_or_c",
    "copy_url",
    "create_database",
    "database_exists",
    "drop_database",
    "get_properties",
    "get_raw_autocommit_connection",
    "kill_other_connections",
    "load_sql_from_file",
    "load_sql_from_folder",
    "metadata_from_session",
    "pg",
    "quoted_identifier",
    "raw_connection",
    "raw_execute",
    "row2dict",
    "session",
    "sql_from_file",
    "sql_from_folder",
    "sql_from_folder_iter",
    "sqlachanges",
    "temporary_database",
]
