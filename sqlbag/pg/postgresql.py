from psycopg2 import errorcodes as pgerrorcodes


unicode = str


def errorcode_from_error(e):
    """
    Get the error code from a particular error/exception caused by PostgreSQL.
    """
    return e.orig.pgcode


def pg_errorname_lookup(pgcode):
    """
    Args:
        pgcode(int): A PostgreSQL error code.

    Returns:
        The error name from a PostgreSQL error code as per: https://www.postgresql.org/docs/9.5/static/errcodes-appendix.html
    """

    return pgerrorcodes.lookup(str(pgcode))
