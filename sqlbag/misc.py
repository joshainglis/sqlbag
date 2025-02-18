import sys
from pathlib import Path
from typing import Generator, List, TextIO, Tuple, Union

from sqlalchemy.orm import Session
from sqlalchemy.engine import Connection

from .sqla import raw_execute


def quoted_identifier(identifier: str) -> str:
    """One-liner to add double-quote marks around an SQL identifier
    (table name, view name, etc), and to escape double-quote marks.

    Args:
        identifier: the unquoted identifier
    """

    return '"{}"'.format(identifier.replace('"', '""'))


def sql_from_file(fpath: Union[str, Path]) -> str:
    """
    Args:
        fpath: The path to the file.

    Returns:
        The file contents as a string, stripped of leading/trailing whitespace.

    Reads a SQL file and returns its content.
    """
    try:
        with open(str(fpath), encoding="utf-8") as f:  # Specify UTF-8 encoding
            return f.read().strip()
    except FileNotFoundError:
        raise FileNotFoundError(f"SQL file not found: {fpath}")
    except Exception as e:
        raise OSError(f"Error reading SQL file {fpath}: {e}")


def sql_from_folder_iter(
    fpath: Union[str, Path],
) -> Generator[Tuple[Path, str], None, None]:
    """
    Args:
        fpath: The path to the folder.
    Yields:
        Tuples of (file path, sql content) for each .sql file in the folder.

    Iterates through all .sql files in a folder (and subfolders).
    """
    folder = Path(fpath)
    if not folder.is_dir():
        raise ValueError(f"Path is not a directory: {fpath}")

    sql_files = sorted(folder.glob("**/*.sql"))

    for sql_file in sql_files:
        try:
            sql = sql_from_file(sql_file)
            if sql:  # Only yield if the file contains SQL
                yield sql_file, sql
        except Exception as e:
            print(f"Error processing {sql_file}: {e}", file=sys.stderr)
            raise


def sql_from_folder(fpath: Union[str, Path]) -> List[str]:
    """
    Args:
        fpath: The path to the folder.

    Returns:
       A list of SQL strings from all .sql files in the folder.
    """
    return [sql for _, sql in sql_from_folder_iter(fpath)]


def load_sql_from_folder(
    s_or_c: Union[Session, Connection],
    fpath: Union[str, Path],
    verbose: bool = False,
    out: TextIO = None,
) -> None:
    """
    Args:
        s_or_c: SQLAlchemy Session or Connection.
        fpath: The path to the folder.
        verbose: Prints information as it loads files.
        out: Output stream for verbose messages (defaults to sys.stdout).

    Executes SQL from all .sql files in a folder.
    """

    if verbose:
        out = out or sys.stdout  # Use sys.stdout if 'out' is None
        out.write(f"Running all .sql files in: {fpath}\n")

    for file_path, text in sql_from_folder_iter(fpath):
        if verbose:
            out.write(f"    Running SQL in: {file_path}\n")
        try:
            raw_execute(s_or_c, text)
        except Exception as e:
            print(f"Error executing SQL from {file_path}: {e}", file=sys.stderr)
            raise


def load_sql_from_file(
    s_or_c: Union[Session, Connection], fpath: Union[str, Path]
) -> str:
    """
    Args:
        s_or_c: SQLAlchemy Session or Connection.
        fpath: The path to the file.
    Returns:
        The SQL that was executed.

    Executes SQL from a single file.
    """
    try:
        text = sql_from_file(fpath)
        if text:
            raw_execute(s_or_c, text)
        return text
    except Exception as e:
        print(f"Error executing SQL from {fpath}: {e}", file=sys.stderr)
        raise
