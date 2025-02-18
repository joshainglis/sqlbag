# sqlbag: various sql boilerplate

This is just a collection of handy code for doing database things.

## What is in the box

Connections, flask setup, SQLAlchemy ORM helpers, temporary database setup and
teardown (handy for integration tests).

## Installation

Simply install with [pip](https://pip.pypa.io):

```shell
pip install sqlbag
```

If you want you can install the database drivers you need at the same time, by
specifying one of the optional bundles.

If you're using postgres, this installs `sqlbag` and `psycopg2`:

```shell
pip install sqlbag[pg]
```

For MariaDB support:

```shell
pip install sqlbag[maria]
```
