"""blobodbc — ODBC query execution returning JSON.

Wraps nanodbc behind a narrow C ABI. Any ODBC-accessible database — SQL Server,
PostgreSQL, MySQL, Oracle — can be queried and the rows come back as JSON.

    >>> import blobodbc
    >>> blobodbc.query_json("DSN=mydsn", "SELECT 1 AS n")
    '[{"n":1}]'

Returning JSON rather than a bound result set is deliberate: it crosses the
language boundary without either side needing the other's type system, which is
the same choice the rest of the blob* family makes.
"""

from __future__ import annotations

import ctypes as _c
import json as _json

from ._native import (
    Error,
    duckdb_extension_path,
    errmsg,
    lib,
    library_path,
    sqlite_extension_path,
    take,
    _enc,
)

__all__ = [
    "query_json", "query_clob", "query_rows", "query", "execute",
    "driver_info", "tables", "columns", "primary_keys", "foreign_keys",
    "query_in_catalog",
    "Error", "errmsg", "library_path",
    "duckdb_extension_path", "sqlite_extension_path",
]


def _bind_array(params):
    """Build the `char **` a positional-bind call expects."""
    vals = [None if p is None else _enc(str(p)) for p in params]
    return (_c.c_char_p * len(vals))(*vals), len(vals)


def query_json(conn_str: str, query: str, params=None) -> str:
    """Run a query, returning a JSON array of objects.

    `params` may be a sequence (positional binds) or a dict (named binds).
    Always bind rather than interpolating — the C API takes parameters for a
    reason.
    """
    if params is None:
        return take(lib.blobodbc_query_json(_enc(conn_str), _enc(query)))
    if isinstance(params, dict):
        return take(lib.blobodbc_query_json_named(
            _enc(conn_str), _enc(query), _enc(_json.dumps(params))))
    arr, n = _bind_array(params)
    return take(lib.blobodbc_query_json_params(_enc(conn_str), _enc(query), arr, n))


def query(conn_str: str, sql: str, params=None):
    """As query_json, but decoded into Python objects."""
    return _json.loads(query_json(conn_str, sql, params))


def query_clob(conn_str: str, sql: str, params=None) -> str:
    """Run a query returning a single text value rather than a row set."""
    if params is None:
        return take(lib.blobodbc_query_clob(_enc(conn_str), _enc(sql)))
    if isinstance(params, dict):
        return take(lib.blobodbc_query_clob_named(
            _enc(conn_str), _enc(sql), _enc(_json.dumps(params))))
    arr, n = _bind_array(params)
    return take(lib.blobodbc_query_clob_params(_enc(conn_str), _enc(sql), arr, n))


def query_rows(conn_str: str, sql: str, params=None) -> str:
    """Row-oriented JSON (arrays rather than objects) — smaller for wide results."""
    if isinstance(params, dict):
        return take(lib.blobodbc_query_rows_named(
            _enc(conn_str), _enc(sql), _enc(_json.dumps(params))))
    return take(lib.blobodbc_query_rows(_enc(conn_str), _enc(sql)))


def execute(conn_str: str, sql: str) -> int:
    """Run a statement for effect. Returns the affected row count, or raises."""
    rc = lib.blobodbc_execute(_enc(conn_str), _enc(sql))
    if rc < 0:
        raise Error(errmsg())
    return rc


def driver_info(conn_str: str) -> str:
    return take(lib.blobodbc_driver_info(_enc(conn_str)))


def query_in_catalog(conn_str: str, catalog: str, sql: str) -> str:
    """Run a query with the connection switched to `catalog` first."""
    return take(lib.blobodbc_query_json_in_catalog(
        _enc(conn_str), _enc(catalog), _enc(sql)))


def _catalog_call(fn, conn_str, catalog, schema, last):
    return take(fn(_enc(conn_str), _enc(catalog), _enc(schema), _enc(last)))


def tables(conn_str: str, catalog: str = "", schema: str = "", type: str = "") -> str:
    return _catalog_call(lib.blobodbc_tables, conn_str, catalog, schema, type)


def columns(conn_str: str, catalog: str = "", schema: str = "", table: str = "") -> str:
    return _catalog_call(lib.blobodbc_columns, conn_str, catalog, schema, table)


def primary_keys(conn_str: str, catalog: str = "", schema: str = "", table: str = "") -> str:
    return _catalog_call(lib.blobodbc_primary_keys, conn_str, catalog, schema, table)


def foreign_keys(conn_str: str, catalog: str = "", schema: str = "", table: str = "") -> str:
    return _catalog_call(lib.blobodbc_foreign_keys, conn_str, catalog, schema, table)
