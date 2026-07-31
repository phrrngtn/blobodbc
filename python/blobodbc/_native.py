"""ctypes binding to libblobodbc — the C ABI declared in include/blobodbc.h.

This replaces the nanobind extension module. The C ABI already exists and both
SQL hosts bind to it, so ctypes over the same symbols removes nanobind,
scikit-build-core, the Python development headers, and the wheel-per-CPython
matrix.

**Every `char *` returned here is heap memory the caller owns.** `take` copies
it into a Python `str` and frees it with `blobodbc_free`, so no pointer into the
C heap ever escapes. `blobodbc_errmsg` is the one exception — it is borrowed and
must not be freed.
"""

from __future__ import annotations

import ctypes
import pathlib

import blobzig

__all__ = ["lib", "Error", "library_path", "duckdb_extension_path",
           "sqlite_extension_path", "take", "errmsg"]

_PKG = pathlib.Path(__file__).resolve().parent
_artifacts = blobzig.Artifacts("blobodbc", package_dir=_PKG, repo_root=_PKG.parents[1])

Error = blobzig.Error


def library_path() -> str:
    """Path to libblobodbc, the shared library behind this module."""
    return _artifacts.library()


def duckdb_extension_path() -> str:
    return _artifacts.duckdb_extension()


def sqlite_extension_path() -> str:
    return _artifacts.sqlite_extension()


lib = _artifacts.load()

_S = ctypes.c_char_p
_P = ctypes.c_void_p     # owned char* — never c_char_p, see take()
_SS = ctypes.POINTER(ctypes.c_char_p)

# Single connection+query. All return owned strings.
for _n in ("blobodbc_query_json", "blobodbc_query_clob", "blobodbc_query_rows",
           "blobodbc_driver_info"):
    fn = getattr(lib, _n, None)
    if fn is not None:
        fn.argtypes = [_S] if _n == "blobodbc_driver_info" else [_S, _S]
        fn.restype = _P

# Positional bind parameters: (conn, query, char **values, int count)
for _n in ("blobodbc_query_json_params", "blobodbc_query_clob_params"):
    fn = getattr(lib, _n, None)
    if fn is not None:
        fn.argtypes = [_S, _S, _SS, ctypes.c_int]
        fn.restype = _P

# Named bind parameters, supplied as a JSON object.
for _n in ("blobodbc_query_json_named", "blobodbc_query_clob_named",
           "blobodbc_query_rows_named"):
    fn = getattr(lib, _n, None)
    if fn is not None:
        fn.argtypes = [_S, _S, _S]
        fn.restype = _P

# Catalog introspection: (conn, catalog, schema, table-or-type)
for _n in ("blobodbc_tables", "blobodbc_columns",
           "blobodbc_primary_keys", "blobodbc_foreign_keys"):
    fn = getattr(lib, _n, None)
    if fn is not None:
        fn.argtypes = [_S, _S, _S, _S]
        fn.restype = _P

lib.blobodbc_query_json_in_catalog.argtypes = [_S, _S, _S]
lib.blobodbc_query_json_in_catalog.restype = _P

lib.blobodbc_execute.argtypes = [_S, _S]
lib.blobodbc_execute.restype = ctypes.c_int

lib.blobodbc_free.argtypes = [_P]
lib.blobodbc_free.restype = None
lib.blobodbc_errmsg.argtypes = []
lib.blobodbc_errmsg.restype = _S   # borrowed — do NOT free


def errmsg() -> str:
    raw = lib.blobodbc_errmsg()
    return raw.decode("utf-8", "replace") if raw else "unknown ODBC error"


def take(ptr) -> str:
    """Copy an owned C string into Python and free it.

    A null pointer means the call failed, and the reason is in errmsg().
    """
    if not ptr:
        raise Error(errmsg())
    try:
        return ctypes.cast(ptr, ctypes.c_char_p).value.decode("utf-8", "replace")
    finally:
        lib.blobodbc_free(ptr)


def _enc(s):
    return s.encode("utf-8") if isinstance(s, str) else s
