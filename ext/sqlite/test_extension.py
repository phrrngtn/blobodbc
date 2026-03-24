"""Smoke test: load the blobodbc SQLite extension and verify functions exist."""
import sqlite3
from blobodbc_sqlite import extension_path

EXPECTED_FUNCTIONS = [
    "bo_query",
    "bo_clob",
    "bo_driver_info",
    "bo_tables",
    "bo_columns",
    "bo_execute",
    "bo_primary_keys",
    "bo_foreign_keys",
]

conn = sqlite3.connect(":memory:")
conn.enable_load_extension(True)
conn.load_extension(extension_path())

for func in EXPECTED_FUNCTIONS:
    try:
        conn.execute(f"SELECT {func}('dummy')")
    except sqlite3.OperationalError as e:
        if "no such function" in str(e).lower():
            raise AssertionError(f"Function {func} not registered") from e

print(f"OK: all {len(EXPECTED_FUNCTIONS)} expected functions present")
