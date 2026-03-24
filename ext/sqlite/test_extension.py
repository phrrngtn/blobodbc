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

registered = set(
    row[0]
    for row in conn.execute("SELECT name FROM pragma_function_list").fetchall()
)

missing = [f for f in EXPECTED_FUNCTIONS if f not in registered]
if missing:
    raise AssertionError(f"Missing functions: {missing}")

print(f"OK: all {len(EXPECTED_FUNCTIONS)} expected functions present in pragma_function_list")
