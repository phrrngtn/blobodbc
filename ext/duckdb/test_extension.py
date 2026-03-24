"""Smoke test: load the blobodbc DuckDB extension and verify functions exist."""
import duckdb
from blobodbc_duckdb import extension_path

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

conn = duckdb.connect(config={"allow_unsigned_extensions": "true"})
conn.execute(f"LOAD '{extension_path()}'")

registered = set(
    row[0]
    for row in conn.execute(
        "SELECT DISTINCT function_name FROM duckdb_functions() WHERE function_name LIKE 'bo_%'"
    ).fetchall()
)

missing = [f for f in EXPECTED_FUNCTIONS if f not in registered]
if missing:
    raise AssertionError(f"Missing functions: {missing}")

print(f"OK: {len(registered)} bo_* functions registered, all {len(EXPECTED_FUNCTIONS)} expected functions present")
