#!/usr/bin/env python3
"""
Test bo_driver_info() via the DuckDB extension.

Usage:
    uv run --with duckdb python test/test_driver_info_duckdb.py [--pretty]

Requires the extension to be built first:
    cd build && cmake .. -DBUILD_DUCKDB_EXTENSION=ON && make
"""

import json
import os
import sys
try:
    import tomllib
except ImportError:
    import tomli as tomllib

import duckdb

EXTENSION_PATH = os.path.join(
    os.path.dirname(__file__), "..", "build", "duckdb", "blobodbc.duckdb_extension"
)


def _load_connections():
    """Load test connections from test/connections.toml, falling back to defaults."""
    config_path = os.path.join(os.path.dirname(__file__), "connections.toml")
    if os.path.exists(config_path):
        with open(config_path, "rb") as f:
            cfg = tomllib.load(f)
        return [(c["connection_string"], c["label"]) for c in cfg.get("connections", [])]
    # Fallback: PostgreSQL via DSN only (no credentials needed)
    return [("DSN=rule4_test;GSSEncMode=disable", "PostgreSQL (Unix socket)")]


CONNECTIONS = _load_connections()


def main():
    pretty = "--pretty" in sys.argv

    conn = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    conn.execute(f"LOAD '{EXTENSION_PATH}'")

    results = []
    for conn_str, label in CONNECTIONS:
        print(f"Probing: {label} ...", file=sys.stderr)
        try:
            raw = conn.execute(
                "SELECT bo_driver_info(?)", [conn_str]
            ).fetchone()[0]
            info = json.loads(raw)
            info["label"] = label
            results.append(info)

            gi = info["get_info"]
            print(
                f"  DBMS: {gi.get('SQL_DBMS_NAME', '?')} "
                f"{gi.get('SQL_DBMS_VER', '?')}",
                file=sys.stderr,
            )
            print(
                f"  Driver: {gi.get('SQL_DRIVER_NAME', '?')} "
                f"{gi.get('SQL_DRIVER_VER', '?')}",
                file=sys.stderr,
            )
            ti = info.get("type_info", [])
            print(f"  Types: {len(ti)} data types supported", file=sys.stderr)
        except Exception as e:
            results.append({"label": label, "error": str(e)})
            print(f"  ERROR: {e}", file=sys.stderr)

    indent = 2 if pretty else None
    print(json.dumps(results, indent=indent, default=str))


if __name__ == "__main__":
    main()
