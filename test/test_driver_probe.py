#!/usr/bin/env python3
"""
Probe ODBC driver capabilities via pyodbc.

Tests SQLGetInfo and SQLGetTypeInfo against each configured backend,
producing a JSON report of driver metadata per connection.

Usage:
    uv run --with pyodbc python test/test_driver_probe.py [--pretty]

Connections are defined in the CONNECTIONS list below. Adjust as needed
for your environment.
"""

import json
import sys

import pyodbc


# ── Connections to probe ──────────────────────────────────────────

CONNECTIONS = [
    (
        "DSN=rule4_test;GSSEncMode=disable",
        "PostgreSQL (Unix socket)",
    ),
    (
        "Driver={ODBC Driver 18 for SQL Server};"
        "Server=localhost,1433;"
        "Database=rule4_test;"
        "UID=rule4;PWD=R4Developer!2024;"
        "TrustServerCertificate=yes",
        "SQL Server (Docker/Rosetta)",
    ),
]


# ── SQLGetInfo info-type catalog ──────────────────────────────────
# (info_type_id, name, return_category)
# return_category: "s" = string, "i" = 32-bit integer/bitmask, "h" = 16-bit smallint

GET_INFO_CATALOG = [
    # Driver / DBMS identification
    (2,     "SQL_DATA_SOURCE_NAME",        "s"),
    (6,     "SQL_DRIVER_NAME",             "s"),
    (7,     "SQL_DRIVER_VER",              "s"),
    (77,    "SQL_DRIVER_ODBC_VER",         "s"),
    (17,    "SQL_DBMS_NAME",               "s"),
    (18,    "SQL_DBMS_VER",                "s"),
    (16,    "SQL_DATABASE_NAME",           "s"),
    (13,    "SQL_SERVER_NAME",             "s"),
    (47,    "SQL_USER_NAME",               "s"),

    # Catalog / schema / naming
    (41,    "SQL_CATALOG_NAME_SEPARATOR",  "s"),
    (42,    "SQL_CATALOG_TERM",            "s"),
    (39,    "SQL_SCHEMA_TERM",             "s"),
    (45,    "SQL_TABLE_TERM",              "s"),
    (55,    "SQL_PROCEDURE_TERM",          "s"),
    (29,    "SQL_IDENTIFIER_QUOTE_CHAR",   "s"),
    (14,    "SQL_SEARCH_PATTERN_ESCAPE",   "s"),
    (114,   "SQL_CATALOG_LOCATION",        "h"),
    (92,    "SQL_CATALOG_USAGE",           "i"),
    (91,    "SQL_SCHEMA_USAGE",            "i"),

    # Identifier case handling
    (84,    "SQL_IDENTIFIER_CASE",         "h"),
    (93,    "SQL_QUOTED_IDENTIFIER_CASE",  "h"),

    # NULL handling
    (85,    "SQL_NULL_COLLATION",          "h"),
    (22,    "SQL_CONCAT_NULL_BEHAVIOR",    "h"),
    (75,    "SQL_NON_NULLABLE_COLUMNS",    "h"),

    # SQL conformance
    (152,   "SQL_ODBC_INTERFACE_CONFORMANCE", "i"),
    (118,   "SQL_SQL_CONFORMANCE",         "i"),
    (73,    "SQL_INTEGRITY",               "s"),

    # SQL feature bitmasks
    (86,    "SQL_ALTER_TABLE",             "i"),
    (132,   "SQL_CREATE_TABLE",            "i"),
    (134,   "SQL_CREATE_VIEW",             "i"),
    (136,   "SQL_DROP_TABLE",              "i"),
    (137,   "SQL_DROP_VIEW",               "i"),
    (167,   "SQL_INSERT_STATEMENT",        "i"),
    (95,    "SQL_SUBQUERIES",              "i"),
    (96,    "SQL_UNION",                   "i"),
    (88,    "SQL_GROUP_BY",                "h"),
    (90,    "SQL_ORDER_BY_COLUMNS_IN_SELECT", "s"),
    (74,    "SQL_CORRELATION_NAME",        "h"),
    (115,   "SQL_OJ_CAPABILITIES",         "i"),

    # Scalar function bitmasks
    (50,    "SQL_STRING_FUNCTIONS",         "i"),
    (16,    "SQL_NUMERIC_FUNCTIONS",        "i"),
    (52,    "SQL_TIMEDATE_FUNCTIONS",       "i"),
    (76,    "SQL_SYSTEM_FUNCTIONS",         "i"),
    (48,    "SQL_CONVERT_FUNCTIONS",        "i"),
    (169,   "SQL_AGGREGATE_FUNCTIONS",      "i"),

    # SQL92 feature bitmasks
    (160,   "SQL_SQL92_PREDICATES",                  "i"),
    (161,   "SQL_SQL92_RELATIONAL_JOIN_OPERATORS",   "i"),
    (165,   "SQL_SQL92_VALUE_EXPRESSIONS",           "i"),
    (164,   "SQL_SQL92_STRING_FUNCTIONS",            "i"),
    (163,   "SQL_SQL92_NUMERIC_VALUE_FUNCTIONS",     "i"),
    (155,   "SQL_SQL92_DATETIME_FUNCTIONS",          "i"),

    # Transaction support
    (46,    "SQL_TXN_CAPABLE",             "h"),
    (26,    "SQL_DEFAULT_TXN_ISOLATION",   "i"),
    (23,    "SQL_CURSOR_COMMIT_BEHAVIOR",  "h"),
    (24,    "SQL_CURSOR_ROLLBACK_BEHAVIOR","h"),

    # Limits
    (34,    "SQL_MAX_CATALOG_NAME_LEN",    "h"),
    (30,    "SQL_MAX_COLUMN_NAME_LEN",     "h"),
    (97,    "SQL_MAX_COLUMNS_IN_GROUP_BY", "h"),
    (99,    "SQL_MAX_COLUMNS_IN_ORDER_BY", "h"),
    (100,   "SQL_MAX_COLUMNS_IN_SELECT",   "h"),
    (31,    "SQL_MAX_CURSOR_NAME_LEN",     "h"),
    (10005, "SQL_MAX_IDENTIFIER_LEN",      "h"),
    (33,    "SQL_MAX_PROCEDURE_NAME_LEN",  "h"),
    (32,    "SQL_MAX_SCHEMA_NAME_LEN",     "h"),
    (35,    "SQL_MAX_TABLE_NAME_LEN",      "h"),
    (105,   "SQL_MAX_STATEMENT_LEN",       "i"),
    (106,   "SQL_MAX_TABLES_IN_SELECT",    "h"),
    (107,   "SQL_MAX_USER_NAME_LEN",       "h"),
    (104,   "SQL_MAX_ROW_SIZE",            "i"),

    # Keywords (driver-specific SQL keywords beyond SQL-92)
    (89,    "SQL_KEYWORDS",                "s"),
]


def probe_get_info(conn):
    """Call SQLGetInfo for each entry in GET_INFO_CATALOG."""
    info = {}
    for type_id, name, _cat in GET_INFO_CATALOG:
        try:
            val = conn.getinfo(type_id)
            info[name] = val
        except pyodbc.Error as e:
            info[name] = {"error": str(e.args[1]) if len(e.args) > 1 else str(e)}
        except Exception as e:
            info[name] = {"error": str(e)}
    return info


def probe_type_info(conn):
    """Call SQLGetTypeInfo to enumerate supported data types."""
    cursor = conn.cursor()
    try:
        cursor.getTypeInfo()
    except Exception as e:
        return {"error": str(e)}

    columns = [desc[0] for desc in cursor.description]
    types = []
    for row in cursor:
        rec = {}
        for i, col_name in enumerate(columns):
            val = row[i]
            if isinstance(val, (bytes, bytearray)):
                val = val.hex()
            rec[col_name] = val
        types.append(rec)
    return types


def probe_connection(conn_str, label):
    """Probe a single connection for all driver metadata."""
    result = {"label": label, "connection_string": conn_str}

    try:
        conn = pyodbc.connect(conn_str, timeout=10)
    except Exception as e:
        result["error"] = str(e)
        return result

    result["get_info"] = probe_get_info(conn)
    result["type_info"] = probe_type_info(conn)

    conn.close()
    return result


def main():
    pretty = "--pretty" in sys.argv

    results = []
    for conn_str, label in CONNECTIONS:
        print(f"Probing: {label} ...", file=sys.stderr)
        r = probe_connection(conn_str, label)
        results.append(r)

        if "error" in r:
            print(f"  ERROR: {r['error']}", file=sys.stderr)
        else:
            gi = r["get_info"]
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
            ti = r["type_info"]
            if isinstance(ti, list):
                print(f"  Types: {len(ti)} data types supported", file=sys.stderr)

    indent = 2 if pretty else None
    print(json.dumps(results, indent=indent, default=str))


if __name__ == "__main__":
    main()
