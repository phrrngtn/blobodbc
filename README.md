# blobodbc

> **Note:** This code is almost entirely AI-authored (Claude, Anthropic), albeit under close human supervision, and is for research and experimentation purposes. Successful experiments may be re-implemented in a more coordinated and curated manner.

ODBC query execution as scalar functions returning JSON.

Wraps [nanodbc](https://github.com/nanodbc/nanodbc) behind a narrow C API.
Queries any ODBC-accessible database (SQL Server, PostgreSQL, MySQL, Oracle,
etc.) and returns results as a JSON array of objects or as a single text value
(CLOB mode, useful for `FOR JSON PATH` on SQL Server).

Builds as a loadable extension for **SQLite**, **DuckDB**, and as a **Python**
module via nanobind. Part of the
[blob extension family](https://github.com/phrrngtn/rule4/blob/main/BLOB_EXTENSIONS.md)
— core C API, thin wrappers for each host, CMake + FetchContent, no shared
libraries.


## Why scalar functions, not table functions?

DuckDB's community [nanodbc extension](https://community-extensions.duckdb.org/)
exposes ODBC results as a table-valued function (`odbc_query` → rows). That is
the right interface for bulk data movement.

blobodbc takes the opposite approach: every function is a **scalar** that
returns a single VARCHAR (containing JSON). This is more composable — you can
chain `jmespath_search(bo_query(...), expr)` in a single expression, nest it
inside CTEs, pass it to `template_render()`, or feed it to `json_each()` /
`unnest()` when you do want rows. No TVF restrictions, no special syntax, works
in any position a scalar works.

The design rationale and broader pattern — JSON as a tunneling mechanism for
structured data through the scalar interface — is described in
[Scalar Functions, JSON Tunneling, and Rule 4](https://github.com/phrrngtn/rule4/blob/main/doc/scalar_json_pattern.md).
blobodbc is the primary implementation of that pattern for ODBC sources.

For the primary use case — catalog metadata, stats histograms, extended
properties — result sets are small and high-structure. JSON is the right
intermediate form. For million-row table scans, use `postgres_scanner`,
`sqlite_scanner`, or the nanodbc community TVF extension.


## C API

All query functions return a `malloc`'d string. The caller must free it with
`blobodbc_free()`. On error, functions return `NULL` and
`blobodbc_errmsg()` returns a thread-local error message.

```c
#include "blobodbc.h"

/* JSON result set — array of objects, column order preserved */
char *blobodbc_query_json(const char *conn_str, const char *query);

char *blobodbc_query_json_params(const char *conn_str, const char *query,
                                  const char **bind_values, int bind_count);

char *blobodbc_query_json_named(const char *conn_str, const char *query,
                                 const char *bind_json);

/* CLOB — first column of first row as text */
char *blobodbc_query_clob(const char *conn_str, const char *query);

char *blobodbc_query_clob_params(const char *conn_str, const char *query,
                                  const char **bind_values, int bind_count);

char *blobodbc_query_clob_named(const char *conn_str, const char *query,
                                 const char *bind_json);

void  blobodbc_free(char *s);
const char *blobodbc_errmsg(void);
```

### Named parameters

The `_named` variants accept a SQL string with `:name` placeholders and a JSON
object mapping names to values:

```sql
SELECT * FROM sys.objects WHERE object_id = :oid AND type = :type
```
```json
{"oid": 42, "type": "U"}
```

The core rewrites `:name` → `?` markers, respecting single-quoted strings.
JSON `null` binds as SQL `NULL`. The same `:name` can appear multiple times.

### Type dispatch

Result-set columns are mapped by ODBC SQL type code:

| SQL types | JSON representation |
|---|---|
| `BIT`, `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT` | JSON number (integer) |
| `FLOAT`, `REAL`, `DOUBLE`, `NUMERIC`, `DECIMAL` | JSON number (float) |
| `CHAR`, `VARCHAR`, `WCHAR`, `WVARCHAR`, `GUID` | JSON string |
| `TYPE_DATE` | JSON string (`YYYY-MM-DD`) |
| `TYPE_TIMESTAMP` | JSON string (`YYYY-MM-DD HH:MM:SS.NNNNNNNNN`) |
| binary types | JSON `null` |

Result sets use `nlohmann::ordered_json` to preserve column order.


## SQLite extension

```sql
.load /path/to/blobodbc

-- Basic query
SELECT bo_query('DSN=mydsn', 'SELECT name, type FROM sys.objects');

-- Positional bind parameters (variadic)
SELECT bo_query('DSN=mydsn',
    'SELECT * FROM t WHERE id = ? AND status = ?', 42, 'active');

-- Named bind parameters
SELECT bo_query_named('DSN=mydsn',
    'SELECT * FROM t WHERE id = :id', '{"id": 42}');

-- CLOB mode (first column of first row)
SELECT bo_clob('DSN=mydsn',
    'SELECT * FROM t FOR JSON PATH');
```

### Registered functions

| Function | Arity | Description |
|---|---|---|
| `bo_query(conn, sql, ...)` | variadic (≥ 2) | JSON array of objects; extra args are positional bind params |
| `bo_clob(conn, sql, ...)` | variadic (≥ 2) | First column of first row; extra args are positional bind params |
| `bo_query_named(conn, sql, bind_json)` | 3 | JSON array with named `:param` binding |
| `bo_clob_named(conn, sql, bind_json)` | 3 | CLOB with named `:param` binding |

Requires a sqlite3 binary with extension loading support (the macOS system
sqlite3 does not — use Homebrew's `sqlite3`).


## DuckDB extension

```sql
SET allow_unsigned_extensions = true;
LOAD '/path/to/blobodbc.duckdb_extension';

-- Basic query
SELECT bo_query('DSN=mydsn', 'SELECT name, type FROM sys.objects');

-- Named bind parameters
SELECT bo_query_named('DSN=mydsn',
    'SELECT * FROM t WHERE id = :id', '{"id": 42}');

-- CLOB mode
SELECT bo_clob('DSN=mydsn',
    'SELECT * FROM t FOR JSON PATH');

-- Unpack JSON into rows
SELECT j->>'name' AS name, j->>'type' AS type
FROM (
    SELECT unnest(from_json(
        bo_query('DSN=mydsn', 'SELECT name, type FROM sys.objects'),
        '["json"]'
    )) AS j
);
```

### Registered functions

| Function | Args | Description |
|---|---|---|
| `bo_query(conn, sql)` | 2 | JSON array of objects |
| `bo_query(conn, sql, jmespath)` | 3 | JSON with JMESPath reshaping (stub — pass `''`) |
| `bo_clob(conn, sql)` | 2 | First column of first row |
| `bo_query_named(conn, sql, bind_json)` | 3 | JSON with named `:param` binding |
| `bo_query_named(conn, sql, bind_json, jmespath)` | 4 | Named + JMESPath (stub — pass `''`) |
| `bo_clob_named(conn, sql, bind_json)` | 3 | CLOB with named `:param` binding |

### JMESPath parameter (stub)

The 3-arg `bo_query` and 4-arg `bo_query_named` overloads accept a JMESPath
expression as the final argument. This is currently a **stub** — passing any
non-empty string produces an error. Pass `''` (empty string) for a no-op.

The architecture is prepared for future implementation using jsoncons (already a
build dependency). The intent is to reshape results before they cross the FFI
boundary — see the [scalar JSON pattern doc](https://github.com/phrrngtn/rule4/blob/main/doc/scalar_json_pattern.md)
for the design rationale. Note that JMESPath as a standalone SQL function is
provided by [blobtemplates](https://github.com/phrrngtn/blobtemplates), not
blobodbc — blobodbc's JMESPath parameter is for internal reshaping only.

### DuckDB extension signing

Locally-built extensions need a metadata footer. After building:

```bash
python3 duckdb_ext/append_metadata.py build/duckdb/blobodbc.so blobodbc.duckdb_extension
```

Or launch DuckDB with `-unsigned`:

```bash
duckdb -unsigned
```


## Python module

```python
import blobodbc_ext

# JSON result set
result = blobodbc_ext.query_json('DSN=mydsn', 'SELECT 1 AS n')
# '[{"n": 1}]'

# Named parameters
result = blobodbc_ext.query_json('DSN=mydsn',
    'SELECT * FROM t WHERE id = :id', '{"id": 42}')

# CLOB
result = blobodbc_ext.query_clob('DSN=mydsn',
    'SELECT * FROM t FOR JSON PATH')
```

Each function has two overloads: `(conn, sql)` and `(conn, sql, bind_json)`.
Errors raise `ValueError` with the nanodbc diagnostic message.

Build with `BUILD_PYTHON_BINDINGS=ON`. The compiled module
(`blobodbc_ext.cpython-*.so`) must be importable — either install the package or
place it on `sys.path`.


## Building

```bash
cmake -B build \
  -DBUILD_SQLITE_EXTENSION=ON \
  -DBUILD_DUCKDB_EXTENSION=ON \
  -DBUILD_PYTHON_BINDINGS=ON \
  ..

cmake --build build
```

All dependencies are fetched at build time via CMake FetchContent:

| Dependency | Version | Purpose |
|---|---|---|
| [nanodbc](https://github.com/nanodbc/nanodbc) | main | ODBC client library |
| [nlohmann/json](https://github.com/nlohmann/json) | v3.11.3 | JSON serialization (`ordered_json` for column-order preservation) |
| [jsoncons](https://github.com/danielaparker/jsoncons) | v1.1.0 | Header-only; internal dependency for future JMESPath reshaping |
| [DuckDB C API](https://github.com/duckdb/extension-template-c) | main | Headers only (for DuckDB extension build) |
| [SQLite amalgamation](https://sqlite.org/) | 3.46.1 | Headers only (for SQLite extension build) |

**System requirement**: unixODBC (`brew install unixodbc` on macOS) and an ODBC
driver for your target database.

### Build targets

| Flag | Target | Output |
|---|---|---|
| `BUILD_SQLITE_EXTENSION` | `blobodbc_sqlite` | `build/sqlite/blobodbc.so` |
| `BUILD_DUCKDB_EXTENSION` | `blobodbc_duckdb` | `build/duckdb/blobodbc.so` |
| `BUILD_PYTHON_BINDINGS` | `blobodbc_ext` | `python/blobodbc/blobodbc_ext.*.so` |

On macOS, the SQLite build also creates a `blobodbc.dylib` symlink for
compatibility.


## Project layout

```
blobodbc/
├── include/
│   └── blobodbc.h              # C API header
├── src/
│   └── blobodbc_core.cpp       # Core implementation (nanodbc + nlohmann/json)
├── sqlite_ext/
│   └── src/
│       └── blobodbc_sqlite.c   # SQLite extension wrapper
├── duckdb_ext/
│   ├── src/
│   │   └── blobodbc_duckdb.c   # DuckDB extension wrapper (C API)
│   └── append_metadata.py      # DuckDB extension signing utility
├── python/
│   └── bindings.cpp            # nanobind Python bindings
├── example/                    # C example (optional)
└── CMakeLists.txt
```

The pattern is shared across the blob extension family: core C/C++
implementation, thin wrappers for each host environment, all statically linked
(no shared library, no IPC).


## Sibling extensions

blobodbc is one of four extensions that follow the same build pattern:

- **[blobtemplates](https://github.com/phrrngtn/blobtemplates)** — Inja
  templates + jsoncons (JMESPath, JSON diff/patch, flatten)
- **[blobboxes](https://github.com/phrrngtn/blobboxes)** — Document extraction
  (PDF, XLSX, DOCX, text) → 5-table relational schema
- **[blobfilters](https://github.com/phrrngtn/blobfilters)** — Roaring bitmap
  domain fingerprinting for column classification

They compose in DuckDB: `jmespath_search(bo_query(...), expr)` chains
blobodbc with blobtemplates in a single expression. See
[BLOB_EXTENSIONS.md](https://github.com/phrrngtn/rule4/blob/main/BLOB_EXTENSIONS.md)
for the full composition diagram.


## License

MIT
