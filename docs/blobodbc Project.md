# blobodbc Project

ODBC query execution as scalar functions returning JSON.

## What it does

Wraps [nanodbc](https://github.com/nanodbc/nanodbc) behind a narrow C API, so any
ODBC-accessible database — SQL Server, PostgreSQL, MySQL, Oracle — can be queried
from inside DuckDB or SQLite and the rows come back as a JSON array.

## Why it matters here

It is how catalog and histogram data reaches the analysis. The column
classification work reads `sys.stats` and `sys.dm_db_stats_histogram` out of SQL
Server and computes features in DuckDB; this is the bridge that makes that one
query rather than an export step.

Returning JSON rather than a bound result set is deliberate: it crosses the
language boundary without either side needing the other's type system, which is
the same choice the rest of the family makes.

## Building

`zig build`. One prerequisite: Zig 0.16.0 — no CMake, no Make, no `configure`.
See [[Building the Blob Family]] for the full instructions, cross-compilation,
testing, and how to verify an extension actually loads.

## The family pattern

blob* extensions share one shape: a **C ABI core** carrying the behaviour, with
thin per-host shims over it — DuckDB, SQLite, and Python through ctypes. The
core is where fixes go, so all three hosts benefit at once; the shims are
deliberately boring. Build scaffolding is shared through
[[blobzig Project|blobzig]].
