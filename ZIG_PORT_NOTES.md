# Zig port — nanodbc removed, core still C++

The build is off CMake and nanodbc is gone. What remains is one C++ file and two
follow-ups, both optional.

## Layout

```
src/odbc.zig   the ODBC layer, in Zig over translate-C'd sql.h / sqlext.h
src/odbc.h     the boundary the C++ calls through
src/blobodbc_core.cpp   still C++ (see below)
include/blobodbc.h      the public C ABI, unchanged
```

## Why the core is still C++

The blocker was nanodbc, not C++. nanodbc is a convenience wrapper over a C API —
the same kind of layer removed elsewhere in this migration — and it had stopped
compiling against a current libc++ (it instantiates
`std::basic_string<unsigned char>`; libc++ removed the primary
`std::char_traits<T>` template per P1148R0). Replacing it was necessary, and it
is done.

Porting the remaining 1,589 lines was staged separately to keep the diff
verifiable, not because it resists porting. Having measured it, it resists less
than expected:

- the whole jsoncons surface is three constructs — `json`, `json_object_arg`,
  `json_array_arg` — across 76 lines. `std.json` covers all of it.
- the `jsoncons_ext/jsonpatch` include is **unused** in this file.
- the bulk of the file is static catalog tables (`g_info_catalog`,
  `g_func_catalog`) plus raw ODBC calls that were never nanodbc's to begin with,
  and both translate to Zig directly.

So a full Zig core is a day's mechanical work, and would drop jsoncons and
`link_libcpp` from this repo entirely.

## Follow-up 1: block fetch (a real regression)

The nanodbc path inspected a prepared statement's column types and chose between
ODBC array fetch (`SQL_ATTR_ROW_ARRAY_SIZE` = 1024) and row-at-a-time, because
array fetch cannot bind LOB / unbounded columns — `SQLFetch` fails with HY109.

`src/odbc.zig` reads with `SQLGetData`, which is LOB-safe for every column, so
the fork is gone — **and so is the block fetch**. Rows now cost one driver
round-trip each rather than one per 1024. That is a throughput regression on tall
result sets, and the original comment is explicit that block fetch was "the
dominant cost for wide/tall result sets".

Restoring it means `SQLBindCol` with column arrays, and a rowset chosen against a
memory budget rather than a flat 1024 — note the old code could allocate ~67 MB
for a single 65535-wide bounded column at 1024 rows, so a budget would be an
improvement on parity, not just a workaround.

The LOB path must stay on `SQLGetData` either way, so this adds a second read
path rather than replacing the current one.

## Follow-up 2: `::` in named-parameter SQL (pre-existing)

`bo_query_named('...', 'SELECT :a::int', ...)` fails: `rewrite_named_params`
treats the second colon of PostgreSQL's `::` cast as starting a placeholder. Its
documented rules never covered `::`, and the rewriter is untouched by this port —
so this is pre-existing, not a regression. Worth fixing when convenient: skip a
doubled colon before scanning for an identifier.

## Verification

Against live targets, through the DuckDB extension (so the whole stack: extension
-> C++ core -> Zig ODBC -> unixODBC -> server):

| Target | Driver | Result |
|---|---|---|
| PostgreSQL 17, local unix socket (`DSN=rule4_test`) | PostgreSQL Unicode | pass |
| DuckDB, `:memory:` | DuckDB Driver | pass |
| SQL Server 2025 on dc1 (`sql2025-dc1`), Kerberos | ODBC Driver 18 | pass |
| PostgreSQL on dc1 (`pg-dc1`, :5433) | PostgreSQL Unicode | not tested — `pg_hba.conf` rejects this host/user |

Three drivers with genuinely different type mappings. Covered: bigint, float8 /
float, text / varchar, NULL, DATE, TIMESTAMP / datetime2, bit — timestamps
byte-identical to the nanodbc path — plus named parameters including NULL
binding, the CLOB path, the typed table function, and `SQLGetInfo` metadata.

Large values through the chunked `SQLGetData` loop: 10,000 chars from PostgreSQL
and **20,000 from a SQL Server `varchar(max)`**. That second one matters — it is
exactly the LOB case the nanodbc path had to detect and fall back to
row-at-a-time for. `SQLGetData` handles it uniformly, which is why the
block-fetch fork disappeared (and why follow-up 1 has to keep this path).

### Connecting to sql2025-dc1 with Kerberos (no SA password needed)

```
Driver={ODBC Driver 18 for SQL Server};
Server=dc1.phrrngtn.arpa,1433;Database=master;
Trusted_Connection=yes;TrustServerCertificate=yes;
ServerSPN=MSSQLSvc/dc1.phrrngtn.arpa:1433
```

`ServerSPN` is not optional here, and the reason is a local trap worth recording.
`/etc/hosts` on this Mac reads:

    192.168.1.6  keycloak.phrrngtn.arpa dc1.phrrngtn.arpa

so **keycloak is the canonical name** for that address. The driver canonicalises
the server name before building the SPN, asks the KDC for
`MSSQLSvc/keycloak.phrrngtn.arpa:1433`, and fails — the keytab holds
`MSSQLSvc/dc1.phrrngtn.arpa:1433` and `MSSQLSvc/dc1:1433`. Setting `ServerSPN`
explicitly bypasses the canonicalisation. Setting `rdns = false` in a client
krb5.conf does **not** help, because the canonicalisation is happening in the
resolver, not in Kerberos.

Using the bare name `Server=dc1` fails differently: it resolves through tailscale
as `dc1.tail90d1f.ts.net` and lands in the `TAIL90D1F.TS.NET` realm.

Kerberos to PostgreSQL over TCP is separately broken in this environment (no SPN
for the localhost/dc1 PG); the unix-socket DSN sidesteps GSSAPI entirely.

One thing the port improves along the way: every failure above was diagnosed
straight from `bo_odbc_errmsg`, which carries the driver's own SQLSTATE and text
via `SQLGetDiagRec` — more than `nanodbc::database_error` surfaced for most
failures.
