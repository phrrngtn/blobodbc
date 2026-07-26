#ifndef BLOBODBC_H
#define BLOBODBC_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Execute an ODBC query and return the result set as a JSON array of objects.
 * Column names become object keys; column order is preserved.
 *
 * conn_str:  ODBC connection string or DSN name
 * query:     SQL query to execute
 *
 * Returns a malloc'd JSON string on success (caller must free with blobodbc_free).
 * Returns NULL on error; call blobodbc_errmsg() for details.
 */
char *blobodbc_query_json(const char *conn_str, const char *query);

/*
 * Execute an ODBC query with positional bind parameters.
 * All bind values are passed as strings.
 *
 * bind_values: array of C strings (positional parameters)
 * bind_count:  number of bind values
 *
 * Returns a malloc'd JSON string on success (caller must free with blobodbc_free).
 * Returns NULL on error; call blobodbc_errmsg() for details.
 */
char *blobodbc_query_json_params(const char *conn_str, const char *query,
                                  const char **bind_values, int bind_count);

/*
 * Execute an ODBC query and return the first column of the first row as text.
 * Useful for queries that return a single CLOB/JSON value (e.g. FOR JSON PATH).
 *
 * Returns a malloc'd string on success (caller must free with blobodbc_free).
 * Returns NULL on error; call blobodbc_errmsg() for details.
 */
char *blobodbc_query_clob(const char *conn_str, const char *query);

/*
 * Execute an ODBC query (CLOB variant) with positional bind parameters.
 */
char *blobodbc_query_clob_params(const char *conn_str, const char *query,
                                  const char **bind_values, int bind_count);

/*
 * Execute an ODBC query with named bind parameters from a JSON object.
 *
 * The query uses :name placeholders (e.g. "WHERE id = :id AND name = :name").
 * bind_json is a JSON object mapping names to values:
 *   {"id": 42, "name": "Alice"}
 *
 * Values are converted to strings for binding. JSON null binds as SQL NULL.
 * The same :name can appear multiple times in the query.
 *
 * Returns a malloc'd JSON string on success (caller must free with blobodbc_free).
 * Returns NULL on error; call blobodbc_errmsg() for details.
 */
char *blobodbc_query_json_named(const char *conn_str, const char *query,
                                 const char *bind_json);

/*
 * Execute an ODBC query (CLOB variant) with named bind parameters.
 */
char *blobodbc_query_clob_named(const char *conn_str, const char *query,
                                 const char *bind_json);

/*
 * Return ODBC driver metadata as a JSON object.
 *
 * Connects to the data source and collects:
 *   - "get_info":  SQLGetInfo values (driver name/version, DBMS identity,
 *                  SQL conformance, feature bitmasks, limits, keywords, etc.)
 *   - "type_info": SQLGetTypeInfo result set (supported data types)
 *
 * conn_str:  ODBC connection string or DSN name
 *
 * Returns a malloc'd JSON string on success (caller must free with blobodbc_free).
 * Returns NULL on error; call blobodbc_errmsg() for details.
 */
char *blobodbc_driver_info(const char *conn_str);

/*
 * Return tables visible via SQLTables as a JSON array of objects.
 *
 * Each object contains: TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE,
 * REMARKS (as returned by the driver).
 *
 * All filter parameters accept NULL for "no restriction" (return all).
 * Non-NULL values are treated as SQL pattern match arguments.
 *
 * catalog: catalog/database pattern, or NULL for all
 * schema:  schema pattern (e.g. "dbo"), or NULL for all
 * type:    comma-separated table types (e.g. "TABLE,VIEW"), or NULL for all
 *
 * Returns a malloc'd JSON string on success (caller must free with blobodbc_free).
 * Returns NULL on error; call blobodbc_errmsg() for details.
 */
char *blobodbc_tables(const char *conn_str,
                      const char *catalog,
                      const char *schema,
                      const char *type);

/*
 * Return columns via SQLColumns as a JSON array of objects.
 *
 * Each object contains the standard SQLColumns result set columns:
 * TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, DATA_TYPE,
 * TYPE_NAME, COLUMN_SIZE, NULLABLE, REMARKS, COLUMN_DEF, ORDINAL_POSITION, etc.
 *
 * All filter parameters accept NULL for "no restriction" (return all).
 * Non-NULL values are treated as SQL pattern match arguments.
 *
 * catalog: catalog/database pattern, or NULL for current
 * schema:  schema name pattern, or NULL for all schemas
 * table:   table name pattern, or NULL for all tables
 *
 * Returns a malloc'd JSON string on success (caller must free with blobodbc_free).
 * Returns NULL on error; call blobodbc_errmsg() for details.
 */
char *blobodbc_columns(const char *conn_str,
                       const char *catalog,
                       const char *schema,
                       const char *table);

/*
 * Execute a query after switching to a specific catalog (database).
 *
 * Sets SQL_ATTR_CURRENT_CATALOG to the given catalog before executing,
 * and restores the original catalog afterward. Safe with connection pooling.
 *
 * conn_str:  ODBC connection string or DSN name
 * catalog:   target database/catalog name
 * query:     SQL query to execute
 *
 * Returns a malloc'd JSON string on success (caller must free with blobodbc_free).
 * Returns NULL on error; call blobodbc_errmsg() for details.
 */
char *blobodbc_query_json_in_catalog(const char *conn_str,
                                      const char *catalog,
                                      const char *query);

/*
 * Execute a SQL statement that does not return a result set.
 * Intended for DDL (CREATE/ALTER/DROP) and plain DML (INSERT/UPDATE/DELETE/MERGE).
 * Returns the number of affected rows (0 for DDL), or -1 on error.
 *
 * For DML with OUTPUT (SQL Server) or RETURNING (PostgreSQL) clauses,
 * use blobodbc_query_json instead — those produce result sets.
 *
 * Call blobodbc_errmsg() for error details.
 */
int blobodbc_execute(const char *conn_str, const char *sql);

/*
 * Return primary keys via SQLPrimaryKeys as a JSON array of objects.
 *
 * Each object contains: TABLE_CAT, TABLE_SCHEM, TABLE_NAME, COLUMN_NAME,
 * KEY_SEQ, PK_NAME.
 *
 * All filter parameters accept NULL for "no restriction".
 */
char *blobodbc_primary_keys(const char *conn_str,
                             const char *catalog,
                             const char *schema,
                             const char *table);

/*
 * Return foreign keys via SQLForeignKeys as a JSON array of objects.
 *
 * Each object contains: PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME,
 * PKCOLUMN_NAME, FKTABLE_CAT, FKTABLE_SCHEM, FKTABLE_NAME,
 * FKCOLUMN_NAME, KEY_SEQ, UPDATE_RULE, DELETE_RULE, FK_NAME, PK_NAME.
 *
 * Two modes:
 *   - Specify fk_* params: "what does this table reference?"
 *   - Specify pk_* params: "what references this table?"
 * All filter parameters accept NULL for "no restriction".
 */
char *blobodbc_foreign_keys(const char *conn_str,
                             const char *pk_catalog,
                             const char *pk_schema,
                             const char *pk_table,
                             const char *fk_catalog,
                             const char *fk_schema,
                             const char *fk_table);

/*
 * Free a string returned by blobodbc_* functions.
 */
void blobodbc_free(char *s);

/*
 * Return the last error message (thread-local).
 * Returns "" if no error has occurred.
 */
const char *blobodbc_errmsg(void);

/* ── Materialized row set ─────────────────────────────────────────────
 *
 * Executes a query and materializes the whole result as UTF-8 strings (one
 * cell per column per row), so a host table function (e.g. DuckDB's
 * bo_query_table) can stream typed-as-VARCHAR columns straight into its
 * vectors — no JSON round-trip. The result is held in memory like the JSON
 * path; intended for bounded catalog/profiling result sets. Benchmarks show
 * this per-cell path beats server-side FOR JSON for these workloads.
 */
typedef struct blobodbc_rowset blobodbc_rowset;

/* Execute `query` and materialize the result. Returns NULL on error
 * (blobodbc_errmsg() for details); free with blobodbc_rowset_free. */
blobodbc_rowset *blobodbc_query_rowset(const char *conn_str, const char *query);

size_t      blobodbc_rowset_ncols(const blobodbc_rowset *rs);
size_t      blobodbc_rowset_nrows(const blobodbc_rowset *rs);
const char *blobodbc_rowset_colname(const blobodbc_rowset *rs, size_t col);
/* Cell value as a NUL-terminated UTF-8 string, or NULL if the cell is SQL NULL. */
const char *blobodbc_rowset_value(const blobodbc_rowset *rs, size_t row, size_t col);
void        blobodbc_rowset_free(blobodbc_rowset *rs);

/* ── Typed materialized row set ───────────────────────────────────────
 *
 * Like blobodbc_query_rowset, but each column keeps its NATIVE type: SQL
 * integer types → INT64, floating/numeric → DOUBLE, everything else → STRING.
 * The host (e.g. bo_query_table_typed) reads each column with the matching
 * accessor and writes into a correctly-typed vector — no string round-trip on
 * numeric columns. NUMERIC/DECIMAL collapse to DOUBLE (precision caveat);
 * dates/binary/guid stay STRING. */

typedef enum {
    BLOBODBC_COL_STRING = 0,
    BLOBODBC_COL_INT64  = 1,
    BLOBODBC_COL_DOUBLE = 2
} blobodbc_coltype;

typedef struct blobodbc_rowset_typed blobodbc_rowset_typed;

blobodbc_rowset_typed *blobodbc_query_rowset_typed(const char *conn_str, const char *query);

size_t      blobodbc_rt_ncols(const blobodbc_rowset_typed *rs);
size_t      blobodbc_rt_nrows(const blobodbc_rowset_typed *rs);
const char *blobodbc_rt_colname(const blobodbc_rowset_typed *rs, size_t col);
int         blobodbc_rt_coltype(const blobodbc_rowset_typed *rs, size_t col);   /* blobodbc_coltype */
int         blobodbc_rt_is_null(const blobodbc_rowset_typed *rs, size_t row, size_t col);
/* Call only the accessor matching the column's coltype (no per-call type check). */
int64_t     blobodbc_rt_i64(const blobodbc_rowset_typed *rs, size_t row, size_t col);
double      blobodbc_rt_f64(const blobodbc_rowset_typed *rs, size_t row, size_t col);
const char *blobodbc_rt_str(const blobodbc_rowset_typed *rs, size_t row, size_t col);
void        blobodbc_rt_free(blobodbc_rowset_typed *rs);

#ifdef __cplusplus
}
#endif

#endif
