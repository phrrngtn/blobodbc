#ifndef BLOBODBC_H
#define BLOBODBC_H

#include <stddef.h>

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
 * Produce an RFC 6902 JSON Patch from source to target.
 * The patch, when applied to source, yields target.
 */
char *blobodbc_json_diff(const char *source_json, const char *target_json);

/*
 * Apply an RFC 6902 JSON Patch to a document, returning the result.
 */
char *blobodbc_json_patch(const char *doc_json, const char *patch_json);

/*
 * Reshape a flat JSON array of objects into a nested object hierarchy.
 *
 * keys_json is a JSON array of field names to nest by, e.g. ["TABLE_SCHEM","TABLE_NAME","COLUMN_NAME"].
 * Key fields are removed from leaf objects; remaining fields become the leaf value.
 */
char *blobodbc_json_nest(const char *data_json, const char *keys_json);

/*
 * Free a string returned by blobodbc_* functions.
 */
void blobodbc_free(char *s);

/*
 * Return the last error message (thread-local).
 * Returns "" if no error has occurred.
 */
const char *blobodbc_errmsg(void);

#ifdef __cplusplus
}
#endif

#endif
