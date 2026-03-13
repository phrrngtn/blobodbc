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
 * Free a string returned by blobodbc_query_*.
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
