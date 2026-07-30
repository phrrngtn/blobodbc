/*
 * The boundary between blobodbc_core.cpp and the Zig ODBC layer (src/odbc.zig).
 *
 * This replaces nanodbc. The surface is deliberately just the part of nanodbc the
 * core actually used — connection, execute, result with typed getters — so the
 * migration is a mechanical substitution rather than a redesign:
 *
 *     nanodbc::connection conn(str)     ->  bo_conn_open(str)
 *     conn.connected()                  ->  bo_conn_alive(conn)
 *     nanodbc::execute(conn, sql, n)    ->  bo_conn_query(conn, sql, n)
 *     result.columns()                  ->  bo_res_ncols(res)
 *     result.column_name(i)             ->  bo_res_colname(res, i)
 *     result.column_datatype(i)         ->  bo_res_coltype(res, i)
 *     result.next()                     ->  bo_res_next(res) == 1
 *     result.is_null(i)                 ->  bo_res_is_null(res, i)
 *     result.get<long long>(i)          ->  bo_res_i64(res, i)
 *     result.get<double>(i)             ->  bo_res_f64(res, i)
 *     result.get<std::string>(i)        ->  bo_res_str(res, i)
 *     get<date>/get<timestamp>          ->  bo_res_str(res, i)   (pre-formatted)
 *     catch (nanodbc::database_error&)  ->  check for NULL / -1, then bo_odbc_errmsg()
 *
 * Dates and timestamps arrive already rendered in the exact formats the old
 * getters produced ("YYYY-MM-DD" and "YYYY-MM-DD HH:MM:SS.fffffffff"), so JSON
 * output is unchanged.
 *
 * Errors are reported as NULL (or a non-zero / -1 return) plus a thread-local
 * message, rather than as exceptions. bo_odbc_errmsg() carries the DRIVER's own
 * diagnostic via SQLGetDiagRec — the SQLSTATE and text — which is strictly more
 * informative than what nanodbc::database_error surfaced for most failures.
 */

#ifndef BLOBODBC_ODBC_H
#define BLOBODBC_ODBC_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct bo_conn bo_conn;
typedef struct bo_result bo_result;

/* Last error on this thread; valid until the next bo_* call. Never NULL. */
const char *bo_odbc_errmsg(void);

/* NULL on failure. */
bo_conn *bo_conn_open(const char *conn_str);
void     bo_conn_close(bo_conn *conn);

/* 0 when the driver reports the connection dead, so the pool can reconnect. */
int      bo_conn_alive(bo_conn *conn);

/* The raw SQLHDBC, for the parts of the core that already call ODBC directly
   (SQLGetInfo, SQLTables, SQLColumns, SQLPrimaryKeys, SQLForeignKeys). */
void    *bo_conn_handle(bo_conn *conn);

/* The shared SQLHENV, for SQLDrivers enumeration. */
void    *bo_env_handle(void);

/* Execute and return a result to iterate; NULL on failure. */
bo_result *bo_conn_query(bo_conn *conn, const char *sql);

/* As above with positional parameters, all bound as strings — which is what the
   nanodbc path did; the driver coerces to each parameter's real type.
   is_null[i] non-zero binds SQL NULL at that position. */
bo_result *bo_conn_query_params(bo_conn *conn, const char *sql,
                                const char *const *values, const int *is_null,
                                int n);

/* Statements with no result set (DDL/DML). 0 on success. */
int      bo_conn_execute(bo_conn *conn, const char *sql);

void        bo_res_free(bo_result *res);
int         bo_res_ncols(bo_result *res);
const char *bo_res_colname(bo_result *res, int col);
int         bo_res_coltype(bo_result *res, int col);   /* the SQL type code */

/* 1 = a row is available, 0 = end of results, -1 = error. */
int         bo_res_next(bo_result *res);

int         bo_res_is_null(bo_result *res, int col);
int64_t     bo_res_i64(bo_result *res, int col);
double      bo_res_f64(bo_result *res, int col);

/* Borrowed, NUL-terminated; valid until the next bo_res_next on this result. */
const char *bo_res_str(bo_result *res, int col);

#ifdef __cplusplus
}
#endif

#endif /* BLOBODBC_ODBC_H */
