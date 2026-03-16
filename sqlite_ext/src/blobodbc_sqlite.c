/*
 * SQLite loadable extension for ODBC queries.
 *
 * Registers:
 *   odbc_query(conn_str, sql)                -> JSON TEXT
 *   odbc_query(conn_str, sql, p1, p2, ...)   -> JSON TEXT (with bind params)
 *   odbc_clob(conn_str, sql)                 -> TEXT
 *   odbc_clob(conn_str, sql, p1, p2, ...)    -> TEXT (with bind params)
 *   odbc_driver_info(conn_str)               -> JSON TEXT
 */

#include <sqlite3ext.h>
SQLITE_EXTENSION_INIT1

#include "blobodbc.h"

#include <stdlib.h>
#include <string.h>

/* ── odbc_query: variadic scalar function ────────────────────────── */

static void odbc_query_func(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc < 2) {
        sqlite3_result_error(ctx, "odbc_query requires at least 2 arguments", -1);
        return;
    }
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL ||
        sqlite3_value_type(argv[1]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    const char *conn_str = (const char *)sqlite3_value_text(argv[0]);
    const char *query    = (const char *)sqlite3_value_text(argv[1]);

    char *result;
    if (argc > 2) {
        int bind_count = argc - 2;
        const char **bind_values = (const char **)malloc(bind_count * sizeof(char *));
        for (int i = 0; i < bind_count; i++) {
            bind_values[i] = (const char *)sqlite3_value_text(argv[i + 2]);
        }
        result = blobodbc_query_json_params(conn_str, query, bind_values, bind_count);
        free(bind_values);
    } else {
        result = blobodbc_query_json(conn_str, query);
    }

    if (result) {
        sqlite3_result_text(ctx, result, -1, SQLITE_TRANSIENT);
        blobodbc_free(result);
    } else {
        sqlite3_result_error(ctx, blobodbc_errmsg(), -1);
    }
}

/* ── odbc_clob: variadic scalar function ─────────────────────────── */

static void odbc_clob_func(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc < 2) {
        sqlite3_result_error(ctx, "odbc_clob requires at least 2 arguments", -1);
        return;
    }
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL ||
        sqlite3_value_type(argv[1]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    const char *conn_str = (const char *)sqlite3_value_text(argv[0]);
    const char *query    = (const char *)sqlite3_value_text(argv[1]);

    char *result;
    if (argc > 2) {
        int bind_count = argc - 2;
        const char **bind_values = (const char **)malloc(bind_count * sizeof(char *));
        for (int i = 0; i < bind_count; i++) {
            bind_values[i] = (const char *)sqlite3_value_text(argv[i + 2]);
        }
        result = blobodbc_query_clob_params(conn_str, query, bind_values, bind_count);
        free(bind_values);
    } else {
        result = blobodbc_query_clob(conn_str, query);
    }

    if (result) {
        sqlite3_result_text(ctx, result, -1, SQLITE_TRANSIENT);
        blobodbc_free(result);
    } else {
        sqlite3_result_error(ctx, blobodbc_errmsg(), -1);
    }
}

/* ── odbc_query_named(conn_str, sql, bind_json) ──────────────────── */

static void odbc_query_named_func(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL ||
        sqlite3_value_type(argv[1]) == SQLITE_NULL ||
        sqlite3_value_type(argv[2]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    const char *conn_str  = (const char *)sqlite3_value_text(argv[0]);
    const char *query     = (const char *)sqlite3_value_text(argv[1]);
    const char *bind_json = (const char *)sqlite3_value_text(argv[2]);

    char *result = blobodbc_query_json_named(conn_str, query, bind_json);
    if (result) {
        sqlite3_result_text(ctx, result, -1, SQLITE_TRANSIENT);
        blobodbc_free(result);
    } else {
        sqlite3_result_error(ctx, blobodbc_errmsg(), -1);
    }
}

/* ── odbc_clob_named(conn_str, sql, bind_json) ───────────────────── */

static void odbc_clob_named_func(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL ||
        sqlite3_value_type(argv[1]) == SQLITE_NULL ||
        sqlite3_value_type(argv[2]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    const char *conn_str  = (const char *)sqlite3_value_text(argv[0]);
    const char *query     = (const char *)sqlite3_value_text(argv[1]);
    const char *bind_json = (const char *)sqlite3_value_text(argv[2]);

    char *result = blobodbc_query_clob_named(conn_str, query, bind_json);
    if (result) {
        sqlite3_result_text(ctx, result, -1, SQLITE_TRANSIENT);
        blobodbc_free(result);
    } else {
        sqlite3_result_error(ctx, blobodbc_errmsg(), -1);
    }
}

/* ── odbc_driver_info(conn_str) ───────────────────────────────────── */

static void odbc_driver_info_func(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    const char *conn_str = (const char *)sqlite3_value_text(argv[0]);

    char *result = blobodbc_driver_info(conn_str);
    if (result) {
        sqlite3_result_text(ctx, result, -1, SQLITE_TRANSIENT);
        blobodbc_free(result);
    } else {
        sqlite3_result_error(ctx, blobodbc_errmsg(), -1);
    }
}

/* ── odbc_tables(conn_str [, schema [, type]]) ────────────────────── */

static void odbc_tables_func(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc < 1) {
        sqlite3_result_error(ctx, "odbc_tables requires at least 1 argument", -1);
        return;
    }
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    const char *conn_str = (const char *)sqlite3_value_text(argv[0]);
    const char *schema = (argc >= 2 && sqlite3_value_type(argv[1]) != SQLITE_NULL)
                          ? (const char *)sqlite3_value_text(argv[1]) : NULL;
    const char *type   = (argc >= 3 && sqlite3_value_type(argv[2]) != SQLITE_NULL)
                          ? (const char *)sqlite3_value_text(argv[2]) : NULL;

    const char *catalog = (argc >= 4 && sqlite3_value_type(argv[3]) != SQLITE_NULL)
                           ? (const char *)sqlite3_value_text(argv[3]) : NULL;

    char *result = blobodbc_tables(conn_str, catalog, schema, type);
    if (result) {
        sqlite3_result_text(ctx, result, -1, SQLITE_TRANSIENT);
        blobodbc_free(result);
    } else {
        sqlite3_result_error(ctx, blobodbc_errmsg(), -1);
    }
}

/* ── odbc_columns(conn_str, table [, schema]) ─────────────────────── */

static void odbc_columns_func(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (argc < 2) {
        sqlite3_result_error(ctx, "odbc_columns requires at least 2 arguments", -1);
        return;
    }
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL ||
        sqlite3_value_type(argv[1]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    const char *conn_str = (const char *)sqlite3_value_text(argv[0]);
    const char *table    = (const char *)sqlite3_value_text(argv[1]);
    const char *schema   = (argc >= 3 && sqlite3_value_type(argv[2]) != SQLITE_NULL)
                            ? (const char *)sqlite3_value_text(argv[2]) : NULL;

    char *result = blobodbc_columns(conn_str, NULL, schema, table);
    if (result) {
        sqlite3_result_text(ctx, result, -1, SQLITE_TRANSIENT);
        blobodbc_free(result);
    } else {
        sqlite3_result_error(ctx, blobodbc_errmsg(), -1);
    }
}

/* ── odbc_query_in_catalog(conn_str, catalog, sql) ────────────────── */

static void odbc_query_in_catalog_func(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL ||
        sqlite3_value_type(argv[1]) == SQLITE_NULL ||
        sqlite3_value_type(argv[2]) == SQLITE_NULL) {
        sqlite3_result_null(ctx);
        return;
    }

    const char *conn_str = (const char *)sqlite3_value_text(argv[0]);
    const char *catalog  = (const char *)sqlite3_value_text(argv[1]);
    const char *query    = (const char *)sqlite3_value_text(argv[2]);

    char *result = blobodbc_query_json_in_catalog(conn_str, catalog, query);
    if (result) {
        sqlite3_result_text(ctx, result, -1, SQLITE_TRANSIENT);
        blobodbc_free(result);
    } else {
        sqlite3_result_error(ctx, blobodbc_errmsg(), -1);
    }
}

/* ── Extension init ──────────────────────────────────────────────── */

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_blobodbc_init(sqlite3 *db, char **pzErrMsg,
                            const sqlite3_api_routines *pApi) {
    int rc;
    SQLITE_EXTENSION_INIT2(pApi);

    rc = sqlite3_create_function(db, "odbc_query", -1,
                                  SQLITE_UTF8, NULL, odbc_query_func, NULL, NULL);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "odbc_clob", -1,
                                  SQLITE_UTF8, NULL, odbc_clob_func, NULL, NULL);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "odbc_query_named", 3,
                                  SQLITE_UTF8, NULL, odbc_query_named_func, NULL, NULL);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "odbc_clob_named", 3,
                                  SQLITE_UTF8, NULL, odbc_clob_named_func, NULL, NULL);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "odbc_driver_info", 1,
                                  SQLITE_UTF8, NULL, odbc_driver_info_func, NULL, NULL);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "odbc_tables", -1,
                                  SQLITE_UTF8, NULL, odbc_tables_func, NULL, NULL);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "odbc_columns", -1,
                                  SQLITE_UTF8, NULL, odbc_columns_func, NULL, NULL);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "odbc_query_in_catalog", 3,
                                  SQLITE_UTF8, NULL, odbc_query_in_catalog_func, NULL, NULL);
    return rc;
}
