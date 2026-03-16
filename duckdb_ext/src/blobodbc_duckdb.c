/*
 * DuckDB C API extension for ODBC queries.
 *
 * Registers:
 *   bo_query(conn_str, sql)                    -> VARCHAR (JSON)
 *   bo_clob(conn_str, sql)                     -> VARCHAR
 *   bo_query_named(conn_str, sql, bind_json)   -> VARCHAR (JSON)
 *   bo_clob_named(conn_str, sql, bind_json)    -> VARCHAR
 *   bo_driver_info(conn_str)                   -> VARCHAR (JSON)
 */

#define DUCKDB_EXTENSION_NAME blobodbc
#include "duckdb_extension.h"

#include "blobodbc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

/* ── String helpers ───────────────────────────────────────────────── */

static const char *str_ptr(duckdb_string_t *s, uint32_t *out_len) {
    uint32_t len = s->value.inlined.length;
    *out_len = len;
    if (len <= 12) {
        return s->value.inlined.inlined;
    }
    return s->value.pointer.ptr;
}

/* Null-terminate a DuckDB string */
static char *str_dup_z(duckdb_string_t *s) {
    uint32_t len;
    const char *p = str_ptr(s, &len);
    char *z = (char *)malloc(len + 1);
    memcpy(z, p, len);
    z[len] = '\0';
    return z;
}

/* ── bo_query(conn_str, sql) -> JSON ─────────────────────────────── */

static void odbc_query_func(duckdb_function_info info,
                              duckdb_data_chunk input,
                              duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector vec1 = duckdb_data_chunk_get_vector(input, 1);

    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    duckdb_string_t *data1 = (duckdb_string_t *)duckdb_vector_get_data(vec1);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);
    uint64_t *val1 = duckdb_vector_get_validity(vec1);

    for (idx_t row = 0; row < size; row++) {
        if ((val0 && !duckdb_validity_row_is_valid(val0, row)) ||
            (val1 && !duckdb_validity_row_is_valid(val1, row))) {
            duckdb_vector_ensure_validity_writable(output);
            duckdb_validity_set_row_invalid(duckdb_vector_get_validity(output), row);
            continue;
        }

        char *conn = str_dup_z(&data0[row]);
        char *sql  = str_dup_z(&data1[row]);

        char *result = blobodbc_query_json(conn, sql);
        free(conn);
        free(sql);

        if (result) {
            duckdb_vector_assign_string_element(output, row, result);
            blobodbc_free(result);
        } else {
            duckdb_scalar_function_set_error(info, blobodbc_errmsg());
            return;
        }
    }
}

/* ── bo_clob(conn_str, sql) -> TEXT ──────────────────────────────── */

static void odbc_clob_func(duckdb_function_info info,
                             duckdb_data_chunk input,
                             duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector vec1 = duckdb_data_chunk_get_vector(input, 1);

    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    duckdb_string_t *data1 = (duckdb_string_t *)duckdb_vector_get_data(vec1);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);
    uint64_t *val1 = duckdb_vector_get_validity(vec1);

    for (idx_t row = 0; row < size; row++) {
        if ((val0 && !duckdb_validity_row_is_valid(val0, row)) ||
            (val1 && !duckdb_validity_row_is_valid(val1, row))) {
            duckdb_vector_ensure_validity_writable(output);
            duckdb_validity_set_row_invalid(duckdb_vector_get_validity(output), row);
            continue;
        }

        char *conn = str_dup_z(&data0[row]);
        char *sql  = str_dup_z(&data1[row]);

        char *result = blobodbc_query_clob(conn, sql);
        free(conn);
        free(sql);

        if (result) {
            duckdb_vector_assign_string_element(output, row, result);
            blobodbc_free(result);
        } else {
            duckdb_scalar_function_set_error(info, blobodbc_errmsg());
            return;
        }
    }
}

/* ── 3-arg named-parameter variants ──────────────────────────────── */

typedef char *(*named_query_fn)(const char *, const char *, const char *);

static void named_func(duckdb_function_info info,
                        duckdb_data_chunk input,
                        duckdb_vector output) {
    named_query_fn fn = (named_query_fn)duckdb_scalar_function_get_extra_info(info);
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector vec1 = duckdb_data_chunk_get_vector(input, 1);
    duckdb_vector vec2 = duckdb_data_chunk_get_vector(input, 2);

    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    duckdb_string_t *data1 = (duckdb_string_t *)duckdb_vector_get_data(vec1);
    duckdb_string_t *data2 = (duckdb_string_t *)duckdb_vector_get_data(vec2);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);
    uint64_t *val1 = duckdb_vector_get_validity(vec1);
    uint64_t *val2 = duckdb_vector_get_validity(vec2);

    for (idx_t row = 0; row < size; row++) {
        if ((val0 && !duckdb_validity_row_is_valid(val0, row)) ||
            (val1 && !duckdb_validity_row_is_valid(val1, row)) ||
            (val2 && !duckdb_validity_row_is_valid(val2, row))) {
            duckdb_vector_ensure_validity_writable(output);
            duckdb_validity_set_row_invalid(duckdb_vector_get_validity(output), row);
            continue;
        }

        char *conn = str_dup_z(&data0[row]);
        char *sql  = str_dup_z(&data1[row]);
        char *bind = str_dup_z(&data2[row]);

        char *result = fn(conn, sql, bind);
        free(conn);
        free(sql);
        free(bind);

        if (result) {
            duckdb_vector_assign_string_element(output, row, result);
            blobodbc_free(result);
        } else {
            duckdb_scalar_function_set_error(info, blobodbc_errmsg());
            return;
        }
    }
}

/* ── bo_driver_info(conn_str) -> JSON ─────────────────────────────── */

static void odbc_driver_info_func(duckdb_function_info info,
                                    duckdb_data_chunk input,
                                    duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);

    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);

    for (idx_t row = 0; row < size; row++) {
        if (val0 && !duckdb_validity_row_is_valid(val0, row)) {
            duckdb_vector_ensure_validity_writable(output);
            duckdb_validity_set_row_invalid(duckdb_vector_get_validity(output), row);
            continue;
        }

        char *conn = str_dup_z(&data0[row]);

        char *result = blobodbc_driver_info(conn);
        free(conn);

        if (result) {
            duckdb_vector_assign_string_element(output, row, result);
            blobodbc_free(result);
        } else {
            duckdb_scalar_function_set_error(info, blobodbc_errmsg());
            return;
        }
    }
}

/* ── bo_tables: 1–4 arg overloads ────────────────────────────────── */
/*   bo_tables(conn_str)                          — all tables        */
/*   bo_tables(conn_str, schema)                  — schema filter     */
/*   bo_tables(conn_str, schema, type)            — + type filter     */
/*   bo_tables(conn_str, schema, type, catalog)   — + catalog         */

static void odbc_tables_func_1(duckdb_function_info info,
                                 duckdb_data_chunk input,
                                 duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);

    for (idx_t row = 0; row < size; row++) {
        if (val0 && !duckdb_validity_row_is_valid(val0, row)) {
            duckdb_vector_ensure_validity_writable(output);
            duckdb_validity_set_row_invalid(duckdb_vector_get_validity(output), row);
            continue;
        }
        char *conn = str_dup_z(&data0[row]);
        char *result = blobodbc_tables(conn, NULL, NULL, NULL);
        free(conn);
        if (result) {
            duckdb_vector_assign_string_element(output, row, result);
            blobodbc_free(result);
        } else {
            duckdb_scalar_function_set_error(info, blobodbc_errmsg());
            return;
        }
    }
}

static void odbc_tables_func_2(duckdb_function_info info,
                                 duckdb_data_chunk input,
                                 duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector vec1 = duckdb_data_chunk_get_vector(input, 1);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    duckdb_string_t *data1 = (duckdb_string_t *)duckdb_vector_get_data(vec1);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);
    uint64_t *val1 = duckdb_vector_get_validity(vec1);

    for (idx_t row = 0; row < size; row++) {
        if ((val0 && !duckdb_validity_row_is_valid(val0, row)) ||
            (val1 && !duckdb_validity_row_is_valid(val1, row))) {
            duckdb_vector_ensure_validity_writable(output);
            duckdb_validity_set_row_invalid(duckdb_vector_get_validity(output), row);
            continue;
        }
        char *conn   = str_dup_z(&data0[row]);
        char *schema = str_dup_z(&data1[row]);
        char *result = blobodbc_tables(conn, NULL, schema, NULL);
        free(conn); free(schema);
        if (result) {
            duckdb_vector_assign_string_element(output, row, result);
            blobodbc_free(result);
        } else {
            duckdb_scalar_function_set_error(info, blobodbc_errmsg());
            return;
        }
    }
}

static void odbc_tables_func_3(duckdb_function_info info,
                                 duckdb_data_chunk input,
                                 duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector vec1 = duckdb_data_chunk_get_vector(input, 1);
    duckdb_vector vec2 = duckdb_data_chunk_get_vector(input, 2);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    duckdb_string_t *data1 = (duckdb_string_t *)duckdb_vector_get_data(vec1);
    duckdb_string_t *data2 = (duckdb_string_t *)duckdb_vector_get_data(vec2);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);
    uint64_t *val1 = duckdb_vector_get_validity(vec1);
    uint64_t *val2 = duckdb_vector_get_validity(vec2);

    for (idx_t row = 0; row < size; row++) {
        if ((val0 && !duckdb_validity_row_is_valid(val0, row)) ||
            (val1 && !duckdb_validity_row_is_valid(val1, row)) ||
            (val2 && !duckdb_validity_row_is_valid(val2, row))) {
            duckdb_vector_ensure_validity_writable(output);
            duckdb_validity_set_row_invalid(duckdb_vector_get_validity(output), row);
            continue;
        }
        char *conn   = str_dup_z(&data0[row]);
        char *schema = str_dup_z(&data1[row]);
        char *type   = str_dup_z(&data2[row]);
        char *result = blobodbc_tables(conn, NULL, schema, type);
        free(conn); free(schema); free(type);
        if (result) {
            duckdb_vector_assign_string_element(output, row, result);
            blobodbc_free(result);
        } else {
            duckdb_scalar_function_set_error(info, blobodbc_errmsg());
            return;
        }
    }
}

static void odbc_tables_func_4(duckdb_function_info info,
                                 duckdb_data_chunk input,
                                 duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector vec1 = duckdb_data_chunk_get_vector(input, 1);
    duckdb_vector vec2 = duckdb_data_chunk_get_vector(input, 2);
    duckdb_vector vec3 = duckdb_data_chunk_get_vector(input, 3);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    duckdb_string_t *data1 = (duckdb_string_t *)duckdb_vector_get_data(vec1);
    duckdb_string_t *data2 = (duckdb_string_t *)duckdb_vector_get_data(vec2);
    duckdb_string_t *data3 = (duckdb_string_t *)duckdb_vector_get_data(vec3);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);
    uint64_t *val1 = duckdb_vector_get_validity(vec1);
    uint64_t *val2 = duckdb_vector_get_validity(vec2);
    uint64_t *val3 = duckdb_vector_get_validity(vec3);

    for (idx_t row = 0; row < size; row++) {
        if ((val0 && !duckdb_validity_row_is_valid(val0, row)) ||
            (val1 && !duckdb_validity_row_is_valid(val1, row)) ||
            (val2 && !duckdb_validity_row_is_valid(val2, row)) ||
            (val3 && !duckdb_validity_row_is_valid(val3, row))) {
            duckdb_vector_ensure_validity_writable(output);
            duckdb_validity_set_row_invalid(duckdb_vector_get_validity(output), row);
            continue;
        }
        char *conn    = str_dup_z(&data0[row]);
        char *schema  = str_dup_z(&data1[row]);
        char *type    = str_dup_z(&data2[row]);
        char *catalog = str_dup_z(&data3[row]);
        char *result  = blobodbc_tables(conn, catalog, schema, type);
        free(conn); free(schema); free(type); free(catalog);
        if (result) {
            duckdb_vector_assign_string_element(output, row, result);
            blobodbc_free(result);
        } else {
            duckdb_scalar_function_set_error(info, blobodbc_errmsg());
            return;
        }
    }
}

/* ── bo_columns: 1–3 arg overloads ──────────────────────────────── */
/*   bo_columns(conn_str)                         — all columns       */
/*   bo_columns(conn_str, schema)                 — schema filter     */
/*   bo_columns(conn_str, schema, table)          — + table filter    */

static void odbc_columns_func_1(duckdb_function_info info,
                                  duckdb_data_chunk input,
                                  duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);

    for (idx_t row = 0; row < size; row++) {
        if (val0 && !duckdb_validity_row_is_valid(val0, row)) {
            duckdb_vector_ensure_validity_writable(output);
            duckdb_validity_set_row_invalid(duckdb_vector_get_validity(output), row);
            continue;
        }
        char *conn = str_dup_z(&data0[row]);
        char *result = blobodbc_columns(conn, NULL, NULL, NULL);
        free(conn);
        if (result) {
            duckdb_vector_assign_string_element(output, row, result);
            blobodbc_free(result);
        } else {
            duckdb_scalar_function_set_error(info, blobodbc_errmsg());
            return;
        }
    }
}

static void odbc_columns_func_2(duckdb_function_info info,
                                  duckdb_data_chunk input,
                                  duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector vec1 = duckdb_data_chunk_get_vector(input, 1);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    duckdb_string_t *data1 = (duckdb_string_t *)duckdb_vector_get_data(vec1);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);
    uint64_t *val1 = duckdb_vector_get_validity(vec1);

    for (idx_t row = 0; row < size; row++) {
        if ((val0 && !duckdb_validity_row_is_valid(val0, row)) ||
            (val1 && !duckdb_validity_row_is_valid(val1, row))) {
            duckdb_vector_ensure_validity_writable(output);
            duckdb_validity_set_row_invalid(duckdb_vector_get_validity(output), row);
            continue;
        }
        char *conn   = str_dup_z(&data0[row]);
        char *schema = str_dup_z(&data1[row]);
        char *result = blobodbc_columns(conn, NULL, schema, NULL);
        free(conn); free(schema);
        if (result) {
            duckdb_vector_assign_string_element(output, row, result);
            blobodbc_free(result);
        } else {
            duckdb_scalar_function_set_error(info, blobodbc_errmsg());
            return;
        }
    }
}

static void odbc_columns_func_3(duckdb_function_info info,
                                  duckdb_data_chunk input,
                                  duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector vec1 = duckdb_data_chunk_get_vector(input, 1);
    duckdb_vector vec2 = duckdb_data_chunk_get_vector(input, 2);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    duckdb_string_t *data1 = (duckdb_string_t *)duckdb_vector_get_data(vec1);
    duckdb_string_t *data2 = (duckdb_string_t *)duckdb_vector_get_data(vec2);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);
    uint64_t *val1 = duckdb_vector_get_validity(vec1);
    uint64_t *val2 = duckdb_vector_get_validity(vec2);

    for (idx_t row = 0; row < size; row++) {
        if ((val0 && !duckdb_validity_row_is_valid(val0, row)) ||
            (val1 && !duckdb_validity_row_is_valid(val1, row)) ||
            (val2 && !duckdb_validity_row_is_valid(val2, row))) {
            duckdb_vector_ensure_validity_writable(output);
            duckdb_validity_set_row_invalid(duckdb_vector_get_validity(output), row);
            continue;
        }
        char *conn   = str_dup_z(&data0[row]);
        char *schema = str_dup_z(&data1[row]);
        char *table  = str_dup_z(&data2[row]);
        char *result = blobodbc_columns(conn, NULL, schema, table);
        free(conn); free(schema); free(table);
        if (result) {
            duckdb_vector_assign_string_element(output, row, result);
            blobodbc_free(result);
        } else {
            duckdb_scalar_function_set_error(info, blobodbc_errmsg());
            return;
        }
    }
}

/* ── bo_query_in_catalog(conn_str, catalog, sql) -> JSON ─────────── */

static void odbc_query_in_catalog_func(duckdb_function_info info,
                                         duckdb_data_chunk input,
                                         duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector vec1 = duckdb_data_chunk_get_vector(input, 1);
    duckdb_vector vec2 = duckdb_data_chunk_get_vector(input, 2);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    duckdb_string_t *data1 = (duckdb_string_t *)duckdb_vector_get_data(vec1);
    duckdb_string_t *data2 = (duckdb_string_t *)duckdb_vector_get_data(vec2);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);
    uint64_t *val1 = duckdb_vector_get_validity(vec1);
    uint64_t *val2 = duckdb_vector_get_validity(vec2);

    for (idx_t row = 0; row < size; row++) {
        if ((val0 && !duckdb_validity_row_is_valid(val0, row)) ||
            (val1 && !duckdb_validity_row_is_valid(val1, row)) ||
            (val2 && !duckdb_validity_row_is_valid(val2, row))) {
            duckdb_vector_ensure_validity_writable(output);
            duckdb_validity_set_row_invalid(duckdb_vector_get_validity(output), row);
            continue;
        }
        char *conn    = str_dup_z(&data0[row]);
        char *catalog = str_dup_z(&data1[row]);
        char *sql     = str_dup_z(&data2[row]);
        char *result  = blobodbc_query_json_in_catalog(conn, catalog, sql);
        free(conn); free(catalog); free(sql);
        if (result) {
            duckdb_vector_assign_string_element(output, row, result);
            blobodbc_free(result);
        } else {
            duckdb_scalar_function_set_error(info, blobodbc_errmsg());
            return;
        }
    }
}

/* ── bo_primary_keys(conn_str, schema, table) -> JSON ────────────── */

static void odbc_primary_keys_func(duckdb_function_info info,
                                     duckdb_data_chunk input,
                                     duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector vec1 = duckdb_data_chunk_get_vector(input, 1);
    duckdb_vector vec2 = duckdb_data_chunk_get_vector(input, 2);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    duckdb_string_t *data1 = (duckdb_string_t *)duckdb_vector_get_data(vec1);
    duckdb_string_t *data2 = (duckdb_string_t *)duckdb_vector_get_data(vec2);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);
    uint64_t *val1 = duckdb_vector_get_validity(vec1);
    uint64_t *val2 = duckdb_vector_get_validity(vec2);

    for (idx_t row = 0; row < size; row++) {
        if ((val0 && !duckdb_validity_row_is_valid(val0, row)) ||
            (val1 && !duckdb_validity_row_is_valid(val1, row)) ||
            (val2 && !duckdb_validity_row_is_valid(val2, row))) {
            duckdb_vector_ensure_validity_writable(output);
            duckdb_validity_set_row_invalid(duckdb_vector_get_validity(output), row);
            continue;
        }
        char *conn   = str_dup_z(&data0[row]);
        char *schema = str_dup_z(&data1[row]);
        char *table  = str_dup_z(&data2[row]);
        char *result = blobodbc_primary_keys(conn, NULL, schema, table);
        free(conn); free(schema); free(table);
        if (result) {
            duckdb_vector_assign_string_element(output, row, result);
            blobodbc_free(result);
        } else {
            duckdb_scalar_function_set_error(info, blobodbc_errmsg());
            return;
        }
    }
}

/* ── bo_foreign_keys(conn_str, schema, table) -> JSON ────────────── */
/* Returns FKs defined ON the specified table (FK side).              */

static void odbc_foreign_keys_func(duckdb_function_info info,
                                     duckdb_data_chunk input,
                                     duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector vec1 = duckdb_data_chunk_get_vector(input, 1);
    duckdb_vector vec2 = duckdb_data_chunk_get_vector(input, 2);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    duckdb_string_t *data1 = (duckdb_string_t *)duckdb_vector_get_data(vec1);
    duckdb_string_t *data2 = (duckdb_string_t *)duckdb_vector_get_data(vec2);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);
    uint64_t *val1 = duckdb_vector_get_validity(vec1);
    uint64_t *val2 = duckdb_vector_get_validity(vec2);

    for (idx_t row = 0; row < size; row++) {
        if ((val0 && !duckdb_validity_row_is_valid(val0, row)) ||
            (val1 && !duckdb_validity_row_is_valid(val1, row)) ||
            (val2 && !duckdb_validity_row_is_valid(val2, row))) {
            duckdb_vector_ensure_validity_writable(output);
            duckdb_validity_set_row_invalid(duckdb_vector_get_validity(output), row);
            continue;
        }
        char *conn   = str_dup_z(&data0[row]);
        char *schema = str_dup_z(&data1[row]);
        char *table  = str_dup_z(&data2[row]);
        /* FK table specified → "what does this table reference?" */
        char *result = blobodbc_foreign_keys(conn,
            NULL, NULL, NULL,           /* PK side: no filter */
            NULL, schema, table);       /* FK side: this table */
        free(conn); free(schema); free(table);
        if (result) {
            duckdb_vector_assign_string_element(output, row, result);
            blobodbc_free(result);
        } else {
            duckdb_scalar_function_set_error(info, blobodbc_errmsg());
            return;
        }
    }
}

/* ── bo_foreign_keys(conn_str, fk_schema, fk_table, pk_schema, pk_table) ── */
/* Full control: specify both sides of the FK relationship.                      */

static void odbc_foreign_keys_func_5(duckdb_function_info info,
                                       duckdb_data_chunk input,
                                       duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector vec1 = duckdb_data_chunk_get_vector(input, 1);
    duckdb_vector vec2 = duckdb_data_chunk_get_vector(input, 2);
    duckdb_vector vec3 = duckdb_data_chunk_get_vector(input, 3);
    duckdb_vector vec4 = duckdb_data_chunk_get_vector(input, 4);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    duckdb_string_t *data1 = (duckdb_string_t *)duckdb_vector_get_data(vec1);
    duckdb_string_t *data2 = (duckdb_string_t *)duckdb_vector_get_data(vec2);
    duckdb_string_t *data3 = (duckdb_string_t *)duckdb_vector_get_data(vec3);
    duckdb_string_t *data4 = (duckdb_string_t *)duckdb_vector_get_data(vec4);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);
    uint64_t *val1 = duckdb_vector_get_validity(vec1);
    uint64_t *val2 = duckdb_vector_get_validity(vec2);
    uint64_t *val3 = duckdb_vector_get_validity(vec3);
    uint64_t *val4 = duckdb_vector_get_validity(vec4);

    for (idx_t row = 0; row < size; row++) {
        if ((val0 && !duckdb_validity_row_is_valid(val0, row)) ||
            (val1 && !duckdb_validity_row_is_valid(val1, row)) ||
            (val2 && !duckdb_validity_row_is_valid(val2, row)) ||
            (val3 && !duckdb_validity_row_is_valid(val3, row)) ||
            (val4 && !duckdb_validity_row_is_valid(val4, row))) {
            duckdb_vector_ensure_validity_writable(output);
            duckdb_validity_set_row_invalid(duckdb_vector_get_validity(output), row);
            continue;
        }
        char *conn      = str_dup_z(&data0[row]);
        char *fk_schema = str_dup_z(&data1[row]);
        char *fk_table  = str_dup_z(&data2[row]);
        char *pk_schema = str_dup_z(&data3[row]);
        char *pk_table  = str_dup_z(&data4[row]);
        char *result = blobodbc_foreign_keys(conn,
            NULL, pk_schema, pk_table,
            NULL, fk_schema, fk_table);
        free(conn); free(fk_schema); free(fk_table);
        free(pk_schema); free(pk_table);
        if (result) {
            duckdb_vector_assign_string_element(output, row, result);
            blobodbc_free(result);
        } else {
            duckdb_scalar_function_set_error(info, blobodbc_errmsg());
            return;
        }
    }
}

/* ── bo_execute(conn_str, sql) -> INTEGER (affected rows) ────────── */

static void odbc_execute_func(duckdb_function_info info,
                                duckdb_data_chunk input,
                                duckdb_vector output) {
    idx_t size = duckdb_data_chunk_get_size(input);
    duckdb_vector vec0 = duckdb_data_chunk_get_vector(input, 0);
    duckdb_vector vec1 = duckdb_data_chunk_get_vector(input, 1);
    duckdb_string_t *data0 = (duckdb_string_t *)duckdb_vector_get_data(vec0);
    duckdb_string_t *data1 = (duckdb_string_t *)duckdb_vector_get_data(vec1);
    uint64_t *val0 = duckdb_vector_get_validity(vec0);
    uint64_t *val1 = duckdb_vector_get_validity(vec1);
    int32_t *result_data = (int32_t *)duckdb_vector_get_data(output);

    for (idx_t row = 0; row < size; row++) {
        if ((val0 && !duckdb_validity_row_is_valid(val0, row)) ||
            (val1 && !duckdb_validity_row_is_valid(val1, row))) {
            duckdb_vector_ensure_validity_writable(output);
            duckdb_validity_set_row_invalid(duckdb_vector_get_validity(output), row);
            continue;
        }
        char *conn = str_dup_z(&data0[row]);
        char *sql  = str_dup_z(&data1[row]);
        int rc = blobodbc_execute(conn, sql);
        free(conn); free(sql);
        if (rc >= 0) {
            result_data[row] = rc;
        } else {
            duckdb_scalar_function_set_error(info, blobodbc_errmsg());
            return;
        }
    }
}

/* ── Register functions ──────────────────────────────────────────── */

static void register_functions(duckdb_connection connection) {
    duckdb_logical_type varchar_type = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);

    {
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "bo_query");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, varchar_type);
        duckdb_scalar_function_set_function(func, odbc_query_func);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
    }

    {
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "bo_clob");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, varchar_type);
        duckdb_scalar_function_set_function(func, odbc_clob_func);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
    }

    /* bo_query_named(conn_str, sql, bind_json) → JSON */
    {
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "bo_query_named");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, varchar_type);
        duckdb_scalar_function_set_extra_info(func, (void *)blobodbc_query_json_named, NULL);
        duckdb_scalar_function_set_function(func, named_func);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
    }

    /* bo_clob_named(conn_str, sql, bind_json) → TEXT */
    {
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "bo_clob_named");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, varchar_type);
        duckdb_scalar_function_set_extra_info(func, (void *)blobodbc_query_clob_named, NULL);
        duckdb_scalar_function_set_function(func, named_func);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
    }

    /* bo_driver_info(conn_str) → JSON */
    {
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "bo_driver_info");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, varchar_type);
        duckdb_scalar_function_set_function(func, odbc_driver_info_func);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
    }

    /* bo_tables(conn_str) → JSON (1-arg) */
    {
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "bo_tables");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, varchar_type);
        duckdb_scalar_function_set_function(func, odbc_tables_func_1);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
    }

    /* bo_tables(conn_str, schema) → JSON (2-arg) */
    {
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "bo_tables");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, varchar_type);
        duckdb_scalar_function_set_function(func, odbc_tables_func_2);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
    }

    /* bo_tables(conn_str, schema, type) → JSON (3-arg) */
    {
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "bo_tables");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, varchar_type);
        duckdb_scalar_function_set_function(func, odbc_tables_func_3);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
    }

    /* bo_tables(conn_str, schema, type, catalog) → JSON (4-arg) */
    {
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "bo_tables");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, varchar_type);
        duckdb_scalar_function_set_function(func, odbc_tables_func_4);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
    }

    /* bo_columns(conn_str) → JSON (1-arg) */
    {
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "bo_columns");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, varchar_type);
        duckdb_scalar_function_set_function(func, odbc_columns_func_1);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
    }

    /* bo_columns(conn_str, schema) → JSON (2-arg) */
    {
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "bo_columns");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, varchar_type);
        duckdb_scalar_function_set_function(func, odbc_columns_func_2);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
    }

    /* bo_columns(conn_str, schema, table) → JSON (3-arg) */
    {
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "bo_columns");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, varchar_type);
        duckdb_scalar_function_set_function(func, odbc_columns_func_3);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
    }

    /* bo_query_in_catalog(conn_str, catalog, sql) → JSON */
    {
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "bo_query_in_catalog");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, varchar_type);
        duckdb_scalar_function_set_function(func, odbc_query_in_catalog_func);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
    }

    /* bo_primary_keys(conn_str, schema, table) → JSON */
    {
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "bo_primary_keys");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, varchar_type);
        duckdb_scalar_function_set_function(func, odbc_primary_keys_func);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
    }

    /* bo_foreign_keys(conn_str, schema, table) → JSON (3-arg: FKs on table) */
    {
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "bo_foreign_keys");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, varchar_type);
        duckdb_scalar_function_set_function(func, odbc_foreign_keys_func);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
    }

    /* bo_foreign_keys(conn_str, fk_schema, fk_table, pk_schema, pk_table) → JSON (5-arg) */
    {
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "bo_foreign_keys");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, varchar_type);
        duckdb_scalar_function_set_function(func, odbc_foreign_keys_func_5);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
    }

    /* bo_execute(conn_str, sql) → INTEGER */
    {
        duckdb_logical_type int_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "bo_execute");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, int_type);
        duckdb_scalar_function_set_function(func, odbc_execute_func);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
        duckdb_destroy_logical_type(&int_type);
    }

    duckdb_destroy_logical_type(&varchar_type);
}

/* ── Extension entrypoint ────────────────────────────────────────── */

DUCKDB_EXTENSION_ENTRYPOINT(duckdb_connection connection,
                             duckdb_extension_info info,
                             struct duckdb_extension_access *access) {
    register_functions(connection);
    return true;
}
