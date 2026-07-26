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
#include <stdint.h>

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

/* ── bo_query_table(conn_str, sql) -> TABLE (single-threaded) ─────────
 *
 * Table-valued variant of bo_query: yields the query's rows as VARCHAR
 * columns instead of a JSON string. The whole result is materialized at BIND
 * time (which is where the output schema is learned); the scan just streams
 * it. Pinned to ONE thread via duckdb_init_set_max_threads(1) — so it uses
 * exactly one ODBC connection AND a single shared cursor is race-free,
 * regardless of the surrounding plan's parallelism. Fetches per-cell straight
 * into DuckDB vectors (faster than server-side FOR JSON — see benchmarks). */

static void query_table_bind(duckdb_bind_info info) {
    duckdb_value p0 = duckdb_bind_get_parameter(info, 0);
    duckdb_value p1 = duckdb_bind_get_parameter(info, 1);
    char *cs  = duckdb_get_varchar(p0);
    char *sql = duckdb_get_varchar(p1);

    blobodbc_rowset *rs = blobodbc_query_rowset(cs, sql);

    duckdb_free(cs);
    duckdb_free(sql);
    duckdb_destroy_value(&p0);
    duckdb_destroy_value(&p1);

    if (!rs) {
        duckdb_bind_set_error(info, blobodbc_errmsg());
        return;
    }

    duckdb_logical_type vt = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    size_t ncols = blobodbc_rowset_ncols(rs);
    for (size_t i = 0; i < ncols; i++)
        duckdb_bind_add_result_column(info, blobodbc_rowset_colname(rs, i), vt);
    duckdb_destroy_logical_type(&vt);

    duckdb_bind_set_bind_data(info, rs, (duckdb_delete_callback_t)blobodbc_rowset_free);
}

static void query_table_init(duckdb_init_info info) {
    duckdb_init_set_max_threads(info, 1);        /* one thread -> one connection, one cursor */
    idx_t *cursor = (idx_t *)malloc(sizeof(idx_t));
    *cursor = 0;
    duckdb_init_set_init_data(info, cursor, free);
}

static void query_table_func(duckdb_function_info info, duckdb_data_chunk output) {
    blobodbc_rowset *rs = (blobodbc_rowset *)duckdb_function_get_bind_data(info);
    idx_t *cursor       = (idx_t *)duckdb_function_get_init_data(info);

    const idx_t nrows = (idx_t)blobodbc_rowset_nrows(rs);
    const idx_t ncols = (idx_t)blobodbc_rowset_ncols(rs);
    const idx_t cap   = duckdb_vector_size();
    const idx_t avail = nrows - *cursor;
    const idx_t count = avail < cap ? avail : cap;

    /* column-major fill: resolve the vector once per column, then loop rows */
    for (idx_t c = 0; c < ncols; c++) {
        duckdb_vector vec = duckdb_data_chunk_get_vector(output, c);
        duckdb_vector_ensure_validity_writable(vec);
        uint64_t *validity = duckdb_vector_get_validity(vec);
        for (idx_t r = 0; r < count; r++) {
            const char *v = blobodbc_rowset_value(rs, (size_t)(*cursor + r), (size_t)c);
            if (v) duckdb_vector_assign_string_element(vec, r, v);
            else   duckdb_validity_set_row_invalid(validity, r);
        }
    }
    *cursor += count;
    duckdb_data_chunk_set_size(output, count);
}

static void register_table_functions(duckdb_connection connection) {
    duckdb_table_function tf = duckdb_create_table_function();
    duckdb_table_function_set_name(tf, "bo_query_table");

    duckdb_logical_type vt = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_table_function_add_parameter(tf, vt);   /* conn_str */
    duckdb_table_function_add_parameter(tf, vt);   /* sql      */
    duckdb_destroy_logical_type(&vt);

    duckdb_table_function_set_bind(tf, query_table_bind);
    duckdb_table_function_set_init(tf, query_table_init);
    duckdb_table_function_set_function(tf, query_table_func);
    duckdb_register_table_function(connection, tf);
    duckdb_destroy_table_function(&tf);
}

/* ── bo_query_table_typed(conn_str, sql) -> TABLE (native columns) ────
 *
 * Like bo_query_table, but each column carries its native type (INT64→BIGINT,
 * DOUBLE, else VARCHAR). The result metadata is parsed once at bind into an
 * array of per-column writer function pointers; the scan calls writers[c] per
 * cell to write straight into the correctly-typed vector — no number→string
 * round-trip. Same single-thread / cached-connection properties as the VARCHAR
 * variant. */

typedef struct {
    duckdb_vector vec;
    void         *data;       /* duckdb_vector_get_data(vec) — fixed-width only */
    uint64_t     *validity;
} col_out;

typedef void (*cell_writer)(const col_out *o, idx_t orow,
                            const blobodbc_rowset_typed *rs, size_t srow, size_t col);

static void w_i64(const col_out *o, idx_t orow, const blobodbc_rowset_typed *rs, size_t srow, size_t col) {
    if (blobodbc_rt_is_null(rs, srow, col)) duckdb_validity_set_row_invalid(o->validity, orow);
    else ((int64_t *)o->data)[orow] = blobodbc_rt_i64(rs, srow, col);
}
static void w_f64(const col_out *o, idx_t orow, const blobodbc_rowset_typed *rs, size_t srow, size_t col) {
    if (blobodbc_rt_is_null(rs, srow, col)) duckdb_validity_set_row_invalid(o->validity, orow);
    else ((double *)o->data)[orow] = blobodbc_rt_f64(rs, srow, col);
}
static void w_str(const col_out *o, idx_t orow, const blobodbc_rowset_typed *rs, size_t srow, size_t col) {
    if (blobodbc_rt_is_null(rs, srow, col)) duckdb_validity_set_row_invalid(o->validity, orow);
    else duckdb_vector_assign_string_element(o->vec, orow, blobodbc_rt_str(rs, srow, col));
}
static cell_writer writer_for(int coltype) {
    switch (coltype) {
        case BLOBODBC_COL_INT64:  return w_i64;
        case BLOBODBC_COL_DOUBLE: return w_f64;
        default:                  return w_str;
    }
}
static duckdb_logical_type ltype_for(int coltype) {
    switch (coltype) {
        case BLOBODBC_COL_INT64:  return duckdb_create_logical_type(DUCKDB_TYPE_BIGINT);
        case BLOBODBC_COL_DOUBLE: return duckdb_create_logical_type(DUCKDB_TYPE_DOUBLE);
        default:                  return duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    }
}

typedef struct {
    blobodbc_rowset_typed *rs;
    cell_writer           *writers;   /* one per column */
} typed_bind_data;

static void typed_bind_destroy(void *p) {
    typed_bind_data *b = (typed_bind_data *)p;
    if (b) { blobodbc_rt_free(b->rs); free(b->writers); free(b); }
}

static void query_table_typed_bind(duckdb_bind_info info) {
    duckdb_value p0 = duckdb_bind_get_parameter(info, 0);
    duckdb_value p1 = duckdb_bind_get_parameter(info, 1);
    char *cs  = duckdb_get_varchar(p0);
    char *sql = duckdb_get_varchar(p1);

    blobodbc_rowset_typed *rs = blobodbc_query_rowset_typed(cs, sql);

    duckdb_free(cs);
    duckdb_free(sql);
    duckdb_destroy_value(&p0);
    duckdb_destroy_value(&p1);

    if (!rs) {
        duckdb_bind_set_error(info, blobodbc_errmsg());
        return;
    }

    size_t ncols = blobodbc_rt_ncols(rs);
    cell_writer *writers = (cell_writer *)malloc(sizeof(cell_writer) * (ncols ? ncols : 1));
    for (size_t i = 0; i < ncols; i++) {
        int ct = blobodbc_rt_coltype(rs, i);
        duckdb_logical_type lt = ltype_for(ct);
        duckdb_bind_add_result_column(info, blobodbc_rt_colname(rs, i), lt);
        duckdb_destroy_logical_type(&lt);
        writers[i] = writer_for(ct);
    }

    typed_bind_data *b = (typed_bind_data *)malloc(sizeof(typed_bind_data));
    b->rs = rs;
    b->writers = writers;
    duckdb_bind_set_bind_data(info, b, typed_bind_destroy);
}

static void query_table_typed_func(duckdb_function_info info, duckdb_data_chunk output) {
    typed_bind_data *b = (typed_bind_data *)duckdb_function_get_bind_data(info);
    idx_t *cursor      = (idx_t *)duckdb_function_get_init_data(info);
    const blobodbc_rowset_typed *rs = b->rs;

    const idx_t nrows = (idx_t)blobodbc_rt_nrows(rs);
    const idx_t ncols = (idx_t)blobodbc_rt_ncols(rs);
    const idx_t cap   = duckdb_vector_size();
    const idx_t avail = nrows - *cursor;
    const idx_t count = avail < cap ? avail : cap;

    for (idx_t c = 0; c < ncols; c++) {
        col_out o;
        o.vec      = duckdb_data_chunk_get_vector(output, c);
        o.data     = duckdb_vector_get_data(o.vec);
        duckdb_vector_ensure_validity_writable(o.vec);
        o.validity = duckdb_vector_get_validity(o.vec);
        cell_writer w = b->writers[c];
        for (idx_t r = 0; r < count; r++)
            w(&o, r, rs, (size_t)(*cursor + r), (size_t)c);
    }
    *cursor += count;
    duckdb_data_chunk_set_size(output, count);
}

static void register_table_functions_typed(duckdb_connection connection) {
    duckdb_table_function tf = duckdb_create_table_function();
    duckdb_table_function_set_name(tf, "bo_query_table_typed");

    duckdb_logical_type vt = duckdb_create_logical_type(DUCKDB_TYPE_VARCHAR);
    duckdb_table_function_add_parameter(tf, vt);   /* conn_str */
    duckdb_table_function_add_parameter(tf, vt);   /* sql      */
    duckdb_destroy_logical_type(&vt);

    duckdb_table_function_set_bind(tf, query_table_typed_bind);
    duckdb_table_function_set_init(tf, query_table_init);   /* shared: max_threads=1 + cursor */
    duckdb_table_function_set_function(tf, query_table_typed_func);
    duckdb_register_table_function(connection, tf);
    duckdb_destroy_table_function(&tf);
}

/* ── Extension entrypoint ────────────────────────────────────────── */

DUCKDB_EXTENSION_ENTRYPOINT(duckdb_connection connection,
                             duckdb_extension_info info,
                             struct duckdb_extension_access *access) {
    register_functions(connection);
    register_table_functions(connection);
    register_table_functions_typed(connection);
    return true;
}
