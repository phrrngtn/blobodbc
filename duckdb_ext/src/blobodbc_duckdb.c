/*
 * DuckDB C API extension for ODBC queries.
 *
 * Registers:
 *   odbc_query(conn_str, sql)                    -> VARCHAR (JSON)
 *   odbc_query(conn_str, sql, jmespath)          -> VARCHAR (JSON, jmespath stub)
 *   odbc_clob(conn_str, sql)                     -> VARCHAR
 *   odbc_query_named(conn_str, sql, bind_json)   -> VARCHAR (JSON)
 *   odbc_query_named(conn_str, sql, bind_json, jmespath) -> VARCHAR (JSON, jmespath stub)
 *   odbc_clob_named(conn_str, sql, bind_json)    -> VARCHAR
 *   odbc_driver_info(conn_str)                   -> VARCHAR (JSON)
 */

#define DUCKDB_EXTENSION_NAME blobodbc
#include "duckdb_extension.h"

#include "blobodbc.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Embedded catalog query data (generated at build time from catalog YAML files) */
extern const unsigned char blobodbc_catalog_json[];
extern const unsigned int blobodbc_catalog_json_len;

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

/* ── odbc_query(conn_str, sql) -> JSON ───────────────────────────── */

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

/* ── odbc_query(conn_str, sql, jmespath) -> JSON ─────────────────── */

static void odbc_query_jmespath_func(duckdb_function_info info,
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

        /* jmespath: empty string means no reshaping */
        uint32_t jmes_len;
        str_ptr(&data2[row], &jmes_len);
        if (jmes_len > 0) {
            duckdb_scalar_function_set_error(info,
                "jmespath parameter not yet implemented — pass empty string");
            return;
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

/* ── odbc_clob(conn_str, sql) -> TEXT ────────────────────────────── */

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

/* ── 4-arg named-parameter + jmespath variant ────────────────────── */

static void named_jmespath_func(duckdb_function_info info,
                                 duckdb_data_chunk input,
                                 duckdb_vector output) {
    named_query_fn fn = (named_query_fn)duckdb_scalar_function_get_extra_info(info);
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

        /* jmespath: empty string means no reshaping */
        uint32_t jmes_len;
        str_ptr(&data3[row], &jmes_len);
        if (jmes_len > 0) {
            duckdb_scalar_function_set_error(info,
                "jmespath parameter not yet implemented — pass empty string");
            return;
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

/* ── odbc_driver_info(conn_str) -> JSON ───────────────────────────── */

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

/* ── odbc_tables: 1–4 arg overloads ──────────────────────────────── */
/*   odbc_tables(conn_str)                          — all tables      */
/*   odbc_tables(conn_str, schema)                  — schema filter   */
/*   odbc_tables(conn_str, schema, type)            — + type filter   */
/*   odbc_tables(conn_str, schema, type, catalog)   — + catalog       */

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

/* ── odbc_columns: 1–3 arg overloads ────────────────────────────── */
/*   odbc_columns(conn_str)                         — all columns     */
/*   odbc_columns(conn_str, schema)                 — schema filter   */
/*   odbc_columns(conn_str, schema, table)          — + table filter  */

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

/* ── odbc_query_in_catalog(conn_str, catalog, sql) -> JSON ───────── */

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

/* ── odbc_primary_keys(conn_str, schema, table) -> JSON ──────────── */

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

/* ── odbc_foreign_keys(conn_str, schema, table) -> JSON ──────────── */
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

/* ── odbc_foreign_keys(conn_str, fk_schema, fk_table, pk_schema, pk_table) ── */
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

/* ── odbc_execute(conn_str, sql) -> INTEGER (affected rows) ──────── */

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
        duckdb_scalar_function_set_name(func, "odbc_query");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, varchar_type);
        duckdb_scalar_function_set_function(func, odbc_query_func);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
    }

    /* odbc_query(conn_str, sql, jmespath) → JSON (3-arg overload) */
    {
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "odbc_query");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, varchar_type);
        duckdb_scalar_function_set_function(func, odbc_query_jmespath_func);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
    }

    {
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "odbc_clob");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, varchar_type);
        duckdb_scalar_function_set_function(func, odbc_clob_func);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
    }

    /* odbc_query_named(conn_str, sql, bind_json) → JSON */
    {
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "odbc_query_named");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, varchar_type);
        duckdb_scalar_function_set_extra_info(func, (void *)blobodbc_query_json_named, NULL);
        duckdb_scalar_function_set_function(func, named_func);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
    }

    /* odbc_query_named(conn_str, sql, bind_json, jmespath) → JSON (4-arg) */
    {
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "odbc_query_named");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, varchar_type);
        duckdb_scalar_function_set_extra_info(func, (void *)blobodbc_query_json_named, NULL);
        duckdb_scalar_function_set_function(func, named_jmespath_func);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
    }

    /* odbc_clob_named(conn_str, sql, bind_json) → TEXT */
    {
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "odbc_clob_named");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, varchar_type);
        duckdb_scalar_function_set_extra_info(func, (void *)blobodbc_query_clob_named, NULL);
        duckdb_scalar_function_set_function(func, named_func);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
    }

    /* odbc_driver_info(conn_str) → JSON */
    {
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "odbc_driver_info");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, varchar_type);
        duckdb_scalar_function_set_function(func, odbc_driver_info_func);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
    }

    /* odbc_tables(conn_str) → JSON (1-arg) */
    {
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "odbc_tables");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, varchar_type);
        duckdb_scalar_function_set_function(func, odbc_tables_func_1);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
    }

    /* odbc_tables(conn_str, schema) → JSON (2-arg) */
    {
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "odbc_tables");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, varchar_type);
        duckdb_scalar_function_set_function(func, odbc_tables_func_2);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
    }

    /* odbc_tables(conn_str, schema, type) → JSON (3-arg) */
    {
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "odbc_tables");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, varchar_type);
        duckdb_scalar_function_set_function(func, odbc_tables_func_3);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
    }

    /* odbc_tables(conn_str, schema, type, catalog) → JSON (4-arg) */
    {
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "odbc_tables");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, varchar_type);
        duckdb_scalar_function_set_function(func, odbc_tables_func_4);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
    }

    /* odbc_columns(conn_str) → JSON (1-arg) */
    {
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "odbc_columns");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, varchar_type);
        duckdb_scalar_function_set_function(func, odbc_columns_func_1);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
    }

    /* odbc_columns(conn_str, schema) → JSON (2-arg) */
    {
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "odbc_columns");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, varchar_type);
        duckdb_scalar_function_set_function(func, odbc_columns_func_2);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
    }

    /* odbc_columns(conn_str, schema, table) → JSON (3-arg) */
    {
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "odbc_columns");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, varchar_type);
        duckdb_scalar_function_set_function(func, odbc_columns_func_3);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
    }

    /* odbc_query_in_catalog(conn_str, catalog, sql) → JSON */
    {
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "odbc_query_in_catalog");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, varchar_type);
        duckdb_scalar_function_set_function(func, odbc_query_in_catalog_func);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
    }

    /* odbc_primary_keys(conn_str, schema, table) → JSON */
    {
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "odbc_primary_keys");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, varchar_type);
        duckdb_scalar_function_set_function(func, odbc_primary_keys_func);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
    }

    /* odbc_foreign_keys(conn_str, schema, table) → JSON (3-arg: FKs on table) */
    {
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "odbc_foreign_keys");
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_add_parameter(func, varchar_type);
        duckdb_scalar_function_set_return_type(func, varchar_type);
        duckdb_scalar_function_set_function(func, odbc_foreign_keys_func);
        duckdb_register_scalar_function(connection, func);
        duckdb_destroy_scalar_function(&func);
    }

    /* odbc_foreign_keys(conn_str, fk_schema, fk_table, pk_schema, pk_table) → JSON (5-arg) */
    {
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "odbc_foreign_keys");
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

    /* odbc_execute(conn_str, sql) → INTEGER */
    {
        duckdb_logical_type int_type = duckdb_create_logical_type(DUCKDB_TYPE_INTEGER);
        duckdb_scalar_function func = duckdb_create_scalar_function();
        duckdb_scalar_function_set_name(func, "odbc_execute");
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

/* ── Populate catalog_queries table ──────────────────────────────── */

static void populate_catalog_queries(duckdb_connection connection) {
    duckdb_state state;
    duckdb_result result;

    /* Create schema and table if they don't exist */
    state = duckdb_query(connection,
        "CREATE SCHEMA IF NOT EXISTS blobodbc", &result);
    duckdb_destroy_result(&result);
    if (state == DuckDBError) return;

    state = duckdb_query(connection,
        "CREATE TABLE IF NOT EXISTS blobodbc.catalog_queries ("
        "  dialect VARCHAR NOT NULL,"
        "  name VARCHAR NOT NULL,"
        "  description VARCHAR,"
        "  sql VARCHAR NOT NULL,"
        "  parameters JSON,"
        "  where_fragments JSON,"
        "  PRIMARY KEY (dialect, name)"
        ")", &result);
    duckdb_destroy_result(&result);
    if (state == DuckDBError) return;

    /* Insert the embedded JSON blob as a single value, then
     * unnest it into rows.  This is a single SQL round-trip
     * rather than N prepared-statement executions. */
    duckdb_prepared_statement stmt;
    state = duckdb_prepare(connection,
        "INSERT OR REPLACE INTO blobodbc.catalog_queries "
        "SELECT "
        "  e->>'dialect' AS dialect,"
        "  e->>'name' AS name,"
        "  e->>'description' AS description,"
        "  e->>'sql' AS sql,"
        "  e->'parameters' AS parameters,"
        "  e->'where_fragments' AS where_fragments "
        "FROM (SELECT unnest(from_json($1::JSON, '[\"json\"]')) AS e)",
        &stmt);
    if (state == DuckDBError) return;

    duckdb_bind_varchar(stmt, 1, (const char *)blobodbc_catalog_json);
    state = duckdb_execute_prepared(stmt, &result);
    duckdb_destroy_result(&result);
    duckdb_destroy_prepare(&stmt);
}

/* ── Create derived views ────────────────────────────────────────── */

static void create_views(duckdb_connection connection) {
    duckdb_state state;
    duckdb_result result;

    /* changelog: human-readable schema change log derived from the
     * TTST reverse patch chain.  Flips reverse-patch semantics to
     * show forward changes (ADD/ALTER/DROP) and parses JSON Pointer
     * paths into schema/table/column/attribute components.
     *
     * Depends on schema_snapshot_patch (created by the intern pipeline).
     * The view is CREATE OR REPLACE so it's safe to re-run on every load.
     * If the table doesn't exist yet, the view is created but will error
     * on query — which is fine, it just means no samples have been interned. */
    state = duckdb_query(connection,
        "CREATE OR REPLACE VIEW blobodbc.changelog AS "
        "WITH PATCHES AS ("
        "    SELECT"
        "        p.dataserver_id,"
        "        p.catalog_name,"
        "        p.schema_name,"
        "        p.kind,"
        "        p.revision_num,"
        "        p.captured_at,"
        "        unnest(from_json(p.patch::JSON, '[\"json\"]')) AS op"
        "    FROM schema_snapshot_patch AS p"
        ") "
        "SELECT"
        "    p.dataserver_id,"
        "    p.catalog_name,"
        "    p.schema_name AS sample_schema,"
        "    p.captured_at,"
        "    p.kind,"
        "    p.revision_num,"
        "    CASE op->>'op'"
        "        WHEN 'remove' THEN 'ADD'"
        "        WHEN 'add'    THEN 'DROP'"
        "        WHEN 'replace' THEN 'ALTER'"
        "    END AS change_type,"
        "    op->>'path' AS path,"
        "    split_part(op->>'path', '/', 2) AS object_schema,"
        "    CASE WHEN length(op->>'path') - length(replace(op->>'path', '/', '')) >= 2"
        "         THEN split_part(op->>'path', '/', 3)"
        "         ELSE NULL"
        "    END AS table_name,"
        "    CASE WHEN length(op->>'path') - length(replace(op->>'path', '/', '')) >= 3"
        "         THEN split_part(op->>'path', '/', 4)"
        "         ELSE NULL"
        "    END AS column_or_constraint,"
        "    CASE WHEN length(op->>'path') - length(replace(op->>'path', '/', '')) >= 4"
        "         THEN split_part(op->>'path', '/', 5)"
        "         ELSE NULL"
        "    END AS attribute,"
        "    CASE op->>'op'"
        "        WHEN 'replace' THEN op->>'value'"
        "        ELSE NULL"
        "    END AS old_value"
        " FROM PATCHES AS p",
        &result);
    duckdb_destroy_result(&result);
}

/* ── Extension entrypoint ────────────────────────────────────────── */

DUCKDB_EXTENSION_ENTRYPOINT(duckdb_connection connection,
                             duckdb_extension_info info,
                             struct duckdb_extension_access *access) {
    register_functions(connection);
    populate_catalog_queries(connection);
    create_views(connection);
    return true;
}
