#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>

#include "blobodbc.h"

namespace nb = nanobind;

static nb::str py_query_json(const std::string &conn_str, const std::string &query) {
    char *result = blobodbc_query_json(conn_str.c_str(), query.c_str());
    if (!result) throw nb::value_error(blobodbc_errmsg());
    nb::str out(result);
    blobodbc_free(result);
    return out;
}

static nb::str py_query_json_named(const std::string &conn_str,
                                    const std::string &query,
                                    const std::string &bind_json) {
    char *result = blobodbc_query_json_named(conn_str.c_str(), query.c_str(),
                                              bind_json.c_str());
    if (!result) throw nb::value_error(blobodbc_errmsg());
    nb::str out(result);
    blobodbc_free(result);
    return out;
}

static nb::str py_query_clob(const std::string &conn_str, const std::string &query) {
    char *result = blobodbc_query_clob(conn_str.c_str(), query.c_str());
    if (!result) throw nb::value_error(blobodbc_errmsg());
    nb::str out(result);
    blobodbc_free(result);
    return out;
}

static nb::str py_query_clob_named(const std::string &conn_str,
                                    const std::string &query,
                                    const std::string &bind_json) {
    char *result = blobodbc_query_clob_named(conn_str.c_str(), query.c_str(),
                                              bind_json.c_str());
    if (!result) throw nb::value_error(blobodbc_errmsg());
    nb::str out(result);
    blobodbc_free(result);
    return out;
}

/* ── Experimental / test bindings ────────────────────────────────── */

static nb::str py_driver_info(const std::string &conn_str) {
    char *result = blobodbc_driver_info(conn_str.c_str());
    if (!result) throw nb::value_error(blobodbc_errmsg());
    nb::str out(result);
    blobodbc_free(result);
    return out;
}

static nb::str py_tables(const std::string &conn_str,
                          nb::object catalog,
                          nb::object schema,
                          nb::object type) {
    std::string cat_s, sch_s, typ_s;
    const char *cat = nullptr, *sch = nullptr, *typ = nullptr;
    if (!catalog.is_none()) { cat_s = nb::cast<std::string>(catalog); cat = cat_s.c_str(); }
    if (!schema.is_none())  { sch_s = nb::cast<std::string>(schema);  sch = sch_s.c_str(); }
    if (!type.is_none())    { typ_s = nb::cast<std::string>(type);    typ = typ_s.c_str(); }

    char *result = blobodbc_tables(conn_str.c_str(), cat, sch, typ);
    if (!result) throw nb::value_error(blobodbc_errmsg());
    nb::str out(result);
    blobodbc_free(result);
    return out;
}

static nb::str py_columns(const std::string &conn_str,
                           nb::object catalog,
                           nb::object schema,
                           nb::object table) {
    std::string cat_s, sch_s, tbl_s;
    const char *cat = nullptr, *sch = nullptr, *tbl = nullptr;
    if (!catalog.is_none()) { cat_s = nb::cast<std::string>(catalog); cat = cat_s.c_str(); }
    if (!schema.is_none())  { sch_s = nb::cast<std::string>(schema);  sch = sch_s.c_str(); }
    if (!table.is_none())   { tbl_s = nb::cast<std::string>(table);   tbl = tbl_s.c_str(); }

    char *result = blobodbc_columns(conn_str.c_str(), cat, sch, tbl);
    if (!result) throw nb::value_error(blobodbc_errmsg());
    nb::str out(result);
    blobodbc_free(result);
    return out;
}

static nb::str py_query_in_catalog(const std::string &conn_str,
                                    const std::string &catalog,
                                    const std::string &query) {
    char *result = blobodbc_query_json_in_catalog(conn_str.c_str(),
                                                    catalog.c_str(),
                                                    query.c_str());
    if (!result) throw nb::value_error(blobodbc_errmsg());
    nb::str out(result);
    blobodbc_free(result);
    return out;
}

static int py_execute(const std::string &conn_str, const std::string &sql) {
    int rc = blobodbc_execute(conn_str.c_str(), sql.c_str());
    if (rc < 0) throw nb::value_error(blobodbc_errmsg());
    return rc;
}

static nb::str py_primary_keys(const std::string &conn_str,
                                const std::string &schema,
                                const std::string &table) {
    char *result = blobodbc_primary_keys(conn_str.c_str(), nullptr,
                                          schema.c_str(), table.c_str());
    if (!result) throw nb::value_error(blobodbc_errmsg());
    nb::str out(result);
    blobodbc_free(result);
    return out;
}

static nb::str py_foreign_keys(const std::string &conn_str,
                                const std::string &fk_schema,
                                const std::string &fk_table,
                                nb::object pk_schema,
                                nb::object pk_table) {
    std::string pks_s, pkt_s;
    const char *pks = nullptr, *pkt = nullptr;
    if (!pk_schema.is_none()) { pks_s = nb::cast<std::string>(pk_schema); pks = pks_s.c_str(); }
    if (!pk_table.is_none())  { pkt_s = nb::cast<std::string>(pk_table);  pkt = pkt_s.c_str(); }

    char *result = blobodbc_foreign_keys(conn_str.c_str(),
        nullptr, pks, pkt,
        nullptr, fk_schema.c_str(), fk_table.c_str());
    if (!result) throw nb::value_error(blobodbc_errmsg());
    nb::str out(result);
    blobodbc_free(result);
    return out;
}

NB_MODULE(blobodbc_ext, m) {
    m.doc() = "ODBC query execution returning JSON or text results";

    m.def("query_json", &py_query_json,
          nb::arg("conn_str"), nb::arg("query"),
          "Execute an ODBC query, return result set as JSON array of objects");

    m.def("query_json", &py_query_json_named,
          nb::arg("conn_str"), nb::arg("query"), nb::arg("bind_json"),
          "Execute an ODBC query with named :param placeholders bound from JSON object");

    m.def("query_clob", &py_query_clob,
          nb::arg("conn_str"), nb::arg("query"),
          "Execute an ODBC query, return first column of first row as text");

    m.def("query_clob", &py_query_clob_named,
          nb::arg("conn_str"), nb::arg("query"), nb::arg("bind_json"),
          "Execute an ODBC query (CLOB) with named :param placeholders bound from JSON object");

    /* Experimental — for testing and out-of-band exploration.
     * These wrap ODBC catalog/metadata functions and are not part of the
     * stable API.  Prefer the DuckDB or SQLite extension for production use. */

    m.def("driver_info", &py_driver_info,
          nb::arg("conn_str"),
          "Return ODBC driver metadata as a JSON string (SQLGetInfo + SQLGetTypeInfo + SQLGetFunctions + SQLDrivers + catalog enumeration)");

    m.def("tables", &py_tables,
          nb::arg("conn_str"),
          nb::arg("catalog") = nb::none(),
          nb::arg("schema") = nb::none(),
          nb::arg("type") = nb::none(),
          "Return tables via SQLTables as a JSON array. All filters optional.");

    m.def("columns", &py_columns,
          nb::arg("conn_str"),
          nb::arg("catalog") = nb::none(),
          nb::arg("schema") = nb::none(),
          nb::arg("table") = nb::none(),
          "Return columns via SQLColumns as a JSON array. All filters optional.");

    m.def("query_in_catalog", &py_query_in_catalog,
          nb::arg("conn_str"), nb::arg("catalog"), nb::arg("query"),
          "Execute a query after switching to a specific catalog (database), then restore the original catalog.");

    m.def("execute", &py_execute,
          nb::arg("conn_str"), nb::arg("sql"),
          "Execute a SQL statement (DDL/DML) without a result set. Returns affected row count.");

    m.def("primary_keys", &py_primary_keys,
          nb::arg("conn_str"), nb::arg("schema"), nb::arg("table"),
          "Return primary key columns via SQLPrimaryKeys as a JSON array.");

    m.def("foreign_keys", &py_foreign_keys,
          nb::arg("conn_str"), nb::arg("fk_schema"), nb::arg("fk_table"),
          nb::arg("pk_schema") = nb::none(),
          nb::arg("pk_table") = nb::none(),
          "Return foreign keys via SQLForeignKeys as a JSON array. "
          "fk_schema/fk_table specify the FK side; pk_schema/pk_table optionally filter the PK side.");
}
