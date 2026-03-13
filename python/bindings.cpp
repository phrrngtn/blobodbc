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
}
