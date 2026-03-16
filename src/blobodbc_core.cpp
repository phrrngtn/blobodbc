#include "blobodbc.h"

#include <nanodbc/nanodbc.h>

#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpatch/jsonpatch.hpp>

#include <sql.h>
#include <sqlext.h>

#include <cstring>
#include <string>
#include <vector>

using json = jsoncons::json;

static thread_local std::string g_errmsg;

static char *strdup_result(const std::string &s) {
    char *out = (char *)malloc(s.size() + 1);
    if (out) memcpy(out, s.data(), s.size() + 1);
    return out;
}

/* ── Column type dispatch ────────────────────────────────────────── */

using column_getter = void (*)(json &, nanodbc::result &, short);

static void get_int_value(json &jv, nanodbc::result &r, short col) {
    jv = r.get<long long>(col);
}

static void get_float_value(json &jv, nanodbc::result &r, short col) {
    jv = r.get<double>(col);
}

static void get_string_value(json &jv, nanodbc::result &r, short col) {
    jv = r.get<std::string>(col);
}

static void get_date_value(json &jv, nanodbc::result &r, short col) {
    nanodbc::date d = r.get<nanodbc::date>(col);
    char buf[32];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", d.year, d.month, d.day);
    jv = std::string(buf);
}

static void get_timestamp_value(json &jv, nanodbc::result &r, short col) {
    nanodbc::timestamp ts = r.get<nanodbc::timestamp>(col);
    char buf[64];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d.%09d",
             ts.year, ts.month, ts.day,
             ts.hour, ts.min, ts.sec, ts.fract);
    jv = std::string(buf);
}

static void get_null_value(json &jv, nanodbc::result &, short) {
    jv = json::null();
}

static column_getter getter_for_column(nanodbc::result &r, short col) {
    int sql_type = r.column_datatype(col);
    switch (sql_type) {
        /* Integers */
        case -7: /* SQL_BIT */
        case -6: /* SQL_TINYINT */
        case  5: /* SQL_SMALLINT */
        case  4: /* SQL_INTEGER */
        case -5: /* SQL_BIGINT */
            return get_int_value;

        /* Floating point / numeric */
        case  6: /* SQL_FLOAT */
        case  7: /* SQL_REAL */
        case  8: /* SQL_DOUBLE */
        case  2: /* SQL_NUMERIC */
        case  3: /* SQL_DECIMAL */
            return get_float_value;

        /* Strings */
        case  1: /* SQL_CHAR */
        case 12: /* SQL_VARCHAR */
        case -1: /* SQL_LONGVARCHAR */
        case -8: /* SQL_WCHAR */
        case -9: /* SQL_WVARCHAR */
        case -10: /* SQL_WLONGVARCHAR */
        case -11: /* SQL_GUID */
            return get_string_value;

        /* Dates and timestamps */
        case 91: /* SQL_TYPE_DATE */
            return get_date_value;
        case 93: /* SQL_TYPE_TIMESTAMP */
            return get_timestamp_value;

        /* Binary — return null rather than crash */
        case -2: /* SQL_BINARY */
        case -3: /* SQL_VARBINARY */
        case -4: /* SQL_LONGVARBINARY */
        default:
            return get_null_value;
    }
}

/* ── Result set → JSON ───────────────────────────────────────────── */

static std::string result_to_json(nanodbc::result &result) {
    int n = result.columns();
    std::vector<std::string> names(n);
    std::vector<column_getter> getters(n);

    for (int i = 0; i < n; i++) {
        names[i] = result.column_name(i);
        getters[i] = getter_for_column(result, i);
    }

    json rows(jsoncons::json_array_arg);

    while (result.next()) {
        json row(jsoncons::json_object_arg);
        for (int i = 0; i < n; i++) {
            json val;
            if (result.is_null(i)) {
                val = json::null();
            } else {
                getters[i](val, result, i);
            }
            row.insert_or_assign(names[i], std::move(val));
        }
        rows.push_back(std::move(row));
    }

    return rows.to_string();
}

/* ── Result set → CLOB (first column of first row) ───────────────── */

static std::string result_to_clob(nanodbc::result &result) {
    std::string clob;
    if (result.next()) {
        clob = result.get<std::string>(0);
        /* Consume remaining rows */
        while (result.next()) {}
    }
    return clob;
}

/* ── Execute helpers ─────────────────────────────────────────────── */

static nanodbc::result execute_query(const char *conn_str, const char *query,
                                      const char **bind_values, int bind_count) {
    nanodbc::connection conn(conn_str);

    if (bind_count > 0 && bind_values) {
        nanodbc::statement stmt(conn);
        nanodbc::prepare(stmt, query);
        std::vector<std::string> vals(bind_count);
        for (int i = 0; i < bind_count; i++) {
            vals[i] = bind_values[i];
            stmt.bind(i, vals[i].c_str());
        }
        return nanodbc::execute(stmt);
    }

    return nanodbc::execute(conn, query);
}

/* ── Named parameter rewriting ───────────────────────────────────
 *
 * Rewrites :name placeholders to positional ? markers and builds
 * the corresponding bind-value array from a JSON object.
 *
 * Rules:
 *   - :name matches [a-zA-Z_][a-zA-Z0-9_]* after a ':'
 *   - ':' inside single-quoted strings is not treated as a placeholder
 *   - The same :name can appear multiple times; each occurrence gets
 *     its own positional ? and bind slot
 *   - JSON null values bind as SQL NULL via nanodbc::null_type
 */

struct NamedBindInfo {
    std::string rewritten_sql;
    std::vector<std::string> values;
    std::vector<bool> is_null;
};

static bool is_ident_start(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
}

static bool is_ident_char(char c) {
    return is_ident_start(c) || (c >= '0' && c <= '9');
}

static NamedBindInfo rewrite_named_params(const char *sql, const json &binds) {
    NamedBindInfo info;
    std::string &out = info.rewritten_sql;
    const char *p = sql;
    bool in_quote = false;

    while (*p) {
        if (*p == '\'') {
            in_quote = !in_quote;
            out += *p++;
        } else if (!in_quote && *p == ':' && is_ident_start(p[1])) {
            /* Extract parameter name */
            const char *start = p + 1;
            const char *end = start;
            while (is_ident_char(*end)) end++;
            std::string name(start, end - start);
            p = end;

            out += '?';

            if (binds.contains(name)) {
                const auto &val = binds[name];
                if (val.is_null()) {
                    info.values.emplace_back();
                    info.is_null.push_back(true);
                } else if (val.is_string()) {
                    info.values.push_back(val.as<std::string>());
                    info.is_null.push_back(false);
                } else {
                    /* numbers, booleans — convert to string */
                    info.values.push_back(val.to_string());
                    info.is_null.push_back(false);
                }
            } else {
                /* Missing key — bind as NULL */
                info.values.emplace_back();
                info.is_null.push_back(true);
            }
        } else {
            out += *p++;
        }
    }

    return info;
}

static nanodbc::result execute_named(const char *conn_str, const char *query,
                                      const char *bind_json) {
    json binds = json::parse(bind_json);
    auto info = rewrite_named_params(query, binds);

    nanodbc::connection conn(conn_str);
    nanodbc::statement stmt(conn);
    nanodbc::prepare(stmt, info.rewritten_sql);

    for (size_t i = 0; i < info.values.size(); i++) {
        if (info.is_null[i]) {
            stmt.bind_null(static_cast<short>(i));
        } else {
            stmt.bind(static_cast<short>(i), info.values[i].c_str());
        }
    }

    return nanodbc::execute(stmt);
}

/* ── Driver metadata (SQLGetInfo + SQLGetTypeInfo) ───────────────── */

struct InfoEntry {
    SQLUSMALLINT type_id;
    const char  *name;
    char         category;   /* 's' = string, 'i' = SQLUINTEGER, 'h' = SQLUSMALLINT */
};

static const InfoEntry g_info_catalog[] = {
    /* Driver / DBMS identification */
    {SQL_DATA_SOURCE_NAME,         "SQL_DATA_SOURCE_NAME",        's'},
    {SQL_DRIVER_NAME,              "SQL_DRIVER_NAME",             's'},
    {SQL_DRIVER_VER,               "SQL_DRIVER_VER",              's'},
    {SQL_DRIVER_ODBC_VER,          "SQL_DRIVER_ODBC_VER",         's'},
    {SQL_DBMS_NAME,                "SQL_DBMS_NAME",               's'},
    {SQL_DBMS_VER,                 "SQL_DBMS_VER",                's'},
    {SQL_DATABASE_NAME,            "SQL_DATABASE_NAME",           's'},
    {SQL_SERVER_NAME,              "SQL_SERVER_NAME",             's'},
    {SQL_USER_NAME,                "SQL_USER_NAME",               's'},

    /* Catalog / schema / naming */
    {SQL_CATALOG_NAME_SEPARATOR,   "SQL_CATALOG_NAME_SEPARATOR",  's'},
    {SQL_CATALOG_TERM,             "SQL_CATALOG_TERM",            's'},
    {SQL_SCHEMA_TERM,              "SQL_SCHEMA_TERM",             's'},
    {SQL_TABLE_TERM,               "SQL_TABLE_TERM",              's'},
    {SQL_PROCEDURE_TERM,           "SQL_PROCEDURE_TERM",          's'},
    {SQL_IDENTIFIER_QUOTE_CHAR,    "SQL_IDENTIFIER_QUOTE_CHAR",   's'},
    {SQL_SEARCH_PATTERN_ESCAPE,    "SQL_SEARCH_PATTERN_ESCAPE",   's'},
    {SQL_CATALOG_LOCATION,         "SQL_CATALOG_LOCATION",        'h'},
    {SQL_CATALOG_USAGE,            "SQL_CATALOG_USAGE",           'i'},
    {SQL_SCHEMA_USAGE,             "SQL_SCHEMA_USAGE",            'i'},

    /* Identifier case handling */
    {SQL_IDENTIFIER_CASE,          "SQL_IDENTIFIER_CASE",         'h'},
    {SQL_QUOTED_IDENTIFIER_CASE,   "SQL_QUOTED_IDENTIFIER_CASE",  'h'},

    /* NULL handling */
    {SQL_NULL_COLLATION,           "SQL_NULL_COLLATION",          'h'},
    {SQL_CONCAT_NULL_BEHAVIOR,     "SQL_CONCAT_NULL_BEHAVIOR",    'h'},
    {SQL_NON_NULLABLE_COLUMNS,     "SQL_NON_NULLABLE_COLUMNS",    'h'},

    /* SQL conformance */
    {SQL_ODBC_INTERFACE_CONFORMANCE, "SQL_ODBC_INTERFACE_CONFORMANCE", 'i'},
    {SQL_INTEGRITY,                "SQL_INTEGRITY",               's'},

    /* SQL feature bitmasks */
    {SQL_ALTER_TABLE,              "SQL_ALTER_TABLE",              'i'},
    {SQL_CREATE_TABLE,             "SQL_CREATE_TABLE",            'i'},
    {SQL_CREATE_VIEW,              "SQL_CREATE_VIEW",             'i'},
    {SQL_DROP_TABLE,               "SQL_DROP_TABLE",              'i'},
    {SQL_DROP_VIEW,                "SQL_DROP_VIEW",               'i'},
    {SQL_INSERT_STATEMENT,         "SQL_INSERT_STATEMENT",        'i'},
    {SQL_SUBQUERIES,               "SQL_SUBQUERIES",              'i'},
    {SQL_UNION,                    "SQL_UNION_STATEMENT",         'i'},
    {SQL_GROUP_BY,                 "SQL_GROUP_BY",                'h'},
    {SQL_ORDER_BY_COLUMNS_IN_SELECT, "SQL_ORDER_BY_COLUMNS_IN_SELECT", 's'},
    {SQL_CORRELATION_NAME,         "SQL_CORRELATION_NAME",        'h'},
    {SQL_OJ_CAPABILITIES,          "SQL_OJ_CAPABILITIES",         'i'},

    /* Scalar function bitmasks */
    {SQL_STRING_FUNCTIONS,         "SQL_STRING_FUNCTIONS",        'i'},
    {SQL_NUMERIC_FUNCTIONS,        "SQL_NUMERIC_FUNCTIONS",       'i'},
    {SQL_TIMEDATE_FUNCTIONS,       "SQL_TIMEDATE_FUNCTIONS",      'i'},
    {SQL_SYSTEM_FUNCTIONS,         "SQL_SYSTEM_FUNCTIONS",        'i'},
    {SQL_CONVERT_FUNCTIONS,        "SQL_CONVERT_FUNCTIONS",       'i'},
    {SQL_AGGREGATE_FUNCTIONS,      "SQL_AGGREGATE_FUNCTIONS",     'i'},

    /* SQL92 feature bitmasks */
    {SQL_SQL92_PREDICATES,                "SQL_SQL92_PREDICATES",                'i'},
    {SQL_SQL92_RELATIONAL_JOIN_OPERATORS, "SQL_SQL92_RELATIONAL_JOIN_OPERATORS", 'i'},
    {SQL_SQL92_VALUE_EXPRESSIONS,         "SQL_SQL92_VALUE_EXPRESSIONS",         'i'},
    {SQL_SQL92_STRING_FUNCTIONS,          "SQL_SQL92_STRING_FUNCTIONS",          'i'},
    {SQL_SQL92_NUMERIC_VALUE_FUNCTIONS,   "SQL_SQL92_NUMERIC_VALUE_FUNCTIONS",   'i'},
    {SQL_SQL92_DATETIME_FUNCTIONS,        "SQL_SQL92_DATETIME_FUNCTIONS",        'i'},

    /* Transaction support */
    {SQL_TXN_CAPABLE,              "SQL_TXN_CAPABLE",             'h'},
    {SQL_DEFAULT_TXN_ISOLATION,    "SQL_DEFAULT_TXN_ISOLATION",   'i'},
    {SQL_CURSOR_COMMIT_BEHAVIOR,   "SQL_CURSOR_COMMIT_BEHAVIOR",  'h'},
    {SQL_CURSOR_ROLLBACK_BEHAVIOR, "SQL_CURSOR_ROLLBACK_BEHAVIOR",'h'},

    /* Limits */
    {SQL_MAX_CATALOG_NAME_LEN,     "SQL_MAX_CATALOG_NAME_LEN",    'h'},
    {SQL_MAX_COLUMN_NAME_LEN,      "SQL_MAX_COLUMN_NAME_LEN",     'h'},
    {SQL_MAX_COLUMNS_IN_GROUP_BY,  "SQL_MAX_COLUMNS_IN_GROUP_BY", 'h'},
    {SQL_MAX_COLUMNS_IN_ORDER_BY,  "SQL_MAX_COLUMNS_IN_ORDER_BY", 'h'},
    {SQL_MAX_COLUMNS_IN_SELECT,    "SQL_MAX_COLUMNS_IN_SELECT",   'h'},
    {SQL_MAX_CURSOR_NAME_LEN,      "SQL_MAX_CURSOR_NAME_LEN",     'h'},
    {SQL_MAX_IDENTIFIER_LEN,       "SQL_MAX_IDENTIFIER_LEN",      'h'},
    {SQL_MAX_PROCEDURE_NAME_LEN,   "SQL_MAX_PROCEDURE_NAME_LEN",  'h'},
    {SQL_MAX_SCHEMA_NAME_LEN,      "SQL_MAX_SCHEMA_NAME_LEN",     'h'},
    {SQL_MAX_TABLE_NAME_LEN,       "SQL_MAX_TABLE_NAME_LEN",      'h'},
    {SQL_MAX_STATEMENT_LEN,        "SQL_MAX_STATEMENT_LEN",       'i'},
    {SQL_MAX_TABLES_IN_SELECT,     "SQL_MAX_TABLES_IN_SELECT",    'h'},
    {SQL_MAX_USER_NAME_LEN,        "SQL_MAX_USER_NAME_LEN",       'h'},
    {SQL_MAX_ROW_SIZE,             "SQL_MAX_ROW_SIZE",            'i'},

    /* Keywords */
    {SQL_KEYWORDS,                 "SQL_KEYWORDS",                's'},
};

static json collect_get_info(SQLHDBC dbc) {
    json info(jsoncons::json_object_arg);

    for (const auto &entry : g_info_catalog) {
        switch (entry.category) {
        case 's': {
            SQLCHAR buf[4096];
            SQLSMALLINT len = 0;
            SQLRETURN rc = SQLGetInfo(dbc, entry.type_id, buf, sizeof(buf), &len);
            if (SQL_SUCCEEDED(rc)) {
                info.insert_or_assign(entry.name,
                    std::string(reinterpret_cast<char *>(buf), len));
            } else {
                info.insert_or_assign(entry.name, json::null());
            }
            break;
        }
        case 'i': {
            SQLUINTEGER val = 0;
            SQLRETURN rc = SQLGetInfo(dbc, entry.type_id, &val, sizeof(val), nullptr);
            if (SQL_SUCCEEDED(rc)) {
                info.insert_or_assign(entry.name, static_cast<int64_t>(val));
            } else {
                info.insert_or_assign(entry.name, json::null());
            }
            break;
        }
        case 'h': {
            SQLUSMALLINT val = 0;
            SQLRETURN rc = SQLGetInfo(dbc, entry.type_id, &val, sizeof(val), nullptr);
            if (SQL_SUCCEEDED(rc)) {
                info.insert_or_assign(entry.name, static_cast<int64_t>(val));
            } else {
                info.insert_or_assign(entry.name, json::null());
            }
            break;
        }
        }
    }

    return info;
}

static json collect_type_info(SQLHDBC dbc) {
    json types(jsoncons::json_array_arg);

    SQLHSTMT stmt = SQL_NULL_HSTMT;
    SQLRETURN rc = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    if (!SQL_SUCCEEDED(rc))
        return types;

    rc = SQLGetTypeInfo(stmt, SQL_ALL_TYPES);
    if (!SQL_SUCCEEDED(rc)) {
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        return types;
    }

    /* Discover result columns */
    SQLSMALLINT ncols = 0;
    SQLNumResultCols(stmt, &ncols);

    struct ColMeta {
        std::string name;
        SQLSMALLINT sql_type;
    };
    std::vector<ColMeta> cols(ncols);
    for (SQLSMALLINT i = 0; i < ncols; i++) {
        SQLCHAR colname[256];
        SQLSMALLINT namelen = 0, datatype = 0, nullable = 0, decdigits = 0;
        SQLULEN colsize = 0;
        SQLDescribeCol(stmt, i + 1, colname, sizeof(colname), &namelen,
                       &datatype, &colsize, &decdigits, &nullable);
        cols[i].name = std::string(reinterpret_cast<char *>(colname), namelen);
        cols[i].sql_type = datatype;
    }

    while (SQLFetch(stmt) == SQL_SUCCESS) {
        json row(jsoncons::json_object_arg);
        for (SQLSMALLINT i = 0; i < ncols; i++) {
            SQLLEN indicator = 0;
            SQLCHAR buf[1024];
            rc = SQLGetData(stmt, i + 1, SQL_C_CHAR, buf, sizeof(buf), &indicator);

            if (!SQL_SUCCEEDED(rc) || indicator == SQL_NULL_DATA) {
                row.insert_or_assign(cols[i].name, json::null());
                continue;
            }

            std::string sval(reinterpret_cast<char *>(buf));

            /* Try to return numbers as numbers */
            switch (cols[i].sql_type) {
            case SQL_SMALLINT:
            case SQL_INTEGER:
            case SQL_TINYINT:
            case SQL_BIGINT:
                try { row.insert_or_assign(cols[i].name, std::stoll(sval)); }
                catch (...) { row.insert_or_assign(cols[i].name, sval); }
                break;
            default:
                row.insert_or_assign(cols[i].name, sval);
                break;
            }
        }
        types.push_back(std::move(row));
    }

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return types;
}

/* ── SQLGetFunctions ─────────────────────────────────────────────── */

struct FuncEntry {
    SQLUSMALLINT id;
    const char  *name;
};

static const FuncEntry g_func_catalog[] = {
    /* ODBC Core functions */
    {SQL_API_SQLALLOCHANDLE,        "SQLAllocHandle"},
    {SQL_API_SQLBINDCOL,            "SQLBindCol"},
    {SQL_API_SQLBINDPARAMETER,      "SQLBindParameter"},
    {SQL_API_SQLBROWSECONNECT,      "SQLBrowseConnect"},
    {SQL_API_SQLBULKOPERATIONS,     "SQLBulkOperations"},
    {SQL_API_SQLCANCEL,             "SQLCancel"},
    {SQL_API_SQLCLOSECURSOR,        "SQLCloseCursor"},
    {SQL_API_SQLCOLATTRIBUTE,       "SQLColAttribute"},
    {SQL_API_SQLCOLUMNS,            "SQLColumns"},
    {SQL_API_SQLCONNECT,            "SQLConnect"},
    {SQL_API_SQLCOPYDESC,           "SQLCopyDesc"},
    {SQL_API_SQLDESCRIBECOL,        "SQLDescribeCol"},
    {SQL_API_SQLDESCRIBEPARAM,      "SQLDescribeParam"},
    {SQL_API_SQLDISCONNECT,         "SQLDisconnect"},
    {SQL_API_SQLDRIVERCONNECT,      "SQLDriverConnect"},
    {SQL_API_SQLENDTRAN,            "SQLEndTran"},
    {SQL_API_SQLEXECDIRECT,         "SQLExecDirect"},
    {SQL_API_SQLEXECUTE,            "SQLExecute"},
    {SQL_API_SQLEXTENDEDFETCH,      "SQLExtendedFetch"},
    {SQL_API_SQLFETCH,              "SQLFetch"},
    {SQL_API_SQLFETCHSCROLL,        "SQLFetchScroll"},
    {SQL_API_SQLFOREIGNKEYS,        "SQLForeignKeys"},
    {SQL_API_SQLFREEHANDLE,         "SQLFreeHandle"},
    {SQL_API_SQLFREESTMT,           "SQLFreeStmt"},
    {SQL_API_SQLGETCONNECTATTR,     "SQLGetConnectAttr"},
    {SQL_API_SQLGETCURSORNAME,      "SQLGetCursorName"},
    {SQL_API_SQLGETDATA,            "SQLGetData"},
    {SQL_API_SQLGETDESCFIELD,       "SQLGetDescField"},
    {SQL_API_SQLGETDESCREC,         "SQLGetDescRec"},
    {SQL_API_SQLGETDIAGFIELD,       "SQLGetDiagField"},
    {SQL_API_SQLGETDIAGREC,         "SQLGetDiagRec"},
    {SQL_API_SQLGETENVATTR,         "SQLGetEnvAttr"},
    {SQL_API_SQLGETFUNCTIONS,       "SQLGetFunctions"},
    {SQL_API_SQLGETINFO,            "SQLGetInfo"},
    {SQL_API_SQLGETSTMTATTR,        "SQLGetStmtAttr"},
    {SQL_API_SQLGETTYPEINFO,        "SQLGetTypeInfo"},
    {SQL_API_SQLMORERESULTS,        "SQLMoreResults"},
    {SQL_API_SQLNATIVESQL,          "SQLNativeSQL"},
    {SQL_API_SQLNUMPARAMS,          "SQLNumParams"},
    {SQL_API_SQLNUMRESULTCOLS,      "SQLNumResultCols"},
    {SQL_API_SQLPARAMDATA,          "SQLParamData"},
    {SQL_API_SQLPREPARE,            "SQLPrepare"},
    {SQL_API_SQLPRIMARYKEYS,        "SQLPrimaryKeys"},
    {SQL_API_SQLPROCEDURECOLUMNS,   "SQLProcedureColumns"},
    {SQL_API_SQLPROCEDURES,         "SQLProcedures"},
    {SQL_API_SQLPUTDATA,            "SQLPutData"},
    {SQL_API_SQLROWCOUNT,           "SQLRowCount"},
    {SQL_API_SQLSETCONNECTATTR,     "SQLSetConnectAttr"},
    {SQL_API_SQLSETCURSORNAME,      "SQLSetCursorName"},
    {SQL_API_SQLSETDESCFIELD,       "SQLSetDescField"},
    {SQL_API_SQLSETDESCREC,         "SQLSetDescRec"},
    {SQL_API_SQLSETENVATTR,         "SQLSetEnvAttr"},
    {SQL_API_SQLSETPOS,             "SQLSetPos"},
    {SQL_API_SQLSETSTMTATTR,        "SQLSetStmtAttr"},
    {SQL_API_SQLSPECIALCOLUMNS,     "SQLSpecialColumns"},
    {SQL_API_SQLSTATISTICS,         "SQLStatistics"},
    {SQL_API_SQLTABLEPRIVILEGES,    "SQLTablePrivileges"},
    {SQL_API_SQLTABLES,             "SQLTables"},
    {SQL_API_SQLCOLUMNPRIVILEGES,   "SQLColumnPrivileges"},
};

static json collect_functions(SQLHDBC dbc) {
    json funcs(jsoncons::json_object_arg);

    for (const auto &entry : g_func_catalog) {
        SQLUSMALLINT supported = SQL_FALSE;
        SQLRETURN rc = SQLGetFunctions(dbc, entry.id, &supported);
        if (SQL_SUCCEEDED(rc)) {
            funcs.insert_or_assign(entry.name, supported == SQL_TRUE);
        } else {
            funcs.insert_or_assign(entry.name, json::null());
        }
    }

    return funcs;
}

/* ── SQLDrivers (environment-level) ─────────────────────────────── */

static json collect_drivers(SQLHENV env) {
    json drivers(jsoncons::json_array_arg);

    SQLCHAR  name[256];
    SQLCHAR  attrs[4096];
    SQLSMALLINT name_len = 0, attrs_len = 0;

    SQLUSMALLINT direction = SQL_FETCH_FIRST;
    while (true) {
        SQLRETURN rc = SQLDrivers(env, direction,
                                   name, sizeof(name), &name_len,
                                   attrs, sizeof(attrs), &attrs_len);
        if (!SQL_SUCCEEDED(rc))
            break;

        json drv(jsoncons::json_object_arg);
        drv.insert_or_assign("name",
            std::string(reinterpret_cast<char *>(name), name_len));

        /* Attributes are key=value pairs separated by NUL bytes */
        json attr_obj(jsoncons::json_object_arg);
        const char *p = reinterpret_cast<char *>(attrs);
        const char *end = p + attrs_len;
        while (p < end && *p) {
            std::string kv(p);
            p += kv.size() + 1;
            auto eq = kv.find('=');
            if (eq != std::string::npos) {
                attr_obj.insert_or_assign(kv.substr(0, eq), kv.substr(eq + 1));
            }
        }
        drv.insert_or_assign("attributes", std::move(attr_obj));
        drivers.push_back(std::move(drv));

        direction = SQL_FETCH_NEXT;
    }

    return drivers;
}

/* ── SQLTables enumeration (catalogs, schemas, table types) ──────── */

/*
 * Helper: call SQLTables in one of its special enumeration modes and
 * collect non-empty columns from each row as an object.
 *
 * The ODBC spec defines three special invocations of SQLTables:
 *   - catalog = SQL_ALL_CATALOGS, schema = "", table = ""  → list catalogs
 *   - catalog = "", schema = SQL_ALL_SCHEMAS, table = ""   → list schemas
 *   - catalog = "", schema = "", table = "", type = SQL_ALL_TABLE_TYPES → list table types
 */
static json sqltables_enumerate(
        SQLHDBC dbc,
        const char *catalog,
        const char *schema,
        const char *table,
        const char *type) {
    json items(jsoncons::json_array_arg);

    SQLHSTMT stmt = SQL_NULL_HSTMT;
    SQLRETURN rc = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    if (!SQL_SUCCEEDED(rc))
        return items;

    rc = SQLTables(stmt,
                   (SQLCHAR *)catalog, SQL_NTS,
                   (SQLCHAR *)schema,  SQL_NTS,
                   (SQLCHAR *)table,   SQL_NTS,
                   (SQLCHAR *)type,    SQL_NTS);
    if (!SQL_SUCCEEDED(rc)) {
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        return items;
    }

    SQLSMALLINT ncols = 0;
    SQLNumResultCols(stmt, &ncols);

    std::vector<std::string> col_names(ncols);
    for (SQLSMALLINT i = 0; i < ncols; i++) {
        SQLCHAR colname[256];
        SQLSMALLINT namelen = 0, datatype = 0, nullable = 0, decdigits = 0;
        SQLULEN colsize = 0;
        SQLDescribeCol(stmt, i + 1, colname, sizeof(colname), &namelen,
                       &datatype, &colsize, &decdigits, &nullable);
        col_names[i] = std::string(reinterpret_cast<char *>(colname), namelen);
    }

    while (SQLFetch(stmt) == SQL_SUCCESS) {
        json row(jsoncons::json_object_arg);
        for (SQLSMALLINT i = 0; i < ncols; i++) {
            SQLLEN indicator = 0;
            SQLCHAR buf[1024];
            rc = SQLGetData(stmt, i + 1, SQL_C_CHAR, buf, sizeof(buf), &indicator);
            if (SQL_SUCCEEDED(rc) && indicator != SQL_NULL_DATA) {
                std::string val(reinterpret_cast<char *>(buf));
                if (!val.empty()) {
                    row.insert_or_assign(col_names[i], val);
                }
            }
        }
        if (!row.empty()) {
            items.push_back(std::move(row));
        }
    }

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return items;
}

static json collect_catalogs(SQLHDBC dbc) {
    return sqltables_enumerate(dbc, "%", "", "", "");
}

static json collect_schemas(SQLHDBC dbc) {
    return sqltables_enumerate(dbc, "", "%", "", "");
}

static json collect_table_types(SQLHDBC dbc) {
    return sqltables_enumerate(dbc, "", "", "", "%");
}

/* ── Generic SQLColumns / SQLTables result-set serialiser ────────── */

/*
 * Fetch a complete ODBC result set as a JSON array of objects.
 * Used for SQLTables and SQLColumns result sets where we want every
 * column the driver returns, with numbers as numbers.
 */
static json stmt_result_to_json(SQLHSTMT stmt) {
    json rows(jsoncons::json_array_arg);

    SQLSMALLINT ncols = 0;
    SQLNumResultCols(stmt, &ncols);

    struct ColMeta {
        std::string name;
        SQLSMALLINT sql_type;
    };
    std::vector<ColMeta> cols(ncols);
    for (SQLSMALLINT i = 0; i < ncols; i++) {
        SQLCHAR colname[256];
        SQLSMALLINT namelen = 0, datatype = 0, nullable = 0, decdigits = 0;
        SQLULEN colsize = 0;
        SQLDescribeCol(stmt, i + 1, colname, sizeof(colname), &namelen,
                       &datatype, &colsize, &decdigits, &nullable);
        cols[i].name = std::string(reinterpret_cast<char *>(colname), namelen);
        cols[i].sql_type = datatype;
    }

    while (SQLFetch(stmt) == SQL_SUCCESS) {
        json row(jsoncons::json_object_arg);
        for (SQLSMALLINT i = 0; i < ncols; i++) {
            SQLLEN indicator = 0;
            SQLCHAR buf[4096];
            SQLRETURN rc = SQLGetData(stmt, i + 1, SQL_C_CHAR,
                                      buf, sizeof(buf), &indicator);
            if (!SQL_SUCCEEDED(rc) || indicator == SQL_NULL_DATA) {
                row.insert_or_assign(cols[i].name, json::null());
                continue;
            }
            std::string sval(reinterpret_cast<char *>(buf));
            switch (cols[i].sql_type) {
            case SQL_SMALLINT:
            case SQL_INTEGER:
            case SQL_TINYINT:
            case SQL_BIGINT:
                try { row.insert_or_assign(cols[i].name, std::stoll(sval)); }
                catch (...) { row.insert_or_assign(cols[i].name, sval); }
                break;
            default:
                row.insert_or_assign(cols[i].name, sval);
                break;
            }
        }
        rows.push_back(std::move(row));
    }

    return rows;
}

static std::string build_tables(const char *conn_str,
                                 const char *catalog,
                                 const char *schema,
                                 const char *type) {
    nanodbc::connection conn(conn_str);
    SQLHDBC dbc = static_cast<SQLHDBC>(conn.native_dbc_handle());

    SQLHSTMT stmt = SQL_NULL_HSTMT;
    SQLRETURN rc = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    if (!SQL_SUCCEEDED(rc))
        throw std::runtime_error("SQLAllocHandle failed for SQLTables");

    /* NULL pointer + 0 length = no restriction (match all).
     * Non-NULL pointer + SQL_NTS = pattern match on that value. */
    rc = SQLTables(stmt,
                   catalog ? (SQLCHAR *)catalog : nullptr,
                   catalog ? SQL_NTS : 0,
                   schema ? (SQLCHAR *)schema : nullptr,
                   schema ? SQL_NTS : 0,
                   nullptr, 0,                  /* table name: all */
                   type ? (SQLCHAR *)type : nullptr,
                   type ? SQL_NTS : 0);
    if (!SQL_SUCCEEDED(rc)) {
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        throw std::runtime_error("SQLTables failed");
    }

    auto result = stmt_result_to_json(stmt);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return result.to_string();
}

static std::string build_columns(const char *conn_str,
                                  const char *catalog,
                                  const char *schema,
                                  const char *table) {
    nanodbc::connection conn(conn_str);
    SQLHDBC dbc = static_cast<SQLHDBC>(conn.native_dbc_handle());

    SQLHSTMT stmt = SQL_NULL_HSTMT;
    SQLRETURN rc = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    if (!SQL_SUCCEEDED(rc))
        throw std::runtime_error("SQLAllocHandle failed for SQLColumns");

    rc = SQLColumns(stmt,
                    catalog ? (SQLCHAR *)catalog : nullptr,
                    catalog ? SQL_NTS : 0,
                    schema ? (SQLCHAR *)schema : nullptr,
                    schema ? SQL_NTS : 0,
                    table ? (SQLCHAR *)table : nullptr,
                    table ? SQL_NTS : 0,
                    nullptr, 0);                /* column name: all */
    if (!SQL_SUCCEEDED(rc)) {
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        throw std::runtime_error("SQLColumns failed");
    }

    auto result = stmt_result_to_json(stmt);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return result.to_string();
}

/* ── SQLPrimaryKeys ──────────────────────────────────────────────── */

static std::string build_primary_keys(const char *conn_str,
                                       const char *catalog,
                                       const char *schema,
                                       const char *table) {
    nanodbc::connection conn(conn_str);
    SQLHDBC dbc = static_cast<SQLHDBC>(conn.native_dbc_handle());

    SQLHSTMT stmt = SQL_NULL_HSTMT;
    SQLRETURN rc = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    if (!SQL_SUCCEEDED(rc))
        throw std::runtime_error("SQLAllocHandle failed for SQLPrimaryKeys");

    rc = SQLPrimaryKeys(stmt,
                        catalog ? (SQLCHAR *)catalog : nullptr,
                        catalog ? SQL_NTS : 0,
                        schema ? (SQLCHAR *)schema : nullptr,
                        schema ? SQL_NTS : 0,
                        table ? (SQLCHAR *)table : nullptr,
                        table ? SQL_NTS : 0);
    if (!SQL_SUCCEEDED(rc)) {
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        throw std::runtime_error("SQLPrimaryKeys failed");
    }

    auto result = stmt_result_to_json(stmt);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return result.to_string();
}

/* ── SQLForeignKeys ─────────────────────────────────────────────── */

/*
 * SQLForeignKeys can be called in two modes:
 *   1. PK table specified → returns all FKs that reference it
 *   2. FK table specified → returns all FKs defined on it
 *
 * We expose both modes via separate parameters. Pass the FK table
 * identifiers to get "what does this table reference?".  Pass the
 * PK table identifiers to get "what references this table?".
 * Pass both for a specific relationship.
 * Pass neither (all NULL) for all FKs visible in the current catalog.
 */
static std::string build_foreign_keys(const char *conn_str,
                                       const char *pk_catalog,
                                       const char *pk_schema,
                                       const char *pk_table,
                                       const char *fk_catalog,
                                       const char *fk_schema,
                                       const char *fk_table) {
    nanodbc::connection conn(conn_str);
    SQLHDBC dbc = static_cast<SQLHDBC>(conn.native_dbc_handle());

    SQLHSTMT stmt = SQL_NULL_HSTMT;
    SQLRETURN rc = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    if (!SQL_SUCCEEDED(rc))
        throw std::runtime_error("SQLAllocHandle failed for SQLForeignKeys");

    rc = SQLForeignKeys(stmt,
                        pk_catalog ? (SQLCHAR *)pk_catalog : nullptr,
                        pk_catalog ? SQL_NTS : 0,
                        pk_schema ? (SQLCHAR *)pk_schema : nullptr,
                        pk_schema ? SQL_NTS : 0,
                        pk_table ? (SQLCHAR *)pk_table : nullptr,
                        pk_table ? SQL_NTS : 0,
                        fk_catalog ? (SQLCHAR *)fk_catalog : nullptr,
                        fk_catalog ? SQL_NTS : 0,
                        fk_schema ? (SQLCHAR *)fk_schema : nullptr,
                        fk_schema ? SQL_NTS : 0,
                        fk_table ? (SQLCHAR *)fk_table : nullptr,
                        fk_table ? SQL_NTS : 0);
    if (!SQL_SUCCEEDED(rc)) {
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        throw std::runtime_error("SQLForeignKeys failed");
    }

    auto result = stmt_result_to_json(stmt);
    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
    return result.to_string();
}

/* ── Execute in specific catalog (database) ─────────────────────── */

static std::string build_query_in_catalog(const char *conn_str,
                                           const char *catalog,
                                           const char *query) {
    nanodbc::connection conn(conn_str);
    SQLHDBC dbc = static_cast<SQLHDBC>(conn.native_dbc_handle());

    /* Save current catalog */
    SQLCHAR orig_catalog[512];
    SQLINTEGER orig_len = 0;
    SQLGetConnectAttr(dbc, SQL_ATTR_CURRENT_CATALOG,
                      orig_catalog, sizeof(orig_catalog), &orig_len);

    /* Switch to target catalog */
    SQLSetConnectAttr(dbc, SQL_ATTR_CURRENT_CATALOG,
                      (SQLPOINTER)catalog, SQL_NTS);

    /* Execute the query */
    std::string json_result;
    try {
        nanodbc::result result = nanodbc::execute(conn, query);
        json_result = result_to_json(result);
    } catch (...) {
        /* Restore original catalog before re-throwing */
        SQLSetConnectAttr(dbc, SQL_ATTR_CURRENT_CATALOG,
                          orig_catalog, orig_len);
        throw;
    }

    /* Restore original catalog */
    SQLSetConnectAttr(dbc, SQL_ATTR_CURRENT_CATALOG,
                      orig_catalog, orig_len);

    return json_result;
}

static std::string build_driver_info(const char *conn_str) {
    nanodbc::connection conn(conn_str);
    SQLHDBC dbc = static_cast<SQLHDBC>(conn.native_dbc_handle());
    SQLHENV env = static_cast<SQLHENV>(conn.native_env_handle());

    json result(jsoncons::json_object_arg);
    result.insert_or_assign("get_info",    collect_get_info(dbc));
    result.insert_or_assign("type_info",   collect_type_info(dbc));
    result.insert_or_assign("functions",   collect_functions(dbc));
    result.insert_or_assign("drivers",     collect_drivers(env));
    result.insert_or_assign("catalogs",    collect_catalogs(dbc));
    result.insert_or_assign("schemas",     collect_schemas(dbc));
    result.insert_or_assign("table_types", collect_table_types(dbc));

    return result.to_string();
}

/* ── C API ───────────────────────────────────────────────────────── */

extern "C" {

char *blobodbc_query_json(const char *conn_str, const char *query) {
    return blobodbc_query_json_params(conn_str, query, nullptr, 0);
}

char *blobodbc_query_json_params(const char *conn_str, const char *query,
                                  const char **bind_values, int bind_count) {
    try {
        g_errmsg.clear();
        auto result = execute_query(conn_str, query, bind_values, bind_count);
        return strdup_result(result_to_json(result));
    } catch (const nanodbc::database_error &e) {
        g_errmsg = e.what();
        return nullptr;
    } catch (const std::exception &e) {
        g_errmsg = e.what();
        return nullptr;
    }
}

char *blobodbc_query_clob(const char *conn_str, const char *query) {
    return blobodbc_query_clob_params(conn_str, query, nullptr, 0);
}

char *blobodbc_query_clob_params(const char *conn_str, const char *query,
                                  const char **bind_values, int bind_count) {
    try {
        g_errmsg.clear();
        auto result = execute_query(conn_str, query, bind_values, bind_count);
        return strdup_result(result_to_clob(result));
    } catch (const nanodbc::database_error &e) {
        g_errmsg = e.what();
        return nullptr;
    } catch (const std::exception &e) {
        g_errmsg = e.what();
        return nullptr;
    }
}

char *blobodbc_query_json_named(const char *conn_str, const char *query,
                                 const char *bind_json) {
    try {
        g_errmsg.clear();
        auto result = execute_named(conn_str, query, bind_json);
        return strdup_result(result_to_json(result));
    } catch (const nanodbc::database_error &e) {
        g_errmsg = e.what();
        return nullptr;
    } catch (const std::exception &e) {
        g_errmsg = e.what();
        return nullptr;
    }
}

char *blobodbc_query_clob_named(const char *conn_str, const char *query,
                                 const char *bind_json) {
    try {
        g_errmsg.clear();
        auto result = execute_named(conn_str, query, bind_json);
        return strdup_result(result_to_clob(result));
    } catch (const nanodbc::database_error &e) {
        g_errmsg = e.what();
        return nullptr;
    } catch (const std::exception &e) {
        g_errmsg = e.what();
        return nullptr;
    }
}

char *blobodbc_driver_info(const char *conn_str) {
    try {
        g_errmsg.clear();
        return strdup_result(build_driver_info(conn_str));
    } catch (const nanodbc::database_error &e) {
        g_errmsg = e.what();
        return nullptr;
    } catch (const std::exception &e) {
        g_errmsg = e.what();
        return nullptr;
    }
}

char *blobodbc_tables(const char *conn_str,
                      const char *catalog,
                      const char *schema,
                      const char *type) {
    try {
        g_errmsg.clear();
        return strdup_result(build_tables(conn_str, catalog, schema, type));
    } catch (const nanodbc::database_error &e) {
        g_errmsg = e.what();
        return nullptr;
    } catch (const std::exception &e) {
        g_errmsg = e.what();
        return nullptr;
    }
}

char *blobodbc_columns(const char *conn_str,
                       const char *catalog,
                       const char *schema,
                       const char *table) {
    try {
        g_errmsg.clear();
        return strdup_result(build_columns(conn_str, catalog, schema, table));
    } catch (const nanodbc::database_error &e) {
        g_errmsg = e.what();
        return nullptr;
    } catch (const std::exception &e) {
        g_errmsg = e.what();
        return nullptr;
    }
}

char *blobodbc_query_json_in_catalog(const char *conn_str,
                                      const char *catalog,
                                      const char *query) {
    try {
        g_errmsg.clear();
        return strdup_result(build_query_in_catalog(conn_str, catalog, query));
    } catch (const nanodbc::database_error &e) {
        g_errmsg = e.what();
        return nullptr;
    } catch (const std::exception &e) {
        g_errmsg = e.what();
        return nullptr;
    }
}

int blobodbc_execute(const char *conn_str, const char *sql) {
    try {
        g_errmsg.clear();
        nanodbc::connection conn(conn_str);
        SQLHDBC dbc = static_cast<SQLHDBC>(conn.native_dbc_handle());
        SQLHSTMT stmt = SQL_NULL_HSTMT;
        SQLRETURN rc = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
        if (!SQL_SUCCEEDED(rc))
            throw std::runtime_error("SQLAllocHandle failed");

        rc = SQLExecDirect(stmt, (SQLCHAR *)sql, SQL_NTS);
        if (!SQL_SUCCEEDED(rc) && rc != SQL_NO_DATA) {
            /* Extract error message from diagnostics */
            SQLCHAR state[6] = {0}, msg[1024] = {0};
            SQLINTEGER native = 0;
            SQLSMALLINT len = 0;
            SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, state, &native,
                          msg, sizeof(msg), &len);
            SQLFreeHandle(SQL_HANDLE_STMT, stmt);
            if (len > 0)
                throw std::runtime_error(std::string(reinterpret_cast<char *>(msg), len));
            else
                throw std::runtime_error(
                    std::string("ODBC error SQLSTATE=") +
                    reinterpret_cast<char *>(state));
        }

        SQLLEN row_count = -1;
        SQLRowCount(stmt, &row_count);
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        /* -1 means "not applicable" (DDL, or driver doesn't report).
         * Clamp to 0 so callers can distinguish error (-1 from the
         * C API wrapper) from "no rows affected". */
        return static_cast<int>(row_count < 0 ? 0 : row_count);
    } catch (const nanodbc::database_error &e) {
        g_errmsg = e.what();
        return -1;
    } catch (const std::exception &e) {
        g_errmsg = e.what();
        return -1;
    }
}

char *blobodbc_primary_keys(const char *conn_str,
                             const char *catalog,
                             const char *schema,
                             const char *table) {
    try {
        g_errmsg.clear();
        return strdup_result(build_primary_keys(conn_str, catalog, schema, table));
    } catch (const nanodbc::database_error &e) {
        g_errmsg = e.what();
        return nullptr;
    } catch (const std::exception &e) {
        g_errmsg = e.what();
        return nullptr;
    }
}

char *blobodbc_foreign_keys(const char *conn_str,
                             const char *pk_catalog,
                             const char *pk_schema,
                             const char *pk_table,
                             const char *fk_catalog,
                             const char *fk_schema,
                             const char *fk_table) {
    try {
        g_errmsg.clear();
        return strdup_result(build_foreign_keys(conn_str,
            pk_catalog, pk_schema, pk_table,
            fk_catalog, fk_schema, fk_table));
    } catch (const nanodbc::database_error &e) {
        g_errmsg = e.what();
        return nullptr;
    } catch (const std::exception &e) {
        g_errmsg = e.what();
        return nullptr;
    }
}

void blobodbc_free(char *s) {
    free(s);
}

const char *blobodbc_errmsg(void) {
    return g_errmsg.c_str();
}

} /* extern "C" */
