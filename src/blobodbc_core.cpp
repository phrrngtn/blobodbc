#include "blobodbc.h"

#include <nanodbc/nanodbc.h>
#include <nlohmann/json.hpp>

#include <cstring>
#include <string>
#include <vector>

static thread_local std::string g_errmsg;

static char *strdup_result(const std::string &s) {
    char *out = (char *)malloc(s.size() + 1);
    if (out) memcpy(out, s.data(), s.size() + 1);
    return out;
}

/* ── Column type dispatch ────────────────────────────────────────── */

using column_getter = void (*)(nlohmann::json &, nanodbc::result &, short);

static void get_int_value(nlohmann::json &jv, nanodbc::result &r, short col) {
    jv = r.get<long long>(col);
}

static void get_float_value(nlohmann::json &jv, nanodbc::result &r, short col) {
    jv = r.get<double>(col);
}

static void get_string_value(nlohmann::json &jv, nanodbc::result &r, short col) {
    jv = r.get<std::string>(col);
}

static void get_date_value(nlohmann::json &jv, nanodbc::result &r, short col) {
    nanodbc::date d = r.get<nanodbc::date>(col);
    char buf[32];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", d.year, d.month, d.day);
    jv = buf;
}

static void get_timestamp_value(nlohmann::json &jv, nanodbc::result &r, short col) {
    nanodbc::timestamp ts = r.get<nanodbc::timestamp>(col);
    char buf[64];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d.%09d",
             ts.year, ts.month, ts.day,
             ts.hour, ts.min, ts.sec, ts.fract);
    jv = buf;
}

static void get_null_value(nlohmann::json &jv, nanodbc::result &, short) {
    jv = nullptr;
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

    nlohmann::ordered_json rows = nlohmann::ordered_json::array();

    while (result.next()) {
        nlohmann::ordered_json row;
        for (int i = 0; i < n; i++) {
            nlohmann::json val;
            if (result.is_null(i)) {
                val = nullptr;
            } else {
                getters[i](val, result, i);
            }
            row[names[i]] = val;
        }
        rows.push_back(std::move(row));
    }

    return rows.dump();
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

static NamedBindInfo rewrite_named_params(const char *sql,
                                            const nlohmann::json &binds) {
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
                auto &val = binds[name];
                if (val.is_null()) {
                    info.values.emplace_back();
                    info.is_null.push_back(true);
                } else if (val.is_string()) {
                    info.values.push_back(val.get<std::string>());
                    info.is_null.push_back(false);
                } else {
                    /* numbers, booleans — convert to string */
                    info.values.push_back(val.dump());
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
    nlohmann::json binds = nlohmann::json::parse(bind_json);
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

void blobodbc_free(char *s) {
    free(s);
}

const char *blobodbc_errmsg(void) {
    return g_errmsg.c_str();
}

} /* extern "C" */
