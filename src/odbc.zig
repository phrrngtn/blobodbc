//! A minimal ODBC layer in Zig, replacing nanodbc.
//!
//! WHY
//!
//! nanodbc is a C++ convenience wrapper over a C API — the kind of layer this
//! migration removes elsewhere (nlohmann/json to std.json, hash-library to
//! std.crypto). It also stopped compiling: it instantiates
//! std::basic_string<unsigned char> and libc++ removed the primary
//! std::char_traits<T> template (P1148R0), so it builds only against AppleClang's
//! older libc++. That made replacing it necessary rather than merely tidy.
//!
//! No Zig "ODBC bindings" package is needed: translate-C over sql.h/sqlext.h
//! produces complete bindings, the same way it does for quickjs.h and
//! duckdb_extension.h. ODBC is plain C.
//!
//! SHAPE
//!
//! This exports a flat C API covering exactly the nanodbc surface the core used:
//! connection, prepared statement, result with typed getters, and a thread-local
//! error string. src/blobodbc_core.cpp calls it through src/odbc.h; everything
//! above that boundary is unchanged.
//!
//! ROW BUFFERING
//!
//! Each fetched row is read into per-column storage in one pass, and the getters
//! then read from that. ODBC does not allow SQLGetData to re-read a column, but
//! the core calls is_null(col) and then get<T>(col) — two reads. nanodbc hid this
//! by binding columns; buffering a row is simpler and gives the same semantics.
//!
//! The C type requested per column mirrors the old getter_for_column exactly, so
//! JSON output is unchanged: integers as SQL_C_SBIGINT, float/numeric as
//! SQL_C_DOUBLE, strings chunked as SQL_C_CHAR, dates and timestamps as their
//! structs (formatted by the caller), binary as null.

const std = @import("std");

pub const c = @cImport({
    @cInclude("sql.h");
    @cInclude("sqlext.h");
});

const alloc = std.heap.c_allocator;

// ── errors ─────────────────────────────────────────────────────────

threadlocal var g_err: [1024]u8 = @splat(0);

fn setErr(msg: []const u8) void {
    const n = @min(msg.len, g_err.len - 1);
    @memcpy(g_err[0..n], msg[0..n]);
    g_err[n] = 0;
}

fn setErrFmt(comptime fmt: []const u8, args: anytype) void {
    const s = std.fmt.bufPrintZ(g_err[0 .. g_err.len - 1], fmt, args) catch {
        g_err[g_err.len - 1] = 0;
        return;
    };
    g_err[s.len] = 0;
}

/// Pull the driver's own diagnostic. Much more useful than a bare return code:
/// this is where "password authentication failed" or "relation does not exist"
/// actually lives.
fn captureDiag(handle_type: c.SQLSMALLINT, handle: ?*anyopaque, context: []const u8) void {
    var state: [7]u8 = @splat(0);
    var native: c.SQLINTEGER = 0;
    var text: [900]u8 = undefined;
    var text_len: c.SQLSMALLINT = 0;

    const rc = c.SQLGetDiagRec(handle_type, handle, 1, &state, &native, &text, text.len, &text_len);
    if (isOk(rc)) {
        const n: usize = @intCast(@max(text_len, 0));
        setErrFmt("{s}: [{s}] {s}", .{ context, std.mem.sliceTo(&state, 0), text[0..@min(n, text.len)] });
    } else {
        setErrFmt("{s}: failed with no diagnostic", .{context});
    }
}

fn isOk(rc: c.SQLRETURN) bool {
    return rc == c.SQL_SUCCESS or rc == c.SQL_SUCCESS_WITH_INFO;
}

/// Widen a date/time field to unsigned before formatting.
///
/// SQL_DATE_STRUCT.year is SQLSMALLINT (i16), and Zig's `{d:0>4}` prints an
/// explicit sign for signed types — which produced "+2026-07-30". These fields
/// are never negative, so format them as unsigned.
fn u16f(v: anytype) u32 {
    return @intCast(v);
}

// ── environment ────────────────────────────────────────────────────

/// One process-wide environment handle, allocated on first use. ODBC allows
/// several, but drivers vary in how well they like it and there is nothing to
/// gain; the driver manager is already thread-safe at this level.
/// Lock-free rather than mutex-guarded: DuckDB calls scalar functions from many
/// threads, and this is on the path of every connection open. A race costs one
/// redundant handle, which the loser frees.
var g_env: std.atomic.Value(?*anyopaque) = .init(null);

fn env() ?*anyopaque {
    if (g_env.load(.acquire)) |e| return e;

    var e: ?*anyopaque = null;
    if (!isOk(c.SQLAllocHandle(c.SQL_HANDLE_ENV, null, &e))) return null;
    // Declaring ODBC 3.x is mandatory before allocating a connection.
    _ = c.SQLSetEnvAttr(e, c.SQL_ATTR_ODBC_VERSION, @ptrFromInt(c.SQL_OV_ODBC3), 0);

    if (g_env.cmpxchgStrong(null, e, .release, .acquire)) |winner| {
        _ = c.SQLFreeHandle(c.SQL_HANDLE_ENV, e); // another thread got there first
        return winner;
    }
    return e;
}

// ── connection ─────────────────────────────────────────────────────

pub const Conn = struct {
    dbc: ?*anyopaque,
};

// ── result ─────────────────────────────────────────────────────────

/// What C type a column is fetched as, chosen once from its SQL type.
const Kind = enum(u8) { int, float, string, date, timestamp, unsupported };

fn kindFor(sql_type: c.SQLSMALLINT) Kind {
    return switch (sql_type) {
        c.SQL_BIT, c.SQL_TINYINT, c.SQL_SMALLINT, c.SQL_INTEGER, c.SQL_BIGINT => .int,
        c.SQL_FLOAT, c.SQL_REAL, c.SQL_DOUBLE, c.SQL_NUMERIC, c.SQL_DECIMAL => .float,
        c.SQL_CHAR, c.SQL_VARCHAR, c.SQL_LONGVARCHAR, c.SQL_WCHAR, c.SQL_WVARCHAR, c.SQL_WLONGVARCHAR, c.SQL_GUID => .string,
        c.SQL_TYPE_DATE => .date,
        c.SQL_TYPE_TIMESTAMP => .timestamp,
        // Binary comes back as null rather than as mojibake, matching the old behaviour.
        else => .unsupported,
    };
}

const Column = struct {
    name: [:0]u8,
    sql_type: c.SQLSMALLINT,
    kind: Kind,

    // Current row's value.
    is_null: bool = true,
    i64v: i64 = 0,
    f64v: f64 = 0,
    /// Owned, NUL-terminated, reused across rows. Holds string values and the
    /// formatted forms of dates and timestamps.
    text: std.ArrayList(u8) = .empty,
};

pub const Result = struct {
    stmt: ?*anyopaque,
    cols: []Column,
};

fn freeResult(r: *Result) void {
    if (r.stmt) |s| {
        _ = c.SQLFreeHandle(c.SQL_HANDLE_STMT, s);
    }
    for (r.cols) |*col| {
        alloc.free(col.name);
        col.text.deinit(alloc);
    }
    alloc.free(r.cols);
    alloc.destroy(r);
}

/// Describe every column once, after execution.
fn describe(r: *Result) !void {
    var ncols: c.SQLSMALLINT = 0;
    if (!isOk(c.SQLNumResultCols(r.stmt, &ncols))) {
        captureDiag(c.SQL_HANDLE_STMT, r.stmt, "SQLNumResultCols");
        return error.Odbc;
    }
    const n: usize = @intCast(@max(ncols, 0));
    r.cols = try alloc.alloc(Column, n);
    errdefer alloc.free(r.cols);

    for (r.cols, 0..) |*col, i| {
        var name_buf: [512]u8 = undefined;
        var name_len: c.SQLSMALLINT = 0;
        var sql_type: c.SQLSMALLINT = 0;
        var size: c.SQLULEN = 0;
        var digits: c.SQLSMALLINT = 0;
        var nullable: c.SQLSMALLINT = 0;
        const rc = c.SQLDescribeCol(r.stmt, @intCast(i + 1), &name_buf, name_buf.len,
            &name_len, &sql_type, &size, &digits, &nullable);
        if (!isOk(rc)) {
            captureDiag(c.SQL_HANDLE_STMT, r.stmt, "SQLDescribeCol");
            return error.Odbc;
        }
        const nl: usize = @intCast(@max(name_len, 0));
        col.* = .{
            .name = try alloc.dupeZ(u8, name_buf[0..@min(nl, name_buf.len)]),
            .sql_type = sql_type,
            .kind = kindFor(sql_type),
        };
    }
}

/// Read one column of the current row into its buffer.
///
/// Strings are chunked: SQLGetData returns SQL_SUCCESS_WITH_INFO and keeps going
/// while data remains, and the reported length can be SQL_NO_TOTAL when the
/// driver does not know it up front. Getting this loop wrong is the classic ODBC
/// silent-truncation bug, so it is written to trust only what each call reports.
fn readColumn(r: *Result, idx: usize) !void {
    var col = &r.cols[idx];
    const cn: c.SQLUSMALLINT = @intCast(idx + 1);
    col.is_null = true;

    switch (col.kind) {
        .unsupported => return,

        .int => {
            var v: i64 = 0;
            var ind: c.SQLLEN = 0;
            const rc = c.SQLGetData(r.stmt, cn, c.SQL_C_SBIGINT, &v, @sizeOf(i64), &ind);
            if (!isOk(rc)) return diagFail(r, "SQLGetData(int)");
            if (ind == c.SQL_NULL_DATA) return;
            col.i64v = v;
            col.is_null = false;
        },

        .float => {
            var v: f64 = 0;
            var ind: c.SQLLEN = 0;
            const rc = c.SQLGetData(r.stmt, cn, c.SQL_C_DOUBLE, &v, @sizeOf(f64), &ind);
            if (!isOk(rc)) return diagFail(r, "SQLGetData(double)");
            if (ind == c.SQL_NULL_DATA) return;
            col.f64v = v;
            col.is_null = false;
        },

        .date => {
            var v: c.SQL_DATE_STRUCT = undefined;
            var ind: c.SQLLEN = 0;
            const rc = c.SQLGetData(r.stmt, cn, c.SQL_C_TYPE_DATE, &v, @sizeOf(@TypeOf(v)), &ind);
            if (!isOk(rc)) return diagFail(r, "SQLGetData(date)");
            if (ind == c.SQL_NULL_DATA) return;
            col.text.clearRetainingCapacity();
            try col.text.print(alloc, "{d:0>4}-{d:0>2}-{d:0>2}", .{ u16f(v.year), u16f(v.month), u16f(v.day) });
            try col.text.append(alloc, 0);
            col.is_null = false;
        },

        .timestamp => {
            var v: c.SQL_TIMESTAMP_STRUCT = undefined;
            var ind: c.SQLLEN = 0;
            const rc = c.SQLGetData(r.stmt, cn, c.SQL_C_TYPE_TIMESTAMP, &v, @sizeOf(@TypeOf(v)), &ind);
            if (!isOk(rc)) return diagFail(r, "SQLGetData(timestamp)");
            if (ind == c.SQL_NULL_DATA) return;
            col.text.clearRetainingCapacity();
            // Same format string the nanodbc path used, so output is unchanged.
            try col.text.print(alloc, "{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}.{d:0>9}",
                .{ u16f(v.year), u16f(v.month), u16f(v.day), u16f(v.hour), u16f(v.minute), u16f(v.second), @as(u32, v.fraction) });
            try col.text.append(alloc, 0);
            col.is_null = false;
        },

        .string => {
            col.text.clearRetainingCapacity();
            var chunk: [4096]u8 = undefined;
            var first = true;
            while (true) {
                var ind: c.SQLLEN = 0;
                const rc = c.SQLGetData(r.stmt, cn, c.SQL_C_CHAR, &chunk, chunk.len, &ind);
                if (rc == c.SQL_NO_DATA) break; // no more chunks
                if (!isOk(rc)) return diagFail(r, "SQLGetData(char)");
                if (first and ind == c.SQL_NULL_DATA) return;
                first = false;

                // The buffer always comes back NUL-terminated, and `ind` is the
                // length that WOULD have been written — larger than the buffer
                // when truncated, or SQL_NO_TOTAL when unknown. Either way the
                // bytes actually present are up to the terminator, so measure
                // rather than trust `ind`.
                const avail = std.mem.sliceTo(&chunk, 0).len;
                try col.text.appendSlice(alloc, chunk[0..avail]);

                // A complete value fits with room for the terminator; only a
                // truncated one fills the buffer.
                if (ind != c.SQL_NO_TOTAL and ind >= 0 and @as(usize, @intCast(ind)) < chunk.len) break;
                if (avail + 1 < chunk.len) break;
            }
            try col.text.append(alloc, 0);
            col.is_null = false;
        },
    }
}

fn diagFail(r: *Result, what: []const u8) error{Odbc} {
    captureDiag(c.SQL_HANDLE_STMT, r.stmt, what);
    return error.Odbc;
}

// ── exported C API (see src/odbc.h) ────────────────────────────────

pub export fn bo_odbc_errmsg() callconv(.c) [*:0]const u8 {
    return @ptrCast(&g_err);
}

pub export fn bo_conn_open(conn_str: [*:0]const u8) callconv(.c) ?*Conn {
    g_err[0] = 0;
    const e = env() orelse {
        setErr("SQLAllocHandle(ENV) failed");
        return null;
    };

    var dbc: ?*anyopaque = null;
    if (!isOk(c.SQLAllocHandle(c.SQL_HANDLE_DBC, e, &dbc))) {
        captureDiag(c.SQL_HANDLE_ENV, e, "SQLAllocHandle(DBC)");
        return null;
    }

    var out: [1024]u8 = undefined;
    var out_len: c.SQLSMALLINT = 0;
    const rc = c.SQLDriverConnect(dbc, null, @constCast(conn_str), c.SQL_NTS,
        &out, out.len, &out_len, c.SQL_DRIVER_NOPROMPT);
    if (!isOk(rc)) {
        captureDiag(c.SQL_HANDLE_DBC, dbc, "SQLDriverConnect");
        _ = c.SQLFreeHandle(c.SQL_HANDLE_DBC, dbc);
        return null;
    }

    const conn = alloc.create(Conn) catch {
        _ = c.SQLDisconnect(dbc);
        _ = c.SQLFreeHandle(c.SQL_HANDLE_DBC, dbc);
        setErr("out of memory");
        return null;
    };
    conn.* = .{ .dbc = dbc };
    return conn;
}

pub export fn bo_conn_close(conn: ?*Conn) callconv(.c) void {
    const cn = conn orelse return;
    if (cn.dbc) |dbc| {
        _ = c.SQLDisconnect(dbc);
        _ = c.SQLFreeHandle(c.SQL_HANDLE_DBC, dbc);
    }
    alloc.destroy(cn);
}

/// Is the connection still usable? Replaces nanodbc's connected() check, which
/// the pool used to decide whether to reconnect after a network drop.
pub export fn bo_conn_alive(conn: ?*Conn) callconv(.c) c_int {
    const cn = conn orelse return 0;
    var dead: c.SQLUINTEGER = 0;
    const rc = c.SQLGetConnectAttr(cn.dbc, c.SQL_ATTR_CONNECTION_DEAD, &dead, 0, null);
    if (!isOk(rc)) return 1; // driver cannot tell us; assume usable
    return if (dead == c.SQL_CD_TRUE) 0 else 1;
}

/// Raw handle, for the parts of the core that already call ODBC directly
/// (SQLGetInfo, SQLTables, SQLColumns, ...).
pub export fn bo_conn_handle(conn: ?*Conn) callconv(.c) ?*anyopaque {
    const cn = conn orelse return null;
    return cn.dbc;
}

/// Prepare and execute `sql`, returning a result to iterate. `rowset` sets
/// SQL_ATTR_ROW_ARRAY_SIZE, as the nanodbc path did.
pub export fn bo_conn_query(conn: ?*Conn, sql: [*:0]const u8, rowset: c_long) callconv(.c) ?*Result {
    g_err[0] = 0;
    const cn = conn orelse {
        setErr("null connection");
        return null;
    };

    var stmt: ?*anyopaque = null;
    if (!isOk(c.SQLAllocHandle(c.SQL_HANDLE_STMT, cn.dbc, &stmt))) {
        captureDiag(c.SQL_HANDLE_DBC, cn.dbc, "SQLAllocHandle(STMT)");
        return null;
    }
    if (rowset > 1) {
        _ = c.SQLSetStmtAttr(stmt, c.SQL_ATTR_ROW_ARRAY_SIZE, @ptrFromInt(@as(usize, @intCast(rowset))), 0);
    }

    if (!isOk(c.SQLExecDirect(stmt, @constCast(sql), c.SQL_NTS))) {
        captureDiag(c.SQL_HANDLE_STMT, stmt, "SQLExecDirect");
        _ = c.SQLFreeHandle(c.SQL_HANDLE_STMT, stmt);
        return null;
    }

    const r = alloc.create(Result) catch {
        _ = c.SQLFreeHandle(c.SQL_HANDLE_STMT, stmt);
        setErr("out of memory");
        return null;
    };
    r.* = .{ .stmt = stmt, .cols = &.{} };
    describe(r) catch {
        freeResult(r);
        return null;
    };
    return r;
}

/// Statements with no result set (DDL/DML). Returns 0 on success.
pub export fn bo_conn_execute(conn: ?*Conn, sql: [*:0]const u8) callconv(.c) c_int {
    g_err[0] = 0;
    const cn = conn orelse {
        setErr("null connection");
        return 1;
    };
    var stmt: ?*anyopaque = null;
    if (!isOk(c.SQLAllocHandle(c.SQL_HANDLE_STMT, cn.dbc, &stmt))) {
        captureDiag(c.SQL_HANDLE_DBC, cn.dbc, "SQLAllocHandle(STMT)");
        return 1;
    }
    defer _ = c.SQLFreeHandle(c.SQL_HANDLE_STMT, stmt);
    if (!isOk(c.SQLExecDirect(stmt, @constCast(sql), c.SQL_NTS))) {
        captureDiag(c.SQL_HANDLE_STMT, stmt, "SQLExecDirect");
        return 1;
    }
    return 0;
}

pub export fn bo_res_free(res: ?*Result) callconv(.c) void {
    freeResult(res orelse return);
}

pub export fn bo_res_ncols(res: ?*Result) callconv(.c) c_int {
    const r = res orelse return 0;
    return @intCast(r.cols.len);
}

pub export fn bo_res_colname(res: ?*Result, col: c_int) callconv(.c) [*:0]const u8 {
    const r = res orelse return "";
    const i: usize = @intCast(col);
    if (i >= r.cols.len) return "";
    return r.cols[i].name.ptr;
}

pub export fn bo_res_coltype(res: ?*Result, col: c_int) callconv(.c) c_int {
    const r = res orelse return 0;
    const i: usize = @intCast(col);
    if (i >= r.cols.len) return 0;
    return @intCast(r.cols[i].sql_type);
}

/// Advance one row. 1 = a row is available, 0 = end of results, -1 = error.
pub export fn bo_res_next(res: ?*Result) callconv(.c) c_int {
    const r = res orelse return -1;
    g_err[0] = 0;

    const rc = c.SQLFetch(r.stmt);
    if (rc == c.SQL_NO_DATA) return 0;
    if (!isOk(rc)) {
        captureDiag(c.SQL_HANDLE_STMT, r.stmt, "SQLFetch");
        return -1;
    }
    // Read every column now: ODBC will not let SQLGetData revisit one, and the
    // caller asks about null-ness and value separately.
    for (0..r.cols.len) |i| {
        readColumn(r, i) catch return -1;
    }
    return 1;
}

pub export fn bo_res_is_null(res: ?*Result, col: c_int) callconv(.c) c_int {
    const r = res orelse return 1;
    const i: usize = @intCast(col);
    if (i >= r.cols.len) return 1;
    return if (r.cols[i].is_null) 1 else 0;
}

pub export fn bo_res_i64(res: ?*Result, col: c_int) callconv(.c) i64 {
    const r = res orelse return 0;
    const i: usize = @intCast(col);
    return if (i < r.cols.len) r.cols[i].i64v else 0;
}

pub export fn bo_res_f64(res: ?*Result, col: c_int) callconv(.c) f64 {
    const r = res orelse return 0;
    const i: usize = @intCast(col);
    return if (i < r.cols.len) r.cols[i].f64v else 0;
}

/// Borrowed, NUL-terminated; valid until the next bo_res_next on this result.
pub export fn bo_res_str(res: ?*Result, col: c_int) callconv(.c) [*:0]const u8 {
    const r = res orelse return "";
    const i: usize = @intCast(col);
    if (i >= r.cols.len) return "";
    const t = r.cols[i].text.items;
    if (t.len == 0) return "";
    return @ptrCast(t.ptr);
}
