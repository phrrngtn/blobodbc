//! Standalone check of src/odbc.zig against the live rule4_test DSN.
const std = @import("std");
const o = @import("odbc");

pub fn main() !void {
    const conn = o.bo_conn_open("DSN=rule4_test") orelse {
        std.debug.print("connect failed: {s}\n", .{o.bo_odbc_errmsg()});
        return error.Connect;
    };
    defer o.bo_conn_close(conn);
    std.debug.print("connected, alive={d}\n", .{o.bo_conn_alive(conn)});

    const sql =
        \\SELECT 42::bigint AS i, 3.5::double precision AS f, 'hello'::text AS s,
        \\       NULL::text AS n, DATE '2026-07-30' AS d,
        \\       TIMESTAMP '2026-07-30 01:02:03.5' AS ts,
        \\       repeat('x', 10000) AS big
    ;
    const res = o.bo_conn_query(conn, sql, 1) orelse {
        std.debug.print("query failed: {s}\n", .{o.bo_odbc_errmsg()});
        return error.Query;
    };
    defer o.bo_res_free(res);

    const n = o.bo_res_ncols(res);
    std.debug.print("ncols={d}\n", .{n});
    while (o.bo_res_next(res) == 1) {
        var i: c_int = 0;
        while (i < n) : (i += 1) {
            const name = o.bo_res_colname(res, i);
            if (o.bo_res_is_null(res, i) == 1) {
                std.debug.print("  {s} = NULL\n", .{name});
                continue;
            }
            const s = std.mem.span(o.bo_res_str(res, i));
            std.debug.print("  {s} type={d} i64={d} f64={d} str.len={d} str={s}\n", .{
                name, o.bo_res_coltype(res, i), o.bo_res_i64(res, i), o.bo_res_f64(res, i),
                s.len, s,
            });
        }
    }
    std.debug.print("done\n", .{});
}
