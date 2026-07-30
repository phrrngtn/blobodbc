//! blobodbc — ODBC access for DuckDB / SQLite / Python.
//!
//! Fifth consumer of blobzig. Shape: the fat library is the system ODBC driver
//! manager (unixODBC), a plain C API reached through nanodbc — a thin C++ wrapper
//! that is one .cpp file, compiled here from the pinned upstream tag.
//!
//! Replaces 122 lines of CMakeLists plus FetchContents of jsoncons and nanodbc,
//! a SQLite amalgamation download (which built a static lib nothing linked — the
//! same dead block that was in blobboxes), and a FetchContent of the DuckDB
//! headers.
//!
//! unixODBC is the one genuine system dependency in the family: a driver manager
//! IS host configuration (odbcinst.ini, installed drivers), so vendoring it would
//! be wrong even if it were easy. Located by probing for a real header, rather
//! than CMake's `execute_process(brew --prefix)` — note that `brew --prefix`
//! prints a path whether or not the formula is installed, which is exactly how
//! the absent HiGHS in blobsolver managed to look present.

const std = @import("std");
const blobzig = @import("blobzig");

const c_flags: []const []const u8 = &.{"-std=c11"};
const cxx_flags: []const []const u8 = &.{ "-std=c++17", "-Wno-deprecated-declarations", "-Wno-error=deprecated-declarations" };

/// Where the ODBC driver manager lives. Probed for an actual header at configure
/// time, so a missing unixODBC is a clear message rather than a link error about
/// SQLAllocHandle.
fn odbcPrefix(b: *std.Build) []const u8 {
    if (b.graph.environ_map.get("UNIXODBC_PREFIX")) |p| return p;
    for ([_][]const u8{
        "/opt/homebrew/opt/unixodbc",
        "/usr/local/opt/unixodbc",
        "/usr",
    }) |prefix| {
        const probe = b.pathJoin(&.{ prefix, "include", "sql.h" });
        if (std.Io.Dir.cwd().access(b.graph.io, probe, .{})) |_| return prefix else |_| {}
    }
    std.debug.panic(
        "blobodbc: unixODBC not found — `brew install unixodbc`, or set UNIXODBC_PREFIX",
        .{},
    );
}

fn base(b: *std.Build, t: std.Build.ResolvedTarget, o: std.builtin.OptimizeMode) *std.Build.Module {
    return b.createModule(.{ .target = t, .optimize = o, .link_libc = true });
}

const Deps = struct {
    nanodbc: *std.Build.Dependency,
    jsoncons: *std.Build.Dependency,
    odbc: []const u8,
};

fn addCore(b: *std.Build, mod: *std.Build.Module, d: Deps) void {
    mod.addIncludePath(b.path("include"));
    mod.addIncludePath(d.jsoncons.path("include"));
    mod.addIncludePath(d.nanodbc.path("."));
    mod.addIncludePath(.{ .cwd_relative = b.pathJoin(&.{ d.odbc, "include" }) });

    mod.addCSourceFile(.{ .file = d.nanodbc.path("nanodbc/nanodbc.cpp"), .flags = cxx_flags });
    mod.addCSourceFile(.{ .file = b.path("src/blobodbc_core.cpp"), .flags = cxx_flags });

    mod.addLibraryPath(.{ .cwd_relative = b.pathJoin(&.{ d.odbc, "lib" }) });
    mod.linkSystemLibrary("odbc", .{});
    mod.link_libcpp = true;
}

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const bz = b.dependency("blobzig", .{ .target = target, .optimize = optimize });
    const d: Deps = .{
        .nanodbc = b.dependency("nanodbc", .{}),
        .jsoncons = b.dependency("jsoncons", .{}),
        .odbc = odbcPrefix(b),
    };

    const core = base(b, target, optimize);
    addCore(b, core, d);

    const duckdb_mod = base(b, target, optimize);
    addCore(b, duckdb_mod, d);
    duckdb_mod.addIncludePath(bz.namedLazyPath("duckdb_capi_include"));
    duckdb_mod.addCSourceFile(.{ .file = b.path("duckdb_ext/src/blobodbc_duckdb.c"), .flags = c_flags });

    const sqlite_mod = base(b, target, optimize);
    addCore(b, sqlite_mod, d);
    sqlite_mod.addIncludePath(bz.namedLazyPath("sqlite_include"));
    sqlite_mod.addCSourceFile(.{ .file = b.path("sqlite_ext/src/blobodbc_sqlite.c"), .flags = c_flags });

    const artifacts = blobzig.addHostExtensions(b, bz, .{
        .name = "blobodbc",
        .target = target,
        .optimize = optimize,
        .core = core,
        .duckdb_module = duckdb_mod,
        .sqlite_module = sqlite_mod,
    });
    artifacts.lib.?.installHeader(b.path("include/blobodbc.h"), "blobodbc.h");
}
