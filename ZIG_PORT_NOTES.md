# Zig port — blocked on nanodbc

`build.zig` here is complete and correct; it is committed and everything in it
works **except** that nanodbc will not compile. Delete nanodbc from the picture
and this repo is done.

## What already works

- unixODBC located by probing for a real `sql.h` (brew, `/usr/local`, `/usr`, or
  `UNIXODBC_PREFIX`). Note this is deliberately *not* `brew --prefix unixodbc`:
  that command prints a path whether or not the formula is installed, which is
  precisely how the absent HiGHS in blobsolver managed to look present.
- jsoncons and nanodbc pinned in `build.zig.zon`, no FetchContent.
- Both shims wired through blobzig, DuckDB metadata footer stamped for the target.
- The dead SQLite-amalgamation block is gone — this repo had the same one as
  blobboxes, downloading and compiling the whole amalgamation into a static
  library that nothing ever linked, purely to get `sqlite3ext.h` on the include
  path. blobzig supplies the header.

## The blocker

nanodbc does not compile against a current libc++, at `v2.14.0` or at `main`
(`fd9b4f5`). Two distinct problems:

| Problem | Occurrences | Fixable by flags? |
|---|---|---|
| `std::wstring_convert` / `codecvt_utf8_utf16` deprecated | 4 | yes — `-Wno-deprecated-declarations` |
| `std::char_traits<unsigned char>` undefined template | 15 | **no** |

The second is fatal. libc++ removed the primary `std::char_traits<T>` template
(P1148R0); only the `char`/`wchar_t`/`char8_t`/`char16_t`/`char32_t`
specialisations remain. nanodbc instantiates `std::basic_string<unsigned char>`
for its wide-string handling, and there is no macro that reinstates the base
template. This is not a Zig issue — nanodbc will fail the same way on any
sufficiently new libc++, including Homebrew LLVM. **The CMake build works only
because AppleClang's older libc++ still has the removed template.** That is a
latent maintenance problem for this repo independent of the Zig migration: the
next Xcode bump could break it.

## The fix, which was always the plan

Drop nanodbc and call the ODBC C API (`sql.h` / `sqlext.h`) directly.

nanodbc is a *convenience wrapper* over a C API — precisely the kind of C++ layer
this migration removes elsewhere (nlohmann/json to `std.json`, hash-library to
`std.crypto`). The fat library here has always been unixODBC, and its C API is
exactly what Zig binds best.

Cost: `src/blobodbc_core.cpp` is 1,525 lines written against nanodbc's C++ types
(`nanodbc::connection`, `nanodbc::result`, exceptions). Rewriting it against
`SQLAllocHandle`/`SQLDriverConnect`/`SQLFetch`/`SQLGetData` with explicit return
codes is a real port, and the error-handling shape changes throughout —
exceptions become `SQLRETURN` checks and `SQLGetDiagRec`. A day, maybe two, and
it wants to be done attentively rather than overnight, because silent truncation
in `SQLGetData` buffer handling is easy to get wrong.

Doing it in Zig rather than C++ is no extra cost — arguably less, since ODBC's
out-parameter-heavy API is more pleasant with Zig error unions than with either
raw C or nanodbc's exceptions.

## Interim option

If you want CMake gone from this repo before that rewrite: pin nanodbc and patch
its `basic_string<unsigned char>` usage to `basic_string<char>` (the bytes are
the same; only the type differs), and carry the patch in `patches/`. That is the
same pattern the blobboxes CMake build uses for its xlnt fix. It works, but it
means maintaining a patch against a dependency you intend to delete, so it is
only worth it if the rewrite is months away.
