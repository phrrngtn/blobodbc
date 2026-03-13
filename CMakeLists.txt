cmake_minimum_required(VERSION 3.20)
project(blobodbc LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)

# ── nlohmann/json ─────────────────────────────────────────────────
FetchContent_Declare(nlohmann_json
  URL "https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz"
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE)

# ── jsoncons (header-only, internal use for JMESPath reshaping) ───
set(JSONCONS_BUILD_TESTS OFF CACHE BOOL "" FORCE)
FetchContent_Declare(jsoncons
  GIT_REPOSITORY https://github.com/danielaparker/jsoncons.git
  GIT_TAG        v1.1.0
  GIT_SHALLOW    TRUE)
FetchContent_MakeAvailable(jsoncons)

# ── nanodbc ───────────────────────────────────────────────────────
FetchContent_Declare(nanodbc
  GIT_REPOSITORY https://github.com/nanodbc/nanodbc.git
  GIT_TAG        main
  GIT_SHALLOW    TRUE)
set(NANODBC_DISABLE_TESTS ON CACHE BOOL "" FORCE)
set(NANODBC_DISABLE_EXAMPLES ON CACHE BOOL "" FORCE)
set(NANODBC_DISABLE_INSTALL ON CACHE BOOL "" FORCE)

FetchContent_MakeAvailable(nlohmann_json nanodbc)

# ── Core sources ──────────────────────────────────────────────────
set(BO_SOURCES src/blobodbc_core.cpp)
set(BO_INCLUDES include ${jsoncons_SOURCE_DIR}/include)
set(BO_LIBS nanodbc nlohmann_json::nlohmann_json)

# ── C example ────────────────────────────────────────────────────
if(EXISTS "${CMAKE_SOURCE_DIR}/example/main.c")
  add_executable(blobodbc_example example/main.c ${BO_SOURCES})
  target_include_directories(blobodbc_example PRIVATE ${BO_INCLUDES})
  target_link_libraries(blobodbc_example PRIVATE ${BO_LIBS})
endif()

# ── Python bindings (nanobind) ───────────────────────────────────
option(BUILD_PYTHON_BINDINGS "Build nanobind Python module" OFF)
if(BUILD_PYTHON_BINDINGS)
  find_package(Python 3.8 REQUIRED COMPONENTS Interpreter Development.Module)
  execute_process(COMMAND "${Python_EXECUTABLE}" -m nanobind --cmake_dir
    OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE NB_DIR)
  list(APPEND CMAKE_PREFIX_PATH "${NB_DIR}")
  find_package(nanobind CONFIG REQUIRED)

  nanobind_add_module(blobodbc_ext NB_STATIC python/bindings.cpp ${BO_SOURCES})
  target_include_directories(blobodbc_ext PRIVATE ${BO_INCLUDES})
  target_link_libraries(blobodbc_ext PRIVATE ${BO_LIBS})
  set_target_properties(blobodbc_ext PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_SOURCE_DIR}/python/blobodbc")
endif()

# ── DuckDB C API headers (for extension build) ──────────────────
# We only need headers from this repo, not its CMake project,
# so use FetchContent_Populate (not MakeAvailable).
FetchContent_Declare(duckdb_capi
  GIT_REPOSITORY https://github.com/duckdb/extension-template-c.git
  GIT_TAG        main
  GIT_SHALLOW    TRUE)
FetchContent_Populate(duckdb_capi)
set(DUCKDB_CAPI_INCLUDE "${duckdb_capi_SOURCE_DIR}/duckdb_capi")

# ── DuckDB extension ────────────────────────────────────────────
option(BUILD_DUCKDB_EXTENSION "Build DuckDB loadable extension" OFF)
if(BUILD_DUCKDB_EXTENSION)
  add_library(blobodbc_duckdb MODULE
    duckdb_ext/src/blobodbc_duckdb.c ${BO_SOURCES})
  target_include_directories(blobodbc_duckdb PRIVATE
    ${BO_INCLUDES} ${DUCKDB_CAPI_INCLUDE})
  target_link_libraries(blobodbc_duckdb PRIVATE ${BO_LIBS})
  set_target_properties(blobodbc_duckdb PROPERTIES
    OUTPUT_NAME "blobodbc"
    PREFIX ""
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/duckdb")
  if(APPLE)
    target_link_options(blobodbc_duckdb PRIVATE -undefined dynamic_lookup)
  endif()
endif()

# ── SQLite extension ────────────────────────────────────────────
option(BUILD_SQLITE_EXTENSION "Build SQLite loadable extension" OFF)
if(BUILD_SQLITE_EXTENSION)
  FetchContent_Declare(sqlite3
    URL "https://www.sqlite.org/2024/sqlite-amalgamation-3460100.zip"
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
  FetchContent_MakeAvailable(sqlite3)

  if(NOT TARGET sqlite3_lib)
    add_library(sqlite3_lib STATIC ${sqlite3_SOURCE_DIR}/sqlite3.c)
    target_include_directories(sqlite3_lib PUBLIC ${sqlite3_SOURCE_DIR})
    target_compile_definitions(sqlite3_lib PRIVATE SQLITE_CORE)
  endif()

  add_library(blobodbc_sqlite MODULE
    sqlite_ext/src/blobodbc_sqlite.c ${BO_SOURCES})
  target_include_directories(blobodbc_sqlite PRIVATE
    ${BO_INCLUDES} ${sqlite3_SOURCE_DIR})
  target_link_libraries(blobodbc_sqlite PRIVATE ${BO_LIBS})
  set_target_properties(blobodbc_sqlite PROPERTIES
    OUTPUT_NAME "blobodbc"
    PREFIX ""
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/sqlite")
  if(APPLE)
    target_link_options(blobodbc_sqlite PRIVATE -undefined dynamic_lookup)
    add_custom_command(TARGET blobodbc_sqlite POST_BUILD
      COMMAND ${CMAKE_COMMAND} -E create_symlink
        blobodbc.so "$<TARGET_FILE_DIR:blobodbc_sqlite>/blobodbc.dylib")
  endif()
endif()
