cmake_minimum_required(VERSION 3.20)
cmake_policy(SET CMP0148 OLD)
project(blobodbc_duckdb LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

set(ROOT "${CMAKE_CURRENT_SOURCE_DIR}")

include(FetchContent)

# ── jsoncons (header-only: JSON, JSON Patch, JMESPath) ────────────
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
FetchContent_MakeAvailable(nanodbc)

# ── Core sources ──────────────────────────────────────────────────
set(BO_SOURCES ${ROOT}/src/blobodbc_core.cpp)
set(BO_INCLUDES ${ROOT}/include ${jsoncons_SOURCE_DIR}/include)
set(BO_LIBS nanodbc)

# ── DuckDB C API headers ─────────────────────────────────────────
FetchContent_Declare(duckdb_capi
    GIT_REPOSITORY https://github.com/duckdb/extension-template-c.git
    GIT_TAG        main
    GIT_SHALLOW    TRUE)
FetchContent_Populate(duckdb_capi)
set(DUCKDB_CAPI_INCLUDE "${duckdb_capi_SOURCE_DIR}/duckdb_capi")

# ── DuckDB extension ────────────────────────────────────────────
add_library(blobodbc_duckdb MODULE
    ${ROOT}/duckdb_ext/src/blobodbc_duckdb.c ${BO_SOURCES})
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

# Generate .duckdb_extension with metadata footer
find_program(PYTHON_EXE NAMES python3 python REQUIRED)
add_custom_command(TARGET blobodbc_duckdb POST_BUILD
    COMMAND ${PYTHON_EXE} "${ROOT}/duckdb_ext/append_metadata.py"
        "$<TARGET_FILE:blobodbc_duckdb>"
        "$<TARGET_FILE_DIR:blobodbc_duckdb>/blobodbc.duckdb_extension"
    COMMENT "Appending DuckDB extension metadata -> blobodbc.duckdb_extension")

install(FILES "${CMAKE_BINARY_DIR}/duckdb/blobodbc.duckdb_extension"
    DESTINATION blobodbc_duckdb/)
