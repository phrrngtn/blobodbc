cmake_minimum_required(VERSION 3.20)
cmake_policy(SET CMP0148 OLD)
project(blobodbc_sqlite LANGUAGES C CXX)

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

# ── SQLite amalgamation ──────────────────────────────────────────────
FetchContent_Declare(sqlite3
    URL "https://www.sqlite.org/2024/sqlite-amalgamation-3460100.zip"
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
FetchContent_MakeAvailable(sqlite3)

# ── Core sources ──────────────────────────────────────────────────
set(BO_SOURCES ${ROOT}/src/blobodbc_core.cpp)
set(BO_INCLUDES ${ROOT}/include ${jsoncons_SOURCE_DIR}/include)
set(BO_LIBS nanodbc)

# ── SQLite extension ────────────────────────────────────────────
add_library(blobodbc_sqlite MODULE
    ${ROOT}/sqlite_ext/src/blobodbc_sqlite.c ${BO_SOURCES})
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

install(TARGETS blobodbc_sqlite LIBRARY DESTINATION blobodbc_sqlite/)
if(APPLE)
    install(CODE "
        execute_process(COMMAND \${CMAKE_COMMAND} -E create_symlink
            blobodbc.so blobodbc.dylib
            WORKING_DIRECTORY \${CMAKE_INSTALL_PREFIX}/blobodbc_sqlite)
    ")
endif()
