-- blobodbc DuckDB extension — SQL smoke tests (requires ODBC driver + SQL Server)
-- Run: duckdb -unsigned -c ".read test/test_duckdb.sql"
-- (after: LOAD 'build/duckdb/blobodbc.duckdb_extension')

-- Connection string for local SQL Server
SET VARIABLE conn = 'Driver={ODBC Driver 18 for SQL Server};Server=localhost,1433;Database=rule4_test;UID=rule4;PWD=R4Developer!2024;TrustServerCertificate=yes;LoginTimeout=10';

.print === bo_query ===
SELECT length(bo_query(getvariable('conn'), 'SELECT 1 AS x')) AS r;

.print === bo_clob ===
SELECT bo_clob(getvariable('conn'), 'SELECT ''hello'' AS msg') AS r;

.print === bo_query_named ===
SELECT bo_query_named(getvariable('conn'),
    'SELECT :val AS v', '{"val":"test"}') AS r;

.print === bo_clob_named ===
SELECT bo_clob_named(getvariable('conn'),
    'SELECT :val AS v', '{"val":"clob_test"}') AS r;

.print === bo_driver_info ===
SELECT json_extract(bo_driver_info(getvariable('conn')), '$.SQL_DBMS_NAME') AS r;

.print === bo_tables ===
SELECT length(bo_tables(getvariable('conn'))) > 2 AS has_tables;

.print === bo_tables (with schema filter) ===
SELECT length(bo_tables(getvariable('conn'), 'dbo')) > 2 AS has_tables;

.print === bo_columns ===
SELECT length(bo_columns(getvariable('conn'), 'spt_monitor')) > 2 AS has_cols;

.print === bo_query_in_catalog ===
SELECT length(bo_query_in_catalog(getvariable('conn'),
    'master', 'SELECT name FROM sys.databases WHERE name = ''master''')) > 2 AS r;

.print === bo_execute ===
SELECT bo_execute(getvariable('conn'),
    'IF OBJECT_ID(''tempdb..#bo_test'') IS NOT NULL DROP TABLE #bo_test;
     CREATE TABLE #bo_test (id INT); INSERT INTO #bo_test VALUES (1),(2),(3)') AS affected;

.print === bo_primary_keys ===
SELECT bo_primary_keys(getvariable('conn'), 'dbo', 'spt_monitor') AS r;

.print === bo_foreign_keys (3-arg) ===
SELECT typeof(bo_foreign_keys(getvariable('conn'), 'dbo', 'spt_monitor')) AS r;

.print === All tests complete ===
