-- blobodbc SQLite extension — SQL smoke tests (requires ODBC driver + SQL Server)
-- Run: sqlite3 :memory: -cmd ".load build/sqlite/blobodbc" < test/test_sqlite.sql

-- Note: SQLite doesn't have SET VARIABLE, so conn string is repeated inline.

SELECT '=== bo_query ===' AS test;
SELECT length(bo_query(
    'Driver={ODBC Driver 18 for SQL Server};Server=localhost,1433;Database=rule4_test;UID=rule4;PWD=R4Developer!2024;TrustServerCertificate=yes;LoginTimeout=10',
    'SELECT 1 AS x')) AS r;

SELECT '=== bo_clob ===' AS test;
SELECT bo_clob(
    'Driver={ODBC Driver 18 for SQL Server};Server=localhost,1433;Database=rule4_test;UID=rule4;PWD=R4Developer!2024;TrustServerCertificate=yes;LoginTimeout=10',
    'SELECT ''hello'' AS msg') AS r;

SELECT '=== bo_query_named ===' AS test;
SELECT bo_query_named(
    'Driver={ODBC Driver 18 for SQL Server};Server=localhost,1433;Database=rule4_test;UID=rule4;PWD=R4Developer!2024;TrustServerCertificate=yes;LoginTimeout=10',
    'SELECT :val AS v', '{"val":"test"}') AS r;

SELECT '=== bo_clob_named ===' AS test;
SELECT bo_clob_named(
    'Driver={ODBC Driver 18 for SQL Server};Server=localhost,1433;Database=rule4_test;UID=rule4;PWD=R4Developer!2024;TrustServerCertificate=yes;LoginTimeout=10',
    'SELECT :val AS v', '{"val":"clob_test"}') AS r;

SELECT '=== bo_driver_info ===' AS test;
SELECT json_extract(bo_driver_info(
    'Driver={ODBC Driver 18 for SQL Server};Server=localhost,1433;Database=rule4_test;UID=rule4;PWD=R4Developer!2024;TrustServerCertificate=yes;LoginTimeout=10'),
    '$.SQL_DBMS_NAME') AS r;

SELECT '=== bo_tables ===' AS test;
SELECT length(bo_tables(
    'Driver={ODBC Driver 18 for SQL Server};Server=localhost,1433;Database=rule4_test;UID=rule4;PWD=R4Developer!2024;TrustServerCertificate=yes;LoginTimeout=10')) > 2 AS has_tables;

SELECT '=== bo_columns ===' AS test;
SELECT length(bo_columns(
    'Driver={ODBC Driver 18 for SQL Server};Server=localhost,1433;Database=rule4_test;UID=rule4;PWD=R4Developer!2024;TrustServerCertificate=yes;LoginTimeout=10',
    'spt_monitor')) > 2 AS has_cols;

SELECT '=== bo_query_in_catalog ===' AS test;
SELECT length(bo_query_in_catalog(
    'Driver={ODBC Driver 18 for SQL Server};Server=localhost,1433;Database=rule4_test;UID=rule4;PWD=R4Developer!2024;TrustServerCertificate=yes;LoginTimeout=10',
    'master', 'SELECT name FROM sys.databases WHERE name = ''master''')) > 2 AS r;

SELECT '=== bo_execute ===' AS test;
SELECT bo_execute(
    'Driver={ODBC Driver 18 for SQL Server};Server=localhost,1433;Database=rule4_test;UID=rule4;PWD=R4Developer!2024;TrustServerCertificate=yes;LoginTimeout=10',
    'IF OBJECT_ID(''tempdb..#bo_test'') IS NOT NULL DROP TABLE #bo_test;
     CREATE TABLE #bo_test (id INT); INSERT INTO #bo_test VALUES (1),(2),(3)') AS affected;

SELECT '=== bo_primary_keys ===' AS test;
SELECT bo_primary_keys(
    'Driver={ODBC Driver 18 for SQL Server};Server=localhost,1433;Database=rule4_test;UID=rule4;PWD=R4Developer!2024;TrustServerCertificate=yes;LoginTimeout=10',
    'dbo', 'spt_monitor') AS r;

SELECT '=== bo_foreign_keys (3-arg) ===' AS test;
SELECT typeof(bo_foreign_keys(
    'Driver={ODBC Driver 18 for SQL Server};Server=localhost,1433;Database=rule4_test;UID=rule4;PWD=R4Developer!2024;TrustServerCertificate=yes;LoginTimeout=10',
    'dbo', 'spt_monitor')) AS r;

SELECT '=== All tests complete ===' AS test;
