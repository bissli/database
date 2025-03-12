"""
Integration tests for SQL Server column aliasing with ODBC Driver 18+.

These tests verify that column names are properly preserved with ODBC Driver 18+,
which corrects the column name truncation issues present in earlier drivers.
"""

import logging

import database as db
import pandas as pd
import pyodbc

logger = logging.getLogger(__name__)


def test_simple_column_aliasing(sconn):
    """Test that long column names are preserved correctly with Driver 18+"""
    # Create a temporary table with columns including very long names
    create_table = """
    CREATE TABLE #test_aliasing (
        id INT,
        very_long_column_name VARCHAR(100),
        another_long_column_name VARCHAR(100),
        extremely_long_column_name_that_would_be_truncated_with_older_drivers VARCHAR(100)
    )
    """

    # Insert some test data
    insert_data = """
    INSERT INTO #test_aliasing
    (id, very_long_column_name, another_long_column_name, extremely_long_column_name_that_would_be_truncated_with_older_drivers)
    VALUES (1, 'Value 1', 'Another 1', 'Long value 1'),
           (2, 'Value 2', 'Another 2', 'Long value 2')
    """

    with db.transaction(sconn) as tx:
        tx.execute(create_table)
        tx.execute(insert_data)

    # Test simple query - columns should be fully preserved without explicit aliasing
    result = db.select(sconn, """SELECT id, very_long_column_name, another_long_column_name,
    extremely_long_column_name_that_would_be_truncated_with_older_drivers FROM #test_aliasing""")

    # Check that column names are preserved completely without truncation
    assert isinstance(result, list)
    assert list(result[0].keys()) == [
        'id',
        'very_long_column_name',
        'another_long_column_name',
        'extremely_long_column_name_that_would_be_truncated_with_older_drivers'
    ]

    # Verify data is correct
    assert len(result) == 2
    assert result[0]['very_long_column_name'] == 'Value 1'
    assert result[1]['another_long_column_name'] == 'Another 2'
    assert result[0]['extremely_long_column_name_that_would_be_truncated_with_older_drivers'] == 'Long value 1'


def test_expression_column_aliasing(sconn):
    """Test column aliasing with expressions and functions

    With ODBC Driver 18+, simple columns don't need aliases but expressions
    still need them to have meaningful names.
    """
    # Create a temporary table
    create_table = """
    CREATE TABLE #test_expression (
        id INT,
        firstName VARCHAR(100),
        lastName VARCHAR(100),
        value DECIMAL(10,2)
    )
    """

    # Insert test data
    insert_data = """
    INSERT INTO #test_expression (id, firstName, lastName, value)
    VALUES (1, 'John', 'Doe', 100.50),
           (2, 'Jane', 'Smith', 200.75)
    """

    with db.transaction(sconn) as tx:
        tx.execute(create_table)
        tx.execute(insert_data)

    # Test query with expressions - only expressions need aliases with Driver 18+
    query = """
    SELECT
        id,
        firstName + ' ' + lastName AS full_name,
        value * 1.1 AS adjusted_value,
        CASE WHEN value > 150 THEN 'High' ELSE 'Low' END AS value_category,
        UPPER(firstName) AS upper_name,
        CONVERT(VARCHAR(10), GETDATE(), 101) AS formatted_date
    FROM #test_expression
    """

    result = db.select(sconn, query)

    # Check that we have the expected columns with correct explicit aliases
    assert isinstance(result, list)
    assert len(result[0]) == 6

    # First column should be 'id' (simple column, no alias needed)
    assert list(result[0].keys())[0] == 'id'

    # Expression columns should have our explicit aliases
    assert 'full_name' in result[0]
    assert 'adjusted_value' in result[0]
    assert 'value_category' in result[0]
    assert 'upper_name' in result[0]
    assert 'formatted_date' in result[0]

    # Test the data content
    assert result[0]['full_name'] == 'John Doe'
    assert float(result[1]['adjusted_value']) > 220.0  # 200.75 * 1.1
    assert result[0]['value_category'] == 'Low'
    assert result[1]['value_category'] == 'High'
    assert result[1]['upper_name'] == 'JANE'


def test_union_query_aliasing(sconn):
    """Test column aliasing with UNION queries

    With ODBC Driver 18+, only the first SELECT statement in a UNION needs
    column names as these determine the output column names.
    """
    # Create two temporary tables
    create_tables = """
    CREATE TABLE #test_union1 (
        id INT,
        description VARCHAR(100)
    );
    CREATE TABLE #test_union2 (
        id INT,
        notes VARCHAR(100)
    )
    """

    # Insert test data
    insert_data = """
    INSERT INTO #test_union1 (id, description) VALUES (1, 'First table');
    INSERT INTO #test_union2 (id, notes) VALUES (2, 'Second table')
    """

    with db.transaction(sconn) as tx:
        tx.execute(create_tables)
        tx.execute(insert_data)

    # Test UNION query - column names come from the first SELECT statement
    query = """
    SELECT id, description FROM #test_union1
    UNION
    SELECT id, notes FROM #test_union2
    """

    # With ODBC Driver 18+, no special processing is needed
    from database.utils.sqlserver_utils import handle_set_operations
    processed_sql = handle_set_operations(query)

    # With Driver 18+, the SQL should be returned unchanged
    assert processed_sql == query

    # Then test the actual SQL execution
    result = db.select(sconn, query)

    # Check results
    assert isinstance(result, list)
    assert len(result[0]) == 2

    # Column names should be from the first SELECT
    assert list(result[0].keys()) == ['id', 'description']

    # Should have 2 rows (one from each table)
    assert len(result) == 2

    # Check both values are in the result
    description_values = [row['description'] for row in result]
    assert 'First table' in description_values
    assert 'Second table' in description_values


def test_subquery_aliasing(sconn):
    """Test column aliasing with subqueries

    With ODBC Driver 18+, subqueries still need their aggregations to have explicit aliases,
    but outer query simple columns don't need aliases.
    """
    # Create temp tables
    create_tables = """
    CREATE TABLE #test_main (
        id INT,
        name VARCHAR(100),
        very_long_column_name VARCHAR(100)
    );
    CREATE TABLE #test_details (
        main_id INT,
        value DECIMAL(10,2),
        another_long_column_name VARCHAR(100)
    )
    """

    # Insert test data
    insert_data = """
    INSERT INTO #test_main (id, name, very_long_column_name) VALUES
        (1, 'Item 1', 'Long value 1'),
        (2, 'Item 2', 'Long value 2');

    INSERT INTO #test_details (main_id, value, another_long_column_name) VALUES
        (1, 10.50, 'Detail 1'),
        (1, 20.75, 'Detail 2'),
        (2, 15.30, 'Detail 3')
    """

    with db.transaction(sconn) as tx:
        tx.execute(create_tables)
        tx.execute(insert_data)

    # Test query with subquery in JOIN - only aggregates need explicit aliases
    query = """
    SELECT
        m.id,
        m.name,
        m.very_long_column_name,
        s.total_value,
        s.detail_count
    FROM #test_main m
    JOIN (
        SELECT
            main_id,
            SUM(value) AS total_value,
            COUNT(*) AS detail_count,
            MAX(another_long_column_name) AS latest_detail
        FROM #test_details
        GROUP BY main_id
    ) s ON m.id = s.main_id
    """

    result = db.select(sconn, query)

    # Check results
    assert isinstance(result, list)

    # Should have expected columns, including very long ones
    expected_keys = ['id', 'name', 'very_long_column_name', 'total_value', 'detail_count']
    for key in expected_keys:
        assert key in result[0]

    # Verify data
    assert len(result) == 2

    # Group the rows by id
    id1_row = next(row for row in result if row['id'] == 1)
    id2_row = next(row for row in result if row['id'] == 2)

    # Verify values
    assert float(id1_row['total_value']) > 30.0  # 10.50 + 20.75
    assert float(id2_row['total_value']) > 15.0  # 15.30
    assert int(id1_row['detail_count']) == 2
    assert int(id2_row['detail_count']) == 1


def test_cte_aliasing(sconn):
    """Test column aliasing with CTE queries

    With ODBC Driver 18+, only aggregation columns and expressions in CTEs
    need explicit aliases.
    """
    # Test the function with a CTE query
    query = """
    WITH cte_data AS (
        SELECT id, name, SUM(value) AS total, COUNT(*) AS record_count
        FROM source_table
        GROUP BY id, name
    )
    SELECT
        c.id,
        c.name,
        c.total,
        c.record_count,
        t.description
    FROM cte_data c
    JOIN lookup_table t ON c.id = t.id
    """

    # Import the function we need to test
    from database.utils.sqlserver_utils import add_explicit_column_aliases
    processed_sql = add_explicit_column_aliases(query)

    # With ODBC Driver 18+, this query should be unchanged since all
    # expressions already have aliases and simple columns don't need them
    assert processed_sql == query

    # Now create test tables and check actual SQL execution
    create_tables = """
    CREATE TABLE #test_cte_source (
        id INT,
        category VARCHAR(50),
        value DECIMAL(10,2),
        additional_long_column_name VARCHAR(100)
    );
    """

    # Insert test data
    insert_data = """
    INSERT INTO #test_cte_source (id, category, value, additional_long_column_name) VALUES
        (1, 'A', 10.5, 'Extra A1'),
        (2, 'A', 20.3, 'Extra A2'),
        (3, 'B', 15.7, 'Extra B1'),
        (4, 'B', 25.2, 'Extra B2')
    """

    with db.transaction(sconn) as tx:
        tx.execute(create_tables)
        tx.execute(insert_data)

    # Test CTE query with actual data using long column names
    query = """
    WITH category_totals AS (
        SELECT
            category,
            additional_long_column_name,
            SUM(value) AS total_value,
            AVG(value) AS avg_value,
            COUNT(*) AS item_count
        FROM #test_cte_source
        GROUP BY category, additional_long_column_name
    )
    SELECT
        ct.category,
        ct.additional_long_column_name,
        ct.total_value,
        ct.avg_value,
        ct.item_count
    FROM category_totals ct
    ORDER BY ct.category, ct.additional_long_column_name
    """

    result = db.select(sconn, query)

    # Check results
    assert isinstance(result, list)

    # Should have expected columns including long name
    expected_keys = ['category', 'additional_long_column_name', 'total_value', 'avg_value', 'item_count']
    for key in expected_keys:
        assert key in result[0]

    # Should have 4 rows (one per category+additional_column combination)
    assert len(result) == 4

    # Group rows by category and additional column
    a_rows_a1 = [row for row in result if row['category'] == 'A' and row['additional_long_column_name'] == 'Extra A1']
    a_rows_a2 = [row for row in result if row['category'] == 'A' and row['additional_long_column_name'] == 'Extra A2']
    b_rows_b1 = [row for row in result if row['category'] == 'B' and row['additional_long_column_name'] == 'Extra B1']
    b_rows_b2 = [row for row in result if row['category'] == 'B' and row['additional_long_column_name'] == 'Extra B2']

    # Check counts
    assert len(a_rows_a1) == 1
    assert len(a_rows_a2) == 1
    assert len(b_rows_b1) == 1
    assert len(b_rows_b2) == 1

    # Check values
    assert float(a_rows_a1[0]['total_value']) == 10.5
    assert float(a_rows_a2[0]['total_value']) == 20.3
    assert int(a_rows_a1[0]['item_count']) == 1

    assert float(b_rows_b1[0]['total_value']) == 15.7
    assert float(b_rows_b2[0]['total_value']) == 25.2
    assert int(b_rows_b2[0]['item_count']) == 1


def test_sqlserver_column_preservation(sqlserver_docker):
    """Directly test SQL Server column name preservation with ODBC Driver 18+."""
    # Import the config to get connection details
    from tests.config import mssql

    # Create a direct connection to SQL Server bypassing the database module
    conn_str = (
        f'DRIVER={{{mssql.driver}}};'
        f'SERVER=localhost,{mssql.port};'
        f'DATABASE={mssql.database};'
        f'UID={mssql.username};'
        f'PWD={mssql.password};'
        f'TrustServerCertificate={mssql.trust_server_certificate};'
        'MARS_Connection=Yes;'
        'ANSI_NULLS=Yes;'
    )

    # Connect directly to SQL Server
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    logger.info('Connected directly to SQL Server')

    # Create a temporary table with very long column names
    cursor.execute("""
    CREATE TABLE #column_preservation_test (
        id INT,
        very_long_column_name VARCHAR(100),
        another_long_column_name VARCHAR(100),
        extremely_long_column_name_that_would_be_truncated_with_older_drivers VARCHAR(100)
    )
    """)

    # Insert test data
    cursor.execute("""
    INSERT INTO #column_preservation_test
    (id, very_long_column_name, another_long_column_name, extremely_long_column_name_that_would_be_truncated_with_older_drivers)
    VALUES
    (1, 'Value 1', 'Another 1', 'Long 1'),
    (2, 'Value 2', 'Another 2', 'Long 2')
    """)

    logger.info('Created test table and inserted data')

    # Execute a simple query without any aliasing
    cursor.execute('SELECT id, very_long_column_name, another_long_column_name, extremely_long_column_name_that_would_be_truncated_with_older_drivers FROM #column_preservation_test')

    # Examine the raw cursor description
    logger.info('Raw cursor description:')
    for i, desc in enumerate(cursor.description):
        logger.info(f"Column {i}: name='{desc[0]}', type={desc[1].__name__ if hasattr(desc[1], '__name__') else desc[1]}")
        if isinstance(desc[0], str):
            byte_repr = ' '.join(f'{ord(c):02x}' for c in desc[0])
            logger.info(f'   Bytes: {byte_repr}')

    # Examine the row structure
    rows = cursor.fetchall()
    if rows:
        row = rows[0]

        # Check if the row has __members__ attribute (pyodbc specific)
        if hasattr(row, '__members__'):
            logger.info(f'Row members: {row.__members__}')

            # Try to access columns by full names
            logger.info('Testing column access:')

            # id column
            logger.info(f"  Row.id exists: {hasattr(row, 'id')}")
            if hasattr(row, 'id'):
                logger.info(f'  Row.id value: {row.id}')

            # very_long_column_name
            logger.info(f"  Row.very_long_column_name exists: {hasattr(row, 'very_long_column_name')}")
            if hasattr(row, 'very_long_column_name'):
                logger.info(f'  Row.very_long_column_name value: {row.very_long_column_name}')

            # extremely long column
            logger.info('  Row.extremely_long_column_name_that_would_be_truncated_with_older_drivers exists: ' +
                        f"{hasattr(row, 'extremely_long_column_name_that_would_be_truncated_with_older_drivers')}")
            if hasattr(row, 'extremely_long_column_name_that_would_be_truncated_with_older_drivers'):
                logger.info(f'  Value: {row.extremely_long_column_name_that_would_be_truncated_with_older_drivers}')

    # Try to create a DataFrame directly
    column_names = [desc[0] for desc in cursor.description]
    df = pd.DataFrame([tuple(row) for row in rows], columns=column_names)
    logger.info(f'DataFrame columns: {df.columns.tolist()}')
    logger.info(f'DataFrame first row:\n{df.iloc[0]}')

    # Now, run a query that uses explicit aliasing (AS [column_name]) to compare behavior
    cursor.execute("""
    SELECT
        id AS [id],
        very_long_column_name AS [very_long_column_name],
        another_long_column_name AS [another_long_column_name],
        extremely_long_column_name_that_would_be_truncated_with_older_drivers AS [extremely_long_column_name_that_would_be_truncated_with_older_drivers]
    FROM #column_preservation_test
    """)

    logger.info('Cursor description with explicit aliases:')
    for i, desc in enumerate(cursor.description):
        logger.info(f"Column {i}: name='{desc[0]}', type={desc[1].__name__ if hasattr(desc[1], '__name__') else desc[1]}")

    rows_with_aliases = cursor.fetchall()
    if rows_with_aliases:
        row = rows_with_aliases[0]
        if hasattr(row, '__members__'):
            logger.info(f'Row members with aliases: {row.__members__}')

    column_names_with_aliases = [desc[0] for desc in cursor.description]
    df_with_aliases = pd.DataFrame([tuple(row) for row in rows_with_aliases], columns=column_names_with_aliases)
    logger.info(f'DataFrame columns with aliases: {df_with_aliases.columns.tolist()}')

    # Clean up
    cursor.execute('DROP TABLE #column_preservation_test')
    conn.close()

    # Verify column names are preserved correctly with ODBC Driver 18+
    original_columns = ['id', 'very_long_column_name', 'another_long_column_name',
                        'extremely_long_column_name_that_would_be_truncated_with_older_drivers']
    assert column_names == original_columns, "Column names should be preserved with Driver 18+, but they don't match the original names"

    # Verify we can access the full column names
    if hasattr(rows[0], 'extremely_long_column_name_that_would_be_truncated_with_older_drivers'):
        logger.info('Confirmed: Very long column name is fully preserved')
    else:
        logger.error('Column name preservation test failed - extremely long name not accessible')
        raise AssertionError('Column name preservation test failed')

    # Verify both with and without explicit aliases have same column names
    assert column_names == column_names_with_aliases, 'Column names should be the same with or without explicit aliases'


if __name__ == '__main__':
    __import__('pytest').main([__file__])
