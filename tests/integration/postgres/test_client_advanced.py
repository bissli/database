import datetime

import database as db
import pytest


def test_insert_rows_bulk(psql_docker, conn):
    """Test inserting a large number of rows in a single operation"""
    # Create a temporary table for bulk testing
    db.execute(conn, """
        CREATE TEMPORARY TABLE bulk_test (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            value NUMERIC,
            date DATE
        )
    """)

    # Generate a large number of test rows
    num_rows = 1000
    test_rows = []

    base_date = datetime.date(2025, 1, 1)

    for i in range(num_rows):
        test_rows.append({
            'name': f'Bulk-{i}',
            'value': float(i * 1.5),
            'date': base_date + datetime.timedelta(days=i % 365)
        })

    # Insert the rows
    rows_inserted = db.insert_rows(conn, 'bulk_test', test_rows)

    # Verify all rows were inserted
    assert rows_inserted == num_rows

    # Check a few random rows
    for i in [0, 42, 999]:
        row = db.select_row(conn, 'SELECT * FROM bulk_test WHERE name = %s', f'Bulk-{i}')
        assert row is not None
        assert row.value == float(i * 1.5)


def test_cte_query(psql_docker, conn):
    """Test Common Table Expressions (CTE) queries"""
    # Create a CTE query
    cte_query = """
        WITH highvalue AS (
            SELECT name, value
            FROM test_table
            WHERE value > 50
            ORDER BY value DESC
        )
        SELECT name, value FROM highvalue
    """

    # Execute the query
    result = db.select(conn, cte_query)

    # Verify results
    assert len(result) >= 2
    assert 'George' in result['name'].values

    # Values should be in descending order
    values = list(result['value'])
    assert values == sorted(values, reverse=True)


def test_error_handling_and_retries(psql_docker, conn):
    """Test error handling and retries"""
    # Test handling of syntax errors
    with pytest.raises(Exception):
        try:
            db.select(conn, 'SELECT * FORM test_table')  # Intentional typo
        finally:
            # Rollback to clear the failed transaction
            conn.rollback()

    # Test handling of constraint violations
    with pytest.raises(Exception):
        try:
            # Try to insert a row with duplicate primary key
            db.insert(conn, 'INSERT INTO test_table (name, value) VALUES (%s, %s)',
                      'Alice', 999)  # 'Alice' already exists
        finally:
            # Rollback to clear the failed transaction
            conn.rollback()

    # Test that connection is still valid after errors
    result = db.select(conn, 'SELECT COUNT(*) AS count FROM test_table')
    assert result is not None


if __name__ == '__main__':
    __import__('pytest').main([__file__])
