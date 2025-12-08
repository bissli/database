"""
Integration tests for PostgreSQL date string handling in CASE expressions.
"""
import datetime
import logging

import database as db
import pandas as pd
from database.types import Column

logger = logging.getLogger(__name__)


def debug_data_loader(data, columns, **kwargs) -> pd.DataFrame:
    """
    Custom data loader that inspects column types and logs info about the 'date' column.

    This verifies that the date column is properly handled as a string by the database driver
    and type resolution system, rather than being converted to a datetime type.
    """
    from database.options import iterdict_data_loader

    # Extract column info for debugging
    column_names = Column.get_names(columns)
    column_types = Column.get_types(columns)

    # Find the 'date' column
    date_column_index = None
    for i, name in enumerate(column_names):
        if name.lower() == 'date':
            date_column_index = i
            break

    # Log information about the 'date' column
    if date_column_index is not None:
        python_type = column_types[date_column_index]
        col_info = columns[date_column_index]

        logger.info(f'Date column info: name={col_info.name}, type_code={col_info.type_code}, python_type={python_type}')

        # Check that the date column's Python type is a string
        assert python_type == str, f"Expected 'date' column to be str, but got {python_type.__name__}"

        # Also verify the first row's value is a string (if data exists)
        if data and len(data) > 0:
            first_row = data[0]
            if isinstance(first_row, dict) and col_info.name in first_row:
                date_value = first_row[col_info.name]
                logger.info(f'First row date value: {date_value} (type: {type(date_value).__name__})')
                assert isinstance(date_value, str), f'Expected date value to be str, but got {type(date_value).__name__}'

    # Pass through to regular pandas loader
    return iterdict_data_loader(data, columns, **kwargs)


def test_postgres_date_string_positional(psql_docker, pg_conn):
    """Test PostgreSQL properly handles date columns that are named 'date'
    and date values cast to strings using positional parameters
    """
    # Set custom data loader
    original_data_loader = pg_conn.options.data_loader
    pg_conn.options.data_loader = debug_data_loader

    try:
        # Create a test table for date operations
        with db.transaction(pg_conn) as tx:
            # Drop table if it exists
            tx.execute('DROP TABLE IF EXISTS date_test')

            # Create a simple table with a date column
            tx.execute("""
            CREATE TABLE date_test (
                id SERIAL PRIMARY KEY,
                event_date DATE
            )
            """)

            # Insert some test dates
            today = datetime.date.today()
            yesterday = today - datetime.timedelta(days=1)
            tomorrow = today + datetime.timedelta(days=1)

            tx.execute("""
            INSERT INTO date_test (event_date) VALUES
            (%s), (%s), (%s)
            """, today, yesterday, tomorrow)

            # Query with a CASE statement that converts dates to strings using positional parameters
            positional_query = """
            SELECT
                (CASE
                    WHEN event_date >= date_trunc('day', %s) AND event_date < date_trunc('day', %s)
                        THEN to_char(%s, 'YYYY-MM-DD')
                    WHEN event_date >= date_trunc('day', %s) AND event_date < date_trunc('day', %s)
                        THEN to_char(%s, 'YYYY-MM-DD')
                    WHEN event_date BETWEEN date_trunc('day', %s) AND %s
                        THEN to_char(%s, 'YYYY-MM-DD')
                    ELSE 'unknown'
                END) AS date,
                COUNT(*) as count,
                MAX(event_date) as actual_date
            FROM date_test
            GROUP BY 1
            ORDER BY date
            """

            rows = tx.select(positional_query,
                             yesterday, today, yesterday,
                             today, tomorrow, today,
                             tomorrow, tomorrow, tomorrow)

            # Verify we got the expected results
            assert len(rows) == 3

            # All results should be strings (not date objects)
            for row in rows:
                assert isinstance(row['date'], str)
                assert isinstance(row['count'], int)
                assert isinstance(row['actual_date'], datetime.date)  # This column should be a date object
                assert len(row['date']) == 10  # YYYY-MM-DD format is 10 chars
    finally:
        # Restore original data loader
        pg_conn.options.data_loader = original_data_loader


def test_postgres_date_string_named(psql_docker, pg_conn):
    """Test PostgreSQL properly handles date columns that are named 'date'
    and date values cast to strings using named parameters
    """
    # Set custom data loader
    original_data_loader = pg_conn.options.data_loader
    pg_conn.options.data_loader = debug_data_loader

    try:
        # Create a test table for date operations
        with db.transaction(pg_conn) as tx:
            # Drop table if it exists
            tx.execute('DROP TABLE IF EXISTS date_test')

            # Create a simple table with a date column
            tx.execute("""
            CREATE TABLE date_test (
                id SERIAL PRIMARY KEY,
                event_date DATE
            )
            """)

            # Insert some test dates
            today = datetime.date.today()
            yesterday = today - datetime.timedelta(days=1)
            tomorrow = today + datetime.timedelta(days=1)

            tx.execute("""
            INSERT INTO date_test (event_date) VALUES
            (%s), (%s), (%s)
            """, today, yesterday, tomorrow)

            # Query with a CASE statement that converts dates to strings using named parameters
            named_query = """
            SELECT
                (CASE
                    WHEN event_date >= date_trunc('day', %(yesterday)s) AND event_date < date_trunc('day', %(today)s)
                        THEN to_char(%(yesterday)s, 'YYYY-MM-DD')
                    WHEN event_date >= date_trunc('day', %(today)s) AND event_date < date_trunc('day', %(tomorrow)s)
                        THEN to_char(%(today)s, 'YYYY-MM-DD')
                    WHEN event_date BETWEEN date_trunc('day', %(tomorrow)s) AND %(tomorrow)s
                        THEN to_char(%(tomorrow)s, 'YYYY-MM-DD')
                    ELSE 'unknown'
                END) AS date,
                'TestType' as type,
                SUM(CASE WHEN event_date = %(ref_date)s THEN 2 ELSE 1 END) as count,
                event_date as actual_date
            FROM date_test
            GROUP BY 1, 2, 4
            ORDER BY date, type
            """

            # Using named parameters only
            named_rows = tx.select(named_query, {
                'yesterday': yesterday,
                'today': today,
                'tomorrow': tomorrow,
                'ref_date': today
            })

            # Verify results
            assert len(named_rows) == 3

            # All date values should be strings, not date objects
            for row in named_rows:
                assert isinstance(row['date'], str)
                assert isinstance(row['type'], str)
                assert isinstance(row['count'], int)
                assert isinstance(row['actual_date'], datetime.date)  # This column should be a date object
    finally:
        # Restore original data loader
        pg_conn.options.data_loader = original_data_loader


if __name__ == '__main__':
    __import__('pytest').main([__file__])
