"""
Integration tests for SQL Server stored procedure handling.

These tests verify the behavior of our enhanced SQL Server stored procedure handling,
particularly focusing on handling "No results" errors properly.
"""

import datetime

import database as db
import pandas as pd


def test_sqlserver_empty_result_procedure(sconn):
    """Test handling of SQL Server stored procedures that don't return results."""

    # Create a procedure that doesn't return results (using NOCOUNT ON)
    with db.transaction(sconn) as tx:
        # First drop if it exists
        tx.execute("IF OBJECT_ID('dbo.test_nocount_proc', 'P') IS NOT NULL DROP PROCEDURE dbo.test_nocount_proc")

        # Create procedure with NOCOUNT ON
        tx.execute("""
        CREATE PROCEDURE dbo.test_nocount_proc
        @date_param DATE
        AS
        BEGIN
            SET NOCOUNT ON;
            -- This procedure performs an action but doesn't return results
            DECLARE @day INT = DAY(@date_param);
            -- No SELECT statement
        END
        """)

    # Test executing with db.select - should handle "No results" error gracefully
    result = db.select(sconn, 'EXEC dbo.test_nocount_proc @date_param=?', datetime.date(2023, 5, 15))

    # Should return an empty list, not error
    assert isinstance(result, list)
    assert len(result) == 0

    # Test with select for procedure with return_all option 
    result = db.select(sconn, 'EXEC dbo.test_nocount_proc @date_param=?', datetime.date(2023, 5, 15), return_all=False)
    assert isinstance(result, list)
    assert len(result) == 0

    # Clean up
    with db.transaction(sconn) as tx:
        tx.execute('DROP PROCEDURE dbo.test_nocount_proc')


def test_sqlserver_procedure_with_results(sconn):
    """Test SQL Server stored procedure that returns data."""

    # Create a procedure that returns results
    with db.transaction(sconn) as tx:
        # First drop if it exists
        tx.execute("IF OBJECT_ID('dbo.test_result_proc', 'P') IS NOT NULL DROP PROCEDURE dbo.test_result_proc")

        # Create procedure that returns data
        tx.execute("""
        CREATE PROCEDURE dbo.test_result_proc
        @date_param DATE
        AS
        BEGIN
            -- Return data about the date
            SELECT
                @date_param AS input_date,
                DATEPART(year, @date_param) AS year,
                DATEPART(month, @date_param) AS month,
                DATEPART(day, @date_param) AS day,
                DATENAME(weekday, @date_param) AS weekday
        END
        """)

    # Test execution with db.select
    result = db.select(sconn, 'EXEC dbo.test_result_proc @date_param=?', datetime.date(2023, 5, 15))

    # Should return data
    assert isinstance(result, list)
    assert len(result) == 1

    # Check the data
    row = result[0]
    assert isinstance(row['input_date'], datetime.date | datetime.datetime)
    assert row['year'] == 2023
    assert row['month'] == 5
    assert row['day'] == 15
    assert isinstance(row['weekday'], str)

    # Test with select for procedure
    result = db.select(sconn, 'EXEC dbo.test_result_proc @date_param=?', datetime.date(2023, 5, 15))
    assert isinstance(result, list)
    assert len(result) == 1

    # Clean up
    with db.transaction(sconn) as tx:
        tx.execute('DROP PROCEDURE dbo.test_result_proc')


def test_sqlserver_procedure_multiple_resultsets(sconn):
    """Test SQL Server stored procedure that returns multiple result sets."""

    # Create a procedure that returns multiple result sets
    with db.transaction(sconn) as tx:
        # First drop if it exists
        tx.execute("IF OBJECT_ID('dbo.test_multi_result_proc', 'P') IS NOT NULL DROP PROCEDURE dbo.test_multi_result_proc")

        # Create procedure that returns multiple result sets
        tx.execute("""
        CREATE PROCEDURE dbo.test_multi_result_proc
        @date_param DATE
        AS
        BEGIN
            -- First result set - date parts
            SELECT
                DATEPART(year, @date_param) AS year,
                DATEPART(month, @date_param) AS month,
                DATEPART(day, @date_param) AS day

            -- Second result set - date names
            SELECT
                DATENAME(month, @date_param) AS month_name,
                DATENAME(weekday, @date_param) AS weekday_name

            -- Third result set - date calculations
            SELECT
                DATEADD(day, 1, @date_param) AS next_day,
                DATEADD(month, 1, @date_param) AS next_month,
                DATEDIFF(day, '2000-01-01', @date_param) AS days_since_2000
        END
        """)

    # Test using select with return_all=True to get all result sets
    results = db.select(
        sconn,
        'EXEC dbo.test_multi_result_proc @date_param=?',
        datetime.date(2023, 5, 15),
        return_all=True
    )

    # Should return three result sets
    assert isinstance(results, list)
    assert len(results) == 3

    # Check each result set
    # First result set - date parts
    assert 'year' in results[0][0]
    assert 'month' in results[0][0]
    assert 'day' in results[0][0]
    assert results[0][0]['year'] == 2023

    # Second result set - date names
    assert 'month_name' in results[1][0]
    assert 'weekday_name' in results[1][0]
    assert isinstance(results[1][0]['month_name'], str)

    # Third result set - date calculations
    assert 'next_day' in results[2][0]
    assert 'next_month' in results[2][0]
    assert isinstance(results[2][0]['next_day'], datetime.date | datetime.datetime)

    # Test default behavior - should return largest result set
    result = db.select(
        sconn,
        'EXEC dbo.test_multi_result_proc @date_param=?',
        datetime.date(2023, 5, 15)
    )

    # Should be one of the result sets
    assert isinstance(result, list)
    assert len(result) > 0

    # Test with prefer_first=True
    result = db.select(
        sconn,
        'EXEC dbo.test_multi_result_proc @date_param=?',
        datetime.date(2023, 5, 15),
        prefer_first=True
    )

    # Should be the first result set
    assert isinstance(result, list)
    assert 'year' in result[0]
    assert 'month' in result[0]
    assert 'day' in result[0]

    # Clean up
    with db.transaction(sconn) as tx:
        tx.execute('DROP PROCEDURE dbo.test_multi_result_proc')


if __name__ == '__main__':
    __import__('pytest').main([__file__])
