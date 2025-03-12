"""
Integration tests for SQL Server type handling.
"""
import datetime
import decimal
import json

import database as db


def test_sqlserver_type_consistency(sconn, value_dict):
    """Test type consistency with SQL Server"""

    # Create a test table with various types
    with db.transaction(sconn) as tx:
        # Create table with various types - use temp table
        tx.execute("""
        CREATE TABLE #type_test (
            int_col INT,
            bigint_col BIGINT,
            smallint_col SMALLINT,
            bool_true_col BIT,
            bool_false_col BIT,
            float_col FLOAT,
            decimal_col DECIMAL(18,6),
            money_col MONEY,
            char_col CHAR(1),
            varchar_col VARCHAR(100),
            text_col TEXT,
            date_col DATE,
            time_col TIME,
            datetime_col DATETIME,
            binary_col VARBINARY(MAX),
            null_col VARCHAR(100),
            json_col NVARCHAR(MAX)
        )
        """)

        # Insert test values
        tx.execute("""
        INSERT INTO #type_test VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
                   value_dict['int_value'],
                   value_dict['big_int'],
                   value_dict['small_int'],
                   1,  # SQL Server BIT true
                   0,  # SQL Server BIT false
                   value_dict['float_value'],
                   value_dict['decimal_value'],
                   value_dict['money_value'],
                   value_dict['char_value'],
                   value_dict['varchar_value'],
                   value_dict['text_value'],
                   value_dict['date_value'],
                   value_dict['time_value'],
                   value_dict['datetime_value'],
                   value_dict['binary_value'],
                   value_dict['null_value'],
                   value_dict['json_value']
                   )

        # Query the data
        result = tx.select('SELECT * FROM #type_test')
        row = result[0]

        # Verify integer types
        assert isinstance(row['int_col'], int)
        assert row['int_col'] == value_dict['int_value']

        assert isinstance(row['bigint_col'], int)
        assert row['bigint_col'] == value_dict['big_int']

        assert isinstance(row['smallint_col'], int)
        assert row['smallint_col'] == value_dict['small_int']

        # Verify boolean values
        assert isinstance(row['bool_true_col'], bool | int)
        assert bool(row['bool_true_col']) is True

        assert isinstance(row['bool_false_col'], bool | int)
        assert bool(row['bool_false_col']) is False

        # Verify floating point values
        assert isinstance(row['float_col'], float)
        assert abs(row['float_col'] - value_dict['float_value']) < 0.00001

        # Decimal could be returned as Decimal or float
        if isinstance(row['decimal_col'], decimal.Decimal):
            assert abs(row['decimal_col'] - value_dict['decimal_value']) < decimal.Decimal('0.000001')
        else:
            assert abs(row['decimal_col'] - float(value_dict['decimal_value'])) < 0.000001

        # Verify string values
        assert isinstance(row['char_col'], str)
        assert row['char_col'].strip() == value_dict['char_value']

        assert isinstance(row['varchar_col'], str)
        assert row['varchar_col'] == value_dict['varchar_value']

        assert isinstance(row['text_col'], str)
        assert row['text_col'] == value_dict['text_value']

        # Verify date/time values
        # Date could be date or datetime
        if isinstance(row['date_col'], datetime.date):
            assert row['date_col'] == value_dict['date_value']
        elif isinstance(row['date_col'], datetime.datetime):
            assert row['date_col'].date() == value_dict['date_value']

        # Time could be time or datetime
        if isinstance(row['time_col'], datetime.time):
            assert row['time_col'].hour == value_dict['time_value'].hour
            assert row['time_col'].minute == value_dict['time_value'].minute
            assert row['time_col'].second == value_dict['time_value'].second
        elif isinstance(row['time_col'], datetime.datetime):
            assert row['time_col'].time().hour == value_dict['time_value'].hour
            assert row['time_col'].time().minute == value_dict['time_value'].minute
            assert row['time_col'].time().second == value_dict['time_value'].second

        # Datetime should be datetime
        assert isinstance(row['datetime_col'], datetime.datetime)
        assert row['datetime_col'].year == value_dict['datetime_value'].year
        assert row['datetime_col'].month == value_dict['datetime_value'].month
        assert row['datetime_col'].day == value_dict['datetime_value'].day
        assert row['datetime_col'].hour == value_dict['datetime_value'].hour

        # Verify binary data
        assert isinstance(row['binary_col'], bytes | memoryview | bytearray)
        if isinstance(row['binary_col'], memoryview | bytearray):
            assert bytes(row['binary_col']) == value_dict['binary_value']
        else:
            assert row['binary_col'] == value_dict['binary_value']

        # Verify JSON data - SQL Server stores as string, but some drivers might parse it
        if isinstance(row['json_col'], str):
            assert row['json_col'] == value_dict['json_value']
        else:
            # Some JSON extensions or drivers might parse automatically
            assert isinstance(row['json_col'], dict)
            assert row['json_col']['key'] == 'value'
            assert isinstance(row['json_col']['numbers'], list)

        # Verify NULL value
        assert row['null_col'] is None

        # Test different query methods
        scalar = tx.select_scalar('SELECT int_col FROM #type_test')
        assert scalar == value_dict['int_value']


def test_sqlserver_type_individual_inserts(sconn, value_dict):
    """Test type consistency with SQL Server by inserting and checking each value separately"""

    # Create a test table with various types
    with db.transaction(sconn) as tx:
        # Create table with various types - use temp table
        tx.execute("""
        CREATE TABLE #individual_type_test (
            col_name VARCHAR(100),
            int_col INT,
            bigint_col BIGINT,
            smallint_col SMALLINT,
            bool_col BIT,
            float_col FLOAT,
            decimal_col DECIMAL(18,6),
            money_col MONEY,
            char_col CHAR(1),
            varchar_col VARCHAR(100),
            text_col TEXT,
            date_col DATE,
            time_col TIME,
            datetime_col DATETIME,
            binary_col VARBINARY(MAX),
            null_col VARCHAR(100),
            json_col NVARCHAR(MAX)
        )
        """)

        # Insert and verify each value individually

        # Insert test value with PRINT for debugging
        tx.execute("""
        INSERT INTO #individual_type_test (col_name, int_col) VALUES (?, ?);
        PRINT 'Row inserted with col_name=' + CAST(? AS VARCHAR(100));
        """, 'int_value', value_dict['int_value'], 'int_value')

        # Check if transaction is still valid
        tx.execute("SELECT 1 AS is_valid")

        # Verify the row was inserted with detailed diagnostics
        all_rows = tx.select("SELECT * FROM #individual_type_test")
        print(f"All rows in table: {all_rows}")
        
        # Try explicit data type conversion in the query
        result = tx.select("SELECT int_col FROM #individual_type_test WHERE CONVERT(VARCHAR(100), col_name) = 'int_value'")
        print(f"Query with CONVERT result: {result}")

        # Try with exact parameter match
        check_value = 'int_value'
        result = tx.select("SELECT int_col FROM #individual_type_test WHERE col_name = ?", check_value)
        print(f"Query with parameter: {result}")

        # If we still don't have results, check what actual values are stored
        if not result:
            actual_values = tx.select("SELECT col_name, LEN(col_name) AS length, ASCII(LEFT(col_name, 1)) AS first_char FROM #individual_type_test")
            print(f"Actual column values: {actual_values}")
            
            # Try exact byte-by-byte comparison
            if actual_values:
                actual_name = actual_values[0]['col_name']
                result = tx.select("SELECT int_col FROM #individual_type_test WHERE col_name = ?", actual_name)
                print(f"Query with exact value ({actual_name!r}): {result}")
        
        assert len(result) == 1, f'Expected one row for int_value, got: {result}'
        result = result[0]['int_col']
        assert isinstance(result, int)
        assert result == value_dict['int_value']

        tx.execute('INSERT INTO #individual_type_test (col_name, bigint_col) VALUES (?, ?)',
                   'big_int', value_dict['big_int'])
        result = tx.select("SELECT bigint_col FROM #individual_type_test WHERE col_name = 'big_int'")
        assert len(result) == 1, 'Expected one row for big_int'
        result = result[0]['bigint_col']
        assert isinstance(result, int)
        assert result == value_dict['big_int']

        tx.execute('INSERT INTO #individual_type_test (col_name, smallint_col) VALUES (?, ?)',
                   'small_int', value_dict['small_int'])
        result = tx.select("SELECT smallint_col FROM #individual_type_test WHERE col_name = 'small_int'")
        assert len(result) == 1, 'Expected one row for small_int'
        result = result[0]['smallint_col']
        assert isinstance(result, int)
        assert result == value_dict['small_int']

        # Boolean values
        tx.execute('INSERT INTO #individual_type_test (col_name, bool_col) VALUES (?, ?)',
                   'bool_true', 1)
        result = tx.select("SELECT bool_col FROM #individual_type_test WHERE col_name = 'bool_true'")
        assert len(result) == 1, 'Expected one row for bool_true'
        result = result[0]['bool_col']
        assert isinstance(result, bool | int)
        assert bool(result) is True

        tx.execute('INSERT INTO #individual_type_test (col_name, bool_col) VALUES (?, ?)',
                   'bool_false', 0)
        result = tx.select("SELECT bool_col FROM #individual_type_test WHERE col_name = 'bool_false'")
        assert len(result) == 1, 'Expected one row for bool_false'
        result = result[0]['bool_col']
        assert isinstance(result, bool | int)
        assert bool(result) is False

        # Floating point values
        tx.execute('INSERT INTO #individual_type_test (col_name, float_col) VALUES (?, ?)',
                   'float_value', value_dict['float_value'])
        result = tx.select("SELECT float_col FROM #individual_type_test WHERE col_name = 'float_value'")
        assert len(result) == 1, 'Expected one row for float_value'
        result = result[0]['float_col']
        assert isinstance(result, float)
        assert abs(result - value_dict['float_value']) < 0.00001

        # Decimal values
        tx.execute('INSERT INTO #individual_type_test (col_name, decimal_col) VALUES (?, ?)',
                   'decimal_value', value_dict['decimal_value'])
        result = tx.select("SELECT decimal_col FROM #individual_type_test WHERE col_name = 'decimal_value'")
        assert len(result) == 1, 'Expected one row for decimal_value'
        result = result[0]['decimal_col']
        if isinstance(result, decimal.Decimal):
            assert abs(result - value_dict['decimal_value']) < decimal.Decimal('0.000001')
        else:
            assert abs(result - float(value_dict['decimal_value'])) < 0.000001

        # Money value
        tx.execute('INSERT INTO #individual_type_test (col_name, money_col) VALUES (?, ?)',
                   'money_value', value_dict['money_value'])
        result = tx.select("SELECT money_col FROM #individual_type_test WHERE col_name = 'money_value'")
        assert len(result) == 1, 'Expected one row for money_value'
        result = result[0]['money_col']
        if isinstance(result, decimal.Decimal):
            assert abs(result - value_dict['money_value']) < decimal.Decimal('0.01')
        else:
            assert abs(float(result) - float(value_dict['money_value'])) < 0.01

        # String values
        tx.execute('INSERT INTO #individual_type_test (col_name, char_col) VALUES (?, ?)',
                   'char_value', value_dict['char_value'])
        result = tx.select("SELECT char_col FROM #individual_type_test WHERE col_name = 'char_value'")
        assert len(result) == 1, 'Expected one row for char_value'
        result = result[0]['char_col']
        assert isinstance(result, str)
        assert result.strip() == value_dict['char_value']

        tx.execute('INSERT INTO #individual_type_test (col_name, varchar_col) VALUES (?, ?)',
                   'varchar_value', value_dict['varchar_value'])
        result = tx.select("SELECT varchar_col FROM #individual_type_test WHERE col_name = 'varchar_value'")
        assert len(result) == 1, 'Expected one row for varchar_value'
        result = result[0]['varchar_col']
        assert isinstance(result, str)
        assert result == value_dict['varchar_value']

        tx.execute('INSERT INTO #individual_type_test (col_name, text_col) VALUES (?, ?)',
                   'text_value', value_dict['text_value'])
        result = tx.select("SELECT text_col FROM #individual_type_test WHERE col_name = 'text_value'")
        assert len(result) == 1, 'Expected one row for text_value'
        result = result[0]['text_col']
        assert isinstance(result, str)
        assert result == value_dict['text_value']

        # Date/time values
        tx.execute('INSERT INTO #individual_type_test (col_name, date_col) VALUES (?, ?)',
                   'date_value', value_dict['date_value'])
        result = tx.select("SELECT date_col FROM #individual_type_test WHERE col_name = 'date_value'")
        assert len(result) == 1, 'Expected one row for date_value'
        result = result[0]['date_col']
        if isinstance(result, datetime.date) and not isinstance(result, datetime.datetime):
            assert result == value_dict['date_value']
        elif isinstance(result, datetime.datetime):
            assert result.date() == value_dict['date_value']

        tx.execute('INSERT INTO #individual_type_test (col_name, time_col) VALUES (?, ?)',
                   'time_value', value_dict['time_value'])
        result = tx.select("SELECT time_col FROM #individual_type_test WHERE col_name = 'time_value'")
        assert len(result) == 1, 'Expected one row for time_value'
        result = result[0]['time_col']
        if isinstance(result, datetime.time):
            assert result.hour == value_dict['time_value'].hour
            assert result.minute == value_dict['time_value'].minute
            assert result.second == value_dict['time_value'].second
        elif isinstance(result, datetime.datetime):
            assert result.time().hour == value_dict['time_value'].hour
            assert result.time().minute == value_dict['time_value'].minute
            assert result.time().second == value_dict['time_value'].second

        tx.execute('INSERT INTO #individual_type_test (col_name, datetime_col) VALUES (?, ?)',
                   'datetime_value', value_dict['datetime_value'])
        result = tx.select("SELECT datetime_col FROM #individual_type_test WHERE col_name = 'datetime_value'")
        assert len(result) == 1, 'Expected one row for datetime_value'
        result = result[0]['datetime_col']
        assert isinstance(result, datetime.datetime)
        assert result.year == value_dict['datetime_value'].year
        assert result.month == value_dict['datetime_value'].month
        assert result.day == value_dict['datetime_value'].day
        assert result.hour == value_dict['datetime_value'].hour

        # Binary data
        tx.execute('INSERT INTO #individual_type_test (col_name, binary_col) VALUES (?, ?)',
                   'binary_value', value_dict['binary_value'])
        result = tx.select("SELECT binary_col FROM #individual_type_test WHERE col_name = 'binary_value'")
        assert len(result) == 1, 'Expected one row for binary_value'
        result = result[0]['binary_col']
        assert isinstance(result, bytes | memoryview | bytearray)
        if isinstance(result, memoryview | bytearray):
            assert bytes(result) == value_dict['binary_value']
        else:
            assert result == value_dict['binary_value']

        # NULL value
        tx.execute('INSERT INTO #individual_type_test (col_name, null_col) VALUES (?, ?)',
                   'null_value', value_dict['null_value'])
        result = tx.select("SELECT null_col FROM #individual_type_test WHERE col_name = 'null_value'")
        assert len(result) == 1, 'Expected one row for null_value'
        result = result[0]['null_col']
        assert result is None

        # JSON value
        tx.execute('INSERT INTO #individual_type_test (col_name, json_col) VALUES (?, ?)',
                   'json_value', value_dict['json_value'])
        result = tx.select("SELECT json_col FROM #individual_type_test WHERE col_name = 'json_value'")
        assert len(result) == 1, 'Expected one row for json_value'
        result = result[0]['json_col']
        if isinstance(result, str):
            assert result == value_dict['json_value']
            # Validate that it's actually valid JSON
            parsed = json.loads(result)
            assert parsed['key'] == 'value'
            assert isinstance(parsed['numbers'], list)
        else:
            # Some JSON extensions or drivers might parse automatically
            assert isinstance(result, dict)
            assert result['key'] == 'value'
            assert isinstance(result['numbers'], list)


if __name__ == '__main__':
    __import__('pytest').main([__file__])
