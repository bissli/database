import datetime
import math
from decimal import Decimal

import database as db
import numpy as np
import pandas as pd
from database.options import iterdict_data_loader


def test_pandas_numpy_data_loader(sconn):
    """Test column type detection when using pandas_numpy_data_loader

    This tests SQL Server column types and verifies that the pandas DataFrame
    contains numpy dtypes as expected.
    """
    # Create a test table with various data types - using temp table for isolation
    create_table_sql = """
    CREATE TABLE #test_pandas_types (
        id_col INT,
        bigint_col BIGINT,
        smallint_col SMALLINT,
        tinyint_col TINYINT,
        bit_col BIT,
        decimal_col DECIMAL(10,2),
        money_col MONEY,
        float_col FLOAT,
        datetime_col DATETIME,
        date_col DATE,
        time_col TIME,
        char_col CHAR(10),
        varchar_col VARCHAR(50),
        nvarchar_col NVARCHAR(100),
        uniqueid_col UNIQUEIDENTIFIER
    )
    """

    # Insert test data with representative values for each type
    insert_sql = """
    INSERT INTO #test_pandas_types VALUES (
        123,                    -- INT
        9223372036854775807,    -- BIGINT (max value)
        32767,                  -- SMALLINT (max value)
        255,                    -- TINYINT (max value)
        1,                      -- BIT
        1234.56,                -- DECIMAL
        $9876.54,               -- MONEY
        3.14159,                -- FLOAT
        '2023-01-15 14:30:00',  -- DATETIME
        '2023-01-15',           -- DATE
        '14:30:00',             -- TIME
        'CHAR10    ',           -- CHAR(10)
        'Variable text',        -- VARCHAR
        N'Unicode text',        -- NVARCHAR
        NEWID()                 -- UNIQUEIDENTIFIER
    )
    """

    # Execute the SQL using transaction to create and populate the table
    with db.transaction(sconn) as tx:
        tx.execute(create_table_sql)
        tx.execute(insert_sql)

    # Query the data
    result = db.select(sconn, 'SELECT * FROM #test_pandas_types')

    # Verify we got a pandas DataFrame
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1

    # Check that the appropriate numpy types are assigned
    assert isinstance(result.iloc[0]['id_col'], np.int64)
    assert isinstance(result.iloc[0]['bigint_col'], np.int64)
    assert isinstance(result.iloc[0]['smallint_col'], np.int64)
    assert isinstance(result.iloc[0]['tinyint_col'], np.int64)
    assert isinstance(result.iloc[0]['bit_col'], np.bool_)
    # Allow either Decimal or np.float64 for numeric columns
    assert isinstance(result.iloc[0]['decimal_col'], np.float64) or isinstance(result.iloc[0]['decimal_col'], Decimal)
    assert isinstance(result.iloc[0]['money_col'], np.float64) or isinstance(result.iloc[0]['money_col'], Decimal)
    assert isinstance(result.iloc[0]['float_col'], np.float64) or isinstance(result.iloc[0]['float_col'], Decimal)

    # Date/time types might be either numpy or Python types
    assert (isinstance(result.iloc[0]['datetime_col'], np.datetime64) or
            isinstance(result.iloc[0]['datetime_col'], datetime.datetime))
    assert (isinstance(result.iloc[0]['date_col'], np.datetime64) or
            isinstance(result.iloc[0]['date_col'], datetime.date))
    assert (isinstance(result.iloc[0]['time_col'], np.datetime64) or
            isinstance(result.iloc[0]['time_col'], datetime.time))

    # Strings may remain as Python strings
    assert isinstance(result.iloc[0]['char_col'], str)
    assert isinstance(result.iloc[0]['varchar_col'], str)
    assert isinstance(result.iloc[0]['nvarchar_col'], str)
    # UUID values may be returned as UUID objects or strings
    assert isinstance(result.iloc[0]['uniqueid_col'], str) or str(result.iloc[0]['uniqueid_col'].__class__.__name__) == 'UUID'

    # Check that values are correctly preserved
    assert result.iloc[0]['id_col'] == 123
    assert result.iloc[0]['bigint_col'] == 9223372036854775807
    assert result.iloc[0]['smallint_col'] == 32767
    assert result.iloc[0]['tinyint_col'] == 255
    assert result.iloc[0]['bit_col'] == np.True_
    assert abs(result.iloc[0]['decimal_col'] - Decimal(1234.56)) < 0.0001
    assert abs(result.iloc[0]['float_col'] - math.pi) < 0.0001
    assert result.iloc[0]['varchar_col'] == 'Variable text'
    assert result.iloc[0]['nvarchar_col'] == 'Unicode text'
    assert result.iloc[0]['char_col'].strip() == 'CHAR10'

    # UUID validation (just check format)
    uuid_val = result.iloc[0]['uniqueid_col']
    # Handle both string and UUID object formats
    uuid_str = str(uuid_val)
    assert len(uuid_str) == 36  # Standard UUID string format

    # Verify column type info is stored in DataFrame attributes
    assert 'column_types' in result.attrs

    type_map = result.attrs['column_types']
    for col in result.columns:
        assert col in type_map
        assert 'python_type' in type_map[col]

    # Check specific type conversions in attribute metadata
    assert type_map['id_col']['python_type'] == 'int'
    assert type_map['bit_col']['python_type'] == 'bool'
    assert type_map['decimal_col']['python_type'] == 'float'
    assert type_map['datetime_col']['python_type'] == 'datetime'
    assert type_map['char_col']['python_type'] == 'str'


def test_iterdict_data_loader(sconn):
    """Test column type detection when using iterdict_data_loader

    This tests SQL Server column types and verifies that the return values
    are native Python types as expected.
    """
    # Save original data loader
    original_data_loader = sconn.options.data_loader

    # Set iterdict_data_loader for this test
    sconn.options.data_loader = iterdict_data_loader

    try:
        # Create a test table with all common SQL Server data types
        create_table_sql = """
        CREATE TABLE #test_dict_types (
            -- Integer types
            int_col INT,
            bigint_col BIGINT,
            smallint_col SMALLINT,
            tinyint_col TINYINT,

            -- Boolean type
            bit_col BIT,

            -- Decimal/numeric types
            decimal_col DECIMAL(18,6),
            numeric_col NUMERIC(18,6),
            money_col MONEY,
            smallmoney_col SMALLMONEY,
            float_col FLOAT,
            real_col REAL,

            -- Date and time types
            datetime_col DATETIME,
            datetime2_col DATETIME2,
            smalldatetime_col SMALLDATETIME,
            date_col DATE,
            time_col TIME,
            datetimeoffset_col DATETIMEOFFSET,

            -- String types
            char_col CHAR(10),
            varchar_col VARCHAR(100),
            nchar_col NCHAR(10),
            nvarchar_col NVARCHAR(100),

            -- Binary types
            binary_col BINARY(10),
            varbinary_col VARBINARY(100),

            -- Other types
            uniqueidentifier_col UNIQUEIDENTIFIER,
            xml_col XML
        )
        """

        # Insert a test row with values for all types
        insert_sql = """
        INSERT INTO #test_dict_types VALUES (
            2147483647,              -- INT (max value)
            9223372036854775807,     -- BIGINT (max value)
            32767,                   -- SMALLINT (max value)
            255,                     -- TINYINT (max value)

            1,                       -- BIT

            123456.789123,           -- DECIMAL
            987654.321987,           -- NUMERIC
            12345.6789,              -- MONEY
            6543.21,                 -- SMALLMONEY
            3.1415926535897931,      -- FLOAT
            2.718281828,             -- REAL

            '2023-03-15 14:30:15.123',  -- DATETIME
            '2023-03-15 14:30:15.1234567',  -- DATETIME2
            '2023-03-15 14:30:00',     -- SMALLDATETIME
            '2023-03-15',              -- DATE
            '14:30:15.1234567',        -- TIME
            '2023-03-15 14:30:15.1234567 +01:00',  -- DATETIMEOFFSET

            'CHAR10    ',              -- CHAR(10)
            'Variable text',           -- VARCHAR
            N'Unicode  ',              -- NCHAR
            N'Unicode variable text',  -- NVARCHAR

            0x0102030405060708090A,       -- BINARY
            0x0102030405060708090A0B0C0D, -- VARBINARY

            NEWID(),                  -- UNIQUEIDENTIFIER
            '<root><element>Test XML</element></root>' -- XML
        )
        """

        with db.transaction(sconn) as tx:
            tx.execute(create_table_sql)
            tx.execute(insert_sql)

        # Query the data
        result = db.select(sconn, 'SELECT * FROM #test_dict_types')

        # Verify the result is a list of dictionaries
        assert isinstance(result, list)
        assert len(result) == 1
        assert isinstance(result[0], dict)

        row = result[0]

        # Check integer types
        assert isinstance(row['int_col'], int)
        assert row['int_col'] == 2147483647
        assert isinstance(row['bigint_col'], int)
        # Allow either int or Decimal for big integers
        if isinstance(row['bigint_col'], Decimal):
            assert row['bigint_col'] == Decimal(9223372036854775807)
        else:
            assert row['bigint_col'] == 9223372036854775807
        assert isinstance(row['smallint_col'], int)
        assert row['smallint_col'] == 32767
        assert isinstance(row['tinyint_col'], int)
        assert row['tinyint_col'] == 255

        # Check boolean type
        assert isinstance(row['bit_col'], bool)
        assert row['bit_col'] is True

        # Check decimal/numeric types
        assert isinstance(row['decimal_col'], float) or isinstance(row['decimal_col'], Decimal)
        if isinstance(row['decimal_col'], Decimal):
            assert abs(row['decimal_col'] - Decimal('123456.789123')) < Decimal('0.000001')
        else:
            assert abs(row['decimal_col'] - 123456.789123) < 0.000001

        assert isinstance(row['numeric_col'], float) or isinstance(row['numeric_col'], Decimal)
        if isinstance(row['numeric_col'], Decimal):
            assert abs(row['numeric_col'] - Decimal('987654.321987')) < Decimal('0.000001')
        else:
            assert abs(row['numeric_col'] - 987654.321987) < 0.000001

        assert isinstance(row['money_col'], float) or isinstance(row['money_col'], Decimal)
        assert isinstance(row['float_col'], float)
        assert abs(row['float_col'] - math.pi) < 0.00001
        assert isinstance(row['real_col'], float)
        assert abs(row['real_col'] - math.e) < 0.0001

        # Check date and time types
        assert isinstance(row['datetime_col'], datetime.datetime)
        assert row['datetime_col'].year == 2023
        assert row['datetime_col'].month == 3
        assert row['datetime_col'].day == 15

        assert isinstance(row['datetime2_col'], datetime.datetime)
        assert row['datetime2_col'].year == 2023
        assert row['datetime2_col'].hour == 14

        assert isinstance(row['smalldatetime_col'], datetime.datetime)
        assert row['smalldatetime_col'].minute == 30

        assert isinstance(row['date_col'], datetime.date) or isinstance(row['date_col'], datetime.datetime)
        assert isinstance(row['time_col'], datetime.time) or isinstance(row['time_col'], datetime.datetime)
        assert isinstance(row['datetimeoffset_col'], datetime.datetime)

        # Check string types
        assert isinstance(row['char_col'], str)
        assert row['char_col'].rstrip() == 'CHAR10'
        assert isinstance(row['varchar_col'], str)
        assert row['varchar_col'] == 'Variable text'
        assert isinstance(row['nchar_col'], str)
        assert row['nchar_col'].rstrip() == 'Unicode'
        assert isinstance(row['nvarchar_col'], str)
        assert row['nvarchar_col'] == 'Unicode variable text'

        # Check binary types (usually bytes or bytearray)
        assert row['binary_col'] is not None
        assert row['varbinary_col'] is not None

        # Check other types
        # SQL Server might return uniqueidentifier as UUID object or as string
        assert isinstance(row['uniqueidentifier_col'], str) or str(row['uniqueidentifier_col'].__class__.__name__) == 'UUID'
        assert len(str(row['uniqueidentifier_col'])) == 36  # UUID format string length

        assert isinstance(row['xml_col'], str)
        assert '<root>' in row['xml_col']

        # Test API functions that use iterdict_data_loader internally
        # -----------------

        # test select_row
        row_result = db.select_row(sconn, 'SELECT int_col, bit_col, decimal_col, datetime_col, varchar_col FROM #test_dict_types')
        assert row_result.int_col == 2147483647
        assert row_result.bit_col == np.True_

        # check decimal value
        if isinstance(row_result.decimal_col, Decimal):
            assert abs(row_result.decimal_col - Decimal('123456.789123')) < Decimal('0.000001')
        else:
            assert abs(row_result.decimal_col - 123456.789123) < 0.000001

        assert isinstance(row_result.datetime_col, datetime.datetime)
        assert row_result.varchar_col == 'Variable text'

        # test select_scalar
        id_value = db.select_scalar(sconn, 'SELECT int_col FROM #test_dict_types')
        assert id_value == 2147483647
        assert isinstance(id_value, int)

        # test select_row_or_none
        row_option = db.select_row_or_none(sconn, 'SELECT int_col, varchar_col FROM #test_dict_types')
        assert row_option is not None
        assert row_option.int_col == 2147483647
        assert row_option.varchar_col == 'Variable text'

        # test None case
        none_row = db.select_row_or_none(sconn, 'SELECT int_col FROM #test_dict_types WHERE int_col = -999')
        assert none_row is None

    finally:
        # Restore original data loader
        sconn.options.data_loader = original_data_loader


def test_sqlserver_datetime_types(sconn):
    """Test SQL Server date/time type handling with edge cases"""
    # Save original data loader
    original_data_loader = sconn.options.data_loader

    # Use iterdict_data_loader for this test
    from database.options import iterdict_data_loader
    sconn.options.data_loader = iterdict_data_loader

    try:
        # Create a temp table with all SQL Server date/time types
        create_table_sql = """
        CREATE TABLE #test_datetime_types (
            -- Standard date/time types
            date_col DATE,
            time_col TIME(7),  -- Max precision
            datetime_col DATETIME,
            smalldatetime_col SMALLDATETIME,
            datetime2_col DATETIME2(7),  -- Max precision
            datetimeoffset_col DATETIMEOFFSET(7),  -- With timezone

            -- Edge cases
            min_date_col DATE,
            max_date_col DATE,
            min_datetime_col DATETIME,
            max_datetime_col DATETIME,
            min_datetime2_col DATETIME2,
            max_datetime2_col DATETIME2,

            -- Date columns with specific naming patterns
            date_field DATE,
            created_at DATETIME2,
            timestamp_value DATETIME,
            time_field TIME
        )
        """

        # Insert values for all types including edge cases
        insert_sql = """
        INSERT INTO #test_datetime_types VALUES (
            -- Standard values
            '2023-06-15',                         -- DATE
            '14:30:15.1234567',                   -- TIME with max precision
            '2023-06-15 14:30:15.123',            -- DATETIME (3ms precision)
            '2023-06-15 14:30:00',                -- SMALLDATETIME (minute precision)
            '2023-06-15 14:30:15.1234567',        -- DATETIME2 with max precision
            '2023-06-15 14:30:15.1234567 +02:00', -- DATETIMEOFFSET with timezone

            -- Edge cases
            '1753-01-01',                         -- Min SQL Server date
            '9999-12-31',                         -- Max SQL Server date
            '1753-01-01 00:00:00.000',            -- Min DATETIME
            '9999-12-31 23:59:59.997',            -- Max DATETIME
            '0001-01-01 00:00:00.0000000',        -- Min DATETIME2
            '9999-12-31 23:59:59.9999999',        -- Max DATETIME2

            -- Date columns with specific naming patterns
            '2023-06-16',                         -- DATE with pattern
            '2023-06-16 15:30:45.1234567',        -- DATETIME2 with pattern
            '2023-06-16 16:45:30.123',            -- DATETIME with pattern
            '16:45:30.1234567'                    -- TIME with pattern
        )
        """

        with db.transaction(sconn) as tx:
            tx.execute(create_table_sql)
            tx.execute(insert_sql)

        # Query the data
        result = db.select(sconn, 'SELECT * FROM #test_datetime_types')

        # Basic test - verify we got a row
        assert isinstance(result, list)
        assert len(result) == 1

        row = result[0]

        # 1. Test standard date/time types and their Python type mappings

        # DATE should be datetime.date (or sometimes datetime.datetime)
        assert isinstance(row['date_col'], datetime.date) or isinstance(row['date_col'], datetime.datetime)
        assert not isinstance(row['date_col'], int), 'DATE column should not be an integer'
        if isinstance(row['date_col'], datetime.date):
            assert row['date_col'].year == 2023
            assert row['date_col'].month == 6
            assert row['date_col'].day == 15
        else:  # datetime.datetime
            assert row['date_col'].year == 2023
            assert row['date_col'].month == 6
            assert row['date_col'].day == 15

        # TIME should be datetime.time (or sometimes datetime.datetime)
        assert isinstance(row['time_col'], datetime.time) or isinstance(row['time_col'], datetime.datetime)
        assert not isinstance(row['time_col'], int), 'TIME column should not be an integer'
        if isinstance(row['time_col'], datetime.time):
            assert row['time_col'].hour == 14
            assert row['time_col'].minute == 30
            assert row['time_col'].second == 15
            # Check microsecond precision if available
            if hasattr(row['time_col'], 'microsecond'):
                assert row['time_col'].microsecond > 0, 'TIME microseconds not preserved'
        else:  # datetime.datetime
            assert row['time_col'].hour == 14
            assert row['time_col'].minute == 30
            assert row['time_col'].second == 15

        # DATETIME should be datetime.datetime
        assert isinstance(row['datetime_col'], datetime.datetime)
        assert not isinstance(row['datetime_col'], int), 'DATETIME column should not be an integer'
        assert row['datetime_col'].year == 2023
        assert row['datetime_col'].month == 6
        assert row['datetime_col'].day == 15
        assert row['datetime_col'].hour == 14
        assert row['datetime_col'].minute == 30
        assert row['datetime_col'].second == 15
        assert row['datetime_col'].microsecond > 0, 'DATETIME milliseconds not preserved'

        # SMALLDATETIME should be datetime.datetime but with minute precision
        assert isinstance(row['smalldatetime_col'], datetime.datetime)
        assert not isinstance(row['smalldatetime_col'], int), 'SMALLDATETIME column should not be an integer'
        assert row['smalldatetime_col'].year == 2023
        assert row['smalldatetime_col'].month == 6
        assert row['smalldatetime_col'].day == 15
        assert row['smalldatetime_col'].hour == 14
        assert row['smalldatetime_col'].minute == 30
        assert row['smalldatetime_col'].second == 0, 'SMALLDATETIME has minute precision'

        # DATETIME2 should be datetime.datetime with microsecond precision
        assert isinstance(row['datetime2_col'], datetime.datetime)
        assert not isinstance(row['datetime2_col'], int), 'DATETIME2 column should not be an integer'
        assert row['datetime2_col'].year == 2023
        assert row['datetime2_col'].month == 6
        assert row['datetime2_col'].day == 15
        assert row['datetime2_col'].hour == 14
        assert row['datetime2_col'].minute == 30
        assert row['datetime2_col'].second == 15
        assert row['datetime2_col'].microsecond > 0, 'DATETIME2 microseconds not preserved'

        # DATETIMEOFFSET should be datetime.datetime
        # Note: Ideally would have timezone info, but we'll check the basic conversion
        assert isinstance(row['datetimeoffset_col'], datetime.datetime)
        assert not isinstance(row['datetimeoffset_col'], int), 'DATETIMEOFFSET column should not be an integer'
        assert row['datetimeoffset_col'].year == 2023
        assert row['datetimeoffset_col'].month == 6
        assert row['datetimeoffset_col'].day == 15

        # 2. Test edge cases

        # Minimum date (1753-01-01 for SQL Server)
        assert isinstance(row['min_date_col'], datetime.date) or isinstance(row['min_date_col'], datetime.datetime)
        if isinstance(row['min_date_col'], datetime.date):
            assert row['min_date_col'].year == 1753
            assert row['min_date_col'].month == 1
            assert row['min_date_col'].day == 1
        else:  # datetime.datetime
            assert row['min_date_col'].year == 1753
            assert row['min_date_col'].month == 1
            assert row['min_date_col'].day == 1

        # Maximum date (9999-12-31 for SQL Server)
        assert isinstance(row['max_date_col'], datetime.date) or isinstance(row['max_date_col'], datetime.datetime)
        if isinstance(row['max_date_col'], datetime.date):
            assert row['max_date_col'].year == 9999
            assert row['max_date_col'].month == 12
            assert row['max_date_col'].day == 31
        else:  # datetime.datetime
            assert row['max_date_col'].year == 9999
            assert row['max_date_col'].month == 12
            assert row['max_date_col'].day == 31

        # Min/max DATETIME
        assert isinstance(row['min_datetime_col'], datetime.datetime)
        assert row['min_datetime_col'].year == 1753
        assert isinstance(row['max_datetime_col'], datetime.datetime)
        assert row['max_datetime_col'].year == 9999

        # Min/max DATETIME2
        assert isinstance(row['min_datetime2_col'], datetime.datetime)
        assert row['min_datetime2_col'].year == 1  # DATETIME2 can go to year 0001
        assert isinstance(row['max_datetime2_col'], datetime.datetime)
        assert row['max_datetime2_col'].year == 9999

        # 3. Test columns with specific naming patterns
        assert isinstance(row['date_field'], datetime.date) or isinstance(row['date_field'], datetime.datetime)
        assert isinstance(row['created_at'], datetime.datetime)
        assert isinstance(row['timestamp_value'], datetime.datetime)
        assert isinstance(row['time_field'], datetime.time) or isinstance(row['time_field'], datetime.datetime)

        # 4. Test select_scalar with date return
        date_value = db.select_scalar(sconn, 'SELECT date_col FROM #test_datetime_types')
        assert isinstance(date_value, datetime.date | datetime.datetime)
        assert not isinstance(date_value, int), 'DATE scalar should not be an integer'

        # 5. Test select_row with multiple date fields
        date_row = db.select_row(sconn, """
            SELECT
                date_col,
                time_col,
                datetime_col
            FROM #test_datetime_types
        """)
        assert isinstance(date_row.date_col, datetime.date | datetime.datetime)
        assert isinstance(date_row.time_col, datetime.time | datetime.datetime)
        assert isinstance(date_row.datetime_col, datetime.datetime)

    finally:
        # Restore original data loader
        sconn.options.data_loader = original_data_loader


if __name__ == '__main__':
    __import__('pytest').main([__file__])
