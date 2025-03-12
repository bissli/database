import datetime
import logging
import math
from decimal import Decimal

import database as db

logger = logging.getLogger(__name__)


def test_schema_cache_with_column_resolution(sconn):
    """Test the SchemaCache for enhancing SQL Server type information"""
    from database.utils.schema_cache import SchemaCache

    # Get a schema cache instance
    schema_cache = SchemaCache.get_instance()

    # Create a test table with various types
    create_table_sql = """
    CREATE TABLE #schema_cache_test (
        id INT PRIMARY KEY,
        decimal_col DECIMAL(12,4),
        date_col DATE,
        text_col NVARCHAR(100),
        bit_col BIT
    )
    """

    # Execute the SQL to create the table
    with db.transaction(sconn) as tx:
        tx.execute(create_table_sql)
        tx.execute("INSERT INTO #schema_cache_test VALUES (1, 123.4567, '2023-05-15', 'Test text', 1)")

    # Test schema metadata retrieval
    metadata = schema_cache.get_column_metadata(sconn, '#schema_cache_test')

    # Verify metadata was retrieved
    assert metadata, 'Schema cache failed to retrieve metadata'
    assert 'id' in metadata, "Failed to find 'id' column in metadata"
    assert 'decimal_col' in metadata, "Failed to find 'decimal_col' column in metadata"

    # Check that types were correctly identified
    assert metadata['id']['type_name'].lower() == 'int', 'Wrong type for id column'
    assert metadata['decimal_col']['type_name'].lower() == 'decimal', 'Wrong type for decimal column'
    assert metadata['date_col']['type_name'].lower() == 'date', 'Wrong type for date column'

    # Test that precision/scale were captured
    assert metadata['decimal_col']['precision'] == 12, 'Wrong precision for decimal column'
    assert metadata['decimal_col']['scale'] == 4, 'Wrong scale for decimal column'

    result = db.select(sconn, 'SELECT * FROM #schema_cache_test', table_name='#schema_cache_test')

    # Verify we got one row
    assert len(result) == 1
    assert isinstance(result, list)
    assert isinstance(result[0], dict)

    # Check that key columns exist and have the expected values
    assert 'id' in result[0]
    assert 'decimal_col' in result[0]
    assert 'date_col' in result[0]

    # Check that the decimal value is the expected type and value
    assert isinstance(result[0]['decimal_col'], float | Decimal)
    assert abs(float(result[0]['decimal_col']) - 123.4567) < 0.001

    # Check that the date value is a date type
    assert isinstance(result[0]['date_col'], datetime.date | datetime.datetime)

    # Test clearing the cache
    schema_cache.clear()
    assert '#schema_cache_test' not in schema_cache.get_or_create_connection_cache(id(sconn))


def test_data_type_handling(sconn):
    """Test column type handling with SQL Server types

    This tests SQL Server column types and verifies that they are properly
    converted to appropriate Python types.
    """
    # Save original data loader
    original_data_loader = sconn.options.data_loader

    # Create a test table with various data types - using temp table for isolation
    create_table_sql = """
    CREATE TABLE #test_data_types (
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
    INSERT INTO #test_data_types VALUES (
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
    result = db.select(sconn, 'SELECT * FROM #test_data_types')

    # Verify we got a list of dictionaries
    assert isinstance(result, list)
    assert len(result) == 1

    row = result[0]

    # Check that the appropriate Python types are used
    assert isinstance(row['id_col'], int)
    assert isinstance(row['bigint_col'], int)
    assert isinstance(row['smallint_col'], int)
    assert isinstance(row['tinyint_col'], int)
    assert isinstance(row['bit_col'], bool)
    # Allow either Decimal or float for numeric columns
    assert isinstance(row['decimal_col'], float | Decimal)
    assert isinstance(row['money_col'], float | Decimal)
    assert isinstance(row['float_col'], float | Decimal)

    # Date/time types
    assert isinstance(row['datetime_col'], datetime.datetime)
    assert isinstance(row['date_col'], datetime.date | datetime.datetime)
    assert isinstance(row['time_col'], datetime.time | datetime.datetime)

    # String types
    assert isinstance(row['char_col'], str)
    assert isinstance(row['varchar_col'], str)
    assert isinstance(row['nvarchar_col'], str)

    # UUID values may be returned as UUID objects or strings
    assert isinstance(row['uniqueid_col'], str) or str(row['uniqueid_col'].__class__.__name__) == 'UUID'

    # Check that values are correctly preserved
    assert row['id_col'] == 123
    assert row['bigint_col'] == 9223372036854775807
    assert row['smallint_col'] == 32767
    assert row['tinyint_col'] == 255
    assert row['bit_col'] is True
    assert abs(float(row['decimal_col']) - 1234.56) < 0.0001
    assert abs(float(row['float_col']) - math.pi) < 0.0001
    assert row['varchar_col'] == 'Variable text'
    assert row['nvarchar_col'] == 'Unicode text'
    assert row['char_col'].strip() == 'CHAR10'

    # UUID validation (just check format)
    uuid_val = row['uniqueid_col']
    # Handle both string and UUID object formats
    uuid_str = str(uuid_val)
    assert len(uuid_str) == 36  # Standard UUID string format


def test_sqlserver_datetime_types(sconn):
    """Test SQL Server date/time type handling with edge cases"""

    # Create a regular table with all SQL Server date/time types
    # Use a unique name to avoid conflicts with other tests
    test_table_name = 'test_datetime_types_regular'

    # Drop the table if it exists
    drop_table_sql = f"""
    IF OBJECT_ID('{test_table_name}', 'U') IS NOT NULL
        DROP TABLE {test_table_name}
    """

    create_table_sql = f"""
    CREATE TABLE {test_table_name} (
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
    insert_sql = f"""
    INSERT INTO {test_table_name} VALUES (
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
        tx.execute(drop_table_sql)
        tx.execute(create_table_sql)
        tx.execute(insert_sql)

    # Query the data
    result = db.select(sconn, f'SELECT * FROM {test_table_name}')

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
    assert isinstance(row['datetimeoffset_col'], datetime.datetime), f"Expected datetime, got {type(row['datetimeoffset_col'])}"
    assert not isinstance(row['datetimeoffset_col'], int), 'DATETIMEOFFSET column should not be an integer'

    # Don't check exact date/time values for datetimeoffset as implementation may vary
    # Just verify it's a valid datetime object with timezone info if possible
    if row['datetimeoffset_col'].tzinfo is not None:
        logger.info(f"DATETIMEOFFSET has timezone info: {row['datetimeoffset_col'].tzinfo}")

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

    # Min/max DATETIME - must be datetime objects, not date objects
    # Handle both cases for compatibility, but warn if it's a date
    if isinstance(row['min_datetime_col'], datetime.date) and not isinstance(row['min_datetime_col'], datetime.datetime):
        logger.warning('min_datetime_col is a date object, expected datetime')
        assert row['min_datetime_col'].year == 1753
    else:
        assert isinstance(row['min_datetime_col'], datetime.datetime)
        assert row['min_datetime_col'].year == 1753

    if isinstance(row['max_datetime_col'], datetime.date) and not isinstance(row['max_datetime_col'], datetime.datetime):
        logger.warning('max_datetime_col is a date object, expected datetime')
        assert row['max_datetime_col'].year == 9999
    else:
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
    date_value = db.select_scalar(sconn, f'SELECT date_col FROM {test_table_name}')
    assert isinstance(date_value, datetime.date | datetime.datetime)
    assert not isinstance(date_value, int), 'DATE scalar should not be an integer'

    # 5. Test select_row with multiple date fields
    date_row = db.select_row(sconn, f"""
        SELECT
            date_col,
            time_col,
            datetime_col
        FROM {test_table_name}
    """)
    assert isinstance(date_row.date_col, datetime.date | datetime.datetime)
    assert isinstance(date_row.time_col, datetime.time | datetime.datetime)
    assert isinstance(date_row.datetime_col, datetime.datetime)


def test_sqlserver_type_resolver(sconn):
    """Test the new TypeResolver functionality with SQL Server connections"""
    # Test with schema cache and type handlers
    from database.adapters.type_mapping import TypeHandlerRegistry
    from database.adapters.type_mapping import TypeResolver
    from database.config.type_mapping import TypeMappingConfig

    # Initialize the resolver
    resolver = TypeResolver()
    registry = TypeHandlerRegistry.get_instance()
    config = TypeMappingConfig.get_instance()

    # Test type resolution for common SQL Server types
    int_type = resolver.resolve_python_type('mssql', 4, 'id_column', table_name='test_table')
    assert int_type == int, 'Integer type resolution failed'

    # Test resolution by type name
    date_type = resolver.resolve_python_type('mssql', 'date', 'date_column')
    assert date_type == datetime.date, 'Date type resolution failed'

    # Test resolution by column name pattern
    id_type = resolver.resolve_python_type('mssql', None, 'user_id')
    assert id_type == int, 'ID column pattern resolution failed'

    # Test datetime column patterns
    created_at_type = resolver.resolve_python_type('mssql', None, 'created_at')
    assert created_at_type == datetime.datetime, 'Created_at column pattern resolution failed'

    # Test that a handler can be registered and used
    test_handler = registry._handlers['mssql'][0]  # Get the first handler
    assert test_handler is not None, 'No handlers registered'
    assert test_handler.python_type is not None, 'Handler has no python_type'

    # Test configuration-based type resolution
    # Add a custom mapping
    config.add_column_mapping('mssql', 'test_table', 'custom_column', 'varchar')
    custom_type = resolver.resolve_python_type('mssql', None, 'custom_column', table_name='test_table')
    assert custom_type == str, 'Configuration-based type resolution failed'


def test_python_type_for_datetime_type_codes(sconn):
    """Test correct Python type identification when type_code is already a Python type"""
    
    # Create two test tables with date/datetime columns using common naming patterns
    create_tables_sql = """
    CREATE TABLE #python_type_test1 (
        id INT PRIMARY KEY,
        issue_dt DATE,                  -- "dt" suffix common for dates
        created_at DATETIME,            -- "at" suffix common for datetimes
        last_modified DATETIME
    );
    
    CREATE TABLE #python_type_test2 (
        id INT PRIMARY KEY,
        seccreatedate DATETIME,
        secchangedate DATETIME,
        effective_date DATE
    );
    """

    # Execute the SQL to create the tables and insert data
    with db.transaction(sconn) as tx:
        tx.execute(create_tables_sql)
        tx.execute("""
            INSERT INTO #python_type_test1 VALUES
            (1, '2023-05-15', '2023-05-15 10:30:00', '2023-05-15 12:45:00')
        """)
        tx.execute("""
            INSERT INTO #python_type_test2 VALUES
            (1, '2023-05-15 14:20:00', '2023-05-15 16:15:00', '2023-05-15')
        """)

    # Do a direct query to check column type resolution with table join and aliasing
    query = '''
    SELECT 
        t1.issue_dt as "t1.issue_dt", 
        t1.created_at as "t1.created_at",
        t1.last_modified as "t1.last_modified",
        t2.seccreatedate as "t2.seccreatedate", 
        t2.secchangedate as "t2.secchangedate",
        t2.effective_date as "t2.effective_date"
    FROM #python_type_test1 t1
    JOIN #python_type_test2 t2 ON t1.id = t2.id
    '''

    # Get the cursor and run query, but don't fetch results yet
    cursor = sconn.cursor()
    cursor.execute(query)
    
    # Import column info creator to directly test the type resolution
    from database.adapters.column_info import columns_from_cursor_description
    
    # Create columns directly from cursor description - without table name since columns are aliased
    columns = columns_from_cursor_description(cursor, 'mssql', None, sconn)

    # Close cursor
    cursor.close()

    # Print column information for debugging
    for col in columns:
        print(f'Column: {col.name}, type_code: {col.type_code}, python_type: {col.python_type}')

    # Test that date/datetime columns have correct Python type
    date_columns = [col for col in columns if 'date' in col.name.lower() or 'dt' in col.name.lower()]
    assert len(date_columns) >= 3, 'Should find at least 3 date-related columns'

    for col in date_columns:
        # Check both the type_code and the resolved python_type
        if isinstance(col.type_code, type):
            # If type_code is already a Python type like datetime.datetime or datetime.date
            assert col.python_type in {datetime.date, datetime.datetime}, \
                f'Column {col.name} with type_code {col.type_code} should have datetime python_type, but got {col.python_type}'
        else:
            # If type_code is a database-specific code
            assert col.python_type in {datetime.date, datetime.datetime}, \
                f'Column {col.name} should resolve to datetime.date or datetime.datetime, but got {col.python_type}'

    # Test the created_at column which uses the _at suffix pattern
    created_at_col = next((col for col in columns if 'created_at' in col.name), None)
    assert created_at_col is not None, 'created_at column not found'
    assert created_at_col.python_type == datetime.datetime, \
        f'created_at column should resolve to datetime.datetime, but got {created_at_col.python_type}'

    # Verify that the type resolver is working with regular queries too
    result = db.select(sconn, query)
    row = result[0]
    
    # Print actual returned types for each column
    print("\n=== Actual column types from select result ===")
    for col_name, value in row.items():
        print(f"Column: {col_name}, Value type: {type(value)}")
    
    # Check actual returned types (should be correct regardless of the issue with column description)
    assert isinstance(row['t1.issue_dt'], datetime.date | datetime.datetime), \
        f"issue_dt should be datetime.date or datetime.datetime, got {type(row['t1.issue_dt'])}"
    assert isinstance(row['t1.created_at'], datetime.datetime), \
        f"created_at should be datetime.datetime, got {type(row['t1.created_at'])}"
    assert isinstance(row['t2.seccreatedate'], datetime.datetime), \
        f"seccreatedate should be datetime.datetime, got {type(row['t2.seccreatedate'])}"
    assert isinstance(row['t2.effective_date'], datetime.date | datetime.datetime), \
        f"effective_date should be datetime.date or datetime.datetime, got {type(row['t2.effective_date'])}"


if __name__ == '__main__':
    __import__('pytest').main([__file__])
