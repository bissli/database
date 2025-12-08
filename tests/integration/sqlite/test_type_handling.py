"""
Integration tests for SQLite type handling.
"""
import datetime
import decimal

import database as db
import pytest
from database.options import iterdict_data_loader


@pytest.mark.sqlite
def test_sqlite_type_consistency(sl_conn, value_dict):
    """Test type consistency with SQLite"""
    # Skip if not connected to SQLite
    if sl_conn.dialect != 'sqlite':
        pytest.skip('Not connected to SQLite')

    # Save original data loader
    original_loader = sl_conn.options.data_loader
    # Use iterdict_data_loader for this test
    sl_conn.options.data_loader = iterdict_data_loader

    try:
        # Create a test table with various types
        with db.transaction(sl_conn) as tx:
            # Create table with various types
            tx.execute("""
            CREATE TABLE IF NOT EXISTS type_test (
                int_col INTEGER,
                bigint_col INTEGER,  -- SQLite uses INTEGER for all ints
                smallint_col INTEGER,
                bool_true_col INTEGER,  -- SQLite uses INTEGER for boolean
                bool_false_col INTEGER,
                float_col REAL,
                decimal_col NUMERIC,
                money_col NUMERIC,
                char_col TEXT,  -- SQLite stores CHAR as TEXT
                varchar_col TEXT,
                text_col TEXT,
                date_col DATE,  -- SQLite has special handling for date/time
                time_col TIME,
                datetime_col DATETIME,
                blob_col BLOB,
                null_col TEXT,
                json_col TEXT   -- SQLite stores JSON as TEXT
            )
            """)

            # Delete existing data
            tx.execute('DELETE FROM type_test')

            # Insert test values
            # Note: SQLite needs some types converted to strings
            tx.execute("""
            INSERT INTO type_test VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                       value_dict['int_value'],
                       value_dict['big_int'],
                       value_dict['small_int'],
                       1,  # SQLite boolean true
                       0,  # SQLite boolean false
                       value_dict['float_value'],
                       str(value_dict['decimal_value']),  # Convert to string for SQLite
                       str(value_dict['money_value']),
                       value_dict['char_value'],
                       value_dict['varchar_value'],
                       value_dict['text_value'],
                       value_dict['date_value'].isoformat(),  # Convert date to string for SQLite
                       value_dict['time_value'].isoformat(),  # Convert time to string for SQLite
                       value_dict['datetime_value'].isoformat(),  # Convert datetime to string for SQLite
                       value_dict['binary_value'],
                       value_dict['null_value'],
                       value_dict['json_value']
                       )

            # Query the data
            result = tx.select('SELECT * FROM type_test')
            row = result[0]

            # Verify integer types
            assert isinstance(row['int_col'], int)
            assert row['int_col'] == value_dict['int_value']

            # For SQLite, all integers are just 'INTEGER'
            assert isinstance(row['bigint_col'], int)
            assert row['bigint_col'] == value_dict['big_int']

            assert isinstance(row['smallint_col'], int)
            assert row['smallint_col'] == value_dict['small_int']

            # Verify boolean values - SQLite uses integers
            assert isinstance(row['bool_true_col'], int)
            assert bool(row['bool_true_col']) is True

            assert isinstance(row['bool_false_col'], int)
            assert bool(row['bool_false_col']) is False

            # Verify floating point values
            assert isinstance(row['float_col'], float)
            assert abs(row['float_col'] - value_dict['float_value']) < 0.00001

            # SQLite NUMERIC is flexible
            # Could be string, Decimal, or float
            if isinstance(row['decimal_col'], str):
                assert abs(float(row['decimal_col']) - float(value_dict['decimal_value'])) < 0.000001
            elif isinstance(row['decimal_col'], decimal.Decimal):
                assert abs(row['decimal_col'] - value_dict['decimal_value']) < decimal.Decimal('0.000001')
            else:
                assert abs(row['decimal_col'] - float(value_dict['decimal_value'])) < 0.000001

            # Verify string values
            assert isinstance(row['char_col'], str)
            assert row['char_col'] == value_dict['char_value']

            assert isinstance(row['varchar_col'], str)
            assert row['varchar_col'] == value_dict['varchar_value']

            assert isinstance(row['text_col'], str)
            assert row['text_col'] == value_dict['text_value']

            # Verify date/time values
            # SQLite date handling depends on adapters
            # With registered adapters, should be Python date/time objects
            if isinstance(row['date_col'], datetime.date | str):
                if isinstance(row['date_col'], str):
                    assert row['date_col'] == value_dict['date_value'].isoformat()
                else:
                    assert row['date_col'] == value_dict['date_value']

            # Time could be time or string
            if isinstance(row['time_col'], datetime.time | str):
                if isinstance(row['time_col'], str):
                    # Compare string representations
                    time_str = value_dict['time_value'].strftime('%H:%M:%S')
                    assert row['time_col'].startswith(time_str)
                else:
                    assert row['time_col'].hour == value_dict['time_value'].hour
                    assert row['time_col'].minute == value_dict['time_value'].minute
                    assert row['time_col'].second == value_dict['time_value'].second

            # Datetime could be datetime or string
            if isinstance(row['datetime_col'], str):
                datetime_str = value_dict['datetime_value'].isoformat()
                assert row['datetime_col'] == datetime_str
            else:
                assert row['datetime_col'].year == value_dict['datetime_value'].year
                assert row['datetime_col'].month == value_dict['datetime_value'].month
                assert row['datetime_col'].day == value_dict['datetime_value'].day
                assert row['datetime_col'].hour == value_dict['datetime_value'].hour

            # Verify binary data
            assert isinstance(row['blob_col'], bytes | memoryview | bytearray)
            if isinstance(row['blob_col'], memoryview | bytearray):
                assert bytes(row['blob_col']) == value_dict['binary_value']
            else:
                assert row['blob_col'] == value_dict['binary_value']

            # Verify JSON data - SQLite stores as string but some drivers might parse it
            if isinstance(row['json_col'], str):
                assert row['json_col'] == value_dict['json_value']
            else:
                # Some advanced SQLite setups might parse JSON automatically
                assert isinstance(row['json_col'], dict)
                assert row['json_col']['key'] == 'value'
                assert isinstance(row['json_col']['numbers'], list)

            # Verify NULL value
            assert row['null_col'] is None

            # Test different query methods
            scalar = tx.select_scalar('SELECT int_col FROM type_test')
            assert scalar == value_dict['int_value']
    finally:
        # Restore original data loader
        sl_conn.options.data_loader = original_loader
