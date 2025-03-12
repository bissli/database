"""
Integration tests for PostgreSQL type handling.
"""
import datetime
import decimal

import database as db


def test_postgres_type_consistency(psql_docker, conn, value_dict):
    """Test type consistency with PostgreSQL"""

    # Create a test table with various types
    with db.transaction(conn) as tx:
        # Drop table if it exists
        tx.execute('DROP TABLE IF EXISTS type_test')

        # Create table with various types
        tx.execute("""
        CREATE TABLE type_test (
            int_col INTEGER,
            bigint_col BIGINT,
            smallint_col SMALLINT,
            bool_true_col BOOLEAN,
            bool_false_col BOOLEAN,
            float_col FLOAT,
            decimal_col DECIMAL(18,6),
            money_col MONEY,
            char_col CHAR(1),
            varchar_col VARCHAR(100),
            text_col TEXT,
            date_col DATE,
            time_col TIME,
            datetime_col TIMESTAMP,
            bytea_col BYTEA,
            null_col VARCHAR(100),
            json_col JSONB
        )
        """)

        # Insert test values
        tx.execute("""
        INSERT INTO type_test VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
                   value_dict['int_value'],
                   value_dict['big_int'],
                   value_dict['small_int'],
                   value_dict['bool_true'],
                   value_dict['bool_false'],
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
        rows = tx.select('SELECT * FROM type_test')
        assert len(rows) == 1
        row = rows[0]

        # Verify integer types
        assert isinstance(row['int_col'], int)
        assert row['int_col'] == value_dict['int_value']

        assert isinstance(row['bigint_col'], int)
        assert row['bigint_col'] == value_dict['big_int']

        assert isinstance(row['smallint_col'], int)
        assert row['smallint_col'] == value_dict['small_int']

        # Verify boolean values
        assert isinstance(row['bool_true_col'], bool)
        assert row['bool_true_col'] is True

        assert isinstance(row['bool_false_col'], bool)
        assert row['bool_false_col'] is False

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
        assert row['char_col'] == value_dict['char_value']

        assert isinstance(row['varchar_col'], str)
        assert row['varchar_col'] == value_dict['varchar_value']

        assert isinstance(row['text_col'], str)
        assert row['text_col'] == value_dict['text_value']

        # Verify date/time values
        # Date could be date or datetime
        if isinstance(row['date_col'], datetime.date):
            assert row['date_col'] == value_dict['date_value']
        else:  # datetime
            assert row['date_col'].date() == value_dict['date_value']

        # Time could be time or datetime
        if isinstance(row['time_col'], datetime.time):
            assert row['time_col'].hour == value_dict['time_value'].hour
            assert row['time_col'].minute == value_dict['time_value'].minute
            assert row['time_col'].second == value_dict['time_value'].second
        else:  # datetime
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
        assert isinstance(row['bytea_col'], bytes | memoryview | bytearray)
        if isinstance(row['bytea_col'], memoryview | bytearray):
            assert bytes(row['bytea_col']) == value_dict['binary_value']
        else:
            assert row['bytea_col'] == value_dict['binary_value']

        # Verify JSON data - PostgreSQL JSONB type can automatically deserialize to Python dict
        if isinstance(row['json_col'], str):
            assert row['json_col'] is not None
            assert '"key":"value"' in row['json_col'].replace(' ', '') or '{"key":"value"' in row['json_col'].replace(' ', '')
        else:
            # Database driver may have deserialized JSON to a dictionary
            assert isinstance(row['json_col'], dict)
            assert 'key' in row['json_col']
            assert row['json_col']['key'] == 'value'
            assert 'numbers' in row['json_col']
            assert isinstance(row['json_col']['numbers'], list)

        # Verify NULL value
        assert row['null_col'] is None

        # Test different query methods
        scalar = tx.select_scalar('SELECT int_col FROM type_test')
        assert scalar == value_dict['int_value']

        date_scalar = tx.select_scalar('SELECT date_col FROM type_test')
        if isinstance(date_scalar, datetime.date):
            assert date_scalar == value_dict['date_value']
        else:  # datetime
            assert date_scalar.date() == value_dict['date_value']


if __name__ == '__main__':
    __import__('pytest').main([__file__])
