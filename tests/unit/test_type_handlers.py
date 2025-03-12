"""
Tests for type handlers to verify they only handle type identification, not conversion.
"""
import datetime
import decimal
import math

import pytest
from database.adapters.type_mapping import SqlServerBinaryHandler
from database.adapters.type_mapping import SqlServerBooleanHandler
from database.adapters.type_mapping import SqlServerDateHandler
from database.adapters.type_mapping import SqlServerDateTimeHandler
from database.adapters.type_mapping import SqlServerDateTimeOffsetHandler
from database.adapters.type_mapping import SqlServerGuidHandler
from database.adapters.type_mapping import SqlServerIntegerHandler
from database.adapters.type_mapping import SqlServerNumericHandler
from database.adapters.type_mapping import SqlServerStringHandler
from database.adapters.type_mapping import SqlServerTimeHandler, TypeHandler


class TestTypeHandlers:
    """Test type handler behavior without conversion"""

    @pytest.fixture
    def test_handler(self):
        """Fixture that returns a concrete TypeHandler implementation for testing"""
        class TestHandler(TypeHandler):
            def __init__(self):
                super().__init__(python_type=str)

            def handles_type(self, type_code, type_name=None):
                return True

        return TestHandler()

    def test_base_typehandler_properties(self, test_handler):
        """Test that the base TypeHandler has expected properties"""
        # Check properties
        assert test_handler.python_type == str

        # Check convert_value is a passthrough
        test_value = 'test string'
        assert test_handler.convert_value(test_value) is test_value

        # Check None handling
        assert test_handler.convert_value(None) is None

    @pytest.fixture
    def integer_handler(self):
        """Fixture that returns a SqlServerIntegerHandler"""
        return SqlServerIntegerHandler()

    def test_sqlserver_integer_handler(self, integer_handler):
        """Test SqlServerIntegerHandler identifies integer types correctly"""
        # Check properties
        assert integer_handler.python_type == int

        # Test type detection
        assert integer_handler.handles_type(4)  # SQL_INTEGER
        assert integer_handler.handles_type(5)  # SQL_SMALLINT
        assert integer_handler.handles_type(-5)  # SQL_BIGINT
        assert integer_handler.handles_type(-6)  # SQL_TINYINT

        # Test type name detection
        assert integer_handler.handles_type(None, 'int')
        assert integer_handler.handles_type(None, 'bigint')
        assert integer_handler.handles_type(None, 'smallint')
        assert integer_handler.handles_type(None, 'tinyint')

        # Check convert_value is a passthrough
        value = 42
        assert integer_handler.convert_value(value) is value
        assert integer_handler.convert_value(None) is None

    @pytest.fixture
    def numeric_handler(self):
        """Fixture that returns a SqlServerNumericHandler"""
        return SqlServerNumericHandler()

    def test_sqlserver_numeric_handler(self, numeric_handler):
        """Test SqlServerNumericHandler identifies numeric types correctly"""
        # Check properties
        assert numeric_handler.python_type == float

        # Test type detection
        assert numeric_handler.handles_type(2)  # SQL_NUMERIC
        assert numeric_handler.handles_type(3)  # SQL_DECIMAL
        assert numeric_handler.handles_type(6)  # SQL_FLOAT
        assert numeric_handler.handles_type(7)  # SQL_REAL

        # Test type name detection
        assert numeric_handler.handles_type(None, 'decimal')
        assert numeric_handler.handles_type(None, 'numeric')
        assert numeric_handler.handles_type(None, 'money')
        assert numeric_handler.handles_type(None, 'float')

        # Check convert_value is a passthrough
        value = math.pi
        assert numeric_handler.convert_value(value) is value

        # Even decimal values should pass through without conversion
        decimal_value = decimal.Decimal('123.45')
        assert numeric_handler.convert_value(decimal_value) is decimal_value

    @pytest.fixture
    def boolean_handler(self):
        """Fixture that returns a SqlServerBooleanHandler"""
        return SqlServerBooleanHandler()

    def test_sqlserver_boolean_handler(self, boolean_handler):
        """Test SqlServerBooleanHandler identifies boolean types correctly"""
        # Check properties
        assert boolean_handler.python_type == bool

        # Test type detection
        assert boolean_handler.handles_type(-7)  # SQL_BIT
        assert boolean_handler.handles_type(None, 'bit')

        # Check convert_value is a passthrough
        assert boolean_handler.convert_value(True) is True
        assert boolean_handler.convert_value(False) is False

    @pytest.fixture
    def date_handler(self):
        """Fixture that returns a SqlServerDateHandler"""
        return SqlServerDateHandler()

    def test_sqlserver_date_handler(self, date_handler):
        """Test SqlServerDateHandler identifies date types correctly"""
        # Check properties
        assert date_handler.python_type == datetime.date

        # Test type detection
        assert date_handler.handles_type(91)  # SQL_TYPE_DATE
        assert date_handler.handles_type(None, 'date')

        # Check convert_value is a passthrough
        date_value = datetime.date(2023, 5, 15)
        assert date_handler.convert_value(date_value) is date_value

    @pytest.fixture
    def time_handler(self):
        """Fixture that returns a SqlServerTimeHandler"""
        return SqlServerTimeHandler()

    def test_sqlserver_time_handler(self, time_handler):
        """Test SqlServerTimeHandler identifies time types correctly"""
        # Check properties
        assert time_handler.python_type == datetime.time

        # Test type detection
        assert time_handler.handles_type(92)  # SQL_TYPE_TIME
        assert time_handler.handles_type(-154)  # SQL_SS_TIME2
        assert time_handler.handles_type(None, 'time')

        # Check convert_value is a passthrough
        time_value = datetime.time(14, 30, 45)
        assert time_handler.convert_value(time_value) is time_value

    @pytest.fixture
    def datetime_handler(self):
        """Fixture that returns a SqlServerDateTimeHandler"""
        return SqlServerDateTimeHandler()

    def test_sqlserver_datetime_handler(self, datetime_handler):
        """Test SqlServerDateTimeHandler identifies datetime types correctly"""
        # Check properties
        assert datetime_handler.python_type == datetime.datetime

        # Test type detection
        assert datetime_handler.handles_type(93)  # SQL_TYPE_TIMESTAMP
        assert datetime_handler.handles_type(None, 'datetime')
        assert datetime_handler.handles_type(None, 'datetime2')

        # Check convert_value is a passthrough
        dt_value = datetime.datetime(2023, 5, 15, 14, 30, 45)
        assert datetime_handler.convert_value(dt_value) is dt_value

    @pytest.fixture
    def datetimeoffset_handler(self):
        """Fixture that returns a SqlServerDateTimeOffsetHandler"""
        return SqlServerDateTimeOffsetHandler()

    def test_sqlserver_datetimeoffset_handler(self, datetimeoffset_handler):
        """Test SqlServerDateTimeOffsetHandler identifies datetimeoffset correctly"""
        # Check properties
        assert datetimeoffset_handler.python_type == datetime.datetime

        # Test type detection
        assert datetimeoffset_handler.handles_type(-155)  # SQL_SS_TIMESTAMPOFFSET
        assert datetimeoffset_handler.handles_type(None, 'datetimeoffset')

        # Make a datetime with timezone
        import pytz
        dt_with_tz = datetime.datetime(2023, 5, 15, 14, 30, 45, tzinfo=pytz.UTC)

        # Check convert_value is a passthrough
        assert datetimeoffset_handler.convert_value(dt_with_tz) is dt_with_tz

    @pytest.fixture
    def string_handler(self):
        """Fixture that returns a SqlServerStringHandler"""
        return SqlServerStringHandler()

    def test_sqlserver_string_handler(self, string_handler):
        """Test SqlServerStringHandler identifies string types correctly"""
        # Check properties
        assert string_handler.python_type == str

        # Test type detection
        assert string_handler.handles_type(1)   # SQL_CHAR
        assert string_handler.handles_type(12)  # SQL_VARCHAR
        assert string_handler.handles_type(-1)  # SQL_LONGVARCHAR
        assert string_handler.handles_type(-8)  # SQL_WCHAR
        assert string_handler.handles_type(-9)  # SQL_WVARCHAR
        assert string_handler.handles_type(-10)  # SQL_WLONGVARCHAR

        # Test type name detection
        assert string_handler.handles_type(None, 'char')
        assert string_handler.handles_type(None, 'varchar')
        assert string_handler.handles_type(None, 'nvarchar')
        assert string_handler.handles_type(None, 'text')

        # Check convert_value is a passthrough
        text = 'test string'
        assert string_handler.convert_value(text) is text

    @pytest.fixture
    def binary_handler(self):
        """Fixture that returns a SqlServerBinaryHandler"""
        return SqlServerBinaryHandler()

    def test_sqlserver_binary_handler(self, binary_handler):
        """Test SqlServerBinaryHandler identifies binary types correctly"""
        # Check properties
        assert binary_handler.python_type == bytes

        # Test type detection
        assert binary_handler.handles_type(-2)  # SQL_BINARY
        assert binary_handler.handles_type(-3)  # SQL_VARBINARY
        assert binary_handler.handles_type(-4)  # SQL_LONGVARBINARY

        # Test type name detection
        assert binary_handler.handles_type(None, 'binary')
        assert binary_handler.handles_type(None, 'varbinary')
        assert binary_handler.handles_type(None, 'image')

        # Check convert_value is a passthrough
        binary_value = b'test binary'
        assert binary_handler.convert_value(binary_value) is binary_value

    @pytest.fixture
    def guid_handler(self):
        """Fixture that returns a SqlServerGuidHandler"""
        return SqlServerGuidHandler()

    def test_sqlserver_guid_handler(self, guid_handler):
        """Test SqlServerGuidHandler identifies uniqueidentifier correctly"""
        # Check properties
        assert guid_handler.python_type == str

        # Test type detection
        assert guid_handler.handles_type(-11)  # SQL_GUID
        assert guid_handler.handles_type(None, 'uniqueidentifier')

        # Check convert_value is a passthrough
        guid_str = '12345678-1234-5678-1234-567812345678'
        assert guid_handler.convert_value(guid_str) is guid_str

        # Even UUID objects should pass through
        import uuid
        guid_obj = uuid.UUID('12345678-1234-5678-1234-567812345678')
        assert guid_handler.convert_value(guid_obj) is guid_obj


if __name__ == '__main__':
    __import__('pytest').main([__file__])
