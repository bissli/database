"""
Tests for database adapter registry to verify comprehensive type conversion coverage.
"""
import datetime
import decimal
import math

import numpy as np
import pandas as pd
from database.adapters.type_conversion import TypeConverter, adapt_date_iso
from database.adapters.type_conversion import adapt_datetime_iso
from database.adapters.type_conversion import get_adapter_registry


def test_adapter_registry_structure():
    """Test that the adapter registry has the expected structure"""
    # Get adapter registry
    registry = get_adapter_registry()

    # Verify methods exist
    assert hasattr(registry, 'postgres')
    assert hasattr(registry, 'sqlite')
    assert hasattr(registry, 'sqlserver')

    # Postgres should return an adapter map
    postgres_adapters = registry.postgres()
    assert postgres_adapters is not None
    assert hasattr(postgres_adapters, 'register_dumper')

    # SQLite should have a function
    assert callable(registry.sqlite)

    # SQL Server method exists but doesn't need to do anything (pyodbc handles conversions)
    assert callable(registry.sqlserver)


def test_postgres_numpy_adapters(mocker):
    """Test that numpy type adapters are registered for PostgreSQL"""
    # Mock register_dumper
    mock_register_dumper = mocker.patch('psycopg.adapt.AdaptersMap.register_dumper')

    # Get adapter registry
    registry = get_adapter_registry()

    # Get PostgreSQL adapters
    postgres_adapters = registry.postgres()

    # Verify the adapter map is being populated with numpy types
    assert mock_register_dumper.called

    # Check for numpy float types
    called_with_numpy = False
    for call in mock_register_dumper.call_args_list:
        args, kwargs = call
        # More flexible check for numpy types (including module name)
        if len(args) >= 2 and any(
            numpy_type.__module__ + '.' + numpy_type.__name__ in str(args[0]) or
            numpy_type.__name__ in str(args[0])
            for numpy_type in (np.float64, np.float32, np.float16, np.floating)
        ):
            called_with_numpy = True
            break

    # If not found with the first check, try a more general check for 'numpy'
    if not called_with_numpy:
        for call in mock_register_dumper.call_args_list:
            args, kwargs = call
            if len(args) >= 2 and ('numpy' in str(args[0]).lower() or 'np.' in str(args[0])):
                called_with_numpy = True
                break

    # Verify numpy adapters were registered
    assert called_with_numpy, 'NumPy float adapters not registered'


def test_sqlite_numpy_adapters(mocker):
    """Test that numpy type adapters are registered for SQLite"""
    # Mock register_adapter
    mock_register_adapter = mocker.patch('sqlite3.register_adapter')

    # Get adapter registry
    registry = get_adapter_registry()

    # Create a mock connection
    mock_conn = mocker.MagicMock()
    mock_conn.execute = mocker.MagicMock()

    # Call the SQLite adapter function
    registry.sqlite(mock_conn)

    # Verify adapter registration was called
    assert mock_register_adapter.called

    # Check that numpy types were registered
    calls_with_numpy = []
    for call in mock_register_adapter.call_args_list:
        args, kwargs = call
        # Check for any numpy type in a more flexible way
        if args and ('numpy' in str(args[0]).lower() or
                     'np.' in str(args[0]) or
                     any(numpy_type.__name__ in str(args[0]) for numpy_type in
                         (np.float64, np.float32, np.int64, np.int32, np.uint64,
                          np.uint32, np.floating, np.integer, np.unsignedinteger))):
            calls_with_numpy.append(args[0])

    # Verify some numpy adapters were registered
    assert len(calls_with_numpy) > 0, 'NumPy adapters not registered for SQLite'


def test_postgres_custom_dumpers():
    """Test custom dumpers for special value handling"""
    # Import the custom dumpers
    from database.adapters.type_conversion import CustomFloatDumper
    # Test Custom Float Dumper NaN handling
    from psycopg.types.numeric import FloatDumper
    assert issubclass(CustomFloatDumper, FloatDumper)

    # Test NaN value handling
    assert TypeConverter.convert_value(np.float64('nan')) is None


def test_sqlite_datetime_adapters():
    """Test datetime adapters for SQLite"""

    # Test date adapter
    date_val = datetime.date(2023, 5, 15)
    iso_date = adapt_date_iso(date_val)
    assert iso_date == '2023-05-15'

    # Test datetime adapter
    dt_val = datetime.datetime(2023, 5, 15, 14, 30, 45)
    iso_dt = adapt_datetime_iso(dt_val)
    assert iso_dt.startswith('2023-05-15T14:30:45')


def test_conversion_consistency():
    """Test that values converted via different paths produce consistent results"""

    # Test NumPy float conversion is consistent
    np_value = np.float64(math.pi)
    python_value = TypeConverter.convert_value(np_value)
    assert python_value == math.pi
    assert isinstance(python_value, float)

    # Test pandas nullable conversion
    pd_value = pd.Series([1, 2, None], dtype='Int64')[0]
    python_pd_value = TypeConverter.convert_value(pd_value)
    assert python_pd_value == 1

    # Test pandas NA handling
    pd_na_value = pd.Series([1, 2, None], dtype='Int64')[2]
    python_pd_na_value = TypeConverter.convert_value(pd_na_value)
    assert python_pd_na_value is None

    # Test decimal handling
    decimal_value = decimal.Decimal('123.45')
    # Converter should leave it as is for database adapter
    python_decimal_value = TypeConverter.convert_value(decimal_value)
    assert python_decimal_value == decimal_value

    # Test multiple parameters
    params = [np_value, pd_value, pd_na_value, decimal_value]
    converted_params = TypeConverter.convert_params(params)
    assert converted_params == [math.pi, 1, None, decimal_value]


if __name__ == '__main__':
    __import__('pytest').main([__file__])
