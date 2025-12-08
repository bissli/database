"""
Tests for database adapter registry to verify comprehensive type conversion coverage.
"""
import datetime
import decimal
import math

import numpy as np
import pandas as pd
from database.types import TypeConverter, get_adapter_registry


def test_adapter_registry_sqlite():
    """Test that the adapter registry has SQLite support"""
    registry = get_adapter_registry()

    # Verify sqlite method exists
    assert hasattr(registry, 'sqlite')
    assert callable(registry.sqlite)


def test_conversion_consistency():
    """Test that values converted via different paths produce consistent results.

    Verifies that different data types (NumPy, Pandas, decimal) are
    consistently converted to their appropriate Python equivalents,
    and edge cases like NaT, NA values are properly handled as NULL.
    """
    # Test NumPy float conversion is consistent
    np_value = np.float64(math.pi)
    python_value = TypeConverter.convert_value(np_value)
    assert python_value == math.pi
    assert isinstance(python_value, float)

    # Test NumPy NaT (Not a Time) conversion
    np_nat_value = np.datetime64('NaT')
    python_nat_value = TypeConverter.convert_value(np_nat_value)
    assert python_nat_value is None

    # Test Python built-in float NaN conversion
    py_nan_value = float('nan')
    python_nan_value = TypeConverter.convert_value(py_nan_value)
    assert python_nan_value is None

    # Test NumPy datetime64 conversion (valid date)
    np_date_value = np.datetime64('2023-01-15')
    python_date_value = TypeConverter.convert_value(np_date_value)
    assert isinstance(python_date_value, datetime.datetime)
    assert python_date_value.year == 2023
    assert python_date_value.month == 1
    assert python_date_value.day == 15

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
    params = [np_value, np_date_value, np_nat_value, pd_value, pd_na_value, decimal_value]
    converted_params = TypeConverter.convert_params(params)
    # Note: np_date_value should be a datetime object
    assert converted_params[0] == math.pi
    assert isinstance(converted_params[1], datetime.datetime)
    assert converted_params[2] is None
    assert converted_params[3] == 1
    assert converted_params[4] is None
    assert converted_params[5] == decimal_value


def test_type_converter_special_strings():
    """Test TypeConverter handles special string values correctly."""
    # Empty string converts to None
    assert TypeConverter.convert_value('') is None

    # Special string values convert to None
    assert TypeConverter.convert_value('null') is None
    assert TypeConverter.convert_value('NULL') is None
    assert TypeConverter.convert_value('nan') is None
    assert TypeConverter.convert_value('NaN') is None
    assert TypeConverter.convert_value('none') is None
    assert TypeConverter.convert_value('None') is None
    assert TypeConverter.convert_value('na') is None
    assert TypeConverter.convert_value('nat') is None

    # Regular strings pass through
    assert TypeConverter.convert_value('hello') == 'hello'
    assert TypeConverter.convert_value('123') == '123'


def test_type_converter_numpy_types():
    """Test TypeConverter handles NumPy types correctly."""
    # Integer types
    assert TypeConverter.convert_value(np.int64(42)) == 42
    assert TypeConverter.convert_value(np.int32(42)) == 42
    assert TypeConverter.convert_value(np.uint64(42)) == 42

    # Float types
    assert TypeConverter.convert_value(np.float64(math.pi)) == math.pi
    assert TypeConverter.convert_value(np.float32(math.pi)) - math.pi < 0.001

    # NaN values
    assert TypeConverter.convert_value(np.float64('nan')) is None
    assert TypeConverter.convert_value(np.float32('nan')) is None


def test_type_converter_dict_params():
    """Test TypeConverter handles dict parameters correctly."""
    params = {
        'name': 'test',
        'value': np.float64(42.5),
        'empty': '',
        'null_str': 'null',
        'nan_val': float('nan')
    }
    converted = TypeConverter.convert_params(params)

    assert converted['name'] == 'test'
    assert converted['value'] == 42.5
    assert converted['empty'] is None
    assert converted['null_str'] is None
    assert converted['nan_val'] is None


if __name__ == '__main__':
    __import__('pytest').main([__file__])
