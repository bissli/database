"""
Test values fixtures for database tests.

This module provides fixture functions that generate test data for database tests,
ensuring consistent test values across different test modules.
"""
import datetime
import decimal
import math

import pytest


@pytest.fixture(scope='module', autouse=True)
def value_dict():
    """Return a dictionary of test values for all major types"""
    return {
        # Integers
        'int_value': 42,
        'big_int': 9223372036854775807,  # Max int64
        'small_int': -32768,  # Min int16

        # Boolean
        'bool_true': True,
        'bool_false': False,

        # Floating point
        'float_value': math.pi,
        'decimal_value': decimal.Decimal('123456.789123'),
        'money_value': decimal.Decimal('9876.54'),

        # String types
        'char_value': 'X',
        'varchar_value': 'Variable length string',
        'text_value': 'Lorem ipsum dolor sit amet, consectetur adipiscing elit.',

        # Date and time
        'date_value': datetime.date(2023, 5, 15),
        'time_value': datetime.time(14, 30, 45),
        'datetime_value': datetime.datetime(2023, 5, 15, 14, 30, 45),

        # Binary data
        'binary_value': b'\x01\x02\x03\x04\x05',

        # NULL values
        'null_value': None,

        # Special values
        'json_value': '{"key": "value", "numbers": [1, 2, 3]}',
    }
