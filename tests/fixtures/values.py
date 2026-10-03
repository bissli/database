"""Sample values covering each major column type.
"""
import datetime
import decimal
import math

import pytest


@pytest.fixture(scope='module')
def value_dict():
    """One sample value per column type, keyed by type.
    """
    return {
        'int_value': 42,
        'big_int': 2**63 - 1,
        'small_int': -2**15,
        'bool_true': True,
        'bool_false': False,
        'float_value': math.pi,
        'decimal_value': decimal.Decimal('123456.789123'),
        'money_value': decimal.Decimal('9876.54'),
        'char_value': 'X',
        'varchar_value': 'Variable length string',
        'text_value': 'Lorem ipsum dolor sit amet, consectetur adipiscing elit.',
        'date_value': datetime.date(2023, 5, 15),
        'time_value': datetime.time(14, 30, 45),
        'datetime_value': datetime.datetime(2023, 5, 15, 14, 30, 45),
        'binary_value': b'\x01\x02\x03\x04\x05',
        'null_value': None,
        'json_value': '{"key": "value", "numbers": [1, 2, 3]}',
        }
