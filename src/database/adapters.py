import datetime
import sqlite3

import dateutil
import numpy as np
import pandas as pd
import psycopg
from psycopg.adapt import Dumper
from psycopg.types.numeric import Float8, FloatDumper, NumericLoader

__all__ = ['register_adapters']

# Type collections for registration
NUMPY_FLOAT_TYPES = (np.float64, np.float32, np.float16)
NUMPY_INT_TYPES = (np.int64, np.int32, np.int16, np.int8)
NUMPY_UINT_TYPES = (np.uint64, np.uint32, np.uint16, np.uint8)
PANDAS_NULLABLE_TYPES = (
    pd.Int64Dtype, pd.Int32Dtype, pd.Int16Dtype, pd.Int8Dtype,
    pd.UInt64Dtype, pd.UInt32Dtype, pd.UInt16Dtype, pd.UInt8Dtype,
    pd.Float64Dtype
)


# == psycopg adapters


class CustomFloatDumper(FloatDumper):
    """Do not store NaN. Use Null"""

    _special = {
        b'inf': b"'Infinity'::float8",
        b'-inf': b"'-Infinity'::float8",
        b'nan': None
    }


class NumpyFloatDumper(Dumper):
    def dump(self, value):
        if value is None:
            return None
        if np.isnan(value):
            return None
        return value


class NumPyIntDumper(Dumper):
    """Handles both signed and unsigned integers"""
    def dump(self, value):
        if value is None:
            return None
        return int(value)


class PandasNullableDumper(Dumper):
    def dump(self, value):
        if pd.isna(value):  # handles pandas NA and numpy nan
            return None
        return value


class NumericMixin:
    def load(self, data) -> float:
        if data is None:
            return None
        if isinstance(data, memoryview):
            data = bytes(data)
        return float(data.decode())


class CustomNumericLoader(NumericMixin, NumericLoader): pass


# == sqlite adapter


def adapt_numpy_float(val):
    if val is None or np.isnan(val):
        return None
    return float(val)


def adapt_numpy_int(val):
    if val is None:
        return None
    return int(val)


def adapt_numpy_uint(val):
    if val is None:
        return None
    return int(val)


def adapt_pandas_nullable(val):
    if pd.isna(val):
        return None
    return val


def adapt_date_iso(val):
    """Adapt datetime.date to ISO 8601 date."""
    return val.isoformat()


def adapt_datetime_iso(val):
    """Adapt datetime.datetime to timezone-naive ISO 8601 date."""
    return val.isoformat()


def convert_date(val):
    """Convert ISO 8601 date to datetime.date object."""
    return dateutil.parser.isoparse(val.decode()).date()


def convert_datetime(val):
    """Convert ISO 8601 datetime to datetime.datetime object."""
    return dateutil.parser.isoparse(val.decode())


# == register


def register_psycopg_types(type_list, dumper_class):
    """Register multiple types with the same dumper for psycopg"""
    for dtype in type_list:
        psycopg.adapters.register_dumper(dtype, dumper_class)


def register_sqlite_types(type_list, adapter_func):
    """Register multiple types with the same adapter for sqlite"""
    for dtype in type_list:
        sqlite3.register_adapter(dtype, adapter_func)


def register_adapters():
    # PostgreSQL base types
    psycopg.adapters.register_dumper(Float8, CustomFloatDumper)
    psycopg.adapters.register_loader('numeric', CustomNumericLoader)

    # Register NumPy and Pandas types for PostgreSQL
    register_psycopg_types(NUMPY_FLOAT_TYPES, NumpyFloatDumper)
    register_psycopg_types(NUMPY_INT_TYPES, NumPyIntDumper)
    register_psycopg_types(NUMPY_UINT_TYPES, NumPyIntDumper)
    register_psycopg_types(PANDAS_NULLABLE_TYPES, PandasNullableDumper)

    # Register datetime types for SQLite
    sqlite3.register_adapter(datetime.date, adapt_date_iso)
    sqlite3.register_adapter(datetime.datetime, adapt_datetime_iso)
    sqlite3.register_converter('date', convert_date)
    sqlite3.register_converter('datetime', convert_datetime)

    # Register NumPy and Pandas types for SQLite
    register_sqlite_types(NUMPY_FLOAT_TYPES, adapt_numpy_float)
    register_sqlite_types(NUMPY_INT_TYPES, adapt_numpy_int)
    register_sqlite_types(NUMPY_UINT_TYPES, adapt_numpy_uint)
    register_sqlite_types(PANDAS_NULLABLE_TYPES, adapt_pandas_nullable)
