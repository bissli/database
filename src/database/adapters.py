import datetime
import sqlite3

import dateutil
import numpy as np
import pandas as pd
import psycopg
import pyarrow as pa
from psycopg.adapt import Dumper
from psycopg.types.numeric import Float8, FloatDumper, NumericLoader

__all__ = ['register_adapters', 'TypeConverter']

# Type collections for registration
NUMPY_FLOAT_TYPES = (np.float64, np.float32, np.float16)
NUMPY_INT_TYPES = (np.int64, np.int32, np.int16, np.int8)
NUMPY_UINT_TYPES = (np.uint64, np.uint32, np.uint16, np.uint8)
PYARROW_NUMERIC_TYPES = (pa.FloatScalar, pa.DoubleScalar)
PANDAS_NULLABLE_TYPES = (
    pd.Int64Dtype, pd.Int32Dtype, pd.Int16Dtype, pd.Int8Dtype,
    pd.UInt64Dtype, pd.UInt32Dtype, pd.UInt16Dtype, pd.UInt8Dtype,
    pd.Float64Dtype
)


class TypeConverter:
    """Universal type conversion for database parameters"""

    @staticmethod
    def convert_value(value):
        """Single-pass type conversion"""
        if value is None:
            return None

        # Handle numpy types efficiently
        if isinstance(value, NUMPY_FLOAT_TYPES):
            return None if np.isnan(value) else float(value)
        if isinstance(value, (NUMPY_INT_TYPES + NUMPY_UINT_TYPES)):
            return int(value)

        # Handle pandas nullable types
        if isinstance(value, PANDAS_NULLABLE_TYPES):
            return None if pd.isna(value) else value

        # Handle pyarrow types
        if isinstance(value, PYARROW_NUMERIC_TYPES):
            if pa.compute.is_null(value).as_py():
                return None
            if hasattr(value, 'as_py'):
                return value.as_py()
            if isinstance(value, pa.Scalar):  # Handle scalar values
                return value.value
            if isinstance(value, pa.Array):   # Handle array values
                return value.to_pylist()
            raise ValueError(f'Unsupported PyArrow type: {type(value)}')

        return value

    @staticmethod
    def convert_params(params):
        """Convert parameter collection"""
        if params is None:
            return None

        if isinstance(params, dict):
            return {k: TypeConverter.convert_value(v) for k, v in params.items()}

        if isinstance(params, list | tuple):
            return [TypeConverter.convert_value(v) for v in params]

        return TypeConverter.convert_value(params)


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
        return TypeConverter.convert_value(value)


class NumPyIntDumper(Dumper):
    """Handles both signed and unsigned integers"""
    def dump(self, value):
        return TypeConverter.convert_value(value)


class PandasNullableDumper(Dumper):
    def dump(self, value):
        return TypeConverter.convert_value(value)


class PyArrowDumper(Dumper):
    """Handle PyArrow scalar values"""
    def dump(self, value):
        return TypeConverter.convert_value(value)


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


def adapt_pyarrow(val):
    """Adapt PyArrow scalar values for SQLite, focusing on null handling"""
    if val is None:
        return None
    if pa.compute.is_null(val).as_py():
        return None
    if hasattr(val, 'as_py'):
        return val.as_py()
    if isinstance(val, pa.Scalar):
        return val.value
    if isinstance(val, pa.Array):
        return val.to_pylist()
    raise ValueError(f'Unsupported PyArrow type: {type(val)}')


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
    """Register only the necessary adapters for each database type"""
    # PostgreSQL adapters
    psycopg.adapters.register_dumper(Float8, CustomFloatDumper)
    psycopg.adapters.register_loader('numeric', CustomNumericLoader)

    # Register NumPy and Pandas types for PostgreSQL
    register_psycopg_types(NUMPY_FLOAT_TYPES, NumpyFloatDumper)
    register_psycopg_types(NUMPY_INT_TYPES, NumPyIntDumper)
    register_psycopg_types(NUMPY_UINT_TYPES, NumPyIntDumper)
    register_psycopg_types(PANDAS_NULLABLE_TYPES, PandasNullableDumper)
    register_psycopg_types(PYARROW_NUMERIC_TYPES, PyArrowDumper)

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
    register_sqlite_types(PYARROW_NUMERIC_TYPES, adapt_pyarrow)
