import datetime
import logging
import sqlite3

import dateutil
import numpy as np
import pandas as pd
import psycopg
import pyarrow as pa
from psycopg.adapt import Dumper
from psycopg.types.numeric import Float8, FloatDumper, NumericLoader

logger = logging.getLogger(__name__)

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

        # Handle pandas types more comprehensively
        if pd.api.types.is_scalar(value) and pd.isna(value):
            return None

        # Handle pandas nullable types (Int64, Float64, etc.)
        if hasattr(value, 'dtype') and pd.api.types.is_dtype_equal(value.dtype, 'object'):
            if pd.isna(value):
                return None

        # Handle normal pandas nullable types
        if isinstance(value, PANDAS_NULLABLE_TYPES):
            return None if pd.isna(value) else value

        # Enhanced PyArrow handling
        if hasattr(value, '_is_arrow_scalar') or isinstance(value, pa.Scalar):
            try:
                if pa.compute.is_null(value).as_py():
                    return None
                if hasattr(value, 'as_py'):
                    return value.as_py()
                return value.value
            except (AttributeError, ValueError) as e:
                # Log the error with type information
                logger.warning(f'Failed to convert PyArrow value of type {type(value)}: {e}')
                return None

        # Handle PyArrow arrays
        if isinstance(value, pa.Array):
            try:
                return value.to_pylist()
            except Exception as e:
                logger.warning(f'Failed to convert PyArrow array: {e}')
                return None

        # Handle PyArrow chunks
        if isinstance(value, pa.ChunkedArray):
            try:
                return value.to_pylist()
            except Exception as e:
                logger.warning(f'Failed to convert PyArrow chunked array: {e}')
                return None

        # Handle PyArrow table columns
        if isinstance(value, pa.Table):
            try:
                return value.to_pandas()
            except Exception as e:
                logger.warning(f'Failed to convert PyArrow table: {e}')
                return None

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


def register_adapters(isolated=False):
    """Register adapters for database connections

    Args:
        isolated: If True, returns adapter mappings instead of registering globally

    Returns
        If isolated=True, returns a dict of database-specific adapter maps
        Otherwise, registers adapters globally and returns None
    """
    if isolated:
        # Create isolated adapter maps for each driver
        postgres_adapters = psycopg.adapt.AdaptersMap()

        # Register PostgreSQL adapters to isolated map
        postgres_adapters.register_dumper(Float8, CustomFloatDumper)
        postgres_adapters.register_loader('numeric', CustomNumericLoader)

        for dtype in NUMPY_FLOAT_TYPES:
            postgres_adapters.register_dumper(dtype, NumpyFloatDumper)

        for dtype in NUMPY_INT_TYPES + NUMPY_UINT_TYPES:
            postgres_adapters.register_dumper(dtype, NumPyIntDumper)

        for dtype in PANDAS_NULLABLE_TYPES:
            postgres_adapters.register_dumper(dtype, PandasNullableDumper)

        for dtype in PYARROW_NUMERIC_TYPES:
            postgres_adapters.register_dumper(dtype, PyArrowDumper)

        # For SQLite and SQL Server, we don't have adapter maps but
        # we can prepare functions to register them individually

        def register_sqlite_adapters(connection):
            """Register adapters for a specific SQLite connection"""
            # Register datetime types
            connection.execute('SELECT 1')  # Force connection to be made

            # These are actually registered globally, but SQLite doesn't
            # support per-connection registration
            sqlite3.register_adapter(datetime.date, adapt_date_iso)
            sqlite3.register_adapter(datetime.datetime, adapt_datetime_iso)
            sqlite3.register_converter('date', convert_date)
            sqlite3.register_converter('datetime', convert_datetime)

            # Register NumPy and Pandas types
            for dtype in NUMPY_FLOAT_TYPES:
                sqlite3.register_adapter(dtype, adapt_numpy_float)

            for dtype in NUMPY_INT_TYPES:
                sqlite3.register_adapter(dtype, adapt_numpy_int)

            for dtype in NUMPY_UINT_TYPES:
                sqlite3.register_adapter(dtype, adapt_numpy_uint)

            for dtype in PANDAS_NULLABLE_TYPES:
                sqlite3.register_adapter(dtype, adapt_pandas_nullable)

            for dtype in PYARROW_NUMERIC_TYPES:
                sqlite3.register_adapter(dtype, adapt_pyarrow)

        # Return the isolated adapter maps
        return {
            'postgres': postgres_adapters,
            'sqlite': register_sqlite_adapters,
            'sqlserver': None  # SQL Server doesn't need adapter registration
        }
    else:
        # Legacy global registration
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


def get_type_converter(cn):
    """Get type converter for the connection's database

    Args:
        cn: Database connection

    Returns
        TypeConverter instance appropriate for the database
    """
    # For now all connections use the same converter
    # In the future, this could return database-specific converter subclasses
    return TypeConverter()
