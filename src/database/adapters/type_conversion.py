"""
Type conversion utilities for database parameters.

This module handles the conversion of Python values to database-compatible formats
when sending parameters to the database (Python → Database direction only).

It provides:
1. A TypeConverter class for direct parameter conversion
2. Database-specific adapters for PostgreSQL, SQLite, and SQL Server
3. Common handling for special string values: empty strings, 'null', 'nan', 'none'
4. Support for NumPy, Pandas, and PyArrow data types

This module is designed to be used both directly with TypeConverter for parameter
conversion and through database-specific adapter registration functions.

"""
import datetime
import logging
import math
import sqlite3
from typing import Any, TypeVar

import dateutil.parser
import numpy as np
import pandas as pd

# Optional PyArrow import
try:
    import pyarrow as pa
    PYARROW_AVAILABLE = True
except ImportError:
    pa = None
    PYARROW_AVAILABLE = False

logger = logging.getLogger(__name__)

# Type definitions for cleaner type hinting
SQLiteConnection = TypeVar('SQLiteConnection')
PostgresConnection = TypeVar('PostgresConnection')
SQLServerConnection = TypeVar('SQLServerConnection')

# Constants
SPECIAL_STRINGS: set[str] = {'null', 'nan', 'none', 'na', 'nat'}

# NumPy type constants
NUMPY_FLOAT_TYPES = (np.floating,)
NUMPY_INT_TYPES = (np.integer, np.unsignedinteger)

# Pandas nullable type constants
PANDAS_NULLABLE_TYPES = (
    pd.Int64Dtype, pd.Int32Dtype, pd.Int16Dtype, pd.Int8Dtype,
    pd.UInt64Dtype, pd.UInt32Dtype, pd.UInt16Dtype, pd.UInt8Dtype,
    pd.Float64Dtype
)

# PyArrow scalar type constants
PYARROW_FLOAT_TYPES = (
    (pa.FloatScalar, pa.DoubleScalar, pa.Int8Scalar, pa.Int16Scalar,
     pa.Int32Scalar, pa.Int64Scalar, pa.UInt8Scalar, pa.UInt16Scalar,
     pa.UInt32Scalar, pa.UInt64Scalar, pa.StringScalar)
    if PYARROW_AVAILABLE else ()
)


def _check_special_string(value: str) -> None:
    """
    Check if a string value should be converted to NULL.

    Returns None if the string should be converted to NULL,
    otherwise returns False to indicate no conversion.
    """
    if value == '' or value.lower() in SPECIAL_STRINGS:
        return None
    return False


def _convert_pyarrow_value(value: Any) -> Any:
    """
    Convert PyArrow value to Python type.

    This is a shared implementation for both PostgreSQL and SQLite adapters.

    Args:
        value: PyArrow value to convert

    Returns
        Converted Python value or None
    """
    # If PyArrow is not available, return value as is
    if not PYARROW_AVAILABLE:
        return value

    # Early return for None
    if value is None:
        return None

    # Check if the value is a null value
    try:
        if pa.compute.is_null(value).as_py():
            return None
    except (AttributeError, TypeError, ValueError) as e:
        logger.debug(f'PyArrow is_null check failed: {e}')
        # Continue with other conversion methods

    # Try as_py method first (most reliable)
    if hasattr(value, 'as_py'):
        try:
            py_value = value.as_py()
            # Handle special string cases
            if isinstance(py_value, str) and _check_special_string(py_value) is None:
                return None
            return py_value
        except Exception as e:
            logger.debug(f'PyArrow as_py conversion failed: {e}')
            # Fall through to next method

    # Try direct value access for Scalar objects
    if pa and isinstance(value, pa.Scalar):
        try:
            py_value = value.value
            if isinstance(py_value, str) and _check_special_string(py_value) is None:
                return None
            return py_value
        except Exception as e:
            logger.debug(f'PyArrow Scalar.value access failed: {e}')
            # Fall through to fallback

    # Handle PyArrow arrays
    if pa and isinstance(value, pa.Array | pa.ChunkedArray):
        try:
            return value.to_pylist()
        except Exception as e:
            logger.debug(f'PyArrow array to_pylist failed: {e}')
            # Continue to fallback

    # Handle PyArrow tables
    if pa and isinstance(value, pa.Table):
        try:
            return value.to_pandas()
        except Exception as e:
            logger.debug(f'PyArrow table to_pandas failed: {e}')
            # Continue to fallback

    # Fallback for other types
    try:
        logger.debug(f'Using fallback for PyArrow type: {type(value)}')
        return str(value)
    except Exception as e:
        logger.debug(f'PyArrow fallback conversion failed: {e}')
        return None


def _convert_numpy_value(val: Any) -> float | int | datetime.datetime | None:
    """
    Convert NumPy numeric value to Python type.

    This is a shared implementation for both NumPy float and integer types,
    as well as datetime64 types (including NaT handling).

    Args:
        val: NumPy value to convert

    Returns
        Converted Python value or None
    """
    if val is None:
        return None

    # Handle NaN values from floating point types
    if isinstance(val, np.floating) and np.isnan(val):
        return None

    # Handle NaT (Not a Time) values
    if isinstance(val, np.datetime64) and np.isnat(val):
        return None

    if isinstance(val, (np.floating, np.integer | np.unsignedinteger)):
        return val.item()

    if isinstance(val, np.datetime64):
        timestamp = val.astype('datetime64[s]').astype(int)
        logger.debug(f'Converting np.datetime64 to Python datetime: {val}')
        return datetime.datetime.utcfromtimestamp(timestamp)

    return val


def _convert_pandas_nullable(val: Any) -> Any:
    """
    Convert Pandas nullable value to Python type.

    This is a shared implementation for Pandas nullable types.

    Args:
        val: Pandas value to convert

    Returns
        Converted Python value or None
    """
    if pd.isna(val):
        return None
    if isinstance(val, str) and _check_special_string(val) is None:
        return None
    return val


class TypeConverter:
    """Universal type conversion for database parameters"""

    @staticmethod
    def convert_value(value: Any) -> Any:
        """Convert a single value to a database-compatible format

        Args:
            value: Any Python value to convert

        Returns
            Converted value suitable for database parameters

        >>> TypeConverter.convert_value(None) is None
        True
        >>> TypeConverter.convert_value(float('nan')) is None
        True
        >>> TypeConverter.convert_value('null') is None
        True
        >>> TypeConverter.convert_value(5)
        5
        """
        if value is None:
            return None

        # Handle NaN and special values
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return None

        # Handle pandas.NaT
        if pd and hasattr(pd, 'NaT') and isinstance(value, type(pd.NaT)):
            return None

        # Handle string special values
        if isinstance(value, str) and _check_special_string(value) is None:
            return None

        # NumPy numeric types
        if isinstance(value, (*NUMPY_FLOAT_TYPES, *NUMPY_INT_TYPES, np.datetime64)):
            return _convert_numpy_value(value)

        # Pandas basic NA handling
        if pd.api.types.is_scalar(value) and pd.isna(value):
            return None

        # Handle object dtype
        if hasattr(value, 'dtype') and pd.api.types.is_dtype_equal(value.dtype, 'object') and pd.isna(value):
            return None

        # Pandas nullable types
        if isinstance(value, PANDAS_NULLABLE_TYPES):
            return _convert_pandas_nullable(value)

        # PyArrow values
        if PYARROW_AVAILABLE:
            if isinstance(value, PYARROW_FLOAT_TYPES):
                return _convert_pyarrow_value(value)

            if isinstance(value, pa.Scalar) or hasattr(value, '_is_arrow_scalar') or \
               isinstance(value, pa.Array | pa.ChunkedArray | pa.Table):
                return _convert_pyarrow_value(value)

        return value

    @staticmethod
    def convert_params(params: Any) -> Any:
        """Convert a collection of parameters for database operations

        Args:
            params: Parameter collection (dict, list, tuple) or single value

        Returns
            Converted parameters suitable for database execution
        """
        if params is None:
            return None

        if isinstance(params, dict):
            return {k: TypeConverter.convert_value(v) for k, v in params.items()}

        if isinstance(params, list | tuple):
            if params and all(isinstance(p, (list, tuple)) for p in params):
                return type(params)(TypeConverter.convert_params(p) for p in params)
            return type(params)(TypeConverter.convert_value(v) for v in params)

        return TypeConverter.convert_value(params)


# SQLite converter functions

def convert_date(val: bytes) -> datetime.date:
    """Convert ISO 8601 date string to date object"""
    return dateutil.parser.isoparse(val.decode()).date()


def convert_datetime(val: bytes) -> datetime.datetime:
    """Convert ISO 8601 datetime string to datetime object"""
    return dateutil.parser.isoparse(val.decode())


class AdapterRegistry:
    """Registry for database-specific type adapters with SQLAlchemy integration"""

    def sqlite(self, connection: SQLiteConnection) -> None:
        """Register SQLite converters for a connection

        Args:
            connection: SQLite connection object

        Note:
            Due to SQLite's architecture, converters are registered globally
            rather than per-connection.
        """
        # Ensure connection is established
        connection.execute('SELECT 1')

        # Register date/time converters - these convert database values TO Python objects
        sqlite3.register_converter('date', convert_date)
        sqlite3.register_converter('datetime', convert_datetime)


def get_adapter_registry() -> AdapterRegistry:
    """Get the adapter registry for database connections

    Returns
        AdapterRegistry instance
    """
    return AdapterRegistry()


if __name__ == '__main__':
    __import__('doctest').testmod(optionflags=4 | 8 | 32)
