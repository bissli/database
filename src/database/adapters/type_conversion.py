"""
Type conversion utilities for database parameters.

This module handles the conversion of Python values to database-compatible formats
when sending parameters to the database (Python → Database direction only).

It provides:
1. A TypeConverter class for direct parameter conversion
2. Database-specific adapters for PostgreSQL, SQLite, and SQL Server
3. Common handling for special string values: empty strings, 'null', 'nan', 'none'
4. Support for NumPy, Pandas, and PyArrow data types

With SQLAlchemy integration, this module:
1. Registers type adapters with SQLAlchemy's type system for parameter handling
2. Continues to provide direct type conversion for explicit parameter handling
3. Maintains compatibility with existing type adapter patterns

Usage:
    # Get adapter registry
    adapter_registry = get_adapter_registry()

    # Apply PostgreSQL adapters to a connection
    conn = psycopg.connect(...)
    conn.adapters.update(adapter_registry.postgres())

    # Register SQLite adapters
    sqlite_conn = sqlite3.connect(...)
    adapter_registry.sqlite(sqlite_conn)

    # Direct parameter conversion
    params = TypeConverter.convert_params(my_params)
    cursor.execute(sql, params)
"""
import datetime
import logging
import math
import sqlite3
from typing import Any, TypeVar

import dateutil.parser
import numpy as np
import pandas as pd
import psycopg
from psycopg.adapt import AdaptersMap, Dumper
from psycopg.types.numeric import Float8, FloatDumper

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
SPECIAL_STRINGS: set[str] = {'null', 'nan', 'none', 'na'}


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
        """
        if value is None:
            return None

        # Handle Python built-in float NaN
        if isinstance(value, float) and math.isnan(value):
            return None

        if isinstance(value, str):
            # Convert empty strings and special string values to NULL
            if _check_special_string(value) is None:
                return None

        # NumPy numeric types (float, int, datetime64)
        if isinstance(value, np.floating | np.integer | np.unsignedinteger | np.datetime64):
            return _convert_numpy_value(value)

        # Pandas types
        if pd.api.types.is_scalar(value) and pd.isna(value):
            return None

        if hasattr(value, 'dtype') and pd.api.types.is_dtype_equal(value.dtype, 'object'):
            if pd.isna(value):
                return None

        # Pandas nullable types
        if isinstance(value, AdapterRegistry.PANDAS_NULLABLE_TYPES):
            return _convert_pandas_nullable(value)

        # PyArrow value handling (scalar, array, table) if available
        if PYARROW_AVAILABLE and (
            isinstance(value, pa.Scalar) or hasattr(value, '_is_arrow_scalar') or
            isinstance(value, pa.Array | pa.ChunkedArray | pa.Table)
        ):
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
            return type(params)(TypeConverter.convert_value(v) for v in params)

        return TypeConverter.convert_value(params)


# PostgreSQL adapter classes
class CustomFloatDumper(FloatDumper):
    """Custom Float dumper that converts NaN to NULL"""

    _special = {
        b'inf': b"'Infinity'::float8",
        b'-inf': b"'-Infinity'::float8",
        b'nan': None  # Convert NaN to NULL
    }

    def dump(self, value):
        """Enhanced dump method with faster NaN checking"""
        if isinstance(value, float) and np.isnan(value):
            return None
        return super().dump(value)


class NumPyDumperMixin:
    """Mixin for NumPy value conversion logic"""

    def convert_numpy(self, value):
        """Convert NumPy value to Python type"""
        return _convert_numpy_value(value)


class NumPyFloatDumper(NumPyDumperMixin, Dumper):
    """Dumper for NumPy float types"""

    def dump(self, value):
        """Convert NumPy float to Python float, handling NaN as NULL"""
        return self.convert_numpy(value)


class NumPyIntDumper(NumPyDumperMixin, Dumper):
    """Dumper for NumPy integer types"""

    def dump(self, value):
        """Convert NumPy integer to Python int"""
        return self.convert_numpy(value)


class PandasNullableDumper(Dumper):
    """Dumper for Pandas nullable types"""

    def dump(self, value):
        """Convert Pandas nullable type, handling NA as NULL"""
        return _convert_pandas_nullable(value)


class PyArrowDumper(Dumper):
    """Dumper for PyArrow scalar values"""

    def dump(self, value):
        """Convert PyArrow scalar, handling null values appropriately"""
        return _convert_pyarrow_value(value)


class NumericMixin:
    """Mixin for numeric type handling"""

    def load(self, data) -> float | None:
        """Load numeric data into Python float"""
        if data is None:
            return None
        if isinstance(data, memoryview):
            data = bytes(data)
        return float(data.decode())


class CustomNumericLoader(NumericMixin):
    """Numeric loader that properly handles NULL values"""

    # Required by psycopg loader registration
    format = 0  # Text format

    def __init__(self, oid=None, name=None):
        """Initialize loader (signature required by psycopg)"""


# SQLite adapter functions
def adapt_date_iso(val: datetime.date) -> str:
    """Convert date to ISO 8601 format string.

    Takes a date object and returns the ISO 8601 string representation.

    >>> import datetime
    >>> adapt_date_iso(datetime.date(2023, 5, 15))
    '2023-05-15'
    """
    return val.isoformat()


def adapt_datetime_iso(val: datetime.datetime) -> str:
    """Convert datetime to ISO 8601 format string.

    Takes a datetime object and returns the ISO 8601 string representation.

    >>> import datetime
    >>> dt = datetime.datetime(2023, 5, 15, 14, 30, 45)
    >>> adapt_datetime_iso(dt).startswith('2023-05-15T14:30:45')
    True
    """
    return val.isoformat()


def convert_date(val: bytes) -> datetime.date:
    """Convert ISO 8601 date string to date object"""
    return dateutil.parser.isoparse(val.decode()).date()


def convert_datetime(val: bytes) -> datetime.datetime:
    """Convert ISO 8601 datetime string to datetime object"""
    return dateutil.parser.isoparse(val.decode())


def adapt_numpy_float(val: np.floating) -> float | None:
    """Convert NumPy float to Python float, handling NaN as NULL.

    >>> import numpy as np
    >>> adapt_numpy_float(np.float64(3.14))
    3.14
    >>> round(adapt_numpy_float(np.float32(123.45)), 6)
    123.449997
    >>> adapt_numpy_float(np.float64('nan')) is None
    True
    """
    return _convert_numpy_value(val)


def adapt_numpy_int(val: np.integer | np.unsignedinteger) -> int:
    """Convert NumPy integer to Python int.

    >>> import numpy as np
    >>> adapt_numpy_int(np.int32(42))
    42
    >>> adapt_numpy_int(np.int64(9999))
    9999
    >>> adapt_numpy_int(np.uint32(123))
    123
    """
    return _convert_numpy_value(val)


def adapt_pandas_nullable(val) -> Any:
    """Convert Pandas nullable type, handling NA as NULL"""
    return _convert_pandas_nullable(val)


def adapt_pyarrow(val: Any) -> Any:
    """Convert PyArrow scalar value, handling null appropriately"""
    return _convert_pyarrow_value(val)


class AdapterRegistry:
    """Registry for database-specific type adapters with SQLAlchemy integration"""

    # NumPy float types
    NUMPY_FLOAT_TYPES = (np.floating,)

    # NumPy integer types
    NUMPY_INT_TYPES = (np.integer, np.unsignedinteger)

    # Pandas nullable types
    PANDAS_NULLABLE_TYPES = (
        pd.Int64Dtype, pd.Int32Dtype, pd.Int16Dtype, pd.Int8Dtype,
        pd.UInt64Dtype, pd.UInt32Dtype, pd.UInt16Dtype, pd.UInt8Dtype,
        pd.Float64Dtype
    )

    # PyArrow scalar types
    PYARROW_FLOAT_TYPES = (
        (pa.FloatScalar, pa.DoubleScalar, pa.Int8Scalar, pa.Int16Scalar,
         pa.Int32Scalar, pa.Int64Scalar, pa.UInt8Scalar, pa.UInt16Scalar,
         pa.UInt32Scalar, pa.UInt64Scalar, pa.StringScalar)
        if PYARROW_AVAILABLE else ()
    )

    def _register_postgres_type_dumpers(self, adapters: AdaptersMap) -> None:
        """Register all type dumpers for PostgreSQL

        Args:
            adapters: PostgreSQL adapters map to update
        """
        # Register float handling
        adapters.register_dumper(Float8, CustomFloatDumper)

        # Register NumPy float types
        for dtype in self.NUMPY_FLOAT_TYPES:
            adapters.register_dumper(dtype, NumPyFloatDumper)

        # Register NumPy int types
        for dtype in self.NUMPY_INT_TYPES:
            adapters.register_dumper(dtype, NumPyIntDumper)

        # Register Pandas nullable types
        for dtype in self.PANDAS_NULLABLE_TYPES:
            adapters.register_dumper(dtype, PandasNullableDumper)

        # Register PyArrow types if available
        if PYARROW_AVAILABLE and self.PYARROW_FLOAT_TYPES:
            for dtype in self.PYARROW_FLOAT_TYPES:
                adapters.register_dumper(dtype, PyArrowDumper)

    def postgres(self) -> AdaptersMap:
        """Create PostgreSQL adapter map

        Returns
            AdaptersMap with all necessary adapters registered
        """
        # Create a fresh adapters map
        postgres_adapters = psycopg.adapt.AdaptersMap(psycopg.adapters)

        # Register all type dumpers
        self._register_postgres_type_dumpers(postgres_adapters)

        # Try to register the numeric loader if possible
        try:
            numeric_oid = psycopg.postgres.types.get('numeric').oid
            postgres_adapters.register_loader(numeric_oid, CustomNumericLoader)
        except Exception as e:
            logger.debug(f'Numeric loader registration skipped: {e}')

        # When using SQLAlchemy, we'll rely on SQLAlchemy's psycopg dialect
        # to register the adapters with each connection
        return postgres_adapters

    def sqlite(self, connection: SQLiteConnection) -> None:
        """Register SQLite adapters for a connection

        Args:
            connection: SQLite connection object

        Note:
            Due to SQLite's architecture, adapters are registered globally
            rather than per-connection.
        """
        # Ensure connection is established
        connection.execute('SELECT 1')

        # Register date/time adapters
        sqlite3.register_adapter(datetime.date, adapt_date_iso)
        sqlite3.register_adapter(datetime.datetime, adapt_datetime_iso)
        sqlite3.register_converter('date', convert_date)
        sqlite3.register_converter('datetime', convert_datetime)

        # Register NumPy float types
        for dtype in self.NUMPY_FLOAT_TYPES:
            sqlite3.register_adapter(dtype, adapt_numpy_float)

        # Register NumPy int types
        for dtype in self.NUMPY_INT_TYPES:
            sqlite3.register_adapter(dtype, adapt_numpy_int)

        # Register Pandas nullable types
        for dtype in self.PANDAS_NULLABLE_TYPES:
            sqlite3.register_adapter(dtype, adapt_pandas_nullable)

        # Register PyArrow types if available
        if PYARROW_AVAILABLE and self.PYARROW_FLOAT_TYPES:
            for dtype in self.PYARROW_FLOAT_TYPES:
                sqlite3.register_adapter(dtype, adapt_pyarrow)

    def sqlserver(self, connection: SQLServerConnection) -> None:
        """Register SQL Server adapters

        SQL Server doesn't require adapter registration as pyodbc handles
        the conversions internally.

        Args:
            connection: Optional connection object (not used)
        """
        # Not needed for SQL Server


def get_adapter_registry() -> AdapterRegistry:
    """Get the adapter registry for database connections

    Returns
        AdapterRegistry instance
    """
    return AdapterRegistry()


if __name__ == '__main__':
    __import__('doctest').testmod(optionflags=4 | 8 | 32)
