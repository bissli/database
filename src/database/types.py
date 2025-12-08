"""
Consolidated type handling for database operations.

This module provides:
- TypeConverter: Convert Python values to database-compatible formats
- Column: Column metadata from cursor descriptions
- resolve_type: Resolve database type codes to Python types
- Row adapters: Convert database rows to dictionaries
"""
import datetime
import logging
import math
import sqlite3
from typing import Any, Self, TypeVar

import dateutil.parser
import numpy as np
import pandas as pd

from libb import attrdict

try:
    import pyarrow as pa
    PYARROW_AVAILABLE = True
except ImportError:
    pa = None
    PYARROW_AVAILABLE = False

logger = logging.getLogger(__name__)

# Type definitions
SQLiteConnection = TypeVar('SQLiteConnection')

# Constants for type conversion
SPECIAL_STRINGS: set[str] = {'null', 'nan', 'none', 'na', 'nat'}
NUMPY_FLOAT_TYPES = (np.floating,)
NUMPY_INT_TYPES = (np.integer, np.unsignedinteger)
PANDAS_NULLABLE_TYPES = (
    pd.Int64Dtype, pd.Int32Dtype, pd.Int16Dtype, pd.Int8Dtype,
    pd.UInt64Dtype, pd.UInt32Dtype, pd.UInt16Dtype, pd.UInt8Dtype,
    pd.Float64Dtype
)
PYARROW_FLOAT_TYPES = (
    (pa.FloatScalar, pa.DoubleScalar, pa.Int8Scalar, pa.Int16Scalar,
     pa.Int32Scalar, pa.Int64Scalar, pa.UInt8Scalar, pa.UInt16Scalar,
     pa.UInt32Scalar, pa.UInt64Scalar, pa.StringScalar)
    if PYARROW_AVAILABLE else ()
)


# Type Converter - Handles Python -> Database value conversion

def _check_special_string(value: str) -> None:
    """Check if a string value should be converted to NULL."""
    if value == '' or value.lower() in SPECIAL_STRINGS:
        return None
    return False


def _convert_pyarrow_value(value: Any) -> Any:
    """Convert PyArrow value to Python type."""
    if not PYARROW_AVAILABLE or value is None:
        return value

    try:
        if pa.compute.is_null(value).as_py():
            return None
    except (AttributeError, TypeError, ValueError):
        pass

    if hasattr(value, 'as_py'):
        try:
            py_value = value.as_py()
            if isinstance(py_value, str) and _check_special_string(py_value) is None:
                return None
            return py_value
        except Exception:
            pass

    if pa and isinstance(value, pa.Scalar):
        try:
            py_value = value.value
            if isinstance(py_value, str) and _check_special_string(py_value) is None:
                return None
            return py_value
        except Exception:
            pass

    if pa and isinstance(value, pa.Array | pa.ChunkedArray):
        try:
            return value.to_pylist()
        except Exception:
            pass

    if pa and isinstance(value, pa.Table):
        try:
            return value.to_pandas()
        except Exception:
            pass

    try:
        return str(value)
    except Exception:
        return None


def _convert_numpy_value(val: Any) -> float | int | datetime.datetime | None:
    """Convert NumPy value to Python type."""
    if val is None:
        return None

    if isinstance(val, np.floating) and np.isnan(val):
        return None

    if isinstance(val, np.datetime64) and np.isnat(val):
        return None

    if isinstance(val, (np.floating, np.integer | np.unsignedinteger)):
        return val.item()

    if isinstance(val, np.datetime64):
        timestamp = val.astype('datetime64[s]').astype(int)
        return datetime.datetime.utcfromtimestamp(timestamp)

    return val


def _convert_pandas_nullable(val: Any) -> Any:
    """Convert Pandas nullable value to Python type."""
    if pd.isna(val):
        return None
    if isinstance(val, str) and _check_special_string(val) is None:
        return None
    return val


class TypeConverter:
    """Universal type conversion for database parameters.

    Handles NumPy, Pandas, and PyArrow types.
    """

    @staticmethod
    def convert_value(value: Any) -> Any:
        """Convert a single value to a database-compatible format."""
        if value is None:
            return None

        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return None

        if pd and hasattr(pd, 'NaT') and isinstance(value, type(pd.NaT)):
            return None

        if isinstance(value, str) and _check_special_string(value) is None:
            return None

        if isinstance(value, (*NUMPY_FLOAT_TYPES, *NUMPY_INT_TYPES, np.datetime64)):
            return _convert_numpy_value(value)

        if pd.api.types.is_scalar(value) and pd.isna(value):
            return None

        if hasattr(value, 'dtype') and pd.api.types.is_dtype_equal(value.dtype, 'object') and pd.isna(value):
            return None

        if isinstance(value, PANDAS_NULLABLE_TYPES):
            return _convert_pandas_nullable(value)

        if PYARROW_AVAILABLE:
            if isinstance(value, PYARROW_FLOAT_TYPES):
                return _convert_pyarrow_value(value)
            if isinstance(value, pa.Scalar) or hasattr(value, '_is_arrow_scalar') or \
               isinstance(value, pa.Array | pa.ChunkedArray | pa.Table):
                return _convert_pyarrow_value(value)

        return value

    @staticmethod
    def convert_params(params: Any) -> Any:
        """Convert a collection of parameters for database operations."""
        if params is None:
            return None

        if isinstance(params, dict):
            return {k: TypeConverter.convert_value(v) for k, v in params.items()}

        if isinstance(params, list | tuple):
            if params and all(isinstance(p, (list, tuple)) for p in params):
                return type(params)(TypeConverter.convert_params(p) for p in params)
            return type(params)(TypeConverter.convert_value(v) for v in params)

        return TypeConverter.convert_value(params)


# Type Resolution - Database type codes -> Python types

from psycopg.postgres import types as pg_types

_oid = lambda x: pg_types.get(x).oid
_aoid = lambda x: pg_types.get(x).array_oid

postgres_types: dict[int, type] = {}

for v in [_oid('"char"'), _oid('bpchar'), _oid('character varying'), _oid('character'),
          _oid('json'), _oid('name'), _oid('text'), _oid('uuid'), _oid('varchar')]:
    postgres_types[v] = str

for v in [_oid('bigint'), _oid('int2'), _oid('int4'), _oid('int8'), _oid('integer')]:
    postgres_types[v] = int

for v in [_oid('float4'), _oid('float8'), _oid('double precision'), _oid('numeric')]:
    postgres_types[v] = float

postgres_types[_oid('date')] = datetime.date

for v in [_oid('time'), _oid('time with time zone'), _oid('time without time zone'),
          _oid('timestamp with time zone'), _oid('timestamp without time zone'),
          _oid('timestamptz'), _oid('timetz'), _oid('timestamp')]:
    postgres_types[v] = datetime.datetime

for v in [_oid('bool'), _oid('boolean')]:
    postgres_types[v] = bool

for v in [_oid('bytea'), _oid('jsonb')]:
    postgres_types[v] = bytes

postgres_types[_aoid('int2vector')] = tuple
for k in tuple(postgres_types):
    postgres_types[_aoid(k)] = tuple


sqlite_types: dict[str, type] = {
    'INTEGER': int,
    'REAL': float,
    'TEXT': str,
    'BLOB': bytes,
    'NUMERIC': float,
    'BOOLEAN': bool,
    'DATE': datetime.date,
    'DATETIME': datetime.datetime,
    'TIME': datetime.time,
}


def resolve_type(
    db_type: str,
    type_code: Any,
    column_name: str | None = None,
    table_name: str | None = None,
    **kwargs
) -> type:
    """Resolve database type code to Python type.

    Priority:
    1. Direct type code lookup (fastest)
    2. Column name patterns
    3. Default to str

    Args:
        db_type: Database type ('postgresql', 'sqlite')
        type_code: Database-specific type code
        column_name: Optional column name for pattern matching
        table_name: Optional table name (unused, for compatibility)
        **kwargs: Additional args (precision, scale - unused)

    Returns
        Python type
    """
    if isinstance(type_code, type):
        return type_code

    if db_type == 'postgresql':
        if type_code in postgres_types:
            return postgres_types[type_code]
    elif db_type == 'sqlite':
        if isinstance(type_code, str):
            base_type = type_code.split('(')[0].upper()
            if base_type in sqlite_types:
                return sqlite_types[base_type]
        if type_code in sqlite_types:
            return sqlite_types[type_code]

    if column_name:
        name_lower = column_name.lower()

        if name_lower.endswith('_id') or name_lower == 'id':
            return int

        if name_lower.endswith(('_datetime', '_at', '_timestamp')) or name_lower == 'timestamp':
            return datetime.datetime

        if name_lower.endswith('_date') or name_lower == 'date':
            return datetime.date

        if name_lower.endswith('_time') or name_lower == 'time':
            return datetime.time

        if (name_lower.startswith('is_') or name_lower.endswith('_flag') or
                name_lower in {'active', 'enabled', 'disabled', 'is_deleted'}):
            return bool

        if (name_lower.endswith(('_price', '_cost', '_amount')) or
                name_lower.startswith(('price_', 'cost_', 'amount_'))):
            return float

    return str


# Column - Metadata from cursor descriptions

class Column:
    """Database column metadata."""

    def __init__(self,
                 name: str,
                 type_code: Any,
                 python_type: type | None = None,
                 display_size: int | None = None,
                 internal_size: int | None = None,
                 precision: int | None = None,
                 scale: int | None = None,
                 nullable: bool | None = None):
        self.name = name
        self.type_code = type_code
        self.python_type = python_type
        self.display_size = display_size
        self.internal_size = internal_size
        self.precision = precision
        self.scale = scale
        self.nullable = nullable

    @classmethod
    def from_cursor_description(cls, description_item: Any, connection_type: str,
                                table_name=None, connection=None) -> Self:
        """Create a Column from cursor description item."""
        if connection_type == 'postgresql':
            column_info = cls._extract_postgres_column_info(description_item)
        elif connection_type == 'sqlite':
            column_info = cls._extract_sqlite_column_info(description_item)
        else:
            column_info = {
                'name': str(description_item[0]) if description_item else None,
                'type_code': None, 'display_size': None, 'internal_size': None,
                'precision': None, 'scale': None, 'nullable': None
            }

        python_type = resolve_type(
            connection_type,
            column_info['type_code'],
            column_name=column_info['name'],
            table_name=table_name,
            column_size=column_info['display_size'],
            precision=column_info['precision'],
            scale=column_info['scale']
        )
        column_info['python_type'] = python_type

        return cls(**column_info)

    @classmethod
    def _extract_postgres_column_info(cls, description_item: Any) -> dict:
        return {
            'name': getattr(description_item, 'name', None),
            'type_code': getattr(description_item, 'type_code', None),
            'display_size': getattr(description_item, 'display_size', None),
            'internal_size': getattr(description_item, 'internal_size', None),
            'precision': getattr(description_item, 'precision', None),
            'scale': getattr(description_item, 'scale', None),
            'nullable': None
        }

    @classmethod
    def _extract_sqlite_column_info(cls, description_item: Any) -> dict:
        if len(description_item) >= 7:
            return {
                'name': description_item[0],
                'type_code': description_item[1],
                'display_size': description_item[2],
                'internal_size': description_item[3],
                'precision': description_item[4],
                'scale': description_item[5],
                'nullable': bool(description_item[6])
            }
        return {
            'name': description_item[0] if len(description_item) > 0 else None,
            'type_code': description_item[1] if len(description_item) > 1 else None,
            'display_size': None, 'internal_size': None,
            'precision': None, 'scale': None, 'nullable': None
        }

    def __repr__(self) -> str:
        return (f'Column(name={self.name!r}, type_code={self.type_code!r}, '
                f'python_type={self.python_type.__name__ if self.python_type else None})')

    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'type_code': self.type_code,
            'python_type': self.python_type.__name__ if self.python_type else None,
            'display_size': self.display_size,
            'internal_size': self.internal_size,
            'precision': self.precision,
            'scale': self.scale,
            'nullable': self.nullable
        }

    @staticmethod
    def get_names(columns: list[Self]) -> list[str]:
        return [col.name for col in columns]

    @staticmethod
    def get_column_by_name(columns: list[Self], name: str) -> Self | None:
        for col in columns:
            if col.name == name:
                return col
        return None

    @staticmethod
    def get_column_types_dict(columns: list[Self]) -> dict[str, dict]:
        return {col.name: col.to_dict() for col in columns}

    @staticmethod
    def get_types(columns: list[Self]) -> list[type | None]:
        return [col.python_type for col in columns]

    @staticmethod
    def create_empty_columns(names: list[str]) -> list[Self]:
        return [Column(name=name, type_code=None) for name in names]


def columns_from_cursor_description(cursor: Any, connection_type: str,
                                    table_name=None, connection=None) -> list[Column]:
    """Create Column objects from cursor description."""
    if cursor.description is None:
        return []
    return [Column.from_cursor_description(desc, connection_type, table_name, connection)
            for desc in cursor.description]


# Row Adapters - Convert database rows to dictionaries

class RowAdapter:
    """Simple row adapter for converting database rows to dictionaries."""

    def __init__(self, row: Any):
        self.row = row

    def to_dict(self) -> dict[str, Any]:
        """Convert row to dictionary."""
        # sqlite3.Row
        if hasattr(self.row, 'keys') and callable(self.row.keys):
            return {key: self.row[key] for key in self.row.keys()}  # noqa: SIM118
        # Already a dict
        if isinstance(self.row, dict):
            return self.row
        # Namedtuple
        if hasattr(self.row, '_asdict'):
            return self.row._asdict()
        # Fallback
        return self.row

    def get_value(self, key: str | None = None) -> Any:
        """Get a value from the row."""
        if key is not None:
            if isinstance(self.row, dict):
                return self.row[key]
            if hasattr(self.row, 'keys'):
                return self.row[key]
            if hasattr(self.row, key):
                return getattr(self.row, key)
            return self.row[key]

        # Get first value
        if hasattr(self.row, 'keys') and callable(self.row.keys):
            keys = list(self.row.keys())
            if keys:
                return self.row[keys[0]]
        if isinstance(self.row, dict) and self.row:
            return next(iter(self.row.values()))
        if hasattr(self.row, '__getitem__'):
            return self.row[0]
        return self.row

    def to_attrdict(self) -> attrdict:
        return attrdict(self.to_dict())

    @staticmethod
    def create(connection, row) -> 'RowAdapter':
        """Factory method - returns simple RowAdapter for all connection types."""
        return RowAdapter(row)

    @staticmethod
    def create_empty_dict(cols: list[str]) -> dict[str, None]:
        return dict.fromkeys(cols)

    @staticmethod
    def create_attrdict_from_cols(cols: list[str]) -> attrdict:
        return attrdict(RowAdapter.create_empty_dict(cols))


# Backwards compatibility aliases
RowStructureAdapter = RowAdapter
PostgreSQLRowAdapter = RowAdapter
SQLiteRowAdapter = RowAdapter


# SQLite Adapters - Database value converters

def convert_date(val: bytes) -> datetime.date:
    """Convert ISO 8601 date string to date object."""
    return dateutil.parser.isoparse(val.decode()).date()


def convert_datetime(val: bytes) -> datetime.datetime:
    """Convert ISO 8601 datetime string to datetime object."""
    return dateutil.parser.isoparse(val.decode())


class AdapterRegistry:
    """Registry for database-specific type adapters."""

    def sqlite(self, connection: SQLiteConnection) -> None:
        """Register SQLite converters for a connection."""
        connection.execute('SELECT 1')
        sqlite3.register_converter('date', convert_date)
        sqlite3.register_converter('datetime', convert_datetime)


def get_adapter_registry() -> AdapterRegistry:
    """Get the adapter registry."""
    return AdapterRegistry()
