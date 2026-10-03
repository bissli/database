"""Value conversion, type resolution, column metadata and row adapters.
"""
import datetime
import logging
import math
import sqlite3
import sys
from dataclasses import dataclass
from typing import Any, Self, TypeVar

import dateutil.parser
import numpy as np
import pandas as pd
from psycopg.postgres import types as pg_types

from libb import attrdict

logger = logging.getLogger(__name__)

SQLiteConnection = TypeVar('SQLiteConnection')

SPECIAL_STRINGS: set[str] = {'null', 'nan', 'none', 'na', 'nat'}
NUMPY_FLOAT_TYPES = (np.floating,)
NUMPY_INT_TYPES = (np.integer, np.unsignedinteger)
NUMPY_BOOL_TYPES = (np.bool_,)
PANDAS_NULLABLE_TYPES = (
    pd.Int64Dtype, pd.Int32Dtype, pd.Int16Dtype, pd.Int8Dtype,
    pd.UInt64Dtype, pd.UInt32Dtype, pd.UInt16Dtype, pd.UInt8Dtype,
    pd.Float64Dtype
)


# --- Python to database value conversion ---

def _empty_string_to_none(py_value: Any) -> Any:
    """None for an empty string, otherwise py_value unchanged.
    """
    if isinstance(py_value, str) and not py_value:
        return None
    return py_value


def null_special_string(value: Any) -> Any:
    """None for '' or a SPECIAL_STRINGS word in any case, else value unchanged.

    Parameters
    ----------
    value : Any
        A bound parameter. Only a str (np.str_ included) or a PyArrow
        string scalar can map to None.

    Returns
    -------
    Any
        None, a PyArrow string scalar's str, or value unchanged.
    """
    pa = sys.modules.get('pyarrow')
    if pa and isinstance(value, pa.StringScalar | pa.LargeStringScalar):
        value = value.as_py()
    if isinstance(value, str) and (not value or value.lower() in SPECIAL_STRINGS):
        return None
    return value


def _convert_pyarrow_value(value: Any) -> Any:
    """Builtin Python value for a PyArrow scalar, array or table.

    Parameters
    ----------
    value : Any
        A PyArrow Scalar, Array, ChunkedArray or Table. Returned
        unchanged when pyarrow is not imported.

    Returns
    -------
    Any
        None for a null or '' scalar, the scalar's Python value, a list
        for an array, a DataFrame for a table, else str(value).
    """
    pa = sys.modules.get('pyarrow')
    if pa is None or value is None:
        return value

    try:
        if pa.compute.is_null(value).as_py():
            return None
    except (AttributeError, TypeError, ValueError):
        pass

    if hasattr(value, 'as_py'):
        try:
            return _empty_string_to_none(value.as_py())
        except (ValueError, TypeError, AttributeError):
            pass

    if isinstance(value, pa.Scalar):
        try:
            return _empty_string_to_none(value.value)
        except (ValueError, TypeError, AttributeError):
            pass

    if isinstance(value, pa.Array | pa.ChunkedArray):
        try:
            return value.to_pylist()
        except (ValueError, TypeError, AttributeError):
            pass

    if isinstance(value, pa.Table):
        try:
            return value.to_pandas()
        except (ValueError, TypeError, AttributeError):
            pass

    try:
        return str(value)
    except (ValueError, TypeError):
        return None


def _convert_numpy_value(val: Any) -> float | int | bool | datetime.datetime | None:
    """Builtin Python counterpart of a NumPy scalar.

    Parameters
    ----------
    val : Any
        A NumPy scalar. Any other value is returned unchanged.

    Returns
    -------
    float | int | bool | datetime.datetime | None
        The unboxed builtin. None for NaN, infinity and NaT. A datetime64
        comes back as a naive UTC datetime, truncated to the second.
    """
    if isinstance(val, np.floating) and (np.isnan(val) or np.isinf(val)):
        return None

    if isinstance(val, np.datetime64) and np.isnat(val):
        return None

    if isinstance(val, (np.floating, np.integer, np.unsignedinteger, np.bool_)):
        return val.item()

    if isinstance(val, np.datetime64):
        timestamp = val.astype('datetime64[s]').astype(int)
        return datetime.datetime.fromtimestamp(
            timestamp, datetime.UTC).replace(tzinfo=None)

    return val


class TypeConverter:
    """Converts NumPy, pandas and PyArrow parameters to driver-ready values.
    """

    @staticmethod
    def convert_value(value: Any) -> Any:
        """Driver-ready form of one bound parameter.

        Parameters
        ----------
        value : Any
            A bound parameter: a builtin, NumPy, pandas or PyArrow value.

        Returns
        -------
        Any
            A driver-ready value. None for None, NaN, infinity, NaT,
            pd.NA, a PyArrow null and ''. Any other str, 'nan' and
            'None' included, comes back unchanged.
        """
        if value is None:
            return None

        value_type = type(value)
        if value_type is int or value_type is bool:
            return value
        if value_type is float:
            if math.isnan(value) or math.isinf(value):
                return None
            return value
        if value_type is str:
            return value or None
        if (value_type is bytes
            or value_type is datetime.date
            or value_type is datetime.datetime):
            return value

        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return None

        if isinstance(value, type(pd.NaT)):
            return None

        if isinstance(value, str) and not value:
            return None

        if isinstance(value, (*NUMPY_FLOAT_TYPES, *NUMPY_INT_TYPES,
                              *NUMPY_BOOL_TYPES, np.datetime64)):
            return _convert_numpy_value(value)

        if pd.api.types.is_scalar(value) and pd.isna(value):
            return None

        if (hasattr(value, 'dtype')
            and pd.api.types.is_dtype_equal(value.dtype, 'object')
            and pd.isna(value)):
            return None

        if isinstance(value, PANDAS_NULLABLE_TYPES):
            if pd.isna(value):
                return None
            return _empty_string_to_none(value)

        pa = sys.modules.get('pyarrow')
        if pa and (isinstance(value, pa.Scalar)
                   or hasattr(value, '_is_arrow_scalar')
                   or isinstance(value, pa.Array | pa.ChunkedArray | pa.Table)):
            return _convert_pyarrow_value(value)

        return value

    @staticmethod
    def convert_params(params: Any) -> Any:
        """Bound parameters with each value passed through convert_value.

        Parameters
        ----------
        params : Any
            A dict, one row as a list or tuple, a batch of rows (a list or
            tuple whose every item is a list or tuple), or a lone value.

        Returns
        -------
        Any
            The same shape and container classes, with values converted.
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


# --- Type resolution ---

def _build_postgres_types() -> dict[int, type]:
    """Python type this library reports for each PostgreSQL type OID.

    Returns
    -------
    dict[int, type]
        Type per OID. The array OID of every mapped type, and of
        int2vector, maps to tuple. A name psycopg does not register is
        skipped.
    """
    types: dict[int, type] = {}

    type_mappings = [
        (str, ['"char"', 'bpchar', 'character varying', 'character',
               'name', 'text', 'uuid', 'varchar']),
        (int, ['bigint', 'int2', 'int4', 'int8', 'integer']),
        (float, ['float4', 'float8', 'double precision', 'numeric']),
        (datetime.date, ['date']),
        (datetime.time, ['time', 'time with time zone', 'time without time zone',
                         'timetz']),
        (datetime.datetime, ['timestamp', 'timestamp with time zone',
                             'timestamp without time zone', 'timestamptz']),
        (bool, ['bool', 'boolean']),
        (bytes, ['bytea']),
        (dict, ['json', 'jsonb']),
    ]

    for py_type, pg_names in type_mappings:
        for name in pg_names:
            type_info = pg_types.get(name)
            if type_info:
                types[type_info.oid] = py_type

    int2vector_info = pg_types.get('int2vector')
    if int2vector_info:
        types[int2vector_info.array_oid] = tuple

    for oid in list(types):
        type_info = pg_types.get(oid)
        if type_info and type_info.array_oid:
            types[type_info.array_oid] = tuple

    return types


postgres_types: dict[int, type] = _build_postgres_types()


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
    type_map: dict | None = None,
    **_,
) -> type:
    """Python type for a column: by type code, else by column name, else str.

    Parameters
    ----------
    db_type : str
        Dialect, 'postgresql' or 'sqlite'. Any other dialect skips the
        built-in type-code maps.
    type_code : Any
        A PostgreSQL OID or a SQLite declared type. A SQLite type
        matches in any case and with any '(...)' size suffix. A Python
        type is returned as is, ahead of every other rule.
    column_name : str or None
        Matched in any case against name patterns ('_id' and 'id' are
        int, '_at' is datetime.datetime, 'is_' is bool, ...) when the
        type code does not resolve.
    table_name : str or None
        Unused.
    type_map : dict or None
        Replaces the dialect's built-in map, even when empty.
    **_
        Cursor metadata (column_size, precision, scale), ignored.

    Returns
    -------
    type
        The resolved type, str when no rule matches.
    """
    if isinstance(type_code, type):
        return type_code

    if type_map is not None:
        if type_code in type_map:
            return type_map[type_code]
        if db_type == 'sqlite' and isinstance(type_code, str):
            base_type = type_code.split('(')[0].upper()
            if base_type in type_map:
                return type_map[base_type]
    elif db_type == 'postgresql':
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

        if (name_lower.endswith(('_datetime', '_at', '_timestamp'))
            or name_lower == 'timestamp'):
            return datetime.datetime

        if name_lower.endswith('_date') or name_lower == 'date':
            return datetime.date

        if name_lower.endswith('_time') or name_lower == 'time':
            return datetime.time

        if (name_lower.startswith('is_')
            or name_lower.endswith('_flag')
            or name_lower in {'active', 'enabled', 'disabled', 'is_deleted'}):
            return bool

        if (name_lower.endswith(('_price', '_cost', '_amount'))
            or name_lower.startswith(('price_', 'cost_', 'amount_'))):
            return float

    return str


@dataclass(frozen=True)
class ColumnInfo:
    """A table column as the schema declares it.

    Attributes
    ----------
    name : str
        Column name.
    type : str
        Declared type text as the database reports it, which may differ in
        case from the DDL. Empty when the column declares no type.
    notnull : bool
        True when the database reports the column not null.
    default : str or None
        Default as SQL expression text ("'x'" for a string), or None.
    primary_key : bool
        True when the column is part of the primary key.
    """
    name: str
    type: str
    notnull: bool
    default: str | None
    primary_key: bool


# --- Column metadata ---

class Column:
    """One result column as a cursor description reports it.

    Parameters
    ----------
    name : str
        Column name.
    type_code : Any
        A PostgreSQL OID or a SQLite declared type, None when the driver
        reports none.
    python_type : type or None
        Resolved Python type.
    display_size, internal_size, precision, scale : int or None
        DB-API description fields, None when the driver omits them.
    nullable : bool or None
        DB-API null_ok. None means unknown.
    """

    def __init__(
        self,
        name: str,
        type_code: Any,
        python_type: type | None = None,
        display_size: int | None = None,
        internal_size: int | None = None,
        precision: int | None = None,
        scale: int | None = None,
        nullable: bool | None = None,
    ) -> None:
        self.name = name
        self.type_code = type_code
        self.python_type = python_type
        self.display_size = display_size
        self.internal_size = internal_size
        self.precision = precision
        self.scale = scale
        self.nullable = nullable

    @classmethod
    def from_cursor_description(
        cls,
        description_item: Any,
        connection_type: str,
        table_name: str | None = None,
        connection: Any = None,
    ) -> Self:
        """Column for one cursor description entry, its type resolved.

        Parameters
        ----------
        description_item : Any
            A psycopg Column for 'postgresql', a DB-API sequence for
            'sqlite'. A SQLite entry shorter than seven fields keeps only
            its name and type code.
        connection_type : str
            Dialect. Any other dialect keeps only str(item[0]) as the
            name.
        table_name : str or None
            Passed to resolve_type, which ignores it.
        connection : Any
            Unused.

        Returns
        -------
        Self
            The column, with python_type from resolve_type.
        """
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
        """Column fields read by attribute from a psycopg Column.
        """
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
        """Column fields read by index from a DB-API description sequence.
        """
        if len(description_item) >= 7:
            return {
                'name': description_item[0],
                'type_code': description_item[1],
                'display_size': description_item[2],
                'internal_size': description_item[3],
                'precision': description_item[4],
                'scale': description_item[5],
                'nullable': (None if description_item[6] is None
                             else bool(description_item[6]))
            }
        return {
            'name': description_item[0] if len(description_item) > 0 else None,
            'type_code': description_item[1] if len(description_item) > 1 else None,
            'display_size': None, 'internal_size': None,
            'precision': None, 'scale': None, 'nullable': None
        }

    def __repr__(self) -> str:
        """Name, type code and python_type's name.
        """
        type_name = self.python_type.__name__ if self.python_type else None
        return (f'Column(name={self.name!r}, type_code={self.type_code!r}, '
                f'python_type={type_name})')

    def to_dict(self) -> dict:
        """Every field by name, with python_type as its __name__ or None.
        """
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
        """Column names in list order.
        """
        return [col.name for col in columns]

    @staticmethod
    def get_column_by_name(columns: list[Self], name: str) -> Self | None:
        """First column named name, or None.
        """
        for col in columns:
            if col.name == name:
                return col
        return None

    @staticmethod
    def get_column_types_dict(columns: list[Self]) -> dict[str, dict]:
        """to_dict() of each column, keyed by column name.
        """
        return {col.name: col.to_dict() for col in columns}

    @staticmethod
    def get_types(columns: list[Self]) -> list[type | None]:
        """python_type of each column, in list order.
        """
        return [col.python_type for col in columns]

    @staticmethod
    def create_empty_columns(names: list[str]) -> list[Self]:
        """One Column per name, every other field None.
        """
        return [Column(name=name, type_code=None) for name in names]


def columns_from_cursor_description(
    cursor: Any,
    connection_type: str,
    table_name: str | None = None,
    connection: Any = None,
) -> list[Column]:
    """One Column per cursor description entry, in cursor order.

    Parameters
    ----------
    cursor : Any
        A DB-API cursor. A None description, as after DDL, yields [].
    connection_type : str
        Dialect, passed to Column.from_cursor_description.
    table_name : str or None
        Passed to Column.from_cursor_description, which ignores it.
    connection : Any
        Unused.

    Returns
    -------
    list[Column]
        The columns, with python_type resolved.
    """
    if cursor.description is None:
        return []
    return [
        Column.from_cursor_description(
            desc, connection_type, table_name, connection)
        for desc in cursor.description
        ]


# --- Row adapters ---

class RowAdapter:
    """Reshapes one driver row without converting any value.

    Parameters
    ----------
    row : Any
        A sqlite3.Row or other mapping, a namedtuple, or a sequence.
    """

    def __init__(self, row: Any) -> None:
        self.row = row

    def to_dict(self) -> dict[str, Any]:
        """The row as a dict keyed by column name.

        Returns
        -------
        dict[str, Any]
            A new dict for a mapping, `_asdict()` for a namedtuple. Any
            other row comes back as is.
        """
        if hasattr(self.row, 'keys') and callable(self.row.keys):
            return {key: self.row[key] for key in self.row.keys()}  # noqa: SIM118
        if hasattr(self.row, '_asdict'):
            return self.row._asdict()
        return self.row

    def get_value(self, key: str | None = None) -> Any:
        """The value of column key, or of the first column.

        Parameters
        ----------
        key : str or None
            Column name. None reads the first column.

        Returns
        -------
        Any
            The value. A row with no __getitem__ comes back whole when key
            is None.

        Raises
        ------
        KeyError
            key is absent from a dict row. Other row types raise what
            their own lookup raises.
        """
        if key is not None:
            if not hasattr(self.row, 'keys') and hasattr(self.row, key):
                return getattr(self.row, key)
            return self.row[key]

        if hasattr(self.row, 'keys') and callable(self.row.keys):
            keys = list(self.row.keys())
            if keys:
                return self.row[keys[0]]
        if hasattr(self.row, '__getitem__'):
            return self.row[0]
        return self.row

    def to_attrdict(self) -> attrdict:
        """to_dict() as an attrdict, read by row.column or row['column'].
        """
        return attrdict(self.to_dict())

    @staticmethod
    def create(connection: Any, row: Any) -> 'RowAdapter':
        """RowAdapter for row. connection is ignored.
        """
        return RowAdapter(row)

    @staticmethod
    def create_empty_dict(cols: list[str]) -> dict[str, None]:
        """Dict mapping each name in cols to None.
        """
        return dict.fromkeys(cols)

    @staticmethod
    def create_attrdict_from_cols(cols: list[str]) -> attrdict:
        """attrdict mapping each name in cols to None.
        """
        return attrdict(RowAdapter.create_empty_dict(cols))


# --- SQLite converters ---

def convert_date(val: bytes) -> datetime.date:
    """Date from a stored ISO 8601 value, any time part dropped.
    """
    return dateutil.parser.isoparse(val.decode()).date()


def convert_datetime(val: bytes) -> datetime.datetime:
    """Datetime from a stored ISO 8601 value, aware only if it holds an offset.
    """
    return dateutil.parser.isoparse(val.decode())


class AdapterRegistry:
    """Registers this library's SQLite converters.
    """

    def sqlite(self, connection: SQLiteConnection) -> None:
        """Register the date and datetime converters, process-wide in sqlite3.
        """
        connection.execute('select 1')
        sqlite3.register_converter('date', convert_date)
        sqlite3.register_converter('datetime', convert_datetime)


def get_adapter_registry() -> AdapterRegistry:
    """A new AdapterRegistry.
    """
    return AdapterRegistry()
