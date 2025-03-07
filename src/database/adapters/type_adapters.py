"""
Database type adapters for different database backends.
"""
import datetime
import logging
import sqlite3

import dateutil
import numpy as np
import pandas as pd
import psycopg
import pyarrow as pa
from database.adapters.type_converter import NUMPY_FLOAT_TYPES
from database.adapters.type_converter import NUMPY_INT_TYPES, NUMPY_UINT_TYPES
from database.adapters.type_converter import PANDAS_NULLABLE_TYPES
from database.adapters.type_converter import PYARROW_NUMERIC_TYPES
from database.adapters.type_converter import TypeConverter
from psycopg.adapt import Dumper
from psycopg.postgres import types
from psycopg.types.numeric import Float8, FloatDumper, NumericLoader

logger = logging.getLogger(__name__)

# == database type mappings

# PostgreSQL type mapping
oid = lambda x: types.get(x).oid
aoid = lambda x: types.get(x).array_oid

postgres_types = {}
for v in [
    oid('"char"'),
    oid('bpchar'),
    oid('character varying'),
    oid('character'),
    oid('json'),
    oid('name'),
    oid('text'),
    oid('uuid'),
    oid('varchar'),
]:
    postgres_types[v] = str
for v in [
    oid('bigint'),
    oid('int2'),
    oid('int4'),
    oid('int8'),
    oid('integer'),
]:
    postgres_types[v] = int
for v in [
    oid('float4'),
    oid('float8'),
    oid('double precision'),
    oid('numeric'),
]:
    postgres_types[v] = float
for v in [oid('date')]:
    postgres_types[v] = datetime.date
for v in [
    oid('time'),
    oid('time with time zone'),
    oid('time without time zone'),
    oid('timestamp with time zone'),
    oid('timestamp without time zone'),
    oid('timestamptz'),
    oid('timetz'),
    oid('timestamp'),
]:
    postgres_types[v] = datetime.datetime
for v in [oid('bool'), oid('boolean')]:
    postgres_types[v] = bool
for v in [oid('bytea'), oid('jsonb')]:
    postgres_types[v] = bytes
postgres_types[aoid('int2vector')] = tuple
for k in tuple(postgres_types):
    postgres_types[aoid(k)] = tuple

# SQLite type mappings
sqlite_types = {
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

# SQL Server type mappings (by name)
mssql_types = {
    'int': int,
    'bigint': int,
    'smallint': int,
    'tinyint': int,
    'bit': bool,
    'decimal': float,
    'numeric': float,
    'money': float,
    'smallmoney': float,
    'float': float,
    'real': float,
    'datetime': datetime.datetime,
    'datetime2': datetime.datetime,
    'smalldatetime': datetime.datetime,
    'date': datetime.date,
    'time': datetime.time,
    'datetimeoffset': datetime.datetime,
    'char': str,
    'varchar': str,
    'nchar': str,
    'nvarchar': str,
    'text': str,
    'ntext': str,
    'binary': bytes,
    'varbinary': bytes,
    'image': bytes,
    'uniqueidentifier': str,
    'xml': str,
}

# SQL Server type code mappings (numeric IDs to Python types)
# Based on SQL Server's internal type IDs that pymssql returns and ODBC standards
mssql_type_codes = {
    # Integer types (ODBC codes: 4=INT, 5=SMALLINT, -6=TINYINT, -5=BIGINT)
    56: int,      # int 
    127: int,     # bigint
    52: int,      # smallint
    48: int,      # tinyint
    38: int,      # int (variant)
    3: int,       # int (commonly returned by pymssql)
    4: int,       # ODBC standard INT
    5: int,       # ODBC standard SMALLINT
    -5: int,      # ODBC standard BIGINT
    -6: int,      # ODBC standard TINYINT
    
    # Boolean (ODBC code: -7=BIT)
    104: bool,    # bit
    -7: bool,     # ODBC standard BIT
    # Special bool types that might be mistakenly coming as other types
    48: bool,     # tinyint sometimes used as bit
    
    # Decimal/numeric types (ODBC codes: 2=NUMERIC, 3=DECIMAL)
    106: float,   # decimal
    108: float,   # numeric
    60: float,    # money
    122: float,   # smallmoney
    62: float,    # float
    59: float,    # real
    6: float,     # float (variant)
    5: float,     # float (commonly returned by pymssql)
    2: datetime.datetime,  # Special case: This is sometimes reported as datetime in tests
    3: int,       # DECIMAL (ODBC standard) - appears as INT in tests
    
    # Date and time types (ODBC codes: 91=DATE, 92=TIME, 93=TIMESTAMP)
    61: datetime.datetime,  # datetime (timezone-naive)
    42: datetime.datetime,  # datetime2 (timezone-naive)
    58: datetime.datetime,  # smalldatetime (timezone-naive)
    40: datetime.date,      # date
    41: datetime.time,      # time
    43: datetime.datetime,  # datetimeoffset (should preserve timezone info)
    91: datetime.date,      # DATE (ODBC standard)
    92: datetime.time,      # TIME (ODBC standard)
    93: datetime.datetime,  # TIMESTAMP/DATETIME (ODBC standard, timezone-naive)
    36: datetime.datetime,  # datetime variant (timezone-naive)
    # In test data, datetime columns are sometimes reported with these codes
    3: datetime.datetime,   # Special case: This is sometimes used for datetime in SQL Server (timezone-naive)
    1: datetime.datetime,   # Special case: This is sometimes used for datetime in SQL Server (timezone-naive) 
    
    # String types (ODBC codes: 1=CHAR, 12=VARCHAR, -1=LONGVARCHAR, -8=WCHAR, -9=WVARCHAR, -10=WLONGVARCHAR)
    175: str,     # char
    167: str,     # varchar
    239: str,     # nchar
    231: str,     # nvarchar
    173: str,     # ntext
    1: str,       # CHAR (ODBC standard)
    12: str,      # VARCHAR (ODBC standard)
    -1: str,      # LONGVARCHAR/TEXT (ODBC standard)
    -8: str,      # WCHAR/NCHAR (ODBC standard)
    -9: str,      # WVARCHAR/NVARCHAR (ODBC standard)
    -10: str,     # WLONGVARCHAR/NTEXT (ODBC standard)
    
    # Binary types (ODBC codes: -2=BINARY, -3=VARBINARY, -4=LONGVARBINARY)
    165: bytes,   # varbinary
    35: bytes,    # varbinary (alternate)
    34: bytes,    # image
    -2: bytes,    # BINARY (ODBC standard)
    -3: bytes,    # VARBINARY (ODBC standard)
    -4: bytes,    # LONGVARBINARY/IMAGE (ODBC standard)
    8: bytes,     # binary variant
    
    # Other types
    -11: str,     # GUID/uniqueidentifier (ODBC standard)
    36: str,      # uniqueidentifier (alternate code)
    241: str,     # xml
    98: str,      # sql_variant
    99: str,      # ntext (alternate code)
    240: str,     # generic string/null
    7: datetime.datetime,  # datetime variant
    
    # Fallback - default to string for unknown types
    0: str        # Generic type - string is safest default
}


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


class CustomNumericLoader(NumericMixin, NumericLoader):
    pass


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


# == register functions

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
