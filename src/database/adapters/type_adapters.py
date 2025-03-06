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
from psycopg.adapt import Dumper
from psycopg.types.numeric import Float8, FloatDumper, NumericLoader
from psycopg.postgres import types

from database.adapters.type_converter import (NUMPY_FLOAT_TYPES, NUMPY_INT_TYPES,
                                            NUMPY_UINT_TYPES, PANDAS_NULLABLE_TYPES,
                                            PYARROW_NUMERIC_TYPES, TypeConverter)

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

# SQL Server type mappings
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
