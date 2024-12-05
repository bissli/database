import datetime
import sqlite3

import dateutil
import numpy as np
import psycopg
from psycopg.adapt import Dumper
from psycopg.types.numeric import Float8, FloatDumper, NumericLoader

__all__ = ['register_adapters']


# == psycopg adapters


class CustomFloatDumper(FloatDumper):
    """Do not store NaN. Use Null"""

    _special = {
        b'inf': b"'Infinity'::float8",
        b'-inf': b"'-Infinity'::float8",
        b'nan': None
    }


class NumpyNanDumper(Dumper):
    def dump(self, value):
        if np.isnan(value):
            return None
        return value


class NumericMixin:
    def load(self, data) -> float:
        if isinstance(data, memoryview):
            data = bytes(data)
        return float(data.decode()) if data is not None else None


class CustomNumericLoader(NumericMixin, NumericLoader): pass


# == sqlite adapter


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


def register_adapters():

    psycopg.adapters.register_dumper(Float8, CustomFloatDumper)
    psycopg.adapters.register_loader('numeric', CustomNumericLoader)

    # numpy
    psycopg.adapters.register_dumper(np.float64, NumpyNanDumper)
    psycopg.adapters.register_dumper(np.float32, NumpyNanDumper)
    psycopg.adapters.register_dumper(np.int64, NumpyNanDumper)
    psycopg.adapters.register_dumper(np.int32, NumpyNanDumper)

    sqlite3.register_adapter(datetime.date, adapt_date_iso)
    sqlite3.register_adapter(datetime.datetime, adapt_datetime_iso)
    sqlite3.register_converter('date', convert_date)
    sqlite3.register_converter('datetime', convert_datetime)
