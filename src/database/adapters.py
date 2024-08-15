import datetime
import sqlite3

import dateutil
import psycopg
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

    sqlite3.register_adapter(datetime.date, adapt_date_iso)
    sqlite3.register_adapter(datetime.datetime, adapt_datetime_iso)
    sqlite3.register_converter('date', convert_date)
    sqlite3.register_converter('datetime', convert_datetime)
