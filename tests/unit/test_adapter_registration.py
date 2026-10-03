"""Tests for SQLite adapter and converter registration."""
import datetime
import sqlite3

import database as db
import database.strategy.sqlite as sqlite_strategy
import pytest
from database.types import convert_date, convert_datetime, get_adapter_registry

CREATE_PROBE_TABLE = """
create table conv_probe (
    label text,
    d date,
    ts datetime,
    txt text
)
"""


@pytest.fixture
def sqlite_conn():
    """In-memory SQLite connection holding a table of declared types."""
    cn = db.connect({'drivername': 'sqlite', 'database': ':memory:'})
    db.execute(cn, CREATE_PROBE_TABLE)
    yield cn
    cn.close()


def test_adapter_registry_registers_both_sqlite_converters(monkeypatch):
    """Verify AdapterRegistry.sqlite() binds each decltype to its converter.

    Mutation: convert_date under 'datetime', or a converter dropped.
    Oracle: hand-written (name, converter) pairs compared by identity.
    """
    registered = []
    monkeypatch.setattr(
        sqlite3,
        'register_converter',
        lambda name, converter: registered.append((name, converter)))

    class ProbeConnection:
        """Records each statement passed to execute().
        """

        def __init__(self):
            self.executed = []

        def execute(self, sql):
            self.executed.append(sql)

    connection = ProbeConnection()
    get_adapter_registry().sqlite(connection)

    assert registered == [
        ('date', convert_date),
        ('datetime', convert_datetime),
        ]
    assert connection.executed == ['select 1']


def test_convert_date_drops_the_time_component():
    """Verify convert_date() returns a date, not the parsed datetime.

    Mutation: dropping the trailing .date() in convert_date.
    Oracle: date(2023, 5, 15) by exact type; datetime subclasses date.
    """
    result = convert_date(b'2023-05-15 14:30:45')

    assert result == datetime.date(2023, 5, 15)
    assert type(result) is datetime.date


def test_convert_datetime_keeps_offset_and_microseconds():
    """Verify convert_datetime() parses full ISO 8601, not a fixed format.

    Mutation: a fixed strptime format in place of isoparse.
    Oracle: hand-computed datetimes, one with a +02:00 offset.
    """
    offset = datetime.timezone(datetime.timedelta(hours=2))

    assert convert_datetime(b'2023-05-15T14:30:45.123456+02:00') == \
        datetime.datetime(2023, 5, 15, 14, 30, 45, 123456, tzinfo=offset)
    assert convert_datetime(b'2023-05-15 14:30:45') == \
        datetime.datetime(2023, 5, 15, 14, 30, 45)
    assert convert_datetime(b'2023-05-15T14:30:45.123456').microsecond == 123456


def test_sqlite_date_converter_runs_once_per_fetched_value():
    """Verify a date column converts once outbound and never inbound.

    Mutation: dropping detect_types from get_engine_kwargs.
    Oracle: a spy on convert_date: no call on insert, one on fetch.
    """
    calls = []

    def counting_convert_date(raw):
        calls.append(raw)
        return convert_date(raw)

    original = sqlite_strategy.convert_date
    sqlite_strategy.convert_date = counting_convert_date
    try:
        cn = db.connect({'drivername': 'sqlite', 'database': ':memory:'})
        db.execute(cn, CREATE_PROBE_TABLE)
        db.execute(
            cn,
            'insert into conv_probe (d) values (?)',
            datetime.date(2023, 5, 15))

        assert calls == []

        value = db.select_scalar(cn, 'select d from conv_probe')
        cn.close()
    finally:
        sqlite_strategy.convert_date = original
        sqlite3.register_converter('date', convert_date)

    assert calls == [b'2023-05-15']
    assert value == datetime.date(2023, 5, 15)
    assert type(value) is datetime.date


def test_sqlite_bind_nulls_only_the_empty_string(sqlite_conn):
    """Verify inbound conversion runs at bind time, on '' only.

    Mutation: dropping convert_params from Cursor.execute.
    Oracle: '' lands as None and 'null' lands as text.
    """
    db.execute(
        sqlite_conn,
        'insert into conv_probe (label, txt) values (?, ?)',
        'empty',
        '')
    db.execute(
        sqlite_conn,
        'insert into conv_probe (label, txt) values (?, ?)',
        'spelled',
        'null')

    assert db.select_column(
        sqlite_conn,
        'select txt from conv_probe order by label') == [None, 'null']


def test_datetime_converter_round_trip(sqlite_conn):
    """Verify a DATETIME column round-trips as datetime.datetime.

    Mutation: register_converter('datetime', convert_date).
    Oracle: a datetime with microseconds, which a date cannot carry.
    """
    db.execute(
        sqlite_conn,
        'insert into conv_probe (label, ts) values (?, ?)',
        'dt',
        datetime.datetime(2023, 5, 15, 14, 30, 45, 123456))

    value = db.select_scalar(
        sqlite_conn,
        'select ts from conv_probe where label = ?',
        'dt')

    assert value == datetime.datetime(2023, 5, 15, 14, 30, 45, 123456)
    assert type(value) is datetime.datetime


def test_sqlite_timestamp_column_reads_through_iso_parser():
    """Verify a TIMESTAMP column parses full ISO 8601, offset and date-only.

    Mutation: dropping the 'timestamp' convert_datetime registration.
    Oracle: hand-computed datetimes, one with a +02:00 offset.
    """
    cn = db.connect({'drivername': 'sqlite', 'database': ':memory:'})
    db.execute(cn, 'create table ts_probe (label text, ts timestamp)')
    db.execute(
        cn,
        'insert into ts_probe values (?, ?), (?, ?)',
        'offset',
        '2023-05-15T14:30:45+02:00',
        'date_only',
        '2023-05-15')

    values = db.select_column(cn, 'select ts from ts_probe order by label')
    cn.close()

    offset = datetime.timezone(datetime.timedelta(hours=2))
    assert values == [
        datetime.datetime(2023, 5, 15),
        datetime.datetime(2023, 5, 15, 14, 30, 45, tzinfo=offset),
        ]


def test_sqlite_binds_dates_as_iso_text_without_stdlib_adapters(sqlite_conn):
    """Verify bound dates store as stdlib-format ISO text via own adapters.

    Mutation: isoformat() for isoformat(' '), or an adapter dropped.
    Oracle: hand-written ISO text, and the stdlib adapters' module.
    """
    db.execute(
        sqlite_conn,
        'insert into conv_probe (label, txt) values (?, ?), (?, ?)',
        'a',
        datetime.datetime(2023, 5, 15, 14, 30, 45, 123456),
        'b',
        datetime.date(2023, 5, 15))

    assert db.select_column(
        sqlite_conn,
        'select txt from conv_probe order by label') == [
        '2023-05-15 14:30:45.123456',
        '2023-05-15',
        ]
    for bound_type in (datetime.date, datetime.datetime):
        adapter = sqlite3.adapters[(bound_type, sqlite3.PrepareProtocol)]
        assert getattr(adapter, '__module__', None) != 'sqlite3.dbapi2', bound_type


def test_sqlite_insert_rows_nulls_special_strings(sqlite_conn):
    """Verify insert_rows stores a spelled null as None.

    Mutation: dropping null_special_string from insert_rows.
    Oracle: 'null' lands as None and '0' lands unchanged.
    """
    db.insert_rows(
        sqlite_conn,
        'conv_probe',
        [{'label': 'a', 'txt': 'null'}, {'label': 'b', 'txt': '0'}])

    assert db.select_column(
        sqlite_conn,
        'select txt from conv_probe order by label') == [None, '0']


def test_sqlite_list_adapter_binds_as_json(sqlite_conn):
    """Verify a list value is stored as JSON text via JsonBindingCursor.

    Mutation: dropping factory=JsonBindingCursor from create_dict_cursor.
    Oracle: hand-written '[1, 2, 3]'.
    """
    db.execute(
        sqlite_conn,
        'insert into conv_probe (label, txt) values (?, ?)',
        'listval',
        [1, 2, 3])

    result = db.select_scalar(
        sqlite_conn,
        'select txt from conv_probe where label = ?',
        'listval')

    assert result == '[1, 2, 3]'


def test_sqlite_named_and_batch_binds_encode_json(sqlite_conn):
    """Verify named and executemany binds store a dict as JSON text.

    Mutation: dropping JsonBindingCursor's dict branch or its executemany.
    Oracle: hand-written JSON text for each bound dict.
    """
    db.execute(
        sqlite_conn,
        'insert into conv_probe (label, txt) values (%(label)s, %(txt)s)',
        {'label': 'named', 'txt': {'b': 2}})
    db.insert_rows(
        sqlite_conn,
        'conv_probe',
        [{'label': 'many', 'txt': {'c': [3]}}])

    assert db.select_column(
        sqlite_conn,
        'select txt from conv_probe order by label') == [
        '{"c": [3]}',
        '{"b": 2}',
        ]


def test_sqlite_json_binding_leaves_raw_sqlite3_connections_alone(sqlite_conn):
    """Verify a package connect leaves dict and list unbindable on raw sqlite3.

    Mutation: a global sqlite3.register_adapter(dict or list, json.dumps).
    Oracle: the stdlib's ProgrammingError on a raw sqlite3 connection.
    """
    db.execute(
        sqlite_conn,
        'insert into conv_probe (label, txt) values (?, ?), (?, ?)',
        'dict',
        {'a': 1},
        'list',
        [1])

    raw = sqlite3.connect(':memory:')
    try:
        for value in ({'a': 1}, [1]):
            with pytest.raises(sqlite3.ProgrammingError):
                raw.execute('select ?', (value,))
    finally:
        raw.close()


if __name__ == '__main__':
    __import__('pytest').main([__file__])
