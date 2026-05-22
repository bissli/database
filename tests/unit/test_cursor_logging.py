"""Unit tests for the dumpsql decorator's logging behavior.

dumpsql wraps every cursor.execute / executemany call. f-string
interpolation evaluates unconditionally, which (a) burns CPU when DEBUG
is off and (b) forces __repr__ on every parameter even when no handler
will emit. Lazy %-format defers both until a handler actually wants the
message.
"""
import logging

import pytest


class _SideEffectRepr:
    """Instances whose __repr__ raises if called — proves lazy formatting."""

    def __repr__(self):
        raise AssertionError('__repr__ called — lazy logging is broken')


@pytest.fixture
def mock_cursor(mocker):
    """A Cursor wired with all I/O mocked. Cheap to construct."""
    from database.cursor import Cursor

    dbapi = mocker.Mock()
    dbapi.execute.return_value = None
    dbapi.executemany.return_value = None
    dbapi.rowcount = 1
    dbapi.statusmessage = 'SELECT 1'

    connwrapper = mocker.Mock()
    connwrapper.in_transaction = False
    connwrapper._addcall = mocker.Mock()

    strategy = mocker.Mock()
    strategy.standardize_sql.side_effect = lambda sql: sql
    strategy.get_placeholder_style.return_value = '%s'

    mocker.patch('database.cursor.TypeConverter.convert_params',
                 side_effect=lambda x: x)
    mocker.patch('database.cursor.ensure_commit')
    mocker.patch('database.cursor.has_placeholders', return_value=True)

    return Cursor(dbapi, connwrapper, strategy)


class TestDumpsqlLazyFormatting:
    """When DEBUG is suppressed, the decorator must not call __repr__ on
    args (proves lazy %-format).
    """

    def test_execute_does_not_call_repr_when_debug_off(self, mock_cursor, caplog):
        # caplog defaults to WARNING — DEBUG records dropped before handler.
        # If the decorator still uses f-strings, __repr__ fires and the test
        # raises.
        logging.getLogger('database.cursor').setLevel(logging.WARNING)
        try:
            mock_cursor.execute('SELECT %s', _SideEffectRepr())
        finally:
            logging.getLogger('database.cursor').setLevel(logging.NOTSET)

    def test_executemany_does_not_call_repr_when_debug_off(self, mock_cursor):
        logging.getLogger('database.cursor').setLevel(logging.WARNING)
        try:
            # executemany passes rows as args[0]; rows-repr should never fire
            # at WARNING level either.
            mock_cursor.executemany('INSERT INTO t VALUES (%s)',
                                    [(_SideEffectRepr(),)])
        finally:
            logging.getLogger('database.cursor').setLevel(logging.NOTSET)


class TestDumpsqlPercentFormatRecord:
    """At DEBUG, the LogRecord should use %-format (record.msg has
    placeholders, record.args holds the raw values) — that proves we
    switched from eager f-string to lazy %-format.
    """

    def test_execute_uses_percent_format_record(self, mock_cursor, caplog):
        with caplog.at_level(logging.DEBUG, logger='database.cursor'):
            mock_cursor.execute('SELECT %s', 42)

        sql_records = [r for r in caplog.records if 'SQL' in r.msg]
        assert sql_records, 'No SQL debug record emitted'
        rec = sql_records[0]
        assert '%s' in rec.msg, (
            f'record.msg has no %s placeholder — looks like eager '
            f'formatting: {rec.msg!r}'
        )
        assert rec.args is not None and rec.args, (
            f'record.args is empty — eager f-string detected: {rec!r}'
        )
