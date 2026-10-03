"""Cursor logging: the dumpsql records and branches seen only in logs.
"""
import logging

import pytest
from database.cursor import Cursor

CURSOR_LOGGER = 'database.cursor'


class _ReprCounter:
    """Parameter stand-in that counts how often __repr__ runs.
    """

    def __init__(self):
        self.calls = 0

    def __repr__(self):
        self.calls += 1
        return '<param>'


def _records(caplog):
    """Every LogRecord emitted by the cursor logger, in order.
    """
    return [r for r in caplog.records if r.name == CURSOR_LOGGER]


def _one_record(caplog, needle):
    """The single cursor record whose formatted message holds needle.
    """
    hits = [r for r in _records(caplog) if needle in r.getMessage()]
    assert len(hits) == 1, f'want 1 record holding {needle!r}, got {len(hits)}'
    return hits[0]


@pytest.fixture
def make_cursor(mocker):
    """Cursor factory over stubs; statusmessage=None mimics a sqlite3 cursor.
    """
    def factory(
            statusmessage='SELECT 1',
            rowcount=1,
            execute_error=None,
            in_transaction=False):
        if statusmessage is None:
            dbapi = mocker.Mock(spec=['execute', 'executemany', 'rowcount'])
        else:
            dbapi = mocker.Mock()
            dbapi.statusmessage = statusmessage
        dbapi.rowcount = rowcount
        if execute_error is not None:
            dbapi.execute.side_effect = execute_error
            dbapi.executemany.side_effect = execute_error

        connwrapper = mocker.Mock()
        connwrapper.in_transaction = in_transaction
        connwrapper.readonly = False

        strategy = mocker.Mock()
        strategy.standardize_sql.side_effect = lambda sql: sql
        strategy.get_placeholder_style.return_value = '%s'

        return Cursor(dbapi, connwrapper, strategy)

    return factory


@pytest.fixture
def fake_clock(mocker):
    """Install a scripted perf_counter tick sequence on database.cursor.
    """
    def install(*ticks):
        remaining = list(ticks)
        mocker.patch(
            'database.cursor.time.perf_counter',
            side_effect=lambda: remaining.pop(0) if remaining else ticks[-1])

    return install


def test_execute_debug_record_keeps_percent_format(make_cursor, caplog):
    """Verify the query record carries the template plus raw args.

    Mutation: f-string in place of the lazy query logger.debug in dumpsql.
    Oracle: hand-written template, arg tuple, and formatted message.
    """
    cursor = make_cursor()
    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        cursor.execute('select %s from t', 42)

    rec = _one_record(caplog, 'args:')
    assert rec.msg == 'SQL:\n%s\nargs: %s'
    assert rec.args == ('select %s from t', (42,))
    assert rec.getMessage() == 'SQL:\nselect %s from t\nargs: (42,)'
    assert rec.levelno == logging.DEBUG


def test_execute_defers_arg_repr_until_a_handler_formats(make_cursor, caplog):
    """Verify parameter __repr__ runs only when a record is formatted.

    Mutation: f-string in place of the lazy query logger.debug in dumpsql.
    Oracle: a __repr__ counting spy at WARNING, then at DEBUG.
    """
    cursor = make_cursor()
    spy = _ReprCounter()

    with caplog.at_level(logging.WARNING, logger=CURSOR_LOGGER):
        cursor.execute('select %s', spy)

    assert spy.calls == 0
    assert cursor.dbapi_cursor.execute.call_count == 1
    assert _records(caplog) == []

    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        cursor.execute('select %s', spy)

    quiet = spy.calls
    rec = [r for r in _records(caplog) if str(r.msg).startswith('SQL:')][0]
    assert rec.getMessage() == 'SQL:\nselect %s\nargs: (<param>,)'
    assert spy.calls > quiet


def test_execute_error_path_defers_arg_repr(make_cursor, caplog):
    """Verify the failure log also defers parameter __repr__.

    Mutation: f-string in the logger.error of dumpsql's except branch.
    Oracle: a __repr__ counting spy at CRITICAL.
    """
    cursor = make_cursor(execute_error=RuntimeError('boom'))
    spy = _ReprCounter()

    with caplog.at_level(logging.CRITICAL, logger=CURSOR_LOGGER):
        with pytest.raises(RuntimeError):
            cursor.execute('select %s', spy)

    assert spy.calls == 0
    assert cursor.connwrapper._addcall.call_count == 1


def test_executemany_logs_row_count_not_row_reprs(make_cursor, caplog):
    """Verify the executemany record reports a row count in place of rows.

    Mutation: len(args) in place of len(args[0]) for row_count in dumpsql.
    Oracle: hand-counted three rows.
    """
    cursor = make_cursor()
    rows = [(1,), (2,), (3,)]
    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        cursor.executemany('insert into t values (%s)', rows)

    rec = _one_record(caplog, 'params:')
    assert rec.msg == 'SQL:\n%s\nparams: %d rows'
    assert rec.args == ('insert into t values (%s)', 3)
    assert rec.getMessage() == 'SQL:\ninsert into t values (%s)\nparams: 3 rows'


def test_executemany_empty_sequence_warns_and_skips_driver(make_cursor, caplog):
    """Verify an empty batch warns, returns 0, and never hits the driver.

    Mutation: dropping the empty-sequence guard in Cursor.executemany.
    Oracle: stub rowcount 1 against the 0 the guard returns.
    """
    cursor = make_cursor(rowcount=1)
    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        result = cursor.executemany('insert into t values (%s)', [])

    assert result == 0
    cursor.dbapi_cursor.executemany.assert_not_called()

    warn = _one_record(caplog, 'no parameter sequences')
    assert warn.levelno == logging.WARNING
    assert warn.getMessage() == 'executemany called with no parameter sequences'
    assert _one_record(caplog, 'params:').args == ('insert into t values (%s)', 0)


def test_execute_error_logs_query_label_and_reraises(make_cursor, caplog):
    """Verify a failed execute logs the query template and re-raises.

    Mutation: dropping dumpsql's bare raise, or taking the is_many branch.
    Oracle: hand-written template and arg tuple on the ERROR record.
    """
    cursor = make_cursor(execute_error=ValueError('nope'))
    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        with pytest.raises(ValueError, match='nope'):
            cursor.execute('delete from t where id = %s', 7)

    rec = _one_record(caplog, 'Error with query')
    assert rec.levelno == logging.ERROR
    assert rec.msg == 'Error with query:\nSQL:\n%s\nargs: %s'
    assert rec.args == ('delete from t where id = %s', (7,))


def test_executemany_error_logs_its_own_label_and_reraises(make_cursor, caplog):
    """Verify a failed executemany logs the executemany template.

    Mutation: flipping `if is_many:` in dumpsql's except branch.
    Oracle: hand-written template and single-element arg tuple.
    """
    cursor = make_cursor(execute_error=ValueError('nope'))
    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        with pytest.raises(ValueError, match='nope'):
            cursor.executemany('insert into t values (%s)', [(1,)])

    rec = _one_record(caplog, 'Error with executemany')
    assert rec.levelno == logging.ERROR
    assert rec.msg == 'Error with executemany:\nSQL:\n%s'
    assert rec.args == ('insert into t values (%s)',)


@pytest.mark.parametrize(('method', 'call_args', 'needle'), [
    ('execute', ('delete from t where id = %s', 7), 'Error with query'),
    ('executemany', ('insert into t values (%s)', [(1,)]), 'Error with executemany'),
    ])
def test_error_record_carries_the_exception(
        make_cursor, caplog, method, call_args, needle):
    """Verify a failed call's ERROR record carries the raised exception.

    Mutation: dropping exc_info=True from either logger.error in dumpsql.
    Oracle: the exception instance handed to the stub driver.
    """
    error = ValueError('nope')
    cursor = make_cursor(execute_error=error)
    with caplog.at_level(logging.ERROR, logger=CURSOR_LOGGER):
        with pytest.raises(ValueError, match='nope'):
            getattr(cursor, method)(*call_args)

    rec = _one_record(caplog, needle)
    assert rec.exc_info is not None
    assert rec.exc_info[1] is error


def test_timing_uses_the_elapsed_delta(make_cursor, caplog, fake_clock):
    """Verify the timer reports stop minus start at four decimals.

    Mutation: dropping `- start` from the elapsed time in dumpsql.
    Oracle: scripted clock, 1000.25 - 1000.00 -> '0.2500s'.
    """
    fake_clock(1000.0, 1000.25)
    cursor = make_cursor()
    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        cursor.execute('select 1')

    cursor.connwrapper._addcall.assert_called_once_with(pytest.approx(0.25))
    rec = _one_record(caplog, 'time:')
    assert rec.getMessage() == 'Query time: 0.2500s'


def test_addcall_records_time_when_query_raises(make_cursor, caplog, fake_clock):
    """Verify call statistics are recorded even on a failed query.

    Mutation: _addcall moved from dumpsql's finally branch into try.
    Oracle: scripted clock, 20.5 - 20.0 = 0.5 on the raising path.
    """
    fake_clock(20.0, 20.5)
    cursor = make_cursor(execute_error=RuntimeError('boom'))
    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        with pytest.raises(RuntimeError):
            cursor.execute('select 1')

    cursor.connwrapper._addcall.assert_called_once_with(pytest.approx(0.5))
    assert _one_record(caplog, 'time:').getMessage() == 'Query time: 0.5000s'


def test_timing_label_follows_is_many(make_cursor, caplog):
    """Verify execute times as 'Query' and executemany as 'Executemany'.

    Mutation: flipping the ternary that sets label in dumpsql.
    Oracle: literal 'Query' and 'Executemany' through both entry points.
    """
    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        make_cursor().execute('select 1')
        one = _one_record(caplog, 'time:').args[0]
        caplog.clear()
        make_cursor().executemany('insert into t values (%s)', [(1,)])
        many = _one_record(caplog, 'time:').args[0]

    assert one == 'Query'
    assert many == 'Executemany'


def test_status_message_logged_when_the_driver_exposes_it(make_cursor, caplog):
    """Verify the driver status line is logged with the query label.

    Mutation: rowcount logged in place of statusmessage in dumpsql.
    Oracle: a stub driver whose statusmessage and rowcount differ.
    """
    cursor = make_cursor(statusmessage='INSERT 0 3', rowcount=3)
    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        cursor.execute('select 1')

    rec = _one_record(caplog, 'result:')
    assert rec.msg == '%s result: %s'
    assert rec.args == ('Query', 'INSERT 0 3')


def test_status_message_skipped_when_the_driver_lacks_it(make_cursor, caplog):
    """Verify a driver cursor without statusmessage still executes.

    Mutation: dropping the hasattr statusmessage guard in dumpsql.
    Oracle: a spec'd driver cursor with no statusmessage attribute.
    """
    cursor = make_cursor(statusmessage=None, rowcount=4)
    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        assert cursor.execute('select 1') == 4

    assert not [r for r in _records(caplog) if 'result:' in r.getMessage()]


def test_no_placeholder_branch_logs_and_drops_args(make_cursor, caplog):
    """Verify args are dropped only when the SQL has no placeholder.

    Mutation: dropping `not` from the has_placeholders test in _execute_query.
    Oracle: 'select 1' against 'select %s', with the branch record as spy.
    """
    bare = make_cursor()
    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        bare.execute('select 1', 99)

    bare.dbapi_cursor.execute.assert_called_once_with('select 1')
    assert _one_record(caplog, 'without placeholders').getMessage() == \
        'Executed query without placeholders (ignoring args)'

    caplog.clear()
    bound = make_cursor()
    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        bound.execute('select %s', 99)

    bound.dbapi_cursor.execute.assert_called_once_with('select %s', (99,))
    assert not [r for r in _records(caplog)
                if 'without placeholders' in r.getMessage()]


def test_batching_splits_only_above_batch_size(make_cursor, caplog):
    """Verify the batch split boundary and the summed rowcount.

    Mutation: `<` in place of `<=` in Cursor.executemany's batch test.
    Oracle: an exact fit of 2 rows at batch_size 2, and hand-split chunks.
    """
    exact = make_cursor(rowcount=1)
    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        result = exact.executemany(
            'insert into t values (%s)', [(1,), (2,)], batch_size=2)
    assert result == 1

    assert exact.dbapi_cursor.executemany.call_count == 1
    assert not [r for r in _records(caplog) if 'Batching' in r.getMessage()]

    caplog.clear()
    split = make_cursor(rowcount=1)
    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        result = split.executemany(
            'insert into t values (%s)', [(1,), (2,), (3,)], batch_size=2)
    assert result == 2

    chunks = [c.args[1] for c in split.dbapi_cursor.executemany.call_args_list]
    assert chunks == [[(1,), (2,)], [(3,)]]
    assert _one_record(caplog, 'Batching').getMessage() == \
        'Batching 3 rows into chunks of 2'


def test_logged_sql_is_caller_not_standardized(make_cursor, caplog):
    """Verify the log shows caller SQL and the driver gets the rewrite.

    Mutation: dropping the standardize_sql call from Cursor.execute.
    Oracle: a strategy stub that rewrites '?' to '%s'.
    """
    cursor = make_cursor()
    cursor._strategy.standardize_sql.side_effect = \
        lambda sql: sql.replace('?', '%s')

    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        cursor.execute('select ?', 5)

    assert _one_record(caplog, 'args:').args == ('select ?', (5,))
    cursor.dbapi_cursor.execute.assert_called_once_with('select %s', (5,))


def test_dumpsql_preserves_the_wrapped_method_identity():
    """Verify the decorator keeps the wrapped method's name and doc.

    Mutation: dropping @wraps(func) from the dumpsql wrapper.
    Oracle: the method names; the bare wrapper's __doc__ is None.
    """
    assert Cursor.execute.__name__ == 'execute'
    assert Cursor.execute.__doc__ is not None
    assert Cursor.executemany.__name__ == 'executemany'
    assert Cursor.executemany.__doc__ is not None


@pytest.mark.parametrize(('method', 'call_args'), [
    ('execute', ('select 1',)),
    ('executemany', ('insert into t values (%s)', [(1,)])),
    ])
def test_auto_commit_only_outside_a_transaction(make_cursor, method, call_args):
    """Verify a call commits outside a transaction and never inside one.

    Mutation: dropping `not` from the auto-commit guard in Cursor.
    Oracle: dbapi_connection.commit spy, in_transaction False then True.
    """
    outer = make_cursor(in_transaction=False)
    getattr(outer, method)(*call_args)
    outer.connwrapper.dbapi_connection.commit.assert_called_once_with()

    inner = make_cursor(in_transaction=True)
    getattr(inner, method)(*call_args)
    inner.connwrapper.dbapi_connection.commit.assert_not_called()


def test_batching_record_keeps_percent_format(make_cursor, caplog):
    """Verify the batching record carries the template plus raw counts.

    Mutation: f-string in place of the lazy batching logger.debug.
    Oracle: the LogRecord msg and its raw args (3, 2).
    """
    cursor = make_cursor(rowcount=1)
    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        cursor.executemany(
            'insert into t values (%s)', [(1,), (2,), (3,)], batch_size=2)

    rec = _one_record(caplog, 'Batching')
    assert rec.msg == 'Batching %d rows into chunks of %d'
    assert rec.args == (3, 2)
    assert rec.getMessage() == 'Batching 3 rows into chunks of 2'


def test_executemany_counts_rows_passed_by_keyword(make_cursor, caplog):
    """Verify the row count also reads a keyword seq_of_parameters.

    Mutation: dropping the seq_of_parameters kwargs fallback in dumpsql.
    Oracle: hand-counted four rows; the mutant reports 0.
    """
    cursor = make_cursor(rowcount=4)
    rows = [(1,), (2,), (3,), (4,)]
    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        result = cursor.executemany(
            'insert into t values (%s)', seq_of_parameters=rows)

    assert result == 4
    cursor.dbapi_cursor.executemany.assert_called_once_with(
        'insert into t values (%s)', rows)

    rec = _one_record(caplog, 'params:')
    assert rec.args == ('insert into t values (%s)', 4)
    assert rec.getMessage() == \
        'SQL:\ninsert into t values (%s)\nparams: 4 rows'


if __name__ == '__main__':
    __import__('pytest').main([__file__])
