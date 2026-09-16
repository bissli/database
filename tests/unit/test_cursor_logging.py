"""Unit tests for the dumpsql decorator and the rest of cursor logging.

dumpsql wraps every cursor.execute / executemany call. f-string
interpolation evaluates unconditionally, which burns CPU when DEBUG is
off and forces __repr__ on every parameter even when no handler will
emit. Lazy %-format defers both until a handler wants the message.

The tests below pin the formatted message content, the raw LogRecord
shape that proves the lazy form survives, and the branches whose only
visible evidence is a log record.
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
    """Factory for a Cursor whose driver cursor and connection are stubs.

    Pass statusmessage=None for a driver cursor that lacks the psycopg
    statusmessage attribute, as sqlite3 does.
    Pass in_transaction=True to simulate an open transaction, which
    suppresses the auto-commit guard.
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

    Mutation: f-string interpolation in place of the lazy
        logger.debug call on the non-many branch of dumpsql.
    Oracle: hand-written template, arg tuple, and formatted message.
    """
    cursor = make_cursor()
    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        cursor.execute('SELECT %s FROM t', 42)

    rec = _one_record(caplog, 'args:')
    assert rec.msg == 'SQL:\n%s\nargs: %s'
    assert rec.args == ('SELECT %s FROM t', (42,))
    assert rec.getMessage() == 'SQL:\nSELECT %s FROM t\nargs: (42,)'
    assert rec.levelno == logging.DEBUG


def test_execute_defers_arg_repr_until_a_handler_formats(make_cursor, caplog):
    """Verify parameter __repr__ runs only when a record is formatted.

    Mutation: f-string interpolation in place of the lazy
        logger.debug call on the non-many branch of dumpsql.
    Oracle: a __repr__ counting spy - zero calls with DEBUG off, and a
        further call once the captured record is formatted.
    """
    cursor = make_cursor()
    spy = _ReprCounter()

    with caplog.at_level(logging.WARNING, logger=CURSOR_LOGGER):
        cursor.execute('SELECT %s', spy)

    assert spy.calls == 0
    assert cursor.dbapi_cursor.execute.call_count == 1
    assert _records(caplog) == []

    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        cursor.execute('SELECT %s', spy)

    quiet = spy.calls
    rec = [r for r in _records(caplog) if str(r.msg).startswith('SQL:')][0]
    assert rec.getMessage() == 'SQL:\nSELECT %s\nargs: (<param>,)'
    assert spy.calls > quiet


def test_execute_error_path_defers_arg_repr(make_cursor, caplog):
    """Verify the failure log also defers parameter __repr__.

    Mutation: f-string interpolation in the except branch of dumpsql,
        where logger.error('Error with query:...', operation, args) is.
    Oracle: a __repr__ counting spy at CRITICAL, where no handler can
        emit the ERROR record.
    """
    cursor = make_cursor(execute_error=RuntimeError('boom'))
    spy = _ReprCounter()

    with caplog.at_level(logging.CRITICAL, logger=CURSOR_LOGGER):
        with pytest.raises(RuntimeError):
            cursor.execute('SELECT %s', spy)

    assert spy.calls == 0
    assert cursor.connwrapper._addcall.call_count == 1


def test_executemany_logs_row_count_not_row_reprs(make_cursor, caplog):
    """Verify the executemany record reports a row count, not the rows.

    Mutation: len(args) in place of len(args[0]) for row_count in
        dumpsql, which reports 1 for any batch.
    Oracle: hand-counted three rows against the formatted message.
    """
    cursor = make_cursor()
    rows = [(1,), (2,), (3,)]
    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        cursor.executemany('INSERT INTO t VALUES (%s)', rows)

    rec = _one_record(caplog, 'params:')
    assert rec.msg == 'SQL:\n%s\nparams: %d rows'
    assert rec.args == ('INSERT INTO t VALUES (%s)', 3)
    assert rec.getMessage() == 'SQL:\nINSERT INTO t VALUES (%s)\nparams: 3 rows'


def test_executemany_empty_sequence_warns_and_skips_driver(make_cursor, caplog):
    """Verify an empty batch warns, returns 0, and never hits the driver.

    Mutation: dropping the `if not seq_of_parameters` guard in
        Cursor.executemany, which then calls the driver with [] and
        returns the stale cursor rowcount.
    Oracle: rowcount 1 on the stub driver versus the 0 the guard owes.
    """
    cursor = make_cursor(rowcount=1)
    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        result = cursor.executemany('INSERT INTO t VALUES (%s)', [])

    assert result == 0
    cursor.dbapi_cursor.executemany.assert_not_called()

    warn = _one_record(caplog, 'no parameter sequences')
    assert warn.levelno == logging.WARNING
    assert warn.getMessage() == 'executemany called with no parameter sequences'
    assert _one_record(caplog, 'params:').args == ('INSERT INTO t VALUES (%s)', 0)


def test_execute_error_logs_query_label_and_reraises(make_cursor, caplog):
    """Verify a failed execute logs the query template and re-raises.

    Mutation: dropping the bare `raise` from the except branch of
        dumpsql, or letting the is_many branch write that message.
    Oracle: hand-written template and arg tuple on the ERROR record.
    """
    cursor = make_cursor(execute_error=ValueError('nope'))
    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        with pytest.raises(ValueError, match='nope'):
            cursor.execute('DELETE FROM t WHERE id = %s', 7)

    rec = _one_record(caplog, 'Error with query')
    assert rec.levelno == logging.ERROR
    assert rec.msg == 'Error with query:\nSQL:\n%s\nargs: %s'
    assert rec.args == ('DELETE FROM t WHERE id = %s', (7,))


def test_executemany_error_logs_its_own_label_and_reraises(make_cursor, caplog):
    """Verify a failed executemany logs the executemany template.

    Mutation: flipping `if is_many:` in the except branch of dumpsql,
        which would log the query template and the parameter rows.
    Oracle: hand-written template and single-element arg tuple.
    """
    cursor = make_cursor(execute_error=ValueError('nope'))
    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        with pytest.raises(ValueError, match='nope'):
            cursor.executemany('INSERT INTO t VALUES (%s)', [(1,)])

    rec = _one_record(caplog, 'Error with executemany')
    assert rec.levelno == logging.ERROR
    assert rec.msg == 'Error with executemany:\nSQL:\n%s'
    assert rec.args == ('INSERT INTO t VALUES (%s)',)


def test_timing_uses_the_elapsed_delta(make_cursor, caplog, fake_clock):
    """Verify the timer reports stop minus start at four decimals.

    Mutation: dropping `- start` from the elapsed computation in the
        finally branch of dumpsql.
    Oracle: a scripted clock, 1000.25 - 1000.00 = 0.25 -> '0.2500s'.
    """
    fake_clock(1000.0, 1000.25)
    cursor = make_cursor()
    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        cursor.execute('SELECT 1')

    cursor.connwrapper._addcall.assert_called_once_with(pytest.approx(0.25))
    rec = _one_record(caplog, 'time:')
    assert rec.getMessage() == 'Query time: 0.2500s'


def test_addcall_records_time_when_query_raises(make_cursor, caplog, fake_clock):
    """Verify call statistics are recorded even on a failed query.

    Mutation: moving self.connwrapper._addcall(elapsed) out of the
        finally branch of dumpsql into the try branch.
    Oracle: a scripted clock, 20.5 - 20.0 = 0.5 on the raising path.
    """
    fake_clock(20.0, 20.5)
    cursor = make_cursor(execute_error=RuntimeError('boom'))
    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        with pytest.raises(RuntimeError):
            cursor.execute('SELECT 1')

    cursor.connwrapper._addcall.assert_called_once_with(pytest.approx(0.5))
    assert _one_record(caplog, 'time:').getMessage() == 'Query time: 0.5000s'


def test_timing_label_follows_is_many(make_cursor, caplog):
    """Verify the label follows is_many, not the reverse.

    Mutation: flipping the ternary that sets label in dumpsql, so
        'Executemany' labels single queries.
    Oracle: differential - the same assertion run through both entry
        points, which must disagree.
    """
    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        make_cursor().execute('SELECT 1')
        one = _one_record(caplog, 'time:').args[0]
        caplog.clear()
        make_cursor().executemany('INSERT INTO t VALUES (%s)', [(1,)])
        many = _one_record(caplog, 'time:').args[0]

    assert one == 'Query'
    assert many == 'Executemany'


def test_status_message_logged_when_the_driver_exposes_it(make_cursor, caplog):
    """Verify the driver status line is logged with the query label.

    Mutation: logging self.dbapi_cursor.rowcount in place of
        statusmessage in dumpsql.
    Oracle: hand-written template and args against a driver whose
        statusmessage and rowcount differ.
    """
    cursor = make_cursor(statusmessage='INSERT 0 3', rowcount=3)
    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        cursor.execute('SELECT 1')

    rec = _one_record(caplog, 'result:')
    assert rec.msg == '%s result: %s'
    assert rec.args == ('Query', 'INSERT 0 3')


def test_status_message_skipped_when_the_driver_lacks_it(make_cursor, caplog):
    """Verify a driver cursor without statusmessage still executes.

    Mutation: dropping the hasattr(self.dbapi_cursor, 'statusmessage')
        guard in dumpsql, which raises AttributeError on sqlite3.
    Oracle: a spec'd driver cursor that has no statusmessage, and the
        absence of any 'result:' record.
    """
    cursor = make_cursor(statusmessage=None, rowcount=4)
    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        assert cursor.execute('SELECT 1') == 4

    assert not [r for r in _records(caplog) if 'result:' in r.getMessage()]


def test_no_placeholder_branch_logs_and_drops_args(make_cursor, caplog):
    """Verify args are dropped only when the SQL has no placeholder.

    Mutation: dropping `not` from `if args and not has_placeholders(sql)`
        in Cursor._execute_query, which would strip the parameters off
        every parameterized statement.
    Oracle: differential - the real has_placeholders over 'SELECT 1'
        versus 'SELECT %s', with the branch marker record as the spy.
    """
    bare = make_cursor()
    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        bare.execute('SELECT 1', 99)

    bare.dbapi_cursor.execute.assert_called_once_with('SELECT 1')
    assert _one_record(caplog, 'without placeholders').getMessage() == \
        'Executed query without placeholders (ignoring args)'

    caplog.clear()
    bound = make_cursor()
    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        bound.execute('SELECT %s', 99)

    bound.dbapi_cursor.execute.assert_called_once_with('SELECT %s', (99,))
    assert not [r for r in _records(caplog)
                if 'without placeholders' in r.getMessage()]


def test_batching_splits_only_above_batch_size(make_cursor, caplog):
    """Verify the batch split boundary and the summed rowcount.

    Mutation: `<` in place of `<=` in the batch-size test in
        Cursor.executemany, an off-by-one that batches an exact fit.
    Oracle: absence of a 'Batching' record with 2 rows at batch_size 2
        (the exact-fit case) - that record appears under the mutant -
        plus hand-computed chunks [[(1,), (2,)], [(3,)]] and summed
        rowcount 2.
    """
    exact = make_cursor(rowcount=1)
    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        result = exact.executemany(
            'INSERT INTO t VALUES (%s)', [(1,), (2,)], batch_size=2)
    assert result == 1

    assert exact.dbapi_cursor.executemany.call_count == 1
    assert not [r for r in _records(caplog) if 'Batching' in r.getMessage()]

    caplog.clear()
    split = make_cursor(rowcount=1)
    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        result = split.executemany(
            'INSERT INTO t VALUES (%s)', [(1,), (2,), (3,)], batch_size=2)
    assert result == 2

    chunks = [c.args[1] for c in split.dbapi_cursor.executemany.call_args_list]
    assert chunks == [[(1,), (2,)], [(3,)]]
    assert _one_record(caplog, 'Batching').getMessage() == \
        'Batching 3 rows into chunks of 2'


def test_logged_sql_is_caller_not_standardized(make_cursor, caplog):
    """Verify caller SQL appears in the log; the driver gets the rewritten form.

    Mutation: dropping `operation = self.strategy.standardize_sql(...)`
        from Cursor.execute, which sends the raw text to the driver.
    Oracle: a strategy stub that rewrites '?' to '%s', so the two texts
        cannot be confused.
    """
    cursor = make_cursor()
    cursor._strategy.standardize_sql.side_effect = \
        lambda sql: sql.replace('?', '%s')

    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        cursor.execute('SELECT ?', 5)

    assert _one_record(caplog, 'args:').args == ('SELECT ?', (5,))
    cursor.dbapi_cursor.execute.assert_called_once_with('SELECT %s', (5,))


def test_dumpsql_preserves_the_wrapped_method_identity():
    """Verify the decorator keeps the wrapped method's name and doc.

    Mutation: dropping @wraps(func) from the dumpsql wrapper, which
        renames every decorated method to 'wrapper' and clears its
        docstring.
    Oracle: the wrapped method's __name__ is the method name and
        __doc__ is not None (without @wraps, wrapper.__doc__ is None).
    """
    assert Cursor.execute.__name__ == 'execute'
    assert Cursor.execute.__doc__ is not None
    assert Cursor.executemany.__name__ == 'executemany'
    assert Cursor.executemany.__doc__ is not None


def test_execute_auto_commit_guard(make_cursor):
    """Verify ensure_commit fires when not in a transaction, not inside one.

    Mutation: dropping `not` from the auto-commit guard in Cursor.execute
        (cursor.py:167), which commits inside an open transaction.
    Oracle: connwrapper.commit spy - called once with in_transaction False,
        never called with in_transaction True.
    """
    outer = make_cursor(in_transaction=False)
    outer.execute('SELECT 1')
    outer.connwrapper.commit.assert_called_once_with()

    inner = make_cursor(in_transaction=True)
    inner.execute('SELECT 1')
    inner.connwrapper.commit.assert_not_called()


def test_executemany_auto_commit_guard(make_cursor):
    """Verify ensure_commit fires when not in a transaction, not inside one.

    Mutation: dropping `not` from the auto-commit guard in
        Cursor.executemany (cursor.py:279), which commits inside an open
        transaction.
    Oracle: connwrapper.commit spy - called once with in_transaction False,
        never called with in_transaction True.
    """
    outer = make_cursor(in_transaction=False)
    outer.executemany('INSERT INTO t VALUES (%s)', [(1,)])
    outer.connwrapper.commit.assert_called_once_with()

    inner = make_cursor(in_transaction=True)
    inner.executemany('INSERT INTO t VALUES (%s)', [(1,)])
    inner.connwrapper.commit.assert_not_called()


def test_batching_record_keeps_percent_format(make_cursor, caplog):
    """Verify the batching record carries the template plus raw counts.

    Mutation: the eager f-string f'Batching {len(seq_of_parameters)}
        rows into chunks of {batch_size}' in place of the lazy
        logger.debug call in Cursor.executemany.
    Oracle: the LogRecord itself - msg still holds both '%d' markers
        and args is the unformatted (3, 2); an f-string pre-renders the
        message and leaves args empty.
    """
    cursor = make_cursor(rowcount=1)
    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        cursor.executemany(
            'INSERT INTO t VALUES (%s)', [(1,), (2,), (3,)], batch_size=2)

    rec = _one_record(caplog, 'Batching')
    assert rec.msg == 'Batching %d rows into chunks of %d'
    assert rec.args == (3, 2)
    assert rec.getMessage() == 'Batching 3 rows into chunks of 2'


def test_executemany_counts_rows_passed_by_keyword(make_cursor, caplog):
    """Verify the row count also reads a keyword seq_of_parameters.

    Mutation: row_count = len(args[0]) if args else 0 in the is_many
        branch of dumpsql, dropping the kwargs.get('seq_of_parameters')
        fallback.
    Oracle: hand-counted four rows against the record args, where the
        mutant reports 0 rows for the same call.
    """
    cursor = make_cursor(rowcount=4)
    rows = [(1,), (2,), (3,), (4,)]
    with caplog.at_level(logging.DEBUG, logger=CURSOR_LOGGER):
        result = cursor.executemany(
            'INSERT INTO t VALUES (%s)', seq_of_parameters=rows)

    assert result == 4
    cursor.dbapi_cursor.executemany.assert_called_once_with(
        'INSERT INTO t VALUES (%s)', rows)

    rec = _one_record(caplog, 'params:')
    assert rec.args == ('INSERT INTO t VALUES (%s)', 4)
    assert rec.getMessage() == \
        'SQL:\nINSERT INTO t VALUES (%s)\nparams: 4 rows'


if __name__ == '__main__':
    __import__('pytest').main([__file__])
