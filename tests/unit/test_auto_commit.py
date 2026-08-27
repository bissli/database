"""
Test auto-commit management and transaction lifecycle.
"""
import threading

import pytest
from database.transaction import Transaction, diagnose_connection
from database.transaction import disable_auto_commit, enable_auto_commit


class RecordingRawConnection:
    """Raw DBAPI-style connection that records commit/rollback order.

    Attributes
    ----------
    autocommit : bool
        Mode the auto-commit machinery toggles.
    calls : list[str]
        Names of the transaction methods called, in order.
    fail_on_commit : bool
        When True, commit() raises after recording the call.
    """

    def __init__(self) -> None:
        self.autocommit = True
        self.calls: list[str] = []
        self.fail_on_commit = False

    def commit(self) -> None:
        """Record the commit and optionally fail like a dead connection.
        """
        self.calls.append('commit')
        if self.fail_on_commit:
            raise RuntimeError('commit failed')

    def rollback(self) -> None:
        """Record the rollback.
        """
        self.calls.append('rollback')


class RecordingConnection:
    """Wrapper exposing a raw connection the way ConnectionWrapper does.

    Attributes
    ----------
    connection : RecordingRawConnection
        Inner connection the transaction is expected to commit.
    driver_connection : RecordingRawConnection
        Same object, reached by get_raw_connection().
    calls : list[str]
        Transaction methods called on the wrapper itself, which the
        source must never use.
    """

    def __init__(self) -> None:
        self.connection = RecordingRawConnection()
        self.driver_connection = self.connection
        self.in_transaction = False
        self.calls: list[str] = []

    def commit(self) -> None:
        """Record a commit aimed at the wrapper instead of the raw handle.
        """
        self.calls.append('commit')

    def rollback(self) -> None:
        """Record a rollback aimed at the wrapper instead of the raw handle.
        """
        self.calls.append('rollback')


class FlagRecordingConnection:
    """Wrapper recording every write to its in_transaction flag.

    Attributes
    ----------
    connection : RecordingRawConnection
        Inner connection the transaction commits or rolls back.
    driver_connection : RecordingRawConnection
        Same object, reached by get_raw_connection().
    flag_writes : list[bool]
        Values written to in_transaction, in order.
    """

    def __init__(self) -> None:
        self.connection = RecordingRawConnection()
        self.driver_connection = self.connection
        self.flag_writes: list[bool] = []
        self._in_transaction = False

    @property
    def in_transaction(self) -> bool:
        """Report the flag the transaction machinery maintains.
        """
        return self._in_transaction

    @in_transaction.setter
    def in_transaction(self, value: bool) -> None:
        self._in_transaction = value
        self.flag_writes.append(value)


class DriverOnlyConnection:
    """Wrapper carrying nothing but a driver connection.

    Attributes
    ----------
    driver_connection : object
        Raw handle returned by get_raw_connection().
    """

    def __init__(self, driver_connection) -> None:
        self.driver_connection = driver_connection


class DualModeRawConnection:
    """Raw connection exposing both autocommit and isolation_level.

    Attributes
    ----------
    autocommit : bool
        Preferred switch, tried first by _set_autocommit().
    isolation_level : str | None
        SQLite-style switch, reached only if autocommit is unusable.
    """

    def __init__(self) -> None:
        self.autocommit = False
        self.isolation_level = 'DEFERRED'


class IsolationOnlyRawConnection:
    """Raw connection exposing only isolation_level, like sqlite3.

    Attributes
    ----------
    isolation_level : str | None
        None means auto-commit, 'DEFERRED' means an explicit transaction.
    """

    def __init__(self) -> None:
        self.isolation_level = 'DEFERRED'


class RaisingExecutionOptionsConnection:
    """Wrapper whose execution_options rejects the isolation level.

    Attributes
    ----------
    driver_connection : DualModeRawConnection
        Raw handle the fallback must still reach.
    """

    def __init__(self, driver_connection) -> None:
        self.driver_connection = driver_connection

    def execution_options(self, **kwargs) -> None:
        """Reject the level the way the sqlite dialect does.
        """
        raise RuntimeError('isolation level not supported')


class LockedAutocommitRawConnection:
    """Raw connection whose autocommit setter always fails.

    Attributes
    ----------
    isolation_level : str | None
        Fallback switch reached after the autocommit setter raises.
    """

    def __init__(self) -> None:
        self.isolation_level = 'DEFERRED'

    @property
    def autocommit(self) -> bool:
        """Report the mode without allowing a change.
        """
        return False

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        raise RuntimeError('cannot set autocommit inside a transaction')


class RecordingCursor:
    """Cursor stub recording the SQL it received and replaying fixed rows.

    Attributes
    ----------
    rows : list[dict] | None
        Rows fetchall() returns; None makes fetchall() raise, as a
        cursor with no result set does.
    executed : tuple | None
        The (sql, args) pair passed to execute().
    """

    def __init__(self, rows) -> None:
        self.rows = rows
        self.executed = None

    def execute(self, sql, args) -> None:
        """Record the prepared statement and its arguments.
        """
        self.executed = (sql, args)

    def fetchall(self):
        """Return the configured rows or raise when there is no result set.
        """
        if self.rows is None:
            raise RuntimeError('no result set')
        return self.rows


class TestAutoCommit:
    """Auto-commit mode selection across the strategy and fallback paths."""

    def test_enable_auto_commit_delegates_to_strategy(self, mocker):
        """Verify enabling routes to strategy.enable_autocommit(raw_conn).

        Mutation: swapping the enable/disable arms of
        _try_strategy_autocommit, or handing it the wrapper instead of
        get_raw_connection(connection).
        Oracle: strategy spy showing which method ran and with what.
        """
        raw = DualModeRawConnection()
        conn = mocker.Mock()
        conn.dialect = 'postgresql'
        conn.driver_connection = raw

        strategy = mocker.Mock()
        mocker.patch('database.transaction.get_db_strategy', return_value=strategy)

        enable_auto_commit(conn)

        strategy.enable_autocommit.assert_called_once_with(raw)
        strategy.disable_autocommit.assert_not_called()

    def test_disable_auto_commit_delegates_to_strategy(self, mocker):
        """Verify disabling routes to strategy.disable_autocommit(raw_conn).

        Mutation: disable_auto_commit() passing enable=True to
        _set_autocommit, or the enable/disable arms being swapped.
        Oracle: strategy spy showing which method ran and with what.
        """
        raw = DualModeRawConnection()
        raw.autocommit = True
        conn = mocker.Mock()
        conn.dialect = 'postgresql'
        conn.driver_connection = raw

        strategy = mocker.Mock()
        mocker.patch('database.transaction.get_db_strategy', return_value=strategy)

        disable_auto_commit(conn)

        strategy.disable_autocommit.assert_called_once_with(raw)
        strategy.enable_autocommit.assert_not_called()

    def test_strategy_success_skips_the_fallback_switches(self, mocker):
        """Verify a successful strategy call ends _set_autocommit().

        Mutation: dropping the early return after
        _try_strategy_autocommit() succeeds, which would then also
        write raw_conn.autocommit and raw_conn.isolation_level.
        Oracle: raw connection left in its pre-call state by a strategy
        spy that touches nothing.
        """
        raw = DualModeRawConnection()
        conn = mocker.Mock()
        conn.dialect = 'sqlite'
        conn.driver_connection = raw

        mocker.patch(
            'database.transaction.get_db_strategy',
            return_value=mocker.Mock())

        enable_auto_commit(conn)

        assert raw.autocommit is False
        assert raw.isolation_level == 'DEFERRED'

    def test_execution_options_isolation_matches_mode(self, mocker):
        """Verify SQLAlchemy isolation is AUTOCOMMIT only when enabling.

        Mutation: swapping 'AUTOCOMMIT' and 'READ COMMITTED' in
        _set_autocommit().
        Oracle: hand-written isolation names per direction.
        """
        on = mocker.Mock(spec=['execution_options', 'driver_connection'])
        off = mocker.Mock(spec=['execution_options', 'driver_connection'])

        enable_auto_commit(on)
        disable_auto_commit(off)

        on.execution_options.assert_called_once_with(isolation_level='AUTOCOMMIT')
        off.execution_options.assert_called_once_with(
            isolation_level='READ COMMITTED')

    def test_strategy_failure_falls_back_to_autocommit_property(self, mocker):
        """Verify a raising strategy is swallowed and the fallback runs.

        Mutation: narrowing the except in _try_strategy_autocommit() so
        the strategy error escapes instead of returning False.
        Oracle: raw autocommit flipped by the fallback branch, proving
        control reached it.
        """
        raw = DualModeRawConnection()
        conn = DriverOnlyConnection(raw)
        conn.dialect = 'postgresql'

        mocker.patch(
            'database.transaction.get_db_strategy',
            side_effect=RuntimeError('no strategy'))

        enable_auto_commit(conn)

        assert raw.autocommit is True

    def test_fallback_autocommit_property_tracks_mode(self):
        """Verify the fallback writes the requested mode and stops there.

        Mutation: raw_conn.autocommit = True in place of
        raw_conn.autocommit = enable, or dropping the return that keeps
        the isolation_level branch from also firing.
        Oracle: hand-written expected value per direction plus an
        untouched isolation_level.
        """
        raw = DualModeRawConnection()
        conn = DriverOnlyConnection(raw)

        enable_auto_commit(conn)
        assert raw.autocommit is True
        assert raw.isolation_level == 'DEFERRED'

        disable_auto_commit(conn)
        assert raw.autocommit is False
        assert raw.isolation_level == 'DEFERRED'

    def test_execution_options_failure_still_reaches_the_fallback(self):
        """Verify a rejected isolation level does not abort the toggle.

        Mutation: narrowing or dropping the except around
        connection.execution_options() in _set_autocommit().
        Oracle: raw autocommit flipped to False by the later branch.
        """
        raw = DualModeRawConnection()
        raw.autocommit = True
        conn = RaisingExecutionOptionsConnection(raw)

        disable_auto_commit(conn)

        assert raw.autocommit is False

    def test_fallback_isolation_level_tracks_mode(self):
        """Verify a sqlite handle gets None to enable, DEFERRED to disable.

        Mutation: swapping the arms of
        level = None if enable else 'DEFERRED'.
        Oracle: hand-written sqlite3 isolation values per direction.
        """
        raw = IsolationOnlyRawConnection()
        conn = DriverOnlyConnection(raw)

        enable_auto_commit(conn)
        assert raw.isolation_level is None

        disable_auto_commit(conn)
        assert raw.isolation_level == 'DEFERRED'

    def test_isolation_level_used_when_autocommit_setter_raises(self):
        """Verify a rejected autocommit write falls through to isolation_level.

        Mutation: narrowing the except around raw_conn.autocommit =
        enable, which would let the setter error escape and never reach
        the isolation_level branch.
        Oracle: isolation_level moved to None by the later branch.
        """
        raw = LockedAutocommitRawConnection()
        conn = DriverOnlyConnection(raw)

        enable_auto_commit(conn)

        assert raw.isolation_level is None


class TestDiagnoseConnection:
    """Connection diagnostics reported to callers."""

    def test_reports_wrapper_state_not_raw_state(self, mocker):
        """Verify dialect, closed and in_transaction come off the wrapper.

        Mutation: reading 'closed' from the raw connection instead of
        the wrapper in diagnose_connection().
        Oracle: wrapper and raw handle deliberately disagree on closed.
        """
        raw = mocker.Mock(spec=['autocommit', 'closed'])
        raw.autocommit = True
        raw.closed = False

        conn = mocker.Mock(spec=['dialect', 'sa_connection', 'driver_connection',
                                 'in_transaction', 'closed'])
        conn.dialect = 'postgresql'
        conn.driver_connection = raw
        conn.in_transaction = True
        conn.closed = True

        info = diagnose_connection(conn)

        assert info['type'] == 'postgresql'
        assert info['auto_commit'] is True
        assert info['in_transaction'] is True
        assert info['closed'] is True
        assert info['is_sqlalchemy'] is True

    def test_autocommit_attribute_wins_over_isolation_level(self):
        """Verify isolation_level is consulted only when autocommit is absent.

        Mutation: guarding the isolation_level branch with
        `if not info['auto_commit']` instead of
        `if info['auto_commit'] is None`.
        Oracle: a handle reporting autocommit False alongside
        isolation_level None, where the two rules disagree.
        """
        raw = DualModeRawConnection()
        raw.isolation_level = None

        info = diagnose_connection(DriverOnlyConnection(raw))

        assert info['auto_commit'] is False

    def test_autocommit_inferred_from_isolation_level(self):
        """Verify isolation_level None reads as auto-commit, DEFERRED as not.

        Mutation: flipping `raw_conn.isolation_level is None` to
        `is not None` in diagnose_connection().
        Oracle: hand-written sqlite3 mapping, both directions.
        """
        raw = IsolationOnlyRawConnection()
        conn = DriverOnlyConnection(raw)

        assert diagnose_connection(conn)['auto_commit'] is False

        raw.isolation_level = None
        assert diagnose_connection(conn)['auto_commit'] is True

    def test_defaults_when_attributes_absent(self):
        """Verify a bare connection yields the documented default report.

        Mutation: keying is_sqlalchemy off 'connection' rather than
        'sa_connection', or dropping the hasattr guard on conn.dialect.
        Oracle: a handle carrying 'connection' but no 'sa_connection',
        where the two attribute names disagree.
        """
        conn = RecordingConnection()

        info = diagnose_connection(conn)

        assert info['type'] == 'unknown'
        assert info['is_sqlalchemy'] is False
        assert info['in_transaction'] is False
        assert info['closed'] is False


class TestTransactionLifecycle:
    """Transaction context manager state, commit choice, and cleanup."""

    def test_commits_raw_connection_and_restores_auto_commit(self):
        """Verify a clean block commits raw handle and re-enables auto-commit.

        Mutation: __enter__ calling enable_auto_commit, __exit__
        committing self.connection instead of its inner .connection, or
        the cleanup dropping enable_auto_commit.
        Oracle: recording connection reporting mode and call order at
        each point.
        """
        conn = RecordingConnection()

        with Transaction(conn):
            assert conn.driver_connection.autocommit is False
            assert conn.in_transaction is True
            assert conn.connection.calls == []

        assert conn.connection.calls == ['commit']
        assert conn.calls == []
        assert conn.driver_connection.autocommit is True
        assert conn.in_transaction is False

    def test_rolls_back_and_reraises_on_exception(self):
        """Verify a failing block rolls back and lets the error propagate.

        Mutation: flipping `if exc_type is not None` in __exit__, or
        swapping cn.rollback() and cn.commit().
        Oracle: recording connection showing rollback and no commit,
        plus the raised error reaching the caller.
        """
        conn = RecordingConnection()

        with pytest.raises(ValueError, match='boom'):
            with Transaction(conn):
                raise ValueError('boom')

        assert conn.connection.calls == ['rollback']
        assert conn.calls == []
        assert conn.driver_connection.autocommit is True
        assert conn.in_transaction is False

    def test_nested_transaction_on_same_connection_rejected(self):
        """Verify a second Transaction for a live connection raises.

        Mutation: dropping the `connection_id in _local.active_transactions`
        guard in Transaction.__init__.
        Oracle: RuntimeError with the nested-transaction message, and an
        outer transaction that still commits afterwards.
        """
        conn = RecordingConnection()

        with Transaction(conn):
            with pytest.raises(RuntimeError, match='Nested transactions'):
                Transaction(conn)

        assert conn.connection.calls == ['commit']

    def test_distinct_connections_run_concurrent_transactions(self):
        """Verify two connections hold transactions at the same time.

        Mutation: the guard in Transaction.__init__ firing whenever any
        transaction is active rather than one for this connection id.
        Oracle: an inner transaction on a second connection that must
        open and commit while the first is live.
        """
        first = RecordingConnection()
        second = RecordingConnection()

        with Transaction(first):
            with Transaction(second):
                pass
            assert second.connection.calls == ['commit']
            assert first.connection.calls == []

        assert first.connection.calls == ['commit']

    def test_transaction_state_is_per_thread(self):
        """Verify another thread may transact the same connection object.

        Mutation: _local = threading.local() replaced by shared module
        state, which would make the second thread hit the nested guard.
        Oracle: child thread's captured exception list, plus the commit
        count on the shared raw connection.
        """
        conn = RecordingConnection()
        child_errors = []

        def run_child():
            try:
                with Transaction(conn):
                    pass
            except Exception as e:
                child_errors.append(e)

        with Transaction(conn):
            child = threading.Thread(target=run_child)
            child.start()
            child.join()
            assert child_errors == []
            assert conn.connection.calls == ['commit']

        assert conn.connection.calls == ['commit', 'commit']

    def test_connection_reusable_after_commit_failure(self):
        """Verify a failed commit still clears state and restores auto-commit.

        Mutation: moving the active_transactions.pop or the
        enable_auto_commit call out of __exit__'s finally block.
        Oracle: a second Transaction on the same connection, which the
        nested guard would reject if cleanup had been skipped.
        """
        conn = RecordingConnection()
        conn.connection.fail_on_commit = True

        with pytest.raises(RuntimeError, match='commit failed'):
            with Transaction(conn):
                pass

        assert conn.driver_connection.autocommit is True
        assert conn.in_transaction is False

        conn.connection.fail_on_commit = False
        with Transaction(conn):
            pass

        assert conn.connection.calls == ['commit', 'commit']

    def test_unentered_transaction_leaves_the_connection_unflagged(self):
        """Verify building a Transaction without entering changes nothing.

        Mutation: setting connection.in_transaction = True in
        Transaction.__init__ instead of __enter__, which strands the
        flag on a transaction whose block never opens.
        Oracle: recording connection reporting the flag, the auto-commit
        mode and an empty call log after construction alone, then a
        later block that must still flag and commit normally.
        """
        conn = RecordingConnection()

        Transaction(conn)

        assert conn.in_transaction is False
        assert conn.driver_connection.autocommit is True
        assert conn.connection.calls == []

        with Transaction(conn):
            assert conn.in_transaction is True

        assert conn.in_transaction is False
        assert conn.connection.calls == ['commit']

    def test_in_transaction_written_only_on_enter_and_exit(self):
        """Verify the flag is written once entering and once leaving.

        Mutation: moving `self.connection.in_transaction = True` from
        __enter__ back into Transaction.__init__.
        Oracle: a connection spying on every write to in_transaction,
        whose log must stay empty until __enter__ runs.
        """
        conn = FlagRecordingConnection()
        transaction = Transaction(conn)

        assert conn.flag_writes == []

        with transaction:
            assert conn.flag_writes == [True]

        assert conn.flag_writes == [True, False]
        assert conn.connection.calls == ['commit']


class TestTransactionExecute:
    """Statement execution and result shaping inside a transaction."""

    def test_execute_without_returnid_delegates_to_connection(self, mocker):
        """Verify a plain execute goes straight to the connection, uncursored.

        Mutation: flipping `if not returnid` in Transaction.execute(),
        which would route plain statements through the cursor path.
        Oracle: connection spy holding the forwarded call plus a
        get_dict_cursor spy that must stay untouched.
        """
        conn = mocker.Mock()
        conn.dialect = 'sqlite'
        conn.execute.return_value = 3
        cursor_factory = mocker.patch('database.transaction.get_dict_cursor')

        with Transaction(conn) as tx:
            rowcount = tx.execute('update foo set a = %s', 1)

        assert rowcount == 3
        conn.execute.assert_called_once_with('update foo set a = %s', 1)
        cursor_factory.assert_not_called()

    def test_execute_with_returnid_prepares_sql_for_the_dialect(self, mocker):
        """Verify returnid execution converts placeholders and unwraps one row.

        Mutation: prepare_query() called with a hardcoded dialect
        instead of self.connection.dialect, or the len(results) == 1
        branch losing to the multi-row branch.
        Oracle: hand-written qmark SQL for sqlite and a hand-written
        scalar for the single row.
        """
        cursor = RecordingCursor([{'id': 7}])
        conn = mocker.Mock()
        conn.dialect = 'sqlite'
        mocker.patch('database.transaction.get_dict_cursor', return_value=cursor)

        with Transaction(conn) as tx:
            result = tx.execute(
                'insert into foo (a, b) values (%s, %s) returning id',
                1, 2, returnid='id')

        assert result == 7
        assert cursor.executed == (
            'insert into foo (a, b) values (?, ?) returning id', (1, 2))

    def test_execute_returns_one_value_per_row_for_many_rows(self, mocker):
        """Verify a multi-row result with a scalar returnid yields a flat list.

        Mutation: `if len(results) == 1` widened to `> 1`, which would
        return only the first row's value.
        Oracle: hand-written list across two rows.
        """
        cursor = RecordingCursor([{'id': 1}, {'id': 2}])
        conn = mocker.Mock()
        conn.dialect = 'sqlite'
        mocker.patch('database.transaction.get_dict_cursor', return_value=cursor)

        with Transaction(conn) as tx:
            result = tx.execute(
                'select id from foo where a = %s', 1,
                returnid='id')

        assert result == [1, 2]

    def test_execute_maps_a_returnid_list_over_each_row(self, mocker):
        """Verify a list returnid yields one value list per row.

        Mutation: the isiterable(returnid) arm returning a single field
        rather than one entry per requested name.
        Oracle: hand-written value lists for a one-row and a two-row
        result.
        """
        conn = mocker.Mock()
        conn.dialect = 'sqlite'

        single = RecordingCursor([{'id': 7, 'name': 'a'}])
        mocker.patch('database.transaction.get_dict_cursor', return_value=single)
        with Transaction(conn) as tx:
            assert tx.execute('insert into foo values (%s) returning id, name',
                              1, returnid=['id', 'name']) == [7, 'a']

        many = RecordingCursor([{'id': 1, 'name': 'a'}, {'id': 2, 'name': 'b'}])
        mocker.patch('database.transaction.get_dict_cursor', return_value=many)
        with Transaction(conn) as tx:
            assert tx.execute('insert into foo values (%s) returning id, name',
                              1, returnid=['id', 'name']) == [[1, 'a'], [2, 'b']]

    def test_execute_returns_none_when_there_is_no_result_set(self, mocker):
        """Verify an empty or absent result set yields None, not an error.

        Mutation: narrowing the except around cursor.fetchall(), which
        would let a cursor with no result set raise at the caller.
        Oracle: both a cursor returning no rows and one that raises on
        fetchall.
        """
        conn = mocker.Mock()
        conn.dialect = 'sqlite'

        empty = RecordingCursor([])
        mocker.patch('database.transaction.get_dict_cursor', return_value=empty)
        with Transaction(conn) as tx:
            assert tx.execute(
                'delete from foo where a = %s', 1,
                returnid='id') is None

        no_result_set = RecordingCursor(None)
        mocker.patch(
            'database.transaction.get_dict_cursor',
            return_value=no_result_set)
        with Transaction(conn) as tx:
            assert tx.execute(
                'delete from foo where a = %s', 1,
                returnid='id') is None

    def test_select_helpers_delegate_to_the_matching_connection_method(self, mocker):
        """Verify each select helper forwards to its namesake, args intact.

        Mutation: a helper delegating to a neighbor, e.g. select_row()
        calling connection.select_row_or_none(), or select() dropping
        **kwargs.
        Oracle: per-method connection spies, each asserted on its own
        forwarded call.
        """
        conn = mocker.Mock()
        conn.dialect = 'sqlite'
        conn.select_scalar.return_value = 9

        with Transaction(conn) as tx:
            tx.select('select * from foo where a = %s', 1, mandatory=True)
            tx.select_column('select a from foo where a = %s', 1)
            tx.select_row('select * from foo where a = %s', 1)
            tx.select_row_or_none('select * from foo where a = %s', 1)
            scalar = tx.select_scalar('select count(1) from foo where a = %s', 1)

        assert scalar == 9
        conn.select.assert_called_once_with(
            'select * from foo where a = %s', 1, mandatory=True)
        conn.select_column.assert_called_once_with(
            'select a from foo where a = %s', 1)
        conn.select_row.assert_called_once_with(
            'select * from foo where a = %s', 1)
        conn.select_row_or_none.assert_called_once_with(
            'select * from foo where a = %s', 1)
        conn.select_scalar.assert_called_once_with(
            'select count(1) from foo where a = %s', 1)


if __name__ == '__main__':
    __import__('pytest').main([__file__])
