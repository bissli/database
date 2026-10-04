"""Auto-commit switching, connection diagnostics, and Transaction.
"""
import threading

import pytest
from database.cache import Cache
from database.transaction import Transaction, diagnose_connection
from database.transaction import disable_auto_commit, enable_auto_commit


class RecordingRawConnection:
    """Raw connection logging commit/rollback; fail_on_* makes either raise.
    """

    def __init__(self) -> None:
        self.autocommit = True
        self.calls: list[str] = []
        self.fail_on_commit = False
        self.fail_on_rollback = False

    def commit(self) -> None:
        """Record the commit and optionally fail like a dead connection.
        """
        self.calls.append('commit')
        if self.fail_on_commit:
            raise RuntimeError('commit failed')

    def rollback(self) -> None:
        """Record the rollback and optionally fail like a dead connection.
        """
        self.calls.append('rollback')
        if self.fail_on_rollback:
            raise RuntimeError('rollback failed')


class RecordingConnection:
    """ConnectionWrapper stand-in whose own calls log must stay empty.
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
    """Wrapper logging every in_transaction write to flag_writes.
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
    """

    def __init__(self, driver_connection) -> None:
        self.driver_connection = driver_connection


class DualModeRawConnection:
    """Raw connection with both autocommit and isolation_level.
    """

    def __init__(self) -> None:
        self.autocommit = False
        self.isolation_level = 'DEFERRED'


class IsolationOnlyRawConnection:
    """Raw connection with only isolation_level, like sqlite3.
    """

    def __init__(self) -> None:
        self.isolation_level = 'DEFERRED'


class RaisingExecutionOptionsConnection:
    """Wrapper whose execution_options raises.
    """

    def __init__(self, driver_connection) -> None:
        self.driver_connection = driver_connection

    def execution_options(self, **kwargs) -> None:
        """Reject the level the way the sqlite dialect does.
        """
        raise RuntimeError('isolation level not supported')


class LockedAutocommitRawConnection:
    """Raw connection whose autocommit setter raises.
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
    """Cursor stub logging execute; rows None makes fetchall raise.
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

        Mutation: swapped enable/disable arms, or the wrapper as raw_conn.
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

        Mutation: disable_auto_commit passing enable=True.
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

        Mutation: dropping the return after the strategy call succeeds.
        Oracle: raw connection left as it was by a no-op strategy spy.
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

        Mutation: swapping 'AUTOCOMMIT' and 'READ COMMITTED'.
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

        Mutation: narrowing the except around the strategy call.
        Oracle: raw autocommit flipped by the fallback branch.
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

        Mutation: autocommit = True in place of = enable, or a dropped return.
        Oracle: expected value per direction, and an untouched isolation_level.
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

        Mutation: narrowing or dropping the except around execution_options().
        Oracle: raw autocommit flipped to False by the later branch.
        """
        raw = DualModeRawConnection()
        raw.autocommit = True
        conn = RaisingExecutionOptionsConnection(raw)

        disable_auto_commit(conn)

        assert raw.autocommit is False

    def test_fallback_isolation_level_tracks_mode(self):
        """Verify a sqlite handle gets None to enable, DEFERRED to disable.

        Mutation: swapping the arms of level = None if enable else 'DEFERRED'.
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

        Mutation: narrowing the except around raw_conn.autocommit = enable.
        Oracle: isolation_level moved to None by the later branch.
        """
        raw = LockedAutocommitRawConnection()
        conn = DriverOnlyConnection(raw)

        enable_auto_commit(conn)

        assert raw.isolation_level is None


class TestDiagnoseConnection:
    """Connection diagnostics reported to callers."""

    def test_reports_wrapper_state_over_raw_state(self, mocker):
        """Verify dialect, closed and in_transaction come off the wrapper.

        Mutation: diagnose_connection reading closed off the raw connection.
        Oracle: wrapper and raw handle disagree on closed.
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

        Mutation: `not info['auto_commit']` in place of `is None`.
        Oracle: autocommit False beside isolation_level None.
        """
        raw = DualModeRawConnection()
        raw.isolation_level = None

        info = diagnose_connection(DriverOnlyConnection(raw))

        assert info['auto_commit'] is False

    def test_autocommit_inferred_from_isolation_level(self):
        """Verify isolation_level None reads as auto-commit, DEFERRED as not.

        Mutation: `is not None` in place of `is None` on isolation_level.
        Oracle: hand-written sqlite3 mapping, both directions.
        """
        raw = IsolationOnlyRawConnection()
        conn = DriverOnlyConnection(raw)

        assert diagnose_connection(conn)['auto_commit'] is False

        raw.isolation_level = None
        assert diagnose_connection(conn)['auto_commit'] is True

    def test_defaults_when_attributes_absent(self):
        """Verify a bare connection yields the documented default report.

        Mutation: is_sqlalchemy keyed off 'connection', or no dialect guard.
        Oracle: a handle with 'connection' but no 'sa_connection'.
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

        Mutation: committing the wrapper, or a swapped or dropped auto-commit.
        Oracle: recording connection's mode and call order at each point.
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

        Mutation: flipping the exc_type test in __exit__.
        Oracle: a rollback and no commit, and the error reaching the caller.
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

        Mutation: dropping the active_transactions guard in __init__.
        Oracle: the nested-transaction RuntimeError, then the outer commit.
        """
        conn = RecordingConnection()

        with Transaction(conn):
            with pytest.raises(RuntimeError, match='Nested transactions'):
                Transaction(conn)

        assert conn.connection.calls == ['commit']

    def test_distinct_connections_run_concurrent_transactions(self):
        """Verify two connections hold transactions at the same time.

        Mutation: the nested guard ignoring the connection id.
        Oracle: a second connection's block commits inside the first.
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

        Mutation: shared module state in place of threading.local().
        Oracle: the child thread's errors, and the shared commit count.
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
        """Verify a failed commit rolls back, clears state, restores auto-commit.

        Mutation: the pop or enable_auto_commit moved out of the finally, or
            the failed commit re-raised without a rollback.
        Oracle: a second Transaction, which the nested guard would reject,
            and the hand-written call log.
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

        assert conn.connection.calls == ['commit', 'rollback', 'commit']

    def test_rollback_empties_schema_and_strategy_caches(self):
        """Verify a rolled-back block drops schema and strategy cache entries.

        Mutation: _rollback skipping either the schema or the strategy clear.
        Oracle: one entry planted in each cache before the block.
        """
        cache = Cache.get_instance()
        cache.get_schema_cache()['t:columns:1'] = ['id', 'v']
        cache.get_cache('primary_keys_test')['t:1'] = ['id']

        with pytest.raises(RuntimeError, match='in the block'):
            with Transaction(RecordingConnection()):
                raise RuntimeError('in the block')

        assert len(cache.get_schema_cache()) == 0
        assert len(cache.get_cache('primary_keys_test')) == 0

    def test_failed_rollback_after_failed_commit_keeps_the_commit_error(self):
        """Verify a failed rollback neither hides the commit error nor the clear.

        Mutation: the rollback error propagating in place of the commit
            error, or the cache clear skipped when rollback raises.
        Oracle: the fake's 'commit failed' message, and a planted entry.
        """
        conn = RecordingConnection()
        conn.connection.fail_on_commit = True
        conn.connection.fail_on_rollback = True
        cache = Cache.get_instance()
        cache.get_schema_cache()['t:columns:1'] = ['id', 'v']

        with pytest.raises(RuntimeError, match='commit failed'):
            with Transaction(conn):
                pass

        assert len(cache.get_schema_cache()) == 0

    def test_unentered_transaction_leaves_the_connection_unflagged(self):
        """Verify building a Transaction without entering changes nothing.

        Mutation: setting in_transaction in __init__ in place of __enter__.
        Oracle: flag, mode and empty call log after construction alone.
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

        Mutation: `in_transaction = True` moved from __enter__ to __init__.
        Oracle: a spy on every in_transaction write.
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

        Mutation: flipping `if not returnid` in Transaction.execute.
        Oracle: connection spy, and a get_dict_cursor spy left untouched.
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

        Mutation: a hardcoded dialect for prepare_query, or the multi-row arm.
        Oracle: hand-written qmark SQL and the single row's scalar.
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

        Mutation: `> 1` in place of `== 1` on len(results).
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

        Mutation: the isiterable(returnid) arm returning a single field.
        Oracle: hand-written value lists for one-row and two-row results.
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
        """Verify an empty or absent result set yields None without raising.

        Mutation: narrowing the except around cursor.fetchall().
        Oracle: a cursor with no rows, and one whose fetchall raises.
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

        Mutation: a helper calling its neighbor, or select dropping **kwargs.
        Oracle: per-method connection spies.
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
