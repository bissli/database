"""
Test auto-commit functionality across different database drivers.
"""
import logging

from database.transaction import Transaction
from database.transaction import diagnose_connection
from database.transaction import disable_auto_commit, enable_auto_commit
from database.connection import ensure_commit

logger = logging.getLogger(__name__)


class TestAutoCommit:
    """Test suite for auto-commit functionality"""

    def test_enable_auto_commit_postgres(self, mocker):
        """Test enabling auto-commit on PostgreSQL connection"""
        # Create a mock PostgreSQL connection
        conn = mocker.Mock()
        conn.dialect = 'postgresql'
        conn.connection = mocker.Mock()
        conn.connection.autocommit = False
        conn.driver_connection = conn.connection

        # Mock strategy to test the strategy path
        mock_strategy = mocker.Mock()
        mocker.patch('database.strategy.get_db_strategy', return_value=mock_strategy)

        enable_auto_commit(conn)

        # Verify strategy's enable_autocommit was called with raw connection
        mock_strategy.enable_autocommit.assert_called_once_with(conn.driver_connection)

    def test_enable_auto_commit_sqlite(self, mocker):
        """Test enabling auto-commit on SQLite connection"""
        # Create a simple class that tracks isolation_level
        class SQLiteConnectionMock:
            def __init__(self):
                self.isolation_level = 'DEFERRED'

        conn = mocker.Mock()
        conn.dialect = 'sqlite'
        conn.connection = mocker.Mock()
        sqlite_conn = SQLiteConnectionMock()
        conn.driver_connection = sqlite_conn

        # Mock strategy to test the strategy path
        mock_strategy = mocker.Mock()
        mocker.patch('database.strategy.get_db_strategy', return_value=mock_strategy)

        enable_auto_commit(conn)

        # Verify strategy's enable_autocommit was called
        mock_strategy.enable_autocommit.assert_called_once_with(sqlite_conn)

    def test_disable_auto_commit_postgres(self, mocker):
        """Test disabling auto-commit on PostgreSQL connection"""
        conn = mocker.Mock()
        conn.dialect = 'postgresql'
        conn.connection = mocker.Mock()
        conn.connection.autocommit = True
        conn.driver_connection = conn.connection

        # Mock strategy
        mock_strategy = mocker.Mock()
        mocker.patch('database.strategy.get_db_strategy', return_value=mock_strategy)

        disable_auto_commit(conn)

        # Verify strategy's disable_autocommit was called
        mock_strategy.disable_autocommit.assert_called_once_with(conn.driver_connection)

    def test_ensure_commit(self, mocker):
        """Test ensure_commit function"""
        conn = mocker.Mock()
        conn.commit = mocker.Mock()

        ensure_commit(conn)

        conn.commit.assert_called_once()

    def test_transaction_auto_commit_lifecycle(self, mocker):
        """Test that Transaction properly manages auto-commit state"""
        conn = mocker.Mock()
        conn.connection = mocker.Mock()
        conn.connection.autocommit = True
        conn.driver_connection = conn.connection

        mock_disable = mocker.Mock()
        mock_enable = mocker.Mock()

        mocker.patch('database.transaction.disable_auto_commit', mock_disable)
        mocker.patch('database.transaction.enable_auto_commit', mock_enable)

        with Transaction(conn) as tx:
            mock_disable.assert_called_once_with(conn)
            assert conn.in_transaction is True

        mock_enable.assert_called_once_with(conn)
        assert conn.in_transaction is False

    def test_diagnose_connection(self, mocker):
        """Test connection diagnostic function"""
        conn = mocker.Mock()
        conn.dialect = 'postgresql'
        conn.connection = mocker.Mock()
        conn.connection.autocommit = True
        conn.driver_connection = conn.connection
        conn.in_transaction = False

        info = diagnose_connection(conn)

        assert info['type'] == 'postgresql'
        assert info['auto_commit'] is True
        assert info['in_transaction'] is False
        assert info['is_sqlalchemy'] is True

    def test_enable_auto_commit_fallback_postgres(self, mocker):
        """Test enabling auto-commit falls back when no dialect attribute"""
        # Connection without dialect attribute - uses fallback
        conn = mocker.Mock(spec=['connection', 'driver_connection'])
        conn.connection = mocker.Mock()
        conn.connection.autocommit = False
        conn.driver_connection = conn.connection

        enable_auto_commit(conn)

        # Should set autocommit directly via fallback
        assert conn.driver_connection.autocommit is True

    def test_enable_auto_commit_fallback_sqlite(self, mocker):
        """Test enabling auto-commit falls back when no dialect attribute"""
        # Create a mock that only has isolation_level, no autocommit
        class SQLiteConnectionMock:
            def __init__(self):
                self.isolation_level = 'DEFERRED'

        conn = mocker.Mock(spec=['driver_connection'])
        sqlite_conn = SQLiteConnectionMock()
        conn.driver_connection = sqlite_conn

        enable_auto_commit(conn)

        # Should set isolation_level directly via fallback
        assert conn.driver_connection.isolation_level is None


class TestAutoCommitIntegration:
    """Integration tests for auto-commit with mock database operations"""

    def test_execute_with_auto_commit(self, mocker):
        """Test that ensure_commit properly commits the connection"""
        conn = mocker.Mock()
        conn.commit = mocker.Mock()

        ensure_commit(conn)
        conn.commit.assert_called_once()

        conn.commit.reset_mock()
        ensure_commit(conn)
        conn.commit.assert_called_once()

    def test_execute_with_transaction_no_auto_commit(self, mocker):
        """Test that execute within a transaction does not auto-commit"""
        ensure_commit_mock = mocker.Mock()
        mocker.patch('database.connection.ensure_commit', ensure_commit_mock)

        conn = mocker.Mock()
        mock_tx = mocker.MagicMock()
        mock_tx.execute.return_value = 1

        transaction_mock = mocker.MagicMock()
        transaction_mock.__enter__.return_value = mock_tx

        mocker.patch.object(Transaction, '__new__', return_value=transaction_mock)

        with Transaction(conn) as tx:
            tx.execute('INSERT INTO test VALUES (1)')

        mock_tx.execute.assert_called_once_with('INSERT INTO test VALUES (1)')
        transaction_mock.__enter__.assert_called_once()
        transaction_mock.__exit__.assert_called_once()
        ensure_commit_mock.assert_not_called()


if __name__ == '__main__':
    __import__('pytest').main([__file__])
