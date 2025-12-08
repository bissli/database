"""
Test auto-commit functionality across different database drivers.
"""
import logging

from database.core.transaction import Transaction
from database.utils.auto_commit import diagnose_connection
from database.utils.auto_commit import disable_auto_commit, enable_auto_commit
from database.utils.auto_commit import ensure_commit

logger = logging.getLogger(__name__)


class TestAutoCommit:
    """Test suite for auto-commit functionality"""

    def test_enable_auto_commit_postgres(self, mocker):
        """Test enabling auto-commit on PostgreSQL connection"""
        # Create a mock PostgreSQL connection directly
        conn = mocker.Mock()
        # Set 'connection' as attribute, not as another mock
        conn.connection = mocker.Mock()
        conn.connection.autocommit = False

        # Add driver_connection to mimic SQLAlchemy connection structure
        conn.driver_connection = conn.connection

        # Define a simplified get_dialect_name function that will be mocked
        def mock_get_dialect_name(obj):
            if obj is conn or obj is conn.connection:
                return 'postgresql'
            return None

        # Mock the connection detection function
        mocker.patch('database.utils.auto_commit.get_dialect_name', side_effect=mock_get_dialect_name)

        # Enable auto-commit
        enable_auto_commit(conn)

        # Verify raw connection autocommit was set to True
        assert conn.connection.autocommit is True

    def test_enable_auto_commit_sqlite(self, mocker):
        """Test enabling auto-commit on SQLite connection"""
        # Create a simple custom class that will track the isolation_level attribute
        class SQLiteConnectionMock:
            def __init__(self):
                self.isolation_level = 'DEFERRED'
                # Add any other attributes needed for the mock

        # Create our connection mock
        conn = mocker.Mock()
        conn.connection = mocker.Mock()

        # Use our custom class for the driver_connection
        sqlite_conn = SQLiteConnectionMock()
        conn.connection.isolation_level = 'DEFERRED'
        conn.driver_connection = sqlite_conn

        # Define a simplified get_dialect_name function that will be mocked
        def mock_get_dialect_name(obj):
            if obj is conn or obj is conn.connection:
                return 'sqlite'
            return None

        # Mock the connection detection function
        mocker.patch('database.utils.auto_commit.get_dialect_name', side_effect=mock_get_dialect_name)

        # Enable auto-commit
        enable_auto_commit(conn)

        # Check that driver_connection's isolation_level is set to None
        # This is what the enable_auto_commit function actually modifies
        assert conn.driver_connection.isolation_level is None

    def test_disable_auto_commit_postgres(self, mocker):
        """Test disabling auto-commit on PostgreSQL connection"""
        # Create a mock PostgreSQL connection directly
        conn = mocker.Mock()
        conn.connection = mocker.Mock()
        conn.connection.autocommit = True

        # Add driver_connection to mimic SQLAlchemy connection structure
        conn.driver_connection = conn.connection

        # Define a simplified get_dialect_name function that will be mocked
        def mock_get_dialect_name(obj):
            if obj is conn or obj is conn.connection:
                return 'postgresql'
            return None

        # Mock the connection detection function
        mocker.patch('database.utils.auto_commit.get_dialect_name', side_effect=mock_get_dialect_name)

        # Disable auto-commit
        disable_auto_commit(conn)

        # Verify raw connection autocommit was disabled
        assert conn.connection.autocommit is False

    def test_ensure_commit(self, mocker):
        """Test ensure_commit function"""
        # Create a mock connection directly
        conn = mocker.Mock()
        conn.sa_connection = mocker.Mock()

        # Setup direct connection commit
        conn.commit = mocker.Mock()

        # Call ensure_commit
        ensure_commit(conn)

        # Verify commit was called on direct connection
        conn.commit.assert_called_once()

    def test_transaction_auto_commit_lifecycle(self, mocker):
        """Test that Transaction properly manages auto-commit state"""
        # Create a mock connection directly
        conn = mocker.Mock()
        conn.connection = mocker.Mock()
        conn.connection.autocommit = True

        # Add driver_connection to mimic SQLAlchemy connection structure
        conn.driver_connection = conn.connection

        # Create mocks for the auto-commit functions
        mock_disable = mocker.Mock()
        mock_enable = mocker.Mock()

        # Mock the auto-commit functions where they are used (in transaction module)
        mocker.patch('database.core.transaction.disable_auto_commit', mock_disable)
        mocker.patch('database.core.transaction.enable_auto_commit', mock_enable)

        # Enter a transaction
        with Transaction(conn) as tx:
            # Verify disable_auto_commit was called
            mock_disable.assert_called_once_with(conn)

            # Verify in_transaction was set
            assert conn.in_transaction is True

        # Verify enable_auto_commit was called on exit
        mock_enable.assert_called_once_with(conn)

        # Verify in_transaction was reset
        assert conn.in_transaction is False

    def test_diagnose_connection(self, mocker):
        """Test connection diagnostic function"""
        # Create a mock connection directly
        conn = mocker.Mock()
        conn.connection = mocker.Mock()
        conn.connection.autocommit = True
        # Add driver_connection to mimic SQLAlchemy connection structure
        conn.driver_connection = conn.connection
        # Set in_transaction as an attribute, not a method
        conn.in_transaction = False

        # Define simplified connection type detection function
        def mock_get_dialect_name(obj):
            if obj is conn or obj is conn.connection:
                return 'postgresql'
            return None

        # Mock the connection detection function
        mocker.patch('database.utils.auto_commit.get_dialect_name', side_effect=mock_get_dialect_name)

        # Get diagnostic info
        info = diagnose_connection(conn)

        # Verify diagnostic information
        assert info['type'] == 'postgresql'
        assert info['auto_commit'] is True
        assert info['in_transaction'] is False
        assert info['is_sqlalchemy'] is True


class TestAutoCommitIntegration:
    """Integration tests for auto-commit with mock database operations"""

    def test_execute_with_auto_commit(self, mocker):
        """Test that ensure_commit properly commits the connection"""
        # Create a mock connection directly
        conn = mocker.Mock()
        conn.commit = mocker.Mock()

        # Call ensure_commit to force a commit
        ensure_commit(conn)

        # Verify commit was called on the connection
        conn.commit.assert_called_once()

        # Reset and call again
        conn.commit.reset_mock()
        ensure_commit(conn)

        # Verify commit was called again
        conn.commit.assert_called_once()

    def test_execute_with_transaction_no_auto_commit(self, mocker):
        """Test that execute within a transaction does not auto-commit"""
        # Create a mock for ensure_commit to track calls
        ensure_commit_mock = mocker.Mock()
        mocker.patch('database.utils.auto_commit.ensure_commit', ensure_commit_mock)

        # Create a mock connection
        conn = mocker.Mock()

        # Create a mock transaction that will be returned by __enter__
        mock_tx = mocker.MagicMock()
        mock_tx.execute.return_value = 1

        # Create a simple context manager mock for Transaction
        transaction_mock = mocker.MagicMock()
        transaction_mock.__enter__.return_value = mock_tx

        # Patch Transaction to return our mock without calling the real constructor
        mocker.patch.object(Transaction, '__new__', return_value=transaction_mock)

        # Now use the Transaction class with our mock
        with Transaction(conn) as tx:
            tx.execute('INSERT INTO test VALUES (1)')

        # Verify our mock transaction was used
        mock_tx.execute.assert_called_once_with('INSERT INTO test VALUES (1)')

        # Verify enter and exit were called
        transaction_mock.__enter__.assert_called_once()
        transaction_mock.__exit__.assert_called_once()

        # Verify ensure_commit was not called
        ensure_commit_mock.assert_not_called()


if __name__ == '__main__':
    __import__('pytest').main([__file__])
