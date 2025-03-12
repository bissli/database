"""
Integration tests for SQLAlchemy database connection functionality.
"""
import concurrent.futures
import threading
import time
from unittest.mock import patch

import pandas as pd
import pytest
from sqlalchemy.pool import NullPool, QueuePool

from database import connect, select, execute
from database.core.transaction import Transaction
from database.adapters.structure import (
    SQLiteRowAdapter, PostgreSQLRowAdapter, SQLServerRowAdapter
)
from database.utils.connection_utils import (
    get_engine_for_options, dispose_all_engines, get_dialect_name
)


class TestSQLAlchemyIntegration:
    """Test SQLAlchemy integration with the database module."""
    
    def test_connection_types(self, mock_postgres_conn, mock_sqlserver_conn, mock_sqlite_conn):
        """Test that our connection type detection functions work with SQLAlchemy connections."""
        from database.utils.connection_utils import (
            is_psycopg_connection, is_pyodbc_connection, is_sqlite3_connection
        )
        
        # Verify postgres connection detection
        assert is_psycopg_connection(mock_postgres_conn)
        assert not is_pyodbc_connection(mock_postgres_conn)
        assert not is_sqlite3_connection(mock_postgres_conn)
        
        # Verify sqlserver connection detection
        assert is_pyodbc_connection(mock_sqlserver_conn)
        assert not is_psycopg_connection(mock_sqlserver_conn)
        assert not is_sqlite3_connection(mock_sqlserver_conn)
        
        # Verify sqlite connection detection
        assert is_sqlite3_connection(mock_sqlite_conn)
        assert not is_psycopg_connection(mock_sqlite_conn)
        assert not is_pyodbc_connection(mock_sqlite_conn)
    
    def test_dialect_name_detection(self, mock_postgres_conn, mock_sqlserver_conn, mock_sqlite_conn):
        """Test that we can detect the correct dialect name from SQLAlchemy connections."""
        assert get_dialect_name(mock_postgres_conn) == 'postgresql'
        assert get_dialect_name(mock_sqlserver_conn) == 'mssql'
        assert get_dialect_name(mock_sqlite_conn) == 'sqlite'
    
    def test_pooling_config(self, postgres_options):
        """Test that pooling configuration is properly applied."""
        # Test with pooling
        engine1 = get_engine_for_options(
            postgres_options, 
            use_pool=True, 
            pool_size=5,
            pool_recycle=300
        )
        assert isinstance(engine1.pool, QueuePool)
        assert engine1.pool.size() == 5
        
        # Test without pooling
        engine2 = get_engine_for_options(
            postgres_options, 
            use_pool=False
        )
        assert isinstance(engine2.pool, NullPool)
        
        # Clean up
        dispose_all_engines()
    
    def test_basic_query_execution(self, mock_postgres_conn):
        """Test that basic query execution works through SQLAlchemy connections."""
        # Setup mock cursor result
        cursor = mock_postgres_conn.cursor()
        cursor.description = [('id', None), ('name', None)]
        cursor.fetchall.return_value = [(1, 'test'), (2, 'test2')]
        
        # Test select function
        df = select(mock_postgres_conn, "SELECT id, name FROM test")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert list(df.columns) == ['id', 'name']
        
        # Test execute function
        cursor.rowcount = 2
        result = execute(mock_postgres_conn, "UPDATE test SET name = 'updated'")
        assert result == 2
    
    def test_transaction_management(self, mock_postgres_conn):
        """Test that transaction management works correctly with SQLAlchemy connections."""
        # Setup cursor result
        cursor = mock_postgres_conn.cursor()
        cursor.description = [('id', None)]
        cursor.fetchall.return_value = [(1,), (2,)]
        
        # Test transaction commit
        with Transaction(mock_postgres_conn) as tx:
            result = tx.select("SELECT id FROM test")
            assert len(result) == 2
        
        # Verify commit was called
        assert mock_postgres_conn.connection.commit.called
        
        # Reset for rollback test
        mock_postgres_conn.connection.commit.reset_mock()
        mock_postgres_conn.connection.rollback.reset_mock()
        
        # Test transaction rollback
        try:
            with Transaction(mock_postgres_conn) as tx:
                tx.execute("SELECT id FROM test")
                raise ValueError("Test exception")
        except ValueError:
            pass
        
        # Verify rollback was called
        assert mock_postgres_conn.connection.rollback.called
    
    def test_row_adapters(self, mock_postgres_conn, mock_sqlserver_conn, mock_sqlite_conn):
        """Test that row adapters still work correctly with SQLAlchemy connections."""
        from database.adapters.structure import RowStructureAdapter
        
        # Test with PostgreSQL
        pg_row = {'id': 1, 'name': 'test'}
        pg_adapter = RowStructureAdapter.create(mock_postgres_conn, pg_row)
        assert isinstance(pg_adapter, PostgreSQLRowAdapter)
        assert pg_adapter.to_dict() == pg_row
        
        # Test with SQL Server
        ss_row = {'id': 2, 'name': 'test2'}
        ss_adapter = RowStructureAdapter.create(mock_sqlserver_conn, ss_row)
        assert isinstance(ss_adapter, SQLServerRowAdapter)
        assert ss_adapter.to_dict() == ss_row
        
        # Test with SQLite
        sl_row = {'id': 3, 'name': 'test3'}
        sl_adapter = RowStructureAdapter.create(mock_sqlite_conn, sl_row)
        assert isinstance(sl_adapter, SQLiteRowAdapter)
        assert sl_adapter.to_dict() == sl_row
    
    def test_pool_exhaustion(self, postgres_options):
        """Test pool exhaustion and recovery."""
        # Create a small pool
        engine = get_engine_for_options(
            postgres_options,
            use_pool=True,
            pool_size=2,
            pool_timeout=1  # Short timeout for testing
        )
        
        # Track connection attempts
        successful = 0
        failed = 0
        
        # Patch actual connection acquisition to avoid real DB connections
        with patch('sqlalchemy.engine.Engine.connect') as mock_connect:
            # Mock successful connections
            mock_connect.side_effect = lambda: engine._connection_cls(engine)
            
            def get_connection():
                nonlocal successful, failed
                try:
                    conn = connect(postgres_options, use_pool=True, pool_max_connections=2)
                    successful += 1
                    return conn
                except Exception:
                    failed += 1
                    return None
            
            # Get connections in parallel to test pool exhaustion
            connections = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(get_connection) for _ in range(5)]
                connections = [f.result() for f in futures if f.result()]
            
            # Check results - we should get exactly pool_size connections
            assert successful <= 2
            assert len(connections) <= 2
            
            # Close connections and verify we can get new ones
            for conn in connections:
                conn.close()
            
            # Reset counters
            successful = 0
            failed = 0
            
            # Try again and verify we can get new connections
            new_conn = get_connection()
            assert new_conn is not None
            assert successful == 1
        
        # Clean up
        dispose_all_engines()
    
    def test_concurrent_queries(self, mock_postgres_conn):
        """Test that concurrent queries work correctly with SQLAlchemy connections."""
        # Number of concurrent queries
        num_queries = 10
        
        # Setup cursor result
        cursor = mock_postgres_conn.cursor()
        cursor.description = [('id', None)]
        cursor.fetchall.return_value = [(1,)]
        
        # Track successful queries
        results = []
        errors = []
        
        def run_query(idx):
            try:
                df = select(mock_postgres_conn, f"SELECT {idx} as id")
                results.append(df)
                return True
            except Exception as e:
                errors.append(e)
                return False
        
        # Run queries in parallel
        threads = []
        for i in range(num_queries):
            thread = threading.Thread(target=run_query, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # All queries should succeed
        assert len(results) == num_queries
        assert len(errors) == 0
    
    def test_connection_validation(self, postgres_options):
        """Test that connection validation works correctly."""
        # Create engine with pre-ping enabled
        engine = get_engine_for_options(
            postgres_options,
            use_pool=True,
            pool_pre_ping=True
        )
        
        # Patch the do_ping method to test validation
        with patch.object(engine.dialect, 'do_ping') as mock_ping:
            # First connection is valid
            mock_ping.return_value = True
            conn1 = connect(postgres_options)
            assert mock_ping.called
            
            # Reset mock
            mock_ping.reset_mock()
            
            # Second connection fails validation, then succeeds
            mock_ping.side_effect = [False, True]
            conn2 = connect(postgres_options)
            
            # Should be called twice - once for the invalid connection, once for the retry
            assert mock_ping.call_count >= 1
            
            # Clean up
            conn1.close()
            conn2.close()
        
        dispose_all_engines()
