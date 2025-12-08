import os
import tempfile
import threading
import time

import database as db
import pytest
import pathlib


@pytest.fixture
def sqlite_file_db():
    """Create a temporary file-based SQLite database for testing transactions"""
    # Create a temporary file
    fd, path = tempfile.mkstemp(suffix='.db')
    os.close(fd)

    # Create connection
    conn = db.connect({
        'drivername': 'sqlite',
        'database': path
    })

    # Create test schema
    create_table = """
    CREATE TABLE test_table (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE,
        value INTEGER
    )
    """
    db.execute(conn, create_table)

    # Insert test data
    insert_data = """
    INSERT INTO test_table (name, value) VALUES
    ('Alice', 10),
    ('Bob', 20),
    ('Charlie', 30)
    """
    db.execute(conn, insert_data)

    yield conn, path

    # Clean up
    conn.close()
    pathlib.Path(path).unlink()


def test_sqlite_transaction_commit(sqlite_file_db):
    """Test that SQLite transactions commit properly"""
    conn, _ = sqlite_file_db

    # Start a transaction
    with db.transaction(conn) as tx:
        tx.execute('INSERT INTO test_table (name, value) VALUES (?, ?)', 'David', 40)
        tx.execute('UPDATE test_table SET value = ? WHERE name = ?', 25, 'Bob')

    # Verify changes were committed
    result = db.select(conn, "SELECT value FROM test_table WHERE name = 'Bob'")
    assert result.iloc[0]['value'] == 25

    result = db.select(conn, "SELECT * FROM test_table WHERE name = 'David'")
    assert not result.empty


def test_sqlite_transaction_rollback(sqlite_file_db):
    """Test that SQLite transactions roll back on error"""
    conn, _ = sqlite_file_db

    # Get original value
    original_value = db.select_scalar(conn, "SELECT value FROM test_table WHERE name = 'Bob'")

    # Start a transaction that will fail
    try:
        with db.transaction(conn) as tx:
            tx.execute('UPDATE test_table SET value = ? WHERE name = ?', 999, 'Bob')
            # This should fail due to unique constraint
            tx.execute('INSERT INTO test_table (name, value) VALUES (?, ?)', 'Alice', 100)
    except:
        pass  # Expected to fail

    # Verify transaction was rolled back
    current_value = db.select_scalar(conn, "SELECT value FROM test_table WHERE name = 'Bob'")
    assert current_value == original_value


def test_sqlite_isolation_levels(sqlite_file_db):
    """Test different SQLite isolation levels"""
    conn, db_path = sqlite_file_db

    # Set up a second connection to the same database
    conn2 = db.connect({
        'drivername': 'sqlite',
        'database': db_path
    })

    # By default SQLite uses "DEFERRED" transactions

    # Start a transaction in conn1
    with db.transaction(conn) as tx:
        tx.execute('UPDATE test_table SET value = ? WHERE name = ?', 15, 'Alice')

        # In a separate transaction on conn2, we should still be able to read the old value
        result = db.select(conn2, "SELECT value FROM test_table WHERE name = 'Alice'")
        assert result.iloc[0]['value'] == 10  # Still the old value

        # Commit the first transaction

    # Now conn2 should see the updated value
    result = db.select(conn2, "SELECT value FROM test_table WHERE name = 'Alice'")
    assert result.iloc[0]['value'] == 15  # New value after commit

    # Clean up
    conn2.close()


def test_sqlite_write_contention(sqlite_file_db):
    """Test SQLite behavior when multiple connections try to write"""
    conn, db_path = sqlite_file_db

    # Set up a second connection to the same database
    conn2 = db.connect({
        'drivername': 'sqlite',
        'database': db_path
    })

    # Flag to signal when first transaction has started
    tx1_started = threading.Event()
    # Flag to signal when both transactions are done
    tx_done = threading.Event()
    # Track any exceptions
    exceptions = []
    # Track commit order
    commit_order = []

    def transaction1():
        try:
            with db.transaction(conn) as tx:
                tx.execute('UPDATE test_table SET value = ? WHERE name = ?', 100, 'Alice')
                tx1_started.set()  # Signal tx1 has started
                # Hold the transaction open for a moment
                time.sleep(0.5)
                commit_order.append(1)
            # Transaction committed
        except Exception as e:
            exceptions.append(e)

    def transaction2():
        try:
            # Wait for tx1 to start
            tx1_started.wait(timeout=1.0)
            with db.transaction(conn2) as tx:
                # This will block until tx1 commits or times out
                tx.execute('UPDATE test_table SET value = ? WHERE name = ?', 200, 'Alice')
                commit_order.append(2)
            # Transaction committed
        except Exception as e:
            exceptions.append(e)
        finally:
            tx_done.set()

    # Start the transactions in separate threads
    t1 = threading.Thread(target=transaction1)
    t2 = threading.Thread(target=transaction2)
    t1.start()
    t2.start()

    # Wait for both to complete
    tx_done.wait(timeout=5.0)
    t1.join(timeout=1.0)
    t2.join(timeout=1.0)

    # Check that both transactions completed
    assert len(exceptions) == 0, f'Transactions raised exceptions: {exceptions}'

    # Check final value - second transaction should win
    final_value = db.select_scalar(conn, "SELECT value FROM test_table WHERE name = 'Alice'")
    assert final_value == 200

    # Check commit order - should be [1, 2]
    assert commit_order == [1, 2]

    # Clean up
    conn2.close()


if __name__ == '__main__':
    __import__('pytest').main([__file__])
