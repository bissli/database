"""Transactions on a SQLite file, seen from a second connection.
"""
import sqlite3
import threading
import time

import database as db
import pytest

INSERT_SQL = 'insert into test_table (name, value) values (?, ?)'
UPDATE_SQL = 'update test_table set value = ? where name = ?'
VALUE_SQL = 'select value from test_table where name = ?'


@pytest.fixture
def sqlite_file_db(tmp_path):
    """(conn, path) of a file database staging Alice 10, Bob 20 and Charlie 30.
    """
    path = str(tmp_path / 'transactions.db')
    conn = db.connect({'drivername': 'sqlite', 'database': path})
    db.execute(conn, """
create table test_table (
    id integer primary key,
    name text unique,
    value integer
)
""")
    db.execute(conn, """
insert into test_table (name, value) values
('Alice', 10),
('Bob', 20),
('Charlie', 30)
""")
    yield conn, path
    conn.close()


def test_sqlite_transaction_commit(sqlite_file_db):
    """Verify qmark statements in a block run and persist after it.

    Mutation: Transaction.execute dropping its values, or a clean rollback.
    Oracle: hand-written rows for the inserted and updated names.
    """
    conn, _ = sqlite_file_db

    with db.transaction(conn) as tx:
        tx.execute(INSERT_SQL, 'David', 40)
        tx.execute(UPDATE_SQL, 25, 'Bob')

    assert db.select_scalar(conn, VALUE_SQL, 'Bob') == 25
    assert db.select_scalar(conn, VALUE_SQL, 'David') == 40


def test_sqlite_transaction_rollback(sqlite_file_db):
    """Verify a failing statement rolls back the block's earlier writes.

    Mutation: __exit__ committing on an exception, or auto-commit left on.
    Oracle: Bob's staged 20, after a second statement breaks the unique key.
    """
    conn, _ = sqlite_file_db

    with pytest.raises(sqlite3.IntegrityError), db.transaction(conn) as tx:
        tx.execute(UPDATE_SQL, 999, 'Bob')
        tx.execute(INSERT_SQL, 'Alice', 100)

    assert db.select_scalar(conn, VALUE_SQL, 'Bob') == 20


def test_sqlite_rollback_undoes_ddl_ahead_of_any_dml(sqlite_file_db):
    """Verify a rollback undoes a rename and a create run before any DML.

    Mutation: disable_autocommit leaving BEGIN to the driver's implicit
        isolation_level 'DEFERRED', which opens a transaction before DML only.
    Oracle: the fixture's one table, test_table, under its own name.
    """
    conn, _ = sqlite_file_db

    with pytest.raises(RuntimeError), db.transaction(conn) as tx:
        tx.execute('alter table test_table rename to renamed_table')
        tx.execute('create table extra_table (x integer)')
        raise RuntimeError('after the DDL')

    assert conn.list_tables() == ['test_table']


def test_sqlite_rollback_undoes_a_write_after_sqlite_rolled_back(sqlite_file_db):
    """Verify a write after SQLite's own mid-block rollback still rolls back.

    Mutation: disable_autocommit setting isolation_level None, so the driver
        opens no new transaction after `insert or rollback` ends the first.
    Oracle: the fixture's three staged names, without David.
    """
    conn, _ = sqlite_file_db

    with pytest.raises(RuntimeError):
        with db.transaction(conn) as tx:
            with pytest.raises(sqlite3.IntegrityError):
                tx.execute(
                    'insert or rollback into test_table (name, value) values (?, ?)',
                    'Alice', 1)
            tx.execute(INSERT_SQL, 'David', 40)
            raise RuntimeError('after the write')

    names = db.select_column(conn, 'select name from test_table order by name')
    assert names == ['Alice', 'Bob', 'Charlie']


def test_sqlite_isolation_levels(sqlite_file_db):
    """Verify another connection sees a block's write only after commit.

    Mutation: __enter__ leaving auto-commit on, so the update commits at once.
    Oracle: a second connection reads 10 inside the block and 15 after it.
    """
    conn, db_path = sqlite_file_db
    other = db.connect({'drivername': 'sqlite', 'database': db_path})

    try:
        with db.transaction(conn) as tx:
            tx.execute(UPDATE_SQL, 15, 'Alice')
            assert db.select_scalar(other, VALUE_SQL, 'Alice') == 10

        assert db.select_scalar(other, VALUE_SQL, 'Alice') == 15
    finally:
        other.close()


def test_sqlite_write_contention(sqlite_file_db):
    """Verify a second writer waits for the first block to commit.

    Mutation: __enter__ leaving auto-commit on, which frees the lock early.
    Oracle: commit order [1, 2] and the second writer's 200 as final value.
    """
    conn, db_path = sqlite_file_db
    other = db.connect({'drivername': 'sqlite', 'database': db_path})
    first_started = threading.Event()
    errors = []
    commit_order = []

    def first_writer():
        try:
            with db.transaction(conn) as tx:
                tx.execute(UPDATE_SQL, 100, 'Alice')
                first_started.set()
                time.sleep(0.5)
                commit_order.append(1)
        except Exception as e:
            errors.append(e)

    def second_writer():
        try:
            first_started.wait(timeout=1.0)
            with db.transaction(other) as tx:
                tx.execute(UPDATE_SQL, 200, 'Alice')
                commit_order.append(2)
        except Exception as e:
            errors.append(e)

    try:
        threads = [
            threading.Thread(target=first_writer),
            threading.Thread(target=second_writer),
            ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5.0)

        assert errors == []
        assert commit_order == [1, 2]
        assert db.select_scalar(conn, VALUE_SQL, 'Alice') == 200
    finally:
        other.close()


if __name__ == '__main__':
    __import__('pytest').main([__file__])
