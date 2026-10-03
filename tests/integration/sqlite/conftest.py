"""Fixtures for SQLite-specific integration tests.
"""
import time

import database as db
import pytest


@pytest.fixture
def test_table_prefix():
    """Table name prefix carrying the current second.
    """
    return f'test_autocommit_{int(time.time())}'


@pytest.fixture
def sqlite_file_conn(tmp_path):
    """Connection to a file database under tmp_path, test_table, three rows.
    """
    conn = db.connect({
        'drivername': 'sqlite',
        'database': str(tmp_path / 'test_sqlite.db'),
        })
    db.execute(conn, """
create table test_table (
    id integer primary key,
    name text not null unique,
    value integer not null
)
""")
    db.execute(conn, """
insert into test_table (name, value) values
('Alice', 10),
('Bob', 20),
('Charlie', 30)
""")

    yield conn

    conn.close()
