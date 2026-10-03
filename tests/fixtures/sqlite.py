import database as db
import pytest


@pytest.fixture
def sl_conn():
    """In-memory SQLite connection with test_table seeded with three rows.
    """
    create_table = """
create table test_table (
    id integer primary key,
    name text not null unique,
    value integer not null
)
"""
    insert_rows = """
insert into test_table (name, value) values
('Alice', 10),
('Bob', 20),
('Charlie', 30)
"""
    conn = db.connect({'drivername': 'sqlite', 'database': ':memory:'})
    db.execute(conn, create_table)
    db.execute(conn, insert_rows)

    yield conn
    conn.close()
