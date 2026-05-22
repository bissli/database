"""
SQLite in-memory connections must share a single DBAPI connection across
reconnects, so data written before a reconnect survives. NullPool would
silently hand out a fresh, empty database on reconnect — a footgun.
"""

import database as db
import pytest
import sqlalchemy as sa


@pytest.mark.sqlite
@pytest.mark.integration
def test_memory_db_data_survives_reconnect():
    """An in-memory SQLite connection must keep its data after sa_connection.close().

    Without StaticPool, the reconnect in ConnectionWrapper.cursor() pulls a
    brand-new (empty) in-memory database from the engine and silently loses
    everything written before the close.
    """
    cn = db.connect({'drivername': 'sqlite', 'database': ':memory:'})
    try:
        db.execute(cn, 'CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)')
        db.execute(cn, "INSERT INTO t (name) VALUES ('alice')")

        cn.sa_connection.close()
        cn.cursor()

        names = db.select_column(cn, 'SELECT name FROM t ORDER BY id')
        assert names == ['alice']
    finally:
        cn.close()


@pytest.mark.sqlite
@pytest.mark.integration
def test_memory_db_uses_static_pool():
    """The engine for ':memory:' must use StaticPool, not NullPool."""
    cn = db.connect({'drivername': 'sqlite', 'database': ':memory:'})
    try:
        assert isinstance(cn.engine.pool, sa.pool.StaticPool)
    finally:
        cn.close()


@pytest.mark.sqlite
@pytest.mark.integration
def test_file_db_still_uses_null_pool(tmp_path):
    """File-based SQLite must keep using NullPool (the historical default)."""
    db_file = tmp_path / 'pool_test.db'
    cn = db.connect({'drivername': 'sqlite', 'database': str(db_file)})
    try:
        assert isinstance(cn.engine.pool, sa.pool.NullPool)
    finally:
        cn.close()
