"""Pool choice for SQLite engines.
"""
import database as db
import pytest
import sqlalchemy as sa

pytestmark = [pytest.mark.sqlite, pytest.mark.integration]


def test_memory_db_data_survives_reconnect():
    """Verify an in-memory database keeps its rows across a reconnect.

    Mutation: NullPool for ':memory:'.
    Oracle: the one row written before sa_connection.close().
    """
    cn = db.connect({'drivername': 'sqlite', 'database': ':memory:'})
    try:
        db.execute(cn, 'create table t (id integer primary key, name text)')
        db.execute(cn, "insert into t (name) values ('alice')")

        cn.sa_connection.close()
        cn.cursor()

        assert db.select_column(cn, 'select name from t order by id') == ['alice']
    finally:
        cn.close()


@pytest.mark.parametrize(('database', 'pool_class'), [
    (':memory:', sa.pool.StaticPool),
    ('pool_test.db', sa.pool.NullPool),
    ])
def test_engine_pool_class_follows_the_database(tmp_path, database, pool_class):
    """Verify ':memory:' takes StaticPool and a file takes NullPool.

    Mutation: StaticPool for every SQLite database.
    Oracle: the pool class each kind of database needs, by hand.
    """
    if database != ':memory:':
        database = str(tmp_path / database)
    cn = db.connect({'drivername': 'sqlite', 'database': database})
    try:
        assert type(cn.engine.pool) is pool_class
    finally:
        cn.close()
