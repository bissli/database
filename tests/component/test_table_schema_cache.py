"""Tests for the per-engine table schema cache in ConnectionWrapper.
"""
import gc

import database as db
import pytest


def test_schema_cache_never_serves_a_collected_engines_entry():
    """Verify a new engine never reads columns cached for a collected one.

    Mutation: the cache keyed by id(self.engine), which CPython reuses
        once the engine is collected.
    Oracle: each connection's own table, with a column name unique to it.
    """
    for idx in range(200):
        cn = db.connect({'drivername': 'sqlite', 'database': ':memory:'})
        db.execute(cn, f'create table t (c{idx} integer primary key)')

        assert cn.get_table_columns('t') == [f'c{idx}']
        assert cn.get_table_primary_keys('t') == [f'c{idx}']

        cn.close()
        del cn
        gc.collect()


if __name__ == '__main__':
    pytest.main([__file__])
