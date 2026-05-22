"""
Database-agnostic tests for empty query result handling.

Runs against both PostgreSQL and SQLite via the parametrized `db_conn`
fixture.
"""
import database as db


def _columns(result):
    """Return the column names from a select result regardless of loader.

    Default pandas loader returns a DataFrame; iterdict loader returns
    a list of dicts. Tests need to inspect column names in both shapes.
    """
    if hasattr(result, 'columns'):
        return list(result.columns)
    if result and hasattr(result[0], 'keys'):
        return list(result[0].keys())
    return []


def test_empty_results_handling(db_conn):
    """Empty selects return an empty (but well-typed) result.

    select_scalar_or_none against COUNT(*) returns 0 (zero rows would
    be wrong here; the aggregate is what's empty-but-zero, not the
    overall result).
    """
    result = db.select(db_conn, 'SELECT * FROM test_table WHERE 1=0')
    assert result is not None
    assert len(result) == 0

    result = db.select_column(db_conn, 'SELECT name FROM test_table WHERE 1=0')
    assert result is not None
    assert isinstance(result, list)
    assert len(result) == 0

    result = db.select_row_or_none(db_conn, 'SELECT * FROM test_table WHERE 1=0')
    assert result is None

    result = db.select_scalar_or_none(db_conn, 'SELECT COUNT(*) FROM test_table WHERE 1=0')
    assert result == 0

    with db.transaction(db_conn) as tx:
        result = tx.select('SELECT * FROM test_table WHERE 1=0')
        assert result is not None
        assert len(result) == 0

        column = tx.select_column('SELECT name FROM test_table WHERE 1=0')
        assert column is not None
        assert isinstance(column, list)
        assert len(column) == 0


def test_empty_results_key_preservation(db_conn, dialect):
    """Selecting from a populated table preserves the SELECT-list column order.

    Empty results don't carry column info on every backend/loader combo,
    so we verify the populated path: column order from SELECT is reflected
    in the returned row's key order.
    """
    if dialect == 'postgresql':
        ts_type = 'TIMESTAMP'
    else:
        ts_type = 'TEXT'  # SQLite stores timestamps as text via converters

    with db.transaction(db_conn) as tx:
        tx.execute('DROP TABLE IF EXISTS empty_test')
        tx.execute(f"""
            CREATE TABLE empty_test (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value DOUBLE PRECISION,
                created_at {ts_type}
            )
        """)

        empty = tx.select('SELECT id, name, value, created_at FROM empty_test')
        assert len(empty) == 0

        tx.execute("INSERT INTO empty_test VALUES (1, 'test', 1.0, CURRENT_TIMESTAMP)")
        populated = tx.select('SELECT id, name, value, created_at FROM empty_test')
        assert len(populated) == 1
        assert _columns(populated) == ['id', 'name', 'value', 'created_at']

        tx.execute('DELETE FROM empty_test')
        tx.execute("INSERT INTO empty_test VALUES (2, 'test2', 2.0, CURRENT_TIMESTAMP)")
        reordered = tx.select('SELECT created_at, name, id FROM empty_test')
        assert _columns(reordered) == ['created_at', 'name', 'id']

        tx.execute('DROP TABLE empty_test')


if __name__ == '__main__':
    __import__('pytest').main([__file__])
