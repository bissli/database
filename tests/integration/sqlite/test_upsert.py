"""
SQLite-specific upsert tests.

Note: Common upsert tests are in tests/integration/common/test_upsert.py
This file contains only SQLite-specific tests (e.g., rowid behavior).
"""
import database as db
import pytest


@pytest.mark.sqlite
def test_upsert_rowid(sl_conn):
    """Test SQLite's rowid handling during upsert.

    This is SQLite-specific because it tests rowid preservation,
    which is unique to SQLite's architecture.
    """
    # Create a new table specifically for this test that relies on rowid
    # Note: This table has no explicit PRIMARY KEY, only a UNIQUE constraint on name
    db.execute(sl_conn, 'DROP TABLE IF EXISTS test_rowid_table')
    db.execute(sl_conn, """
        CREATE TABLE test_rowid_table (
            name TEXT UNIQUE NOT NULL,
            value INTEGER NOT NULL
        )
    """)

    try:
        # Insert initial data
        db.insert(sl_conn, 'INSERT INTO test_rowid_table (name, value) VALUES (%s, %s)', 'First', 100)
        db.insert(sl_conn, 'INSERT INTO test_rowid_table (name, value) VALUES (%s, %s)', 'Second', 200)

        # Get rowids of initial data
        first_rowid = db.select_scalar(sl_conn, 'SELECT rowid FROM test_rowid_table WHERE name = %s', 'First')
        second_rowid = db.select_scalar(sl_conn, 'SELECT rowid FROM test_rowid_table WHERE name = %s', 'Second')

        # Insert a new row using upsert (uses UNIQUE column 'name' for conflict detection)
        new_rows = [{'name': 'Third', 'value': 300}]
        db.upsert_rows(sl_conn, 'test_rowid_table', new_rows)

        # Update an existing row (uses UNIQUE column 'name' for conflict detection)
        update_rows = [{'name': 'First', 'value': 150}]
        db.upsert_rows(sl_conn, 'test_rowid_table', update_rows, update_cols_always=['value'])

        # Verify the rowid is preserved after update
        first_rowid_after = db.select_scalar(sl_conn, 'SELECT rowid FROM test_rowid_table WHERE name = %s', 'First')
        assert first_rowid == first_rowid_after, 'SQLite rowid should be preserved during upsert'

        # Verify the value was updated
        first_value = db.select_scalar(sl_conn, 'SELECT value FROM test_rowid_table WHERE name = %s', 'First')
        assert first_value == 150, 'Value should be updated to 150'

        # Get the rowid of the third row
        third_rowid = db.select_scalar(sl_conn, 'SELECT rowid FROM test_rowid_table WHERE name = %s', 'Third')
        assert third_rowid > second_rowid, 'New row should have higher rowid'
    finally:
        db.execute(sl_conn, 'DROP TABLE IF EXISTS test_rowid_table')


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
