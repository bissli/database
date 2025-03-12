import database as db
import pytest
from database.adapters.structure import RowStructureAdapter


@pytest.fixture
def sqlite_conn():
    """Create an SQLite database connection for testing adapters"""
    # Create connection
    conn = db.connect({
        'drivername': 'sqlite',
        'database': ':memory:'
    })

    # Create test schema with primary key and constraints
    create_table = """
    CREATE TABLE test_table (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        value INTEGER NOT NULL
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

    yield conn
    conn.close()


def test_sqlite_row_adapter(sqlite_conn):
    """Test SQLite row adapter functionality"""
    # Fetch a row directly using the SQLite cursor
    cursor = sqlite_conn.cursor()
    cursor.execute("SELECT * FROM test_table WHERE name = 'Alice'")
    sqlite_row = cursor.fetchone()

    # Use the adapter
    adapter = RowStructureAdapter.create(sqlite_conn, sqlite_row)

    # Test to_dict
    row_dict = adapter.to_dict()
    assert row_dict['name'] == 'Alice'
    assert row_dict['value'] == 10

    # Test get_value with specific key
    assert adapter.get_value('name') == 'Alice'

    # Test get_value without key (should return first value)
    assert adapter.get_value() == 1  # id column

    # Test to_attrdict
    attr_dict = adapter.to_attrdict()
    assert attr_dict.name == 'Alice'
    assert attr_dict.value == 10


def test_sqlite_adapter_in_select_column(sqlite_conn):
    """Test adapter is used properly in select_column"""
    # Get a column using select_column
    names = db.select_column(sqlite_conn, 'SELECT name FROM test_table ORDER BY id')
    assert names == ['Alice', 'Bob', 'Charlie']

    # Get a column with just one value
    name = db.select_column(sqlite_conn, 'SELECT name FROM test_table WHERE id = 1')
    assert name == ['Alice']

    # Get a numeric column
    values = db.select_column(sqlite_conn, 'SELECT value FROM test_table ORDER BY id')
    assert values == [10, 20, 30]


def test_sqlite_adapter_in_select_scalar(sqlite_conn):
    """Test adapter is used properly in select_scalar"""
    # Get a scalar value
    name = db.select_scalar(sqlite_conn, 'SELECT name FROM test_table WHERE id = 1')
    assert name == 'Alice'

    # Get a numeric scalar
    value = db.select_scalar(sqlite_conn, 'SELECT value FROM test_table WHERE id = 1')
    assert value == 10

    # Test with no results should fail with assertion error
    with pytest.raises(AssertionError):
        db.select_scalar(sqlite_conn, 'SELECT name FROM test_table WHERE id = 999')


def test_sqlite_adapter_in_select_row(sqlite_conn):
    """Test adapter is used properly in select_row"""
    # Get a row
    row = db.select_row(sqlite_conn, 'SELECT * FROM test_table WHERE id = 1')
    assert row.name == 'Alice'
    assert row.value == 10

    # Test with no results should fail with assertion error
    with pytest.raises(AssertionError):
        db.select_row(sqlite_conn, 'SELECT * FROM test_table WHERE id = 999')


if __name__ == '__main__':
    __import__('pytest').main([__file__])
