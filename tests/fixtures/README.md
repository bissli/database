# Test Fixtures

This directory contains pytest fixtures for the database library test suite.

## Fixture Files

| File | Purpose |
|------|---------|
| `mocks.py` | Mock connection fixtures for unit tests |
| `postgres.py` | Real PostgreSQL fixtures via testcontainers |
| `sqlite.py` | Real SQLite in-memory fixtures |
| `sqlserver.py` | SQL Server fixtures (optional) |
| `values.py` | Common test value fixtures |

## Connection Fixtures

### Real Database Connections

| Fixture | Database | Scope | Use Case |
|---------|----------|-------|----------|
| `pg_conn` | PostgreSQL | function | Integration tests requiring real PostgreSQL |
| `sl_conn` | SQLite | function | Integration tests requiring real SQLite |
| `psql_docker` | PostgreSQL | session | Container management (used by `pg_conn`) |
| `sqlite_file_conn` | SQLite | function | File-based SQLite with test data |

### Mock Connections

| Fixture | Purpose |
|---------|---------|
| `simple_mock_postgresql_connection` | Lightweight PostgreSQL mock for type detection |
| `simple_mock_sqlite_connection` | Lightweight SQLite mock for type detection |
| `simple_mock_unknown_connection` | Unknown DB type for error testing |
| `create_simple_mock_connection` | Factory fixture for creating mock connections |
| `mock_postgres_conn` | Full PostgreSQL mock with patching |
| `mock_sqlite_conn` | Full SQLite mock with patching |

## Parametrized Fixtures

The `tests/integration/common/conftest.py` provides:

| Fixture | Purpose |
|---------|---------|
| `db_conn` | Runs tests against both PostgreSQL and SQLite |
| `dialect` | Returns dialect name from connection |

## Usage Guidelines

### Unit Tests
Use mock fixtures from `mocks.py`:
```python
def test_type_detection(simple_mock_postgresql_connection):
    # Fast, no real database required
    assert is_postgresql_connection(simple_mock_postgresql_connection)
```

### Integration Tests
Use real connection fixtures:
```python
@pytest.mark.postgres
def test_postgres_feature(pg_conn):
    result = db.select(pg_conn, "SELECT 1")
    assert len(result) == 1

@pytest.mark.sqlite
def test_sqlite_feature(sl_conn):
    result = db.select(sl_conn, "SELECT 1")
    assert len(result) == 1
```

### Cross-Database Tests
Use the parametrized `db_conn` fixture:
```python
# tests/integration/common/test_example.py
def test_select_works_on_both(db_conn):
    # Runs twice: once with PostgreSQL, once with SQLite
    result = db.select(db_conn, "SELECT 1 as value")
    assert len(result) == 1
```

## Test Data

Both `pg_conn` and `sl_conn` create a `test_table` with:
```sql
CREATE TABLE test_table (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    value INTEGER NOT NULL
);

-- Data:
-- (1, 'Alice', 10)
-- (2, 'Bob', 20)
-- (3, 'Charlie', 30)
```

## Markers

Use pytest markers to control which tests run:
```bash
pytest -m postgres     # PostgreSQL tests only
pytest -m sqlite       # SQLite tests only
pytest -m "not slow"   # Skip slow tests
```
