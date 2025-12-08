# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Global Conventions

@~/.config/aider/CONVENTIONS.md
@~/.config/aider/STYLE.md

## Build and Test Commands

```bash
# Install dependencies
poetry install --extras test

# Run all tests (includes coverage)
pytest

# Run unit tests only (fast, no DB required)
pytest tests/unit/

# Run integration tests (requires database)
pytest tests/integration/

# Run tests by database type
pytest -m postgres
pytest -m sqlite

# Run a single test file
pytest tests/unit/test_parameters.py

# Run a single test function
pytest tests/unit/test_parameters.py::test_function_name

# Skip slow/integration tests
pytest -m "not slow and not integration"

# Run with verbose output
pytest -v --log-cli-level=DEBUG
```

## Architecture Overview

This is a Python database abstraction library providing a unified interface for PostgreSQL and SQLite.

### Module Structure

```
src/database/
├── __init__.py       # Public API exports (see below)
├── connection.py     # ConnectionWrapper, connect(), engine/pool management
├── transaction.py    # Transaction context manager, auto-commit control
├── query.py          # Query operations: select, select_row, select_scalar, execute
├── data.py           # Data operations: insert, update, delete, SQL generation
├── schema.py         # Schema operations: get_columns, get_primary_keys
├── cursor.py         # DB-API 2.0 cursor with batch execution
├── types.py          # TypeConverter, Row adapters, Column metadata
├── sql.py            # SQL processing, parameter handling, placeholder conversion
├── upsert.py         # INSERT OR UPDATE logic for PostgreSQL and SQLite
├── cache.py          # TTL caching for schema metadata
├── options.py        # DatabaseOptions dataclass, data loaders
├── exceptions.py     # Custom exception hierarchy
└── strategy/         # Database-specific implementations (PostgreSQL, SQLite)
    ├── __init__.py   # Strategy factory: get_db_strategy()
    ├── base.py       # DatabaseStrategy abstract base class
    ├── postgres.py   # PostgreSQL-specific operations
    └── sqlite.py     # SQLite-specific operations
```

### Key Design Patterns

**Strategy Pattern** (`strategy/`): Each database has its own strategy class inheriting from `DatabaseStrategy`. The strategy is selected automatically based on connection type. Schema operations are cached via `@cacheable_strategy`.

**Single Conversion Principle** (`types.py`): Type conversion happens once in each direction:
- Inbound: `TypeConverter` during parameter binding in cursor
- Outbound: Database driver adapters only (no application-level post-processing)

**Transaction Management** (`transaction.py`): Thread-local state tracking prevents nested transactions. Auto-commit is controlled via `disable_auto_commit()`/`enable_auto_commit()` context.

### Critical Files

- `src/database/sql.py`: SQL processing, parameter handling, `%s`/`?` placeholder conversion
- `src/database/types.py`: TypeConverter, Row adapters, Column metadata, type resolution
- `src/database/cursor.py`: DB-API 2.0 cursor implementation with batch execution
- `src/database/upsert.py`: Complex INSERT OR UPDATE logic for PostgreSQL and SQLite

### Connection Flow

1. `connect()` creates `ConnectionWrapper` around SQLAlchemy engine
2. SQLAlchemy handles connection pooling (QueuePool)
3. Raw DBAPI connection accessible via `.connection` property
4. `DatabaseOptions` dataclass configures all connection parameters

### Test Fixtures

- `tests/fixtures/mocks.py`: Comprehensive mock connections for PostgreSQL and SQLite - use these for unit tests
- `tests/fixtures/postgres.py`, `tests/fixtures/sqlite.py`: Database-specific fixtures for integration tests
- pytest markers: `postgres`, `sqlite`, `unit`, `integration`, `slow`

### Public API

```python
# Connection
connect(options)

# Queries
select(cn, sql, *args)           # Returns DataFrame
select_row(cn, sql, *args)       # Returns single row (raises if not exactly 1)
select_row_or_none(cn, sql, *args)
select_scalar(cn, sql, *args)    # Returns single value (raises if not exactly 1)
select_scalar_or_none(cn, sql, *args)
select_column(cn, sql, *args)    # Returns list of first column values
execute(cn, sql, *args)          # Returns rowcount

# Data operations
insert(cn, sql, *args)
insert_row(cn, table, data)
insert_rows(cn, table, rows)
update(cn, sql, *args)
update_row(cn, table, data, keys)
update_or_insert(cn, table, data, keys)
delete(cn, sql, *args)
upsert_rows(cn, table, rows, conflict_columns)

# Transactions
with transaction(cn) as tx:
    tx.execute(sql, *args)
```

### SQL Parameter Style

The library uses `%s` placeholders for PostgreSQL (pyformat style) and `?` for SQLite (qmark style). Conversion happens automatically based on connection type. Named parameters (`%(name)s`) are also supported.
