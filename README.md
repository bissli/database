# Database Module

A Python database interface for PostgreSQL and SQLite with one API.

[![License: OSL-3.0](https://img.shields.io/badge/License-OSL--3.0-blue.svg)](https://opensource.org/licenses/OSL-3.0)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

The [API Reference](docs/README.md) holds the detailed documentation.

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Concepts](#core-concepts)
  - [Connections](#connections)
  - [Reader Endpoints](#reader-endpoints)
  - [Queries](#queries)
  - [Transactions](#transactions)
- [Common Usage Patterns](#common-usage-patterns)
  - [Basic Data Operations](#basic-data-operations)
  - [Error Handling](#error-handling)

## Installation

```bash
pip install git+https://github.com/bissli/database
```

The [Installation section of the full documentation](docs/README.md#installation) lists the dependencies.

## Quick Start

```python
import database as db

# Connect to a database
cn = db.connect({
    'drivername': 'postgresql',  # or 'sqlite'
    'database': 'your_database',
    'hostname': 'localhost',
    'username': 'your_username',
    'password': 'your_password',
    'port': 5432
})

# Execute a simple query
result = db.select(cn, 'SELECT * FROM users WHERE active = %s', True)
print(result)  # Returns pandas DataFrame

# Insert data
db.insert(cn, 'INSERT INTO users (name, email) VALUES (%s, %s)',
          'John Doe', 'john@example.com')

# Update data
db.update(cn, 'UPDATE users SET active = %s WHERE id = %s', False, 42)

# Close the connection when done
cn.close()
```

[Connection Management](docs/README.md#connection-management) covers the other connection options.

## Core Concepts

### Connections

One `connect()` call opens either database:

```python
import database as db

# PostgreSQL connection
pg_cn = db.connect({
    'drivername': 'postgresql',
    'database': 'your_database',
    'hostname': 'localhost',
    'username': 'your_username',
    'password': 'your_password',
    'port': 5432
})

# SQLite connection
sqlite_cn = db.connect({
    'drivername': 'sqlite',
    'database': 'database.db'  # or ':memory:' for in-memory database
})
```

Pooling and the other options are in [Connection Management](docs/README.md#connection-management).

### Reader Endpoints

A cluster that publishes a separate reader endpoint can serve read-only
work from its replicas instead of its writer:

```python
import database as db

options = {
    'drivername': 'postgresql',
    'database': 'your_database',
    'hostname': 'cluster.cluster-abc123.us-east-1.rds.amazonaws.com',
    'reader_hostname': 'cluster.cluster-ro-abc123.us-east-1.rds.amazonaws.com',
    'username': 'your_username',
    'password': 'your_password',
    'port': 5432
}

reader = db.connect(options, role='reader')

db.select(reader, 'SELECT count(*) FROM orders')   # served by a replica
db.insert_row(reader, 'orders', ('id',), (1,))      # raises ReadOnlyError
db.execute(reader, 'DELETE FROM orders')           # refused by the server
```

A reader is read-only in two places. The library refuses its own write
methods in process, before a statement exists. The session is read-only on
the server, so a statement written by hand is refused there, at statement
start, before a row moves. A reader may not change that session setting.

A database declaring no reader endpoint still answers `role='reader'`,
falling back to the writer endpoint and keeping both. `role` is keyword
only, and defaults to `'writer'`, so an existing caller is unaffected.

SQLite has no reader endpoint, so the reader host values are ignored there
and `role='reader'` opens the same file read-only. A SQLite reader also
takes `open_mode`, `'ro'` or `'immutable'`. Either raises at connect on a
missing file and creates nothing. `'immutable'` also takes no lock on the
file. Both are described in the
[SQLite open modes documentation](docs/README.md#sqlite-open-modes).

[Reader Endpoints](docs/README.md#reader-endpoints-and-read-only-connections)
covers endpoint selection, the full list of refused operations, and
pooling.

### Queries

The query functions are the same on both backends:

```python
# Basic SELECT query returning a pandas DataFrame
users = db.select(cn, 'SELECT * FROM users WHERE active = %s', True)

# Get a single row as an attribute dictionary
user = db.select_row(cn, 'SELECT * FROM users WHERE id = %s', 42)
print(user.name)  # Access columns as attributes

# Get a single value
count = db.select_scalar(cn, 'SELECT COUNT(*) FROM users')

# Get a single column as a list
emails = db.select_column(cn, 'SELECT email FROM users')
```

`select_row` and `select_scalar` raise `db.ValidationError` when the
query returns zero rows or more than one. `select_row_or_none` and
`select_scalar_or_none` return `None` on zero rows, and still raise
`db.ValidationError` on more than one.

[Query Operations](docs/README.md#query-operations) covers the rest.

### Bulk Operations

```python
# Upsert: INSERT ... ON CONFLICT DO UPDATE, on the primary key by default
db.upsert_rows(cn, 'users',
               ({'email': 'a@example.com', 'name': 'Alice'},
                {'email': 'b@example.com', 'name': 'Bob'}),
               conflict_columns=['email'],
               update_cols_always=['name'])

# COPY FROM (PostgreSQL only) - bulk load from a CSV file
with open('users.csv') as f:
    db.copy_from(cn, 'users', f, columns=['email', 'name'])
```

### Transactions

The `transaction` context manager runs a block as one transaction:

```python
with db.transaction(cn) as tx:
    # All operations in this block are part of a single transaction
    tx.execute('INSERT INTO users (name, email) VALUES (%s, %s)', 
              'John Doe', 'john@example.com')
    
    tx.execute('INSERT INTO user_roles (user_id, role) VALUES (%s, %s)', 
              1, 'admin')
    
    # If any operation fails, all changes are rolled back
```

Outside a block each statement commits as soon as it finishes.
[Transaction Management](docs/README.md#transaction-management) covers
`RETURNING` values and isolation levels.

## Common Usage Patterns

### Basic Data Operations

```python
import database as db

# Connect to database
cn = db.connect({
    'drivername': 'postgresql',
    'database': 'your_database',
    'hostname': 'localhost',
    'username': 'your_username',
    'password': 'your_password'
})

# Query data
users = db.select(cn, 'SELECT * FROM users WHERE active = %s', True)

# Insert data
db.insert(cn, 'INSERT INTO users (name, email) VALUES (%s, %s)',
         'Jane Smith', 'jane@example.com')

# Update data
db.update(cn, 'UPDATE users SET active = %s WHERE email = %s',
         False, 'jane@example.com')

# Delete data
db.delete(cn, 'DELETE FROM users WHERE active = %s', False)

# Close connection when done
cn.close()
```

### Error Handling

```python
try:
    # Execute query that might fail
    db.execute(cn, 'INSERT INTO users (email) VALUES (%s)', 'duplicate@example.com')
except db.UniqueViolation:
    # Handle duplicate key error
    print("User with this email already exists")
except db.IntegrityError as e:
    # Handle other constraint violations
    print(f"Constraint violation: {e}")
except db.DatabaseError as e:
    # Handle any database error
    print(f"Database error: {e}")
finally:
    # Always close connection
    cn.close()
```

[Advanced Features](docs/README.md#advanced-features) covers data
loaders, caching, and parameter handling.
