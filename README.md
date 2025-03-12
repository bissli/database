# Database Module

A comprehensive Python database interface supporting PostgreSQL, SQLite, and SQL Server with a consistent API.

[![License: OSL-3.0](https://img.shields.io/badge/License-OSL--3.0-blue.svg)](https://opensource.org/licenses/OSL-3.0)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

For detailed documentation, see the [API Reference](docs/README.md)

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Concepts](#core-concepts)
  - [Connections](#connections)
  - [Queries](#queries)
  - [Transactions](#transactions)
- [Common Usage Patterns](#common-usage-patterns)
  - [Basic Data Operations](#basic-data-operations)
  - [Error Handling](#error-handling)

## Installation

```bash
pip install git+https://github.com/bissli/database
```

For detailed installation options, see the [Installation section in the full documentation](docs/README.md#installation).

## Quick Start

```python
import database as db

# Connect to a database
cn = db.connect({
    'drivername': 'postgresql',  # or 'sqlite', 'mssql'
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

For more connection options and advanced queries, see the [detailed documentation](docs/README.md#connection-management).

## Core Concepts

### Connections

The Database Module provides a unified connection interface for different database types:

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

# SQL Server connection
mssql_cn = db.connect({
    'drivername': 'mssql',
    'database': 'your_database',
    'hostname': 'localhost',
    'username': 'your_username',
    'password': 'your_password',
    'port': 1433
})
```

For more connection options including pooling, see the [Connection Management documentation](docs/README.md#connection-management).

### Queries

The module provides consistent query functions across all database backends:

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

For more query operations, see the [Query Operations documentation](docs/README.md#query-operations).

### Transactions

Use the transaction context manager for atomic operations:

```python
with db.transaction(cn) as tx:
    # All operations in this block are part of a single transaction
    tx.execute('INSERT INTO users (name, email) VALUES (%s, %s)', 
              'John Doe', 'john@example.com')
    
    tx.execute('INSERT INTO user_roles (user_id, role) VALUES (%s, %s)', 
              1, 'admin')
    
    # If any operation fails, all changes are rolled back
```

For transaction isolation levels and advanced features, see the [Transaction Management documentation](docs/README.md#transaction-management).

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

For more advanced features and detailed API documentation, see the [complete documentation](docs/README.md#advanced-features).
