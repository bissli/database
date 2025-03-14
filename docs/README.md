# Database Module Documentation

[← Back to Main README](../README.md)

This document provides detailed API documentation and advanced usage information for the Database Module.

## Table of Contents

- [Installation](#installation)
- [Connection Management](#connection-management)
  - [Creating Connections](#creating-connections)
  - [Connection Options](#connection-options)
  - [Connection Pooling](#connection-pooling)
  - [Configuration File Pattern](#configuration-file-pattern)
- [Query Operations](#query-operations)
  - [Basic Operations](#basic-operations)
  - [Row and Value Operations](#row-and-value-operations)
  - [Result Handling](#result-handling)
  - [Empty Result Handling](#empty-result-handling)
  - [Type Information](#type-information)
  - [Column Static Helpers](#column-static-helpers)
  - [Stored Procedures and Multiple Result Sets](#stored-procedures-and-multiple-result-sets)
  - [SQL Parameter Handling](#sql-parameter-handling)
    - [LIKE Clauses and Percent Signs](#like-clauses-and-percent-signs)
    - [IS NULL / IS NOT NULL Handling](#is-null--is-not-null-handling)
    - [IN Clause Parameter Conventions](#in-clause-parameter-conventions)
    - [Troubleshooting Parameter Issues](#troubleshooting-parameter-issues)
- [Data Manipulation](#data-manipulation)
  - [Insert Operations](#insert-operations)
  - [Update Operations](#update-operations)
  - [Delete Operations](#delete-operations)
  - [Multiple-Row Operations](#multiple-row-operations)
- [Transaction Management](#transaction-management)
  - [Using Transactions](#using-transactions)
  - [Transaction Isolation Levels](#transaction-isolation-levels)
  - [Database-Specific Transaction Behavior](#database-specific-transaction-behavior)
- [Type System](#type-system)
  - [Type Conversion](#type-conversion)
  - [Type Handling](#type-handling)
  - [Database-Specific Type Handling](#database-specific-type-handling)
  - [Empty Result Type Handling](#empty-result-type-handling)
  - [Type Conversion Architecture](#type-conversion-architecture)
- [Schema Operations](#schema-operations)
  - [Table Sequence Operations](#table-sequence-operations)
  - [Table Maintenance Operations](#table-maintenance-operations)
  - [Database-Specific Schema Operations](#database-specific-schema-operations)
- [Advanced Features](#advanced-features)
  - [SQL Query Helpers](#sql-query-helpers)
  - [Custom Data Loaders](#custom-data-loaders)
  - [Connection Pooling](#connection-pooling)
  - [Caching](#caching)
  - [Parameter Handling](#sql-parameter-handling)
  - [Common Table Expressions (CTEs)](#common-table-expressions-ctes)
- [Database-Specific Features](#database-specific-features)
  - [PostgreSQL Features](#postgresql-features)
  - [SQLite Features](#sqlite-features)
  - [SQL Server Features](#sql-server-features)
- [API Reference](#api-reference)
  - [Core Functions](#core-functions)
  - [Query Operations](#query-operations-1)
  - [Data Operations](#data-operations)
  - [Schema Operations](#schema-operations-1)
  - [Connection Utilities](#connection-utilities)
  - [Exception Types](#exception-types)

## Installation

```bash
pip install git+https://github.com/bissli/database
```

### Dependencies

- PostgreSQL: `psycopg`
- SQLite: Included in Python standard library
- SQL Server: `pymssql`
- Data handling: `pandas`, `numpy`
- Utilities: `pyarrow` (optional)

## Connection Management

### Creating Connections

```python
import database as db

# Connect to a database with a dictionary config
cn = db.connect({
    'drivername': 'postgresql',  # or 'sqlite', 'mssql'
    'database': 'your_database',
    'hostname': 'localhost',
    'username': 'your_username',
    'password': 'your_password',
    'port': 5432
})

# SQLite connection (minimal)
sqlite_cn = db.connect({
    'drivername': 'sqlite',
    'database': 'database.db'  # use ':memory:' for in-memory database
})

# Connection with configuration object
from libb import Setting
postgres_config = Setting()
postgres_config.drivername = 'postgresql'
postgres_config.database = 'your_database'
postgres_config.hostname = 'localhost'
postgres_config.username = 'your_username'
postgres_config.password = 'your_password'
postgres_config.port = 5432

cn = db.connect(postgres_config)

# Connection with pool
pooled_cn = db.connect({
    'drivername': 'postgresql',
    'database': 'your_database',
    'hostname': 'localhost',
    'username': 'your_username',
    'password': 'your_password',
    'port': 5432
}, use_pool=True, pool_max_connections=10)
```

### Connection Options

The `DatabaseOptions` class controls connection behavior:

```python
from database.options import DatabaseOptions

options = DatabaseOptions(
    drivername='postgresql',
    hostname='localhost',
    username='your_username',
    password='your_password',
    database='your_database',
    port=5432,
    timeout=30,
    appname='my_application',  # Application name for connection
    cleanup=True,              # Auto-close on garbage collection
    check_connection=True,     # Enable auto-reconnect
    data_loader=None,          # Custom data loader function (defaults to pandas)
    # Connection pooling parameters
    use_pool=False,            # Enable connection pooling
    pool_max_connections=5,    # Maximum connections in pool
    pool_max_idle_time=300,    # Maximum seconds a connection can be idle
    pool_wait_timeout=30       # Maximum seconds to wait for a connection
)

cn = db.connect(options)
```

### Configuration File Pattern

A common pattern is to create a module-level config.py file with `Setting` objects for different database environments:

```python
# config.py
from database.options import iterdict_data_loader
from libb import Setting

Setting.unlock()

# PostgreSQL configuration
postgresql = Setting()
postgresql.drivername = 'postgresql'
postgresql.hostname = 'localhost'
postgresql.username = 'postgres'
postgresql.password = 'postgres'
postgresql.database = 'test_db'
postgresql.port = 5432
postgresql.timeout = 30
postgresql.data_loader = iterdict_data_loader
postgresql.use_pool = False

# SQLite configuration
sqlite = Setting()
sqlite.drivername = 'sqlite'
sqlite.database = 'database.db'
sqlite.data_loader = iterdict_data_loader
sqlite.use_pool = False

# SQL Server configuration
mssql = Setting()
mssql.drivername = 'mssql'
mssql.hostname = 'localhost'
mssql.username = 'sa'
mssql.password = 'StrongPassword123!'
mssql.database = 'master'
mssql.port = 1433
mssql.timeout = 30
mssql.driver = 'ODBC Driver 18 for SQL Server'
mssql.data_loader = iterdict_data_loader

Setting.lock()
```

Then import these settings to connect to different environments:

```python
# Import from config file
from config import postgresql, sqlite, mssql

# Connect to PostgreSQL
pg_cn = db.connect(postgresql)

# Connect to SQLite
lite_cn = db.connect(sqlite)

# Connect to SQL Server
ms_cn = db.connect(mssql)

# You can also override settings temporarily
from copy import copy
temp_config = copy(postgresql)
temp_config.use_pool = True
temp_config.pool_max_connections = 10
cn_pool = db.connect(temp_config)
```

This pattern provides:
1. Separation of configuration from code
2. Centralized management of connection settings
3. Easy switching between development/testing/production environments
4. Type safety through the `Setting` object's structure

### Connection Pooling

Connection pooling can be configured either through the `DatabaseOptions` object or as parameters to `connect()`:

```python
# Method 1: Using DatabaseOptions
options = DatabaseOptions(
    drivername='postgres',
    database='your_database',
    hostname='localhost',
    username='your_username',
    password='your_password',
    port=5432,
    # Pooling configuration
    use_pool=True,
    pool_max_connections=10,
    pool_max_idle_time=300,
    pool_wait_timeout=30
)
cn = db.connect(options)

# Method 2: Using dictionary with pooling parameters
cn = db.connect({
    'drivername': 'postgresql',
    'database': 'your_database',
    'hostname': 'localhost',
    'username': 'your_username',
    'password': 'your_password',
    'port': 5432,
    'use_pool': True,
    'pool_max_connections': 10,
    'pool_max_idle_time': 300,
    'pool_wait_timeout': 30
})

# Method 3: Using a configuration object
from libb import Setting
postgres_config = Setting()
postgres_config.drivername = 'postgresql'
postgres_config.database = 'your_database'
postgres_config.hostname = 'localhost'
postgres_config.username = 'your_username'
postgres_config.password = 'your_password'
postgres_config.port = 5432
postgres_config.use_pool = True
postgres_config.pool_max_connections = 10
postgres_config.pool_max_idle_time = 300
postgres_config.pool_wait_timeout = 30

cn = db.connect(postgres_config)

# Use the connection normally
result = db.select(cn, 'SELECT * FROM users')

# When you're done with the connection
cn.close()  # Returns connection to pool instead of closing
```

## Query Operations

### Basic Operations

#### execute

Execute arbitrary SQL statements:

```python
# Execute a statement that doesn't return data
row_count = db.execute(cn, 'CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT, age INTEGER)')

# Execute with parameters
row_count = db.execute(cn, 'DELETE FROM users WHERE age < %s', 18)
```

#### select

Query data and return as pandas DataFrame:

```python
# Basic SELECT
result = db.select(cn, 'SELECT id, name, age FROM users WHERE age >= %s', 18)

# SELECT with multiple parameters
result = db.select(cn, 'SELECT * FROM users WHERE age BETWEEN %s AND %s', 18, 65)

# SELECT with named parameters
result = db.select(cn, 'SELECT * FROM users WHERE name = %(name)s AND age = %(age)s',
                  {'name': 'John', 'age': 30})
```

### Row and Value Operations

#### select_row

Fetch a single row as an attribute dictionary:

```python
user = db.select_row(cn, 'SELECT * FROM users WHERE id = %s', 42)
print(user.name)  # Access columns as attributes
print(user.age)
```

#### select_scalar

Fetch a single value:

```python
count = db.select_scalar(cn, 'SELECT COUNT(*) FROM users')
name = db.select_scalar(cn, 'SELECT name FROM users WHERE id = %s', 42)
```

#### select_column

Fetch a single column as a list:

```python
user_ids = db.select_column(cn, 'SELECT id FROM users ORDER BY id')
active_names = db.select_column(cn, 'SELECT name FROM users WHERE active = %s', True)
```

### Empty Result Handling

All query operations return consistent empty structures rather than `None` when no results are found, with column information preserved:

```python
# Query with no matching results
empty_result = db.select(cn, 'SELECT id, name, email FROM users WHERE 1=0')
print(type(empty_result))          # <class 'pandas.core.frame.DataFrame'>
print(len(empty_result))           # 0 (empty DataFrame)
print(list(empty_result.columns))  # ['id', 'name', 'email'] (column structure preserved)

# Empty column query
empty_column = db.select_column(cn, 'SELECT name FROM users WHERE 1=0')
print(type(empty_column))  # <class 'list'>
print(len(empty_column))   # 0 (empty list)

# Functions designed to return None for empty results still do so
none_value = db.select_row_or_none(cn, 'SELECT * FROM users WHERE 1=0')
print(none_value)          # None

# Multiple result sets
empty_results = db.callproc(cn, '''
    SELECT id, name FROM users WHERE 1=0;
    SELECT email, status FROM users WHERE 1=0;
''', return_all=True)
print(type(empty_results))                 # <class 'list'>
print(len(empty_results))                  # 2 (list of empty DataFrames)
print(list(empty_results[0].columns))      # ['id', 'name'] (first result set columns)
print(list(empty_results[1].columns))      # ['email', 'status'] (second result set columns)
```

The enhanced empty result handling ensures:
1. Column structure is preserved in empty DataFrames
2. Column order matches the query's SELECT clause
3. Multiple empty result sets preserve their individual column structures
4. Consistent behavior when working with empty results
5. No special handling needed for `None` cases

This makes your code more robust when dealing with queries that might return no rows, as the column structure is still available for processing.

### Type Information

The module preserves column type information across all database backends:

```python
# Get DataFrame with type information
result = db.select(cn, "SELECT id, name, price, created_at FROM products")

# Access type information stored in DataFrame attributes
column_types = result.attrs.get('column_types', {})
print(f"ID type: {column_types['id']['python_type']}")  # "int"
print(f"Name type: {column_types['name']['python_type']}")  # "str"
print(f"Price type: {column_types['price']['python_type']}")  # "float"
print(f"Created at type: {column_types['created_at']['python_type']}")  # "datetime"

# Type information is preserved in empty result sets too
empty_result = db.select(cn, "SELECT id, name FROM products WHERE 1=0")
assert len(empty_result) == 0
empty_types = empty_result.attrs.get('column_types', {})
print(f"ID type: {empty_types['id']['python_type']}")  # Still knows it's "int"
```

### Column Static Helpers

The `Column` class provides static helper methods to simplify working with columns, particularly useful in custom data loaders:

```python
# Get column names from a list of Column objects
column_names = Column.get_names(columns)  # Returns ['id', 'name', 'email', ...]

# Find a column by name
email_column = Column.get_column_by_name(columns, 'email')

# Get dictionary of column type information (useful for serialization)
type_dict = Column.get_column_types_dict(columns)

# Create empty columns from just names (for testing or placeholders)
empty_columns = Column.create_empty_columns(['id', 'name', 'email'])
```

You can also use a custom loader to handle type information directly:

```python
# Create a custom data loader that uses type information
def typed_dict_loader(data, column_info, **kwargs):
    result = {
        'data': list(data),
        'columns': column_info.names,
        'column_types': {}
    }

    # Add type information by column
    for i, name in enumerate(column_info.names):
        col_type = column_info.get_column_type(i)
        if col_type:
            result['column_types'][name] = {
                'type_name': col_type.name,
                'python_type': col_type.python_type.__name__,
                'nullable': col_type.nullable
            }

    return result

# Use the custom loader with a connection
cn.options.data_loader = typed_dict_loader
result = db.select(cn, "SELECT * FROM users")

# Now you have direct access to the type information
for name, type_info in result['column_types'].items():
    print(f"{name}: {type_info['python_type']}")
```

Type information is mapped consistently across all database backends:
- PostgreSQL: Uses the native type system with OIDs
- SQLite: Maps type strings to Python types
- SQL Server: Maps SQL Server type names to Python types

### Stored Procedures and Multiple Result Sets

The `select` function handles stored procedures and multiple result sets:

```python
# Basic stored procedure call
result = db.select(cn, 'EXEC get_users_by_status @status=?', 'active')

# SQL Server stored procedure with named parameters
result = db.select(cn, 'EXEC calculate_stats @start_date=?, @end_date=?',
                  '2023-01-01', '2023-12-31')

# PostgreSQL stored procedure
result = db.select(cn, 'CALL get_users_by_status(%s)', 'active')

# With multiple statements that return different result sets
result_sets = db.select(cn, """
    SELECT id, name FROM users WHERE active = true;
    SELECT COUNT(*) AS total_count FROM users;
""", return_all=True)

# Return the largest result set (default behavior)
largest_result = db.select(cn, multi_statement_sql)

# Return the first result set regardless of size
first_result = db.select(cn, multi_statement_sql, prefer_first=True)

# Return all result sets as a list
all_results = db.select(cn, multi_statement_sql, return_all=True)
```

The enhanced `select` function is particularly useful for:
- Stored procedures that return multiple result sets
- SQL Server procedures with both NOCOUNT ON and OFF
- Multiple SQL statements that each return different data
- Any scenario where you need to handle sequential result sets from a single query

## Data Manipulation

### Insert Operations

#### insert

Insert a single row:

```python
db.insert(cn, 'INSERT INTO users (name, email) VALUES (%s, %s)',
         'Jane Smith', 'jane@example.com')
```

#### insert_identity

Insert a row and return the auto-generated identity/sequence value (SQL Server):

```python
user_id = db.insert_identity(cn, 'INSERT INTO users (name, email) VALUES (%s, %s)',
                            'Jane Smith', 'jane@example.com')
print(f"New user ID: {user_id}")
```

#### insert_row

Insert a row with named columns:

```python
db.insert_row(cn, 'users',
             ['name', 'email', 'age'],
             ['Jane Smith', 'jane@example.com', 30])
```

#### insert_rows

Insert multiple rows at once:

```python
rows = [
    {'name': 'John Doe', 'email': 'john@example.com', 'age': 25},
    {'name': 'Jane Smith', 'email': 'jane@example.com', 'age': 30},
    {'name': 'Bob Johnson', 'email': 'bob@example.com', 'age': 45}
]
db.insert_rows(cn, 'users', rows)
```

### Update Operations

#### update

Update existing records:

```python
db.update(cn, 'UPDATE users SET active = %s WHERE last_login < %s',
         False, '2023-01-01')
```

#### update_row

Update a specific row with named columns:

```python
db.update_row(cn, 'users',
             keyfields=['id'], keyvalues=[42],
             datafields=['name', 'active'], datavalues=['Updated Name', True])
```

#### update_or_insert

Try to update a row, insert if it doesn't exist:

```python
db.update_or_insert(
    cn,
    update_sql='UPDATE users SET active = %s WHERE email = %s',
    insert_sql='INSERT INTO users (email, active) VALUES (%s, %s)',
    True, 'new@example.com'
)
```

#### upsert_rows

Insert or update multiple rows based on primary key:

```python
rows = [
    {'id': 1, 'name': 'John Doe', 'email': 'john@example.com'},
    {'id': 2, 'name': 'Jane Smith', 'email': 'jane@example.com'},
    {'id': 3, 'name': 'New User', 'email': 'new@example.com'}
]

# Update all columns on conflict
db.upsert_rows(cn, 'users', rows)

# Only update specific columns on conflict
db.upsert_rows(cn, 'users', rows, update_cols_key=['id'], update_cols_always=['name'])

# Only update null values on conflict
db.upsert_rows(cn, 'users', rows, update_cols_key=['id'], update_cols_ifnull=['email'])

# Reset sequence after operation (for auto-increment columns)
db.upsert_rows(cn, 'users', rows, reset_sequence=True)
```

Database-specific behavior:
- **PostgreSQL**: Uses `INSERT ... ON CONFLICT DO UPDATE`
- **SQLite**: Uses `INSERT ... ON CONFLICT DO UPDATE`
- **SQL Server**: Uses `MERGE INTO` statement with specialized handling for NULL values

### Delete Operations

#### delete

Delete records:

```python
db.delete(cn, 'DELETE FROM users WHERE active = %s', False)
```

### Multiple-Row Operations

The module includes several operations optimized for working with multiple rows:

```python
# Bulk insert with efficient parameter handling
users = [
    {'name': 'User 1', 'email': 'user1@example.com'},
    {'name': 'User 2', 'email': 'user2@example.com'},
    # ... potentially thousands of rows
]
db.insert_rows(cn, 'users', users)

# Upsert (update or insert) with different update behaviors
db.upsert_rows(cn, 'products', products,
               update_cols_key=['product_code'],  # Identify records by product_code
               update_cols_always=['name', 'price'],  # Always update these fields
               update_cols_ifnull=['description'])  # Only update if target is NULL
```

These operations are implemented with efficiency in mind:
- Proper parameter batching for optimal performance
- Database-specific SQL generation for best behavior on each engine
- Transaction management to ensure atomicity
- Built-in error handling with appropriate exceptions

## Transaction Management

### Using Transactions

Use the `transaction` context manager for atomic operations:

```python
with db.transaction(cn) as tx:
    # All operations in this block are part of a single transaction
    tx.execute('INSERT INTO users (name) VALUES (%s)', 'User 1')
    tx.execute('INSERT INTO profiles (user_id, bio) VALUES (%s, %s)', 1, 'Bio text')

    # Can also run SELECT queries within transaction
    user = tx.select('SELECT * FROM users WHERE id = %s', 1)

    # Transaction supports all the same query operations as regular connections
    row = tx.select_row('SELECT * FROM users WHERE id = %s', 1)
    value = tx.select_scalar('SELECT COUNT(*) FROM users')
    column = tx.select_column('SELECT name FROM users ORDER BY id')

    # Use RETURNING clauses to get inserted IDs or other values
    user_id = tx.execute('INSERT INTO users (name) VALUES (%s) RETURNING id', 'User 2', returnid='id')

    # Return multiple values from an INSERT
    id_val, created_at = tx.execute(
        'INSERT INTO audit_log (action, user_id) VALUES (%s, %s) RETURNING id, created_at',
        'new_user', 1,
        returnid=['id', 'created_at']
    )

    # Return values from multiple rows (returns a list of lists)
    results = tx.execute('''
        UPDATE products SET on_sale = true
        WHERE category = 'clothing'
        RETURNING id, name, price''',
        returnid=['id', 'name', 'price']
    )

    # Each result is a list of values matching the returnid fields
    for product_id, product_name, price in results:
        print(f"Product {product_name} (ID: {product_id}) now on sale at ${price}")

    # If any operation fails, the entire transaction is rolled back
    # If all operations succeed, the transaction is committed automatically
```

The transaction context manager supports all the same query operations as regular connections:

```python
with db.transaction(cn) as tx:
    # Execute standard operations
    tx.execute(sql, *args)

    # Execute and return values from RETURNING clause
    id_val = tx.execute(sql, *args, returnid='id')
    col1, col2 = tx.execute(sql, *args, returnid=['col1', 'col2'])

    # Select data
    result = tx.select(sql, *args)

    # Get a single row or value
    row = tx.select_row(sql, *args)
    row_or_none = tx.select_row_or_none(sql, *args)
    value = tx.select_scalar(sql, *args)
    column = tx.select_column(sql, *args)
```

### Transaction Isolation Levels

Database transactions support different isolation levels that control how concurrent transactions interact:

```python
# PostgreSQL with explicit isolation level
with db.transaction(cn, isolation_level='SERIALIZABLE') as tx:
    # Operations with serializable isolation
    tx.execute('UPDATE accounts SET balance = balance - %s WHERE id = %s', 100, 1)
    tx.execute('UPDATE accounts SET balance = balance + %s WHERE id = %s', 100, 2)
```

Available isolation levels:
- **READ UNCOMMITTED**: Lowest isolation, allows dirty reads (SQL Server, PostgreSQL)
- **READ COMMITTED**: Prevents dirty reads (Default for PostgreSQL, SQL Server)
- **REPEATABLE READ**: Prevents non-repeatable reads (PostgreSQL, SQL Server)
- **SERIALIZABLE**: Highest isolation, prevents all concurrency issues (All databases)

### Database-Specific Transaction Behavior

Isolation levels and transaction behavior vary by database type:

#### PostgreSQL
- Supports full ACID transactions with multiple isolation levels
- Uses READ COMMITTED as the default isolation level
- Supports explicit isolation level specification
- Example:
  ```python
  with db.transaction(cn, isolation_level='SERIALIZABLE') as tx:
      # PostgreSQL serializable transaction
  ```

#### SQLite
- Uses deferred transactions by default (DEFERRED)
- Supports IMMEDIATE and EXCLUSIVE modes
- EXCLUSIVE locks the entire database for writing
- Example:
  ```python
  with db.transaction(cn) as tx:  # Uses SQLite defaults
      # SQLite transaction
  ```

#### SQL Server
- Supports full ACID transactions with multiple isolation levels
- Uses READ COMMITTED as the default isolation level
- Supports snapshot isolation via SNAPSHOT isolation level
- Example:
  ```python
  with db.transaction(cn, isolation_level='SNAPSHOT') as tx:
      # SQL Server snapshot isolation
  ```

## Type System

The database module provides a clean, predictable type handling system with a clear flow of responsibility:

- **Database → Python**: Types are converted exactly once by the database drivers and their registered adapters
- **Python → Database**: Python types are converted to database types during parameter binding via `TypeConverter`

This single-conversion approach ensures values maintain their integrity without unnecessary transformations.

### Type Conversion

The module automatically handles common Python, NumPy, and Pandas data types:

- Python native types: `str`, `int`, `float`, `bool`, `datetime`, etc.
- NumPy types: `np.float64`, `np.int64`, etc.
- Pandas types: `pd.NA`, nullable integer types
- PyArrow scalar types

NULL values are handled consistently across databases:
- Python `None` values are converted to database NULL
- NaN values in NumPy float types are converted to NULL
- Pandas NA/NaT values are converted to NULL

```python
import numpy as np
import pandas as pd
import datetime

# All these values are automatically converted appropriately
db.execute(cn, """
    INSERT INTO data_types_test
    (int_col, float_col, bool_col, date_col, null_col, nan_col)
    VALUES (%s, %s, %s, %s, %s, %s)
""",
    42,                           # int -> INTEGER
    np.float64(3.14),             # numpy float -> FLOAT
    True,                         # bool -> BOOLEAN
    datetime.date(2023, 1, 1),    # date -> DATE
    None,                         # None -> NULL
    np.nan                        # NaN -> NULL
)

# Pandas NA values are converted to NULL
db.execute(cn, "INSERT INTO users (name, age) VALUES (%s, %s)",
          "User with missing age", pd.NA)

# NumPy arrays are converted to lists
data = np.array([1, 2, 3])
db.execute(cn, "INSERT INTO array_test (values) VALUES (%s)", data)  # Inserts [1, 2, 3]
```

### Type Handling

The module preserves type information from database to Python:

```python
# Query with mixed types
result = db.select(cn, """
    SELECT
        id,                      -- integer
        name,                    -- string
        created_at,              -- timestamp
        is_active,               -- boolean
        score,                   -- float
        metadata                 -- json
    FROM users
    WHERE id = %s
""", 1)

# Type information is stored in result attributes
types = result.attrs.get('column_types', {})
for col, type_info in types.items():
    print(f"{col}: {type_info['python_type']}")

# Types are automatically converted to appropriate Python types
user = db.select_row(cn, "SELECT * FROM users WHERE id = %s", 1)
print(type(user.id))           # <class 'int'>
print(type(user.name))         # <class 'str'>
print(type(user.created_at))   # <class 'datetime.datetime'>
print(type(user.is_active))    # <class 'bool'>
print(type(user.score))        # <class 'float'>
```

### Database-Specific Type Handling

#### PostgreSQL

PostgreSQL has excellent type support with specialized handlers:

```python
# JSON/JSONB handling
db.execute(cn, "INSERT INTO configs (settings) VALUES (%s)",
          {"theme": "dark", "notifications": True})  # Dict automatically converted to JSON

# Array types
db.execute(cn, "INSERT INTO tags (item_id, tags) VALUES (%s, %s)",
          1, ["important", "urgent"])  # List converted to array

# Custom types
from psycopg.types.json import Json
db.execute(cn, "INSERT INTO data (payload) VALUES (%s)",
          Json({"custom": "payload"}))  # Explicit JSON conversion

# Range types
result = db.select(cn, "SELECT daterange(start_date, end_date) as date_range FROM events")
```

#### SQLite

SQLite uses dynamic typing with affinity:

```python
# SQLite handles most Python types naturally
db.execute(cn, "INSERT INTO items (name, count, price, available) VALUES (?, ?, ?, ?)",
          "Product", 5, 9.99, True)

# ISO-format for dates
db.execute(cn, "INSERT INTO events (name, event_date) VALUES (?, ?)",
          "Meeting", datetime.date(2023, 1, 15))  # Stored as ISO string

# JSON as text
db.execute(cn, "INSERT INTO settings (config) VALUES (?)",
          json.dumps({"theme": "light"}))  # Manual JSON serialization
```

#### SQL Server

SQL Server has comprehensive type mapping:

```python
# SQL Server date/time handling
db.execute(cn, "INSERT INTO appointments (title, scheduled_at) VALUES (?, ?)",
          "Doctor", datetime.datetime(2023, 2, 15, 14, 30))

# Decimal precision handling
from decimal import Decimal
db.execute(cn, "INSERT INTO finances (amount) VALUES (?)",
          Decimal('1234.56'))

# Binary data
db.execute(cn, "INSERT INTO files (filename, data) VALUES (?, ?)",
          "document.pdf", b'binary data here')
```

### Empty Result Type Handling

All query operations maintain type information even with empty results:

```python
# Empty result retains column types
empty_result = db.select(cn, "SELECT id, name, created_at FROM users WHERE 1=0")
print(len(empty_result))  # 0
print(list(empty_result.columns))  # ['id', 'name', 'created_at']

# Type information still available
types = empty_result.attrs.get('column_types', {})
for col, type_info in types.items():
    print(f"{col}: {type_info['python_type']}")
```

### Type Conversion Architecture

The module follows these architectural principles:

1. **Single Conversion Point**: Values are converted exactly once, when crossing the database boundary
   - Going in (Python → Database): Handled by `TypeConverter` during parameter binding
   - Coming out (Database → Python): Handled solely by database drivers and their registered adapters

2. **Clear Separation of Responsibilities**:
   - **Database Adapters**: Handle the actual type conversion (registered via `type_adapters.py`)
   - **Column Class**: Provides type metadata only, never performs conversions
   - **Row Adapters**: Handle structure mapping only (dict/row format), not conversion
   - **Type Handlers**: Identify appropriate Python types, never convert values
   - **Type Resolver**: Maps database type codes to Python types

3. **Consistent Flow of Data**:
   ```
   [Python Values] → TypeConverter → Database Driver → Database
   Database → Database Driver → Registered Adapters → [Python Values]
   ```

This architecture provides several benefits:
- **Performance**: Values traverse a shorter, more direct path
- **Correctness**: No information loss from multiple conversions
- **Consistency**: Values behave predictably across database backends
- **Maintainability**: Clear responsibility boundaries make code easier to update

## Schema Operations

The module provides several operations to manage database schema objects like tables and sequences.

### Table Sequence Operations

#### reset_table_sequence

Reset auto-increment sequence for a table:

```python
db.reset_table_sequence(cn, 'users')
db.reset_table_sequence(cn, 'users', identity='user_id')  # Specify column name
```

This function:
1. Identifies the identity/sequence column for the table
2. Determines the next available ID value by finding the maximum existing value
3. Resets the sequence to the correct next value
4. Works across PostgreSQL, SQLite, and SQL Server with database-specific implementations

### Table Maintenance Operations

#### vacuum_table

Reclaim space and optimize a table:

```python
db.vacuum_table(cn, 'users')
```

Different behavior by database:
- **PostgreSQL**: Executes `VACUUM users`
- **SQLite**: Executes `VACUUM` on the entire database if supported
- **SQL Server**: Reorganizes table indexes

#### reindex_table

Rebuild indexes for a table:

```python
db.reindex_table(cn, 'users')
```

Different behavior by database:
- **PostgreSQL**: Executes `REINDEX TABLE users`
- **SQLite**: Rebuilds all indexes on the table
- **SQL Server**: Rebuilds all indexes using `ALTER INDEX ... REBUILD`

#### cluster_table

Order table data according to an index (PostgreSQL only):

```python
db.cluster_table(cn, 'users', 'users_email_idx')
```

### Database-Specific Schema Operations

#### PostgreSQL Schema Operations

```python
# VACUUM operation (requires autocommit)
db.vacuum_table(cn, 'users')

# REINDEX operation
db.reindex_table(cn, 'users')

# CLUSTER operation (reorders table data according to an index)
db.cluster_table(cn, 'users', 'users_email_idx')

# Get primary key columns
primary_keys = db.get_table_primary_keys(cn, 'users')  # ['id']

# Get all columns with types
columns = db.get_table_columns(cn, 'users')  # {'id': 'integer', 'name': 'text', ...}
```

#### SQLite Schema Operations

```python
# SQLite VACUUM (operates on entire database)
db.vacuum_table(cn)

# Get table information
columns = db.get_table_columns(cn, 'users')

# Reset AUTOINCREMENT sequence
db.reset_table_sequence(cn, 'users')
```

#### SQL Server Schema Operations

```python
# Reset IDENTITY column
db.reset_table_sequence(cn, 'users')

# Table reorganization
db.reindex_table(cn, 'users')  # Rebuilds all indexes

# Get table columns with types
columns = db.get_table_columns(cn, 'users')  # {'id': 'int', 'name': 'nvarchar', ...}

# Get identity columns
identity_columns = db.get_sequence_columns(cn, 'users')  # ['id']
```

## Advanced Features

### SQL Query Helpers

The module provides several utilities to handle SQL formatting and parameter handling:

```python
from database.utils.sql import quote_identifier, handle_in_clause_params

# Quote identifiers for different databases
table_name = quote_identifier('postgres', 'my_table')  # Returns "my_table"
column_name = quote_identifier('sqlserver', 'user_id')  # Returns [user_id]

# Handle IN clauses with list parameters
sql = "SELECT * FROM users WHERE status IN %s"
params = [('active', 'pending', 'new')]
new_sql, new_params = handle_in_clause_params(sql, params)
# new_sql becomes "SELECT * FROM users WHERE status IN (%s, %s, %s)"
# new_params becomes ('active', 'pending', 'new')
```

### Custom Data Loaders

You can control how query results are returned:

```python
from database.options import pandas_numpy_data_loader, pandas_pyarrow_data_loader, iterdict_data_loader

# Use standard pandas with NumPy backend (default)
cn = db.connect({
    'drivername': 'postgres',
    'database': 'your_database',
    # other connection parameters...
    'data_loader': pandas_numpy_data_loader
})

# Use pandas with PyArrow backend
cn = db.connect({
    'drivername': 'postgres',
    'database': 'your_database',
    # other connection parameters...
    'data_loader': pandas_pyarrow_data_loader
})

# Use simple dictionary results (no pandas dependency)
cn = db.connect({
    'drivername': 'postgres',
    'database': 'your_database',
    # other connection parameters...
    'data_loader': iterdict_data_loader
})

# Define your own custom data loader
def my_custom_loader(data, columns, **kwargs):
    """
    Args:
         Raw data rows from database
        columns: List of Column objects with names and types
        kwargs: Additional options
    Returns:
        Processed data in your preferred format
    """
    # Use Column static helpers to get information
    column_names = Column.get_names(columns)
    return [dict(zip(column_names, row)) for row in data]

# Custom loader that leverages column type information
def typed_dict_loader(data, columns, **kwargs):
    """Return results with type information"""
    result = {
        'data': list(data),
        'columns': Column.get_names(columns),
        'column_types': {}
    }

    # Use Column static helpers for type information
    result['column_types'] = Column.get_column_types_dict(columns)

    return result

# You can also create loaders that use different return formats
def xml_data_loader(data, column_info, **kwargs):
    """Return query results as XML string"""
    import xml.etree.ElementTree as ET
    from xml.dom import minidom

    root = ET.Element("results")
    for row in
        row_elem = ET.SubElement(root, "row")
        for i, col in enumerate(column_info.names):
            col_elem = ET.SubElement(row_elem, col)
            col_elem.text = str(row[col]) if row[col] is not None else ""

    xml_str = ET.tostring(root, encoding='unicode')
    # Pretty print
    return minidom.parseString(xml_str).toprettyxml(indent="  ")

cn = db.connect({
    'drivername': 'postgres',
    'database': 'your_database',
    'data_loader': my_custom_loader # or xml_data_loader or typed_dict_loader
})
```

### Caching

The module includes caching utilities for performance optimization:

```python
from database.utils.cache import TTLCacheManager
import cachetools

# Create or get a TTL cache
cache = TTLCacheManager.get_cache('my_cache', maxsize=100, ttl=300)

# Use with cachetools decorators
@cachetools.cached(cache=cache)
def fetch_user(user_id):
    return db.select_row(cn, "SELECT * FROM users WHERE id = %s", user_id)

# Get cache statistics
stats = TTLCacheManager.get_stats()
print(stats['my_cache']['size'])  # Current number of items in cache

# Clear all caches
TTLCacheManager.clear_all()
```

### SQL Parameter Handling

The module automatically adapts SQL parameters based on database type and handles special cases like SQL `IN` clauses, LIKE patterns, and NULL values.

Note that SQL keywords such as NULL, IN, and LIKE are not case sensitive.

#### LIKE Clauses and Percent Signs

| What you write | What the driver receives |
|----------------|--------------------------|
| `db.select(cn, "SELECT * FROM users WHERE name LIKE 'test%'")` | `"SELECT * FROM users WHERE name LIKE 'test%%'"` |
| `db.select(cn, "SELECT * FROM users WHERE code LIKE '%%CODE'")` | `"SELECT * FROM users WHERE code LIKE '%%CODE'"` |
| `db.select(cn, "SELECT * FROM users WHERE name LIKE %s", "test%")` | `"SELECT * FROM users WHERE name LIKE %s", "test%"` |
| `db.select(cn, "SELECT * FROM products WHERE code LIKE 'PRD-%' AND name LIKE %s", "Chair%")` | `"SELECT * FROM products WHERE code LIKE 'PRD-%%' AND name LIKE %s", "Chair%"` |

The module automatically escapes percent signs in string literals while preserving percent signs in parameters.

#### IS NULL / IS NOT NULL Handling

| What you write | What the driver receives |
|----------------|--------------------------|
| `db.select(cn, "SELECT * FROM users WHERE last_login IS NULL")` | `"SELECT * FROM users WHERE last_login IS NULL"` |
| `db.select(cn, "SELECT * FROM users WHERE email IS NOT NULL")` | `"SELECT * FROM users WHERE email IS NOT NULL"` |
| `db.select(cn, "SELECT * FROM users WHERE last_login IS %s", None)` | `"SELECT * FROM users WHERE last_login IS NULL"` |
| `db.select(cn, "SELECT * FROM users WHERE email IS NOT %s", None)` | `"SELECT * FROM users WHERE email IS NOT NULL"` |
| `db.select(cn, "SELECT * FROM orders WHERE date > %s AND tracking_number IS NULL", some_date)` | `"SELECT * FROM orders WHERE date > %s AND tracking_number IS NULL", some_date` |

The module handles `NULL` values consistently across all database backends.

#### IN Clause Parameter Handling

| What you write | What the driver receives |
|----------------|--------------------------|
| `db.select(cn, "SELECT * FROM users WHERE id IN %s", [1, 2, 3])` | `"SELECT * FROM users WHERE id IN (%s, %s, %s)", 1, 2, 3` |
| `db.select(cn, "SELECT * FROM users WHERE status IN %s", ['active'])` | `"SELECT * FROM users WHERE status IN (%s)", "active"` |
| `db.select(cn, "SELECT * FROM users WHERE id IN %s", ([1, 2, 3],))` | `"SELECT * FROM users WHERE id IN (%s, %s, %s)", 1, 2, 3` |
| `db.select(cn, "SELECT * FROM users WHERE id IN %(ids)s", {'ids': [1, 2, 3]})` | `"SELECT * FROM users WHERE id IN (%s, %s, %s)", 1, 2, 3` |
| `db.select(cn, """SELECT * FROM products WHERE category IN %(cat)s AND status IN %(status)s""", {'cat': ['electronics'], 'status': ['active', 'new']})` | `"SELECT * FROM products WHERE category IN (%s) AND status IN (%s, %s)", "electronics", "active", "new"` |

The module automatically handles all necessary SQL and parameter transformations for each database backend.

### Common Table Expressions (CTEs)

The module fully supports advanced SQL features like CTEs:

```python
# PostgreSQL CTE example
result = db.select(cn, """
    WITH active_users AS (
        SELECT id, name, email
        FROM users
        WHERE active = true
    )
    SELECT * FROM active_users
    WHERE email LIKE %s
""", '%@example.com')

# More complex CTE with multiple references
result = db.select(cn, """
    WITH
    recent_orders AS (
        SELECT * FROM orders WHERE order_date > %s
    ),
    user_stats AS (
        SELECT
            user_id,
            COUNT(*) as order_count,
            SUM(amount) as total_spent
        FROM recent_orders
        GROUP BY user_id
    )
    SELECT
        u.id,
        u.name,
        COALESCE(us.order_count, 0) as order_count,
        COALESCE(us.total_spent, 0) as total_spent
    FROM users u
    LEFT JOIN user_stats us ON u.id = us.user_id
    ORDER BY total_spent DESC, name
""", '2023-01-01')

# Recursive CTE (SQL Server and PostgreSQL)
result = db.select(cn, """
    WITH RECURSIVE org_hierarchy AS (
        -- Base case: top-level employees (no manager)
        SELECT id, name, manager_id, 0 as level
        FROM employees
        WHERE manager_id IS NULL

        UNION ALL

        -- Recursive case: employees with managers
        SELECT e.id, e.name, e.manager_id, oh.level + 1
        FROM employees e
        JOIN org_hierarchy oh ON e.manager_id = oh.id
    )
    SELECT * FROM org_hierarchy ORDER BY level, name
""")
```

The module ensures:
1. Proper parameter handling in CTE clauses
2. Consistent return types and column handling
3. Support for recursive CTEs where the database allows
4. Full transaction integration with CTEs

## Database-Specific Features

### PostgreSQL Features

PostgreSQL offers advanced features with specialized support in the module:

```python
# VACUUM operation (requires autocommit)
db.vacuum_table(cn, 'users')

# REINDEX operation
db.reindex_table(cn, 'users')

# CLUSTER operation (reorders table data according to an index)
db.cluster_table(cn, 'users', 'users_email_idx')

# JSON data support
result = db.select(cn, "SELECT data->>'name' as name FROM users WHERE id = %s", 1)

# Array operations
db.execute(cn, "UPDATE products SET tags = %s WHERE id = %s",
          ['sale', 'clearance'], 101)

result = db.select(cn, "SELECT * FROM products WHERE %s = ANY(tags)", 'sale')

# Range types
db.execute(cn, """
    INSERT INTO events (name, date_range)
    VALUES (%s, daterange(%s, %s, '[]'))
""", 'Conference', '2023-06-01', '2023-06-05')

# Full Text Search
result = db.select(cn, """
    SELECT id, title, ts_rank(search_vector, to_tsquery(%s)) AS rank
    FROM articles
    WHERE search_vector @@ to_tsquery(%s)
    ORDER BY rank DESC
""", 'postgresql & database', 'postgresql & database')
```

### SQLite Features

SQLite provides simplicity with effective features:

```python
# SQLite VACUUM (operates on entire database)
db.vacuum_table(cn)

# In-memory database
cn = db.connect({
    'drivername': 'sqlite',
    'database': ':memory:'
})

# Enable foreign key constraints
cn.connection.execute('PRAGMA foreign_keys = ON')

# JSON functions (with SQLite 3.38+)
result = db.select(cn, """
    SELECT json_extract(data, '$.name') as name
    FROM configs
    WHERE id = ?
""", 1)

# Full-text search (with FTS5 extension)
db.execute(cn, """
    CREATE VIRTUAL TABLE IF NOT EXISTS article_fts USING fts5(
        title, body, tokenize='porter'
    )
""")

db.execute(cn, "INSERT INTO article_fts VALUES(?, ?)",
         "SQLite Tutorial", "This is a tutorial about SQLite database")

result = db.select(cn, "SELECT * FROM article_fts WHERE article_fts MATCH ?", "tutorial")
```

### SQL Server Features

SQL Server provides enterprise features with specialized support:

```python
# Identity insert
user_id = db.insert_identity(cn,
    'INSERT INTO users (name, email) VALUES (%s, %s)',
    'John Smith', 'john@example.com')

# Table reorganization
db.reindex_table(cn, 'users')  # Rebuilds all indexes

# Stored procedure execution with SQL Server syntax
result = db.select(cn, 'EXEC get_user_data @user_id=?, @include_inactive=?',
                  42, False)

# Multiple result sets from stored procedure
results = db.select(cn, 'EXEC get_sales_report @region=?, @year=?',
                  'Northeast', 2023, return_all=True)

# Enhanced ODBC Driver 18+ support
# - Full column name preservation (no truncation)
# - Improved handling of expressions and unnamed columns
# - Automatic type conversion for date/time values

# Table-Valued Parameters (TVPs)
# (Requires pyodbc with proper TVP support)
import pyodbc

# First create a SQL Server table type
# CREATE TYPE UserTableType AS TABLE (
#    name NVARCHAR(100),
#    email NVARCHAR(100)
# )

# Then create a stored procedure that uses it
# CREATE PROCEDURE bulk_insert_users @users UserTableType READONLY
# AS
# BEGIN
#    INSERT INTO users (name, email)
#    SELECT name, email FROM @users
# END

# Create a driver connection with pyodbc for TVP support
odbc_cn = pyodbc.connect(connection_string)

# Create a TVP
tvp = pyodbc.TVP("UserTableType")
tvp.add_column("name", str)
tvp.add_column("email", str)
for user in users_to_insert:
    tvp.add_row(user["name"], user["email"])

# Execute the stored procedure with the TVP
cursor = odbc_cn.cursor()
cursor.execute("{CALL bulk_insert_users(?)}", tvp)
odbc_cn.commit()
```

## API Reference

The following is a complete reference of the public API functions and types.

### Core Functions

| Function | Description | Parameters | Returns |
|----------|-------------|------------|---------|
| `connect(options, **kwargs)` | Create database connection | `options`: Connection options dictionary or object<br>`**kwargs`: Additional connection options | `ConnectionWrapper` |
| `execute(cn, sql, *args)` | Execute SQL statement | `cn`: Database connection<br>`sql`: SQL statement<br>`*args`: Query parameters | Row count or specified return data |
| `transaction(cn)` | Create transaction context manager | `cn`: Database connection | `Transaction` context manager |

### Query Operations

| Function | Description | Parameters | Returns |
|----------|-------------|------------|---------|
| `select(cn, sql, *args, **kwargs)` | Execute SELECT query or stored procedure | `cn`: Database connection<br>`sql`: SELECT statement<br>`*args`: Query parameters<br>`**kwargs`: Additional options | DataFrame or list |
| `select_row(cn, sql, *args)` | Execute query, return single row | `cn`: Database connection<br>`sql`: SELECT statement<br>`*args`: Query parameters | Row as attribute dictionary |
| `select_row_or_none(cn, sql, *args)` | Like select_row but returns None if no rows | `cn`: Database connection<br>`sql`: SELECT statement<br>`*args`: Query parameters | Row as attribute dictionary or None |
| `select_scalar(cn, sql, *args)` | Execute query, return single value | `cn`: Database connection<br>`sql`: SELECT statement<br>`*args`: Query parameters | Single value |
| `select_scalar_or_none(cn, sql, *args)` | Like select_scalar but returns None if no rows | `cn`: Database connection<br>`sql`: SELECT statement<br>`*args`: Query parameters | Single value or None |
| `select_column(cn, sql, *args)` | Execute query, return single column | `cn`: Database connection<br>`sql`: SELECT statement<br>`*args`: Query parameters | List of values |
| `select_column_unique(cn, sql, *args)` | Execute query, return unique column values | `cn`: Database connection<br>`sql`: SELECT statement<br>`*args`: Query parameters | Set of unique values |

### Data Operations

| Function | Description | Parameters | Returns |
|----------|-------------|------------|---------|
| `insert(cn, sql, *args)` | Execute INSERT statement | `cn`: Database connection<br>`sql`: INSERT statement<br>`*args`: Query parameters | Row count |
| `insert_identity(cn, sql, *args)` | Execute INSERT and return identity value | `cn`: Database connection<br>`sql`: INSERT statement<br>`*args`: Query parameters | Identity value |
| `update(cn, sql, *args)` | Execute UPDATE statement | `cn`: Database connection<br>`sql`: UPDATE statement<br>`*args`: Query parameters | Row count |
| `delete(cn, sql, *args)` | Execute DELETE statement | `cn`: Database connection<br>`sql`: DELETE statement<br>`*args`: Query parameters | Row count |
| `insert_row(cn, table, fields, values)` | Insert single row with named fields | `cn`: Database connection<br>`table`: Table name<br>`fields`: List of column names<br>`values`: List of values | None |
| `insert_rows(cn, table, rows)` | Insert multiple rows | `cn`: Database connection<br>`table`: Table name<br>`rows`: List of dictionaries | None |
| `update_row(cn, table, keyfields, keyvalues, datafields, datavalues)` | Update single row with named fields | `cn`: Database connection<br>`table`: Table name<br>`keyfields`: List of key column names<br>`keyvalues`: List of key values<br>`datafields`: List of data column names<br>`datavalues`: List of data values | None |
| `update_or_insert(cn, update_sql, insert_sql, *args)` | Try update, insert if not exists | `cn`: Database connection<br>`update_sql`: UPDATE statement<br>`insert_sql`: INSERT statement<br>`*args`: Query parameters | None |
| `upsert_rows(cn, table, rows, **kwargs)` | Insert or update multiple rows based on keys | `cn`: Database connection<br>`table`: Table name<br>`rows`: List of dictionaries<br>`**kwargs`: Additional options | None |

### Schema Operations

| Function | Description | Parameters | Returns |
|----------|-------------|------------|---------|
| `reset_table_sequence(cn, table, identity=None)` | Reset table's auto-increment sequence | `cn`: Database connection<br>`table`: Table name<br>`identity`: Optional identity column name | None |
| `vacuum_table(cn, table)` | Optimize table, reclaiming space | `cn`: Database connection<br>`table`: Table name | None |
| `reindex_table(cn, table)` | Rebuild table indexes | `cn`: Database connection<br>`table`: Table name | None |
| `cluster_table(cn, table, index=None)` | Order table data according to an index | `cn`: Database connection<br>`table`: Table name<br>`index`: Optional index name | None |
| `get_table_columns(cn, table)` | Get all column names for a table | `cn`: Database connection<br>`table`: Table name | Dictionary mapping column names to types |
| `get_table_primary_keys(cn, table)` | Get primary key columns for a table | `cn`: Database connection<br>`table`: Table name | List of column names |
| `get_sequence_columns(cn, table)` | Get sequence/identity columns for a table | `cn`: Database connection<br>`table`: Table name | List of column names |
| `table_fields(cn, table)` | Get ordered list of all table columns | `cn`: Database connection<br>`table`: Table name | List of column names |
| `table_data(cn, table, columns=[])` | Get table data by columns | `cn`: Database connection<br>`table`: Table name<br>`columns`: Optional list of column names | DataFrame with table data |

### Connection Utilities

| Function | Description | Parameters | Returns |
|----------|-------------|------------|---------|
| `isconnection(obj)` | Check if object is a database connection | `obj`: Object to check | Boolean |
| `is_psycopg_connection(obj)` | Check if PostgreSQL connection | `obj`: Object to check | Boolean |
| `is_pymssql_connection(obj)` | Check if SQL Server connection | `obj`: Object to check | Boolean |
| `is_sqlite3_connection(obj)` | Check if SQLite connection | `obj`: Object to check | Boolean |

### Exception Types

| Exception | Description |
|-----------|-------------|
| `DatabaseError` | Base class for all database errors |
| `DbConnectionError` | Connection issues |
| `IntegrityError` | Constraint violations |
| `ProgrammingError` | SQL syntax errors |
| `OperationalError` | Database operational issues |
| `UniqueViolation` | Unique constraint violations |
| `ConnectionError` | Custom connection errors |
| `IntegrityViolationError` | Custom constraint errors |
| `QueryError` | Query execution errors |
| `TypeConversionError` | Type conversion errors |
