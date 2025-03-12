# Database Module

A comprehensive Python database interface supporting PostgreSQL, SQLite, and SQL Server with a consistent API.

[![License: OSL-3.0](https://img.shields.io/badge/License-OSL--3.0-blue.svg)](https://opensource.org/licenses/OSL-3.0)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)

## Table of Contents

- [Installation](#installation) | [Quick Start](#quick-start) | [Connection Management](#connection-management)
- **Database Operations**: [Basic](#basic-operations) | [Query](#query-operations) | [Data Manipulation](#data-manipulation) | [Schema](#schema-operations)
- **Advanced Features**: [Transactions](#working-with-transactions) | [Error Handling](#error-handling) | [Type Handling](#type-handling) | [Empty Results](#empty-result-handling)
- **Extensions**: [SQL Helpers](#sql-query-helpers) | [Custom Loaders](#custom-data-loaders) | [Pooling](#connection-pooling) | [CTEs](#common-table-expressions-ctes)
- **Database-Specific**: [PostgreSQL](#postgresql-specific-features) | [SQLite](#sqlite-specific-features) | [SQL Server](#sql-server-specific-features)
- [SQLAlchemy 2.0 Integration](#sqlalchemy-20-integration)
- [API Reference](#api-reference)

<details>
<summary>Detailed Table of Contents</summary>

- [Installation](#installation)
  - [Dependencies](#dependencies)
- [Quick Start](#quick-start)
- [Connection Management](#connection-management)
  - [Creating a Connection](#creating-a-connection)
  - [Connection Options](#connection-options)
  - [Connection Pooling](#connection-pooling)
- [Database Operations](#database-operations)
  - [Basic Operations](#basic-operations)
    - [execute](#execute)
    - [select](#select)
    - [Empty Result Handling](#empty-result-handling)
    - [callproc](#callproc)
    - [select_row](#select_row)
    - [select_scalar](#select_scalar)
    - [select_column](#select_column)
  - [Data Manipulation](#data-manipulation)
    - [insert](#insert)
    - [insert_identity](#insert_identity)
    - [insert_row](#insert_row)
    - [insert_rows](#insert_rows)
    - [update](#update)
    - [update_row](#update_row)
    - [update_or_insert](#update_or_insert)
    - [upsert_rows](#upsert_rows)
    - [delete](#delete)
  - [Schema Operations](#schema-operations)
    - [reset_table_sequence](#reset_table_sequence)
    - [vacuum_table](#vacuum_table)
    - [reindex_table](#reindex_table)
    - [cluster_table](#cluster_table)
- [Working with Transactions](#working-with-transactions)
- [Error Handling](#error-handling)
  - [Exception Types](#exception-types)
  - [Automatic Connection Recovery](#automatic-connection-recovery)
- [Type Handling](#type-handling)
  - [Type Conversion](#type-conversion)
  - [Database-Specific Type Handling](#database-specific-type-handling)
  - [Empty Result Handling](#empty-result-handling)
- [Advanced Features](#advanced-features)
  - [SQL Query Helpers](#sql-query-helpers)
  - [Custom Data Loaders](#custom-data-loaders)
  - [Parameter Handling](#parameter-handling)
  - [Common Table Expressions](#common-table-expressions-ctes)
  - [Caching](#caching)
- [Database-Specific Features](#database-specific-features)
  - [PostgreSQL Features](#postgresql-specific-features)
  - [SQLite Features](#sqlite-specific-features)
  - [SQL Server Features](#sql-server-specific-features)
- [API Reference](#api-reference)
  - [Core Functions](#core-functions)
  - [Query Operations](#query-operations-1)
  - [Data Operations](#data-operations)
  - [Schema Operations](#schema-operations-1)
  - [Connection Utilities](#connection-utilities)
  - [Exception Types](#exception-types-1)

</details>

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

## Quick Start

```python
import database as db

# Connect to a database
cn = db.connect({
    'drivername': 'postgres',  # or 'sqlite', 'sqlserver'
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

## Connection Management

### Creating a Connection

```python
# Simple connection with dictionary
cn = db.connect({
    'drivername': 'postgres',
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
postgres_config.drivername = 'postgres'
postgres_config.database = 'your_database'
postgres_config.hostname = 'localhost'
postgres_config.username = 'your_username'
postgres_config.password = 'your_password'
postgres_config.port = 5432

cn = db.connect(postgres_config)

# Connection with pool
pooled_cn = db.connect({
    'drivername': 'postgres',
    'database': 'your_database',
    'hostname': 'localhost',
    'username': 'your_username',
    'password': 'your_password',
    'port': 5432
}, use_pool=True, pool_max_connections=10)
```

#### Configuration File Pattern

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
mssql.username = 'admin'
mssql.password = 'admin'
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

### Connection Options

The `DatabaseOptions` class controls connection behavior:

```python
from database.options import DatabaseOptions

options = DatabaseOptions(
    drivername='postgres',
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

## Database Operations

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

##### Empty Result Handling

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

##### Type Information

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

#### Stored Procedures and Multiple Result Sets

The `select` function now handles stored procedures and multiple result sets:

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

### Data Manipulation

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

#### delete

Delete records:

```python
db.delete(cn, 'DELETE FROM users WHERE active = %s', False)
```

### Schema Operations

#### reset_table_sequence

Reset auto-increment sequence for a table:

```python
db.reset_table_sequence(cn, 'users')
db.reset_table_sequence(cn, 'users', identity='user_id')  # Specify column name
```

#### vacuum_table

Reclaim space and optimize a table:

```python
db.vacuum_table(cn, 'users')
```

#### reindex_table

Rebuild indexes for a table:

```python
db.reindex_table(cn, 'users')
```

#### cluster_table

Order table data according to an index (PostgreSQL only):

```python
db.cluster_table(cn, 'users', 'users_email_idx')
```

## Working with Transactions

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

Isolation levels and transaction behavior vary by database type:
- **PostgreSQL**: Supports full ACID transactions with multiple isolation levels
- **SQLite**: Uses deferred transactions by default (DEFERRED)
- **SQL Server**: Supports full ACID transactions

## Error Handling

### Exception Types

```python
try:
    db.execute(conn, 'INSERT INTO users (email) VALUES (%s)', 'duplicate@example.com')
except db.UniqueViolation as e:
    print("Duplicate key error:", e)
except db.IntegrityError as e:
    print("Constraint violation:", e)
except db.ProgrammingError as e:
    print("SQL error:", e)
except db.DbConnectionError as e:
    print("Connection error:", e)
except db.DatabaseError as e:
    print("General database error:", e)
```

The module provides these exception types:
- `DatabaseError`: Base class for all database errors
- `DbConnectionError`: Connection issues
- `IntegrityError`: Constraint violations
- `ProgrammingError`: SQL syntax/query errors
- `OperationalError`: Database operational issues
- `UniqueViolation`: Unique constraint violations
- `IntegrityViolationError`: Custom constraint errors
- `QueryError`: Query execution errors
- `TypeConversionError`: Type conversion errors

### Automatic Connection Recovery

The module includes automatic connection recovery capabilities:

```python
# Enable connection checking (on by default)
options = DatabaseOptions(
    drivername='postgres',
    # other connection parameters...
    check_connection=True
)

cn = db.connect(options)

# If a connection fails, it will automatically reconnect on next query
try:
    db.select(cn, "SELECT * FROM users")
except db.DbConnectionError:
    # The next attempt should automatically reconnect
    db.select(cn, "SELECT * FROM users")
```

You can customize the retry behavior using the check_connection decorator:

```python
from database.core.transaction import check_connection

@check_connection(max_retries=5, retry_delay=1, retry_backoff=2.0)
def my_database_function(cn, param1, param2):
    # Function will retry up to 5 times with exponential backoff
    # if a connection error occurs
    return db.select(cn, "SELECT * FROM my_table WHERE param = %s", param1)
```

## Type Handling

The database module provides a clean, predictable type handling system with a clear flow of responsibility:

- **Database → Python**: Types are converted exactly once by the database drivers and their registered adapters
- **Python → Database**: Python types are converted to database types during parameter binding via `TypeConverter`

This single-conversion approach ensures values maintain their integrity without unnecessary transformations.

### SQL Server Enhancements

The module includes special enhancements for SQL Server with ODBC Driver 18+:

1. **Preserved Column Names**: Full column name preservation without truncation, enabling use of long descriptive names
2. **Improved Type Resolution**: Enhanced date/time handling with proper type conversion, including support for:
   - `DATE`, `TIME`, `DATETIME`, `DATETIME2`, `DATETIMEOFFSET`, and `SMALLDATETIME`
3. **Stored Procedure Support**:
   - Named parameter handling (e.g., `@param_name=?`)
   - Multiple result set handling
   - NULL handling
4. **Result Processing**:
   - Automatic handling of expressions and subqueries
   - Special handling for complex queries like CTEs, UNION, etc.
5. **Error Handling**:
   - Improved error diagnostics for parameter mismatches
   - Automatic recovery for common SQL Server errors

The module handles common Python, NumPy, and Pandas data types:

- Python native types: `str`, `int`, `float`, `bool`, `datetime`, etc.
- NumPy types: `np.float64`, `np.int64`, etc.
- Pandas types: `pd.NA`, nullable integer types
- PyArrow scalar types

NULL values are handled consistently across databases:
- Python `None` values are converted to database NULL
- NaN values in NumPy float types are converted to NULL
- Pandas NA/NaT values are converted to NULL

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

### Type System Components

The type system consists of these main components:

- **Type Adapters** (`type_adapters.py`):
  - Register converters with database drivers
  - Primary conversion point for Database → Python direction
  - Example: `CustomFloatDumper` for PostgreSQL NaN handling

- **Type Converter** (`type_converter.py`):
  - Converts Python values to database-compatible formats for parameters
  - Handles special types like NumPy arrays, pandas NA values
  - Only processes Python → Database direction

- **Type Registry** (`type_registry.py`):
  - Central registry for all type mapping information
  - Provides consistent type resolution across components

- **Type Resolver** (`type_resolver.py`):
  - Maps database-specific type codes to Python types
  - Uses context (column names, table) for better type resolution

- **Column** (`column_info.py`):
  - Stores type metadata for result columns
  - Provides type information to data loaders
  - Never performs conversions

### Database-Specific Handling

- **PostgreSQL**:
  - Custom adapters for numeric types with special NaN handling
  - Array type support via arrays.py module
  - JSON/JSONB support with automatic conversion

- **SQLite**:
  - Date/time handling through ISO-format converters
  - Runtime type detection via column affinity system
  - Built-in adapters for core Python types

- **SQL Server**:
  - Comprehensive type code mapping for ODBC types
  - Special handling for DATETIMEOFFSET via output converters
  - Timezone handling for datetime values

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
    for row in data:
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
    'drivername': 'postgres',
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
postgres_config.drivername = 'postgres'
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

### Parameter Handling

The module automatically adapts SQL parameters based on database type and handles:

- Converting `?` to `%s` placeholders between SQLite and PostgreSQL/SQL Server
- Special handling for SQL `IN` clauses with list/tuple parameters:

```python
# This works seamlessly across all database types
user_ids = [1, 2, 3]
db.select(cn, "SELECT * FROM users WHERE id IN %s", (user_ids,))

# For named parameters (PostgreSQL/SQL Server)
statuses = ['active', 'pending']
db.select(cn, "SELECT * FROM users WHERE status IN %(statuses)s",
         {'statuses': statuses})
```

#### IN Clause Parameter Conventions

When working with SQL `IN` clauses, you have several ways to pass parameters:

1. **Inline List Parameters (Simplified Syntax):**
   ```python
   # You can pass list parameters directly for IN clauses
   db.select(cn, "SELECT * FROM users WHERE id IN %s", [1, 2, 3])

   # This also works with single-item lists
   db.select(cn, "SELECT * FROM users WHERE status IN %s", ['active'])

   # And with multiple IN clauses
   db.select(cn, "SELECT * FROM users WHERE id IN %s AND status IN %s",
            [1, 2, 3], ['active', 'pending'])
   ```
   This format provides a more intuitive syntax that aligns with how you would naturally
   write SQL queries with list parameters. The module automatically expands these into
   the appropriate placeholders and parameter values for the database driver.

2. **Wrapped List Parameters (Standard DB-API Compatibility):**
   ```python
   # Pass a sequence as a single parameter in a tuple
   user_ids = [1, 2, 3]
   db.select(cn, "SELECT * FROM users WHERE id IN %s", (user_ids,))

   # This also works with a tuple directly
   db.select(cn, "SELECT * FROM users WHERE id IN %s", ([1, 2, 3],))
   ```

3. **Named parameters with tuple/list:**
   ```python
   # Use a dictionary with named parameter
   user_ids = [1, 2, 3]
   db.select(cn, "SELECT * FROM users WHERE id IN %(ids)s", {'ids': user_ids})

   # Works with single-item tuples/lists too
   db.select(cn, "SELECT * FROM users WHERE status IN %(status)s", {'status': ['active']})
   ```

3. **Multiple IN clauses:**
   ```python
   # Multiple IN clauses with positional parameters
   db.select(cn, "SELECT * FROM users WHERE id IN %s AND status IN %s",
            ([1, 2, 3],), (['active', 'pending'],))

   # Multiple IN clauses with named parameters
   db.select(cn, """
       SELECT * FROM users
       WHERE id IN %(ids)s
       AND status IN %(statuses)s
       AND type IN %(types)s
   """, {
       'ids': [1, 2, 3],
       'statuses': ['active', 'pending'],
       'types': ['admin']  # Single item still works
   })
   ```

4. **Empty sequences:**
   ```python
   # Empty sequences are handled gracefully (converts to NULL)
   db.select(cn, "SELECT * FROM users WHERE id IN %s", ([],))

   # Same for named parameters
   db.select(cn, "SELECT * FROM users WHERE id IN %(ids)s", {'ids': []})
   ```

5. **Using with transactions:**
   ```python
   with db.transaction(cn) as tx:
       # Works exactly the same in transactions
       tx.execute("INSERT INTO users (id, status) VALUES (%s, %s)", 1, 'active')

       # With IN clause parameters
       tx.execute("DELETE FROM users WHERE id IN %s", ([1, 2, 3],))

       # Or with direct list parameters
       tx.execute("UPDATE users SET status = 'inactive' WHERE id IN %s AND status IN %s",
                [1, 2, 3], ['active', 'pending'])

       # Or with named parameters
       tx.execute("INSERT INTO user_status (user_id, status) SELECT id, 'archived' FROM users WHERE status IN %(statuses)s",
                {'statuses': ['active', 'suspended']})
   ```

The module automatically expands these parameters into the appropriate SQL, handling
the placement of placeholders and parameter values correctly across all supported
database types.

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
```

## Database-Specific Features

### PostgreSQL-Specific Features

```python
# VACUUM operation (requires autocommit)
db.vacuum_table(cn, 'users')

# REINDEX operation
db.reindex_table(cn, 'users')

# CLUSTER operation (reorders table data according to an index)
db.cluster_table(cn, 'users', 'users_email_idx')

# JSON data support
result = db.select(cn, "SELECT data->>'name' as name FROM users WHERE id = %s", 1)
```

### SQLite-Specific Features

```python
# SQLite VACUUM (operates on entire database)
db.vacuum_table(cn, 'users')

# In-memory database
cn = db.connect({
    'drivername': 'sqlite',
    'database': ':memory:'
})

# Enable foreign key constraints
cn.connection.execute('PRAGMA foreign_keys = ON')
```

### SQL Server-Specific Features

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
```

## API Reference

The following is a complete reference of the public API functions and types.

### Core Functions

- `connect(options, **kwargs)` - Create database connection
- `execute(cn, sql, *args)` - Execute SQL statement
- `transaction(cn)` - Create transaction context manager

### Query Operations

- `select(cn, sql, *args, **kwargs)` - Execute SELECT query or stored procedure, return DataFrame or list
- `select_row(cn, sql, *args)` - Execute query, return single row as attrdict
- `select_row_or_none(cn, sql, *args)` - Like select_row but returns None if no rows
- `select_scalar(cn, sql, *args)` - Execute query, return single value
- `select_scalar_or_none(cn, sql, *args)` - Like select_scalar but returns None if no rows
- `select_column(cn, sql, *args)` - Execute query, return single column as list
- `select_column_unique(cn, sql, *args)` - Execute query, return unique column values as set

### Data Operations

- `insert(cn, sql, *args)` - Execute INSERT statement
- `insert_identity(cn, sql, *args)` - Execute INSERT and return identity value (SQL Server)
- `update(cn, sql, *args)` - Execute UPDATE statement
- `delete(cn, sql, *args)` - Execute DELETE statement
- `insert_row(cn, table, fields, values)` - Insert single row with named fields
- `insert_rows(cn, table, rows)` - Insert multiple rows from dictionaries
- `update_row(cn, table, keyfields, keyvalues, datafields, datavalues)` - Update single row with named fields
- `update_or_insert(cn, update_sql, insert_sql, *args)` - Try update, insert if not exists
- `upsert_rows(cn, table, rows, **kwargs)` - Insert or update multiple rows based on keys

### Schema Operations

- `reset_table_sequence(cn, table, identity=None)` - Reset table's auto-increment sequence
- `vacuum_table(cn, table)` - Optimize table, reclaiming space
- `reindex_table(cn, table)` - Rebuild table indexes
- `cluster_table(cn, table, index=None)` - Order table data according to an index
- `get_table_columns(cn, table)` - Get all column names for a table
- `get_table_primary_keys(cn, table)` - Get primary key columns for a table
- `get_sequence_columns(cn, table)` - Get sequence/identity columns for a table
- `table_fields(cn, table)` - Get ordered list of all table columns
- `table_data(cn, table, columns=[])` - Get table data by columns

### Connection Utilities

- `isconnection(obj)` - Check if object is a database connection
- `is_psycopg_connection(obj)` - Check if PostgreSQL connection
- `is_pymssql_connection(obj)` - Check if SQL Server connection
- `is_sqlite3_connection(obj)` - Check if SQLite connection

### Exception Types

- `DatabaseError` - Base class for all database errors
- `DbConnectionError` - Connection issues
- `IntegrityError` - Constraint violations
- `ProgrammingError` - SQL syntax errors
- `OperationalError` - Database operational issues
- `UniqueViolation` - Unique constraint violations
- `ConnectionError` - Custom connection errors
- `IntegrityViolationError` - Custom constraint errors
- `QueryError` - Query execution errors
- `TypeConversionError` - Type conversion errors

## SQLAlchemy 2.0 Integration

The database module uses SQLAlchemy 2.0 for connection management and pooling. This makes our connection handling more robust while maintaining complete API compatibility with existing code.

### Key Changes

- SQLAlchemy 2.0 handles all connection management and pooling
- Custom connection pool implementation has been replaced with SQLAlchemy's built-in pooling
- Connection type detection functions have been enhanced to work with SQLAlchemy connections
- Type adapters continue to work with the underlying database drivers
- Standard SQLAlchemy dialect names are used internally ('postgresql', 'mssql', 'sqlite')

### Benefits

- More robust connection validation and error handling
- Industry-standard connection pooling with configurable parameters
- Automatic connection recycling after idle timeouts
- Proper connection validation with pre-ping
- Reduced code complexity for connection management

### Usage

The public API remains unchanged. Existing code will continue to work without modification:

```python
# Create a connection (now uses SQLAlchemy under the hood)
cn = connect('postgres://user:pass@localhost/dbname')

# Use connection pooling (now maps to SQLAlchemy pool parameters)
cn = connect(
    'postgres://user:pass@localhost/dbname',
    use_pool=True,
    pool_max_connections=10,
    pool_max_idle_time=600
)

# All existing query methods work exactly the same
results = select(cn, "SELECT * FROM my_table")
```

### Technical Details

- SQLAlchemy is used exclusively for connection management and pooling
- We are NOT using SQLAlchemy ORM or query building features
- Raw DBAPI connections are still accessible via the `connection` attribute
- Connection type detection functions continue to work with SQLAlchemy connections
- Engine instances are managed in a thread-safe registry with proper cleanup
