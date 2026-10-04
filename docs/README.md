# Database Module Documentation

[← Back to Main README](../README.md)

Detailed API documentation and advanced usage for the Database Module.

## Table of Contents

- [Installation](#installation)
- [Connection Management](#connection-management)
  - [Creating Connections](#creating-connections)
  - [Connection Options](#connection-options)
  - [Reader Endpoints and Read-Only Connections](#reader-endpoints-and-read-only-connections)
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
  - [Commits Outside a Transaction](#commits-outside-a-transaction)
  - [Isolation Levels](#isolation-levels)
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
- Data handling: `pandas`, `numpy`
- Utilities: `pyarrow` (optional)

## Connection Management

### Creating Connections

```python
import database as db

# Connect to a database with a dictionary config
cn = db.connect({
    'drivername': 'postgresql',  # or 'sqlite'
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
    'port': 5432,
    'use_pool': True,
    'pool_max_connections': 10
})

# Options as bare keyword arguments
cn = db.connect(drivername='sqlite', database='database.db')
```

`connect` reads keyword options only when `options` is `None`. With a
dict, object or `DatabaseOptions` in `options`, every option goes inside
it.

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
    data_loader=None,          # Custom data loader function (defaults to pandas)
    # Reader endpoint parameters
    reader_hostname=None,      # Host serving the cluster's replicas
    reader_port=0,             # Port on that host
    # Connection pooling parameters
    use_pool=False,            # Enable connection pooling
    pool_max_connections=5,    # Maximum connections in pool
    pool_max_idle_time=300,    # Maximum seconds a connection can be idle
    pool_wait_timeout=30,      # Maximum seconds to wait for a connection
    # SQLite parameters
    journal_mode='wal',        # 'wal', 'delete', 'truncate' or 'persist'
    open_mode=None             # None, 'ro' or 'immutable'; needs role='reader'
)

cn = db.connect(options)
```

A SQLite writer sets `journal_mode` on every connect. `'wal'` pairs with
`synchronous = NORMAL` and every other mode with `synchronous = FULL`.
Only WAL is stored in the database file, so one writer on the default
`'wal'` converts the file for every later opener. A store that must stay
in `delete` mode needs `journal_mode='delete'` on every writer that opens
it.

Switching a WAL file to another mode raises `database is locked` while
any other connection has read the file since opening it. A store already
in WAL is converted with nothing else connected.

In a rollback mode a reader's open transaction blocks a writer's commit.
The commit waits up to the 5000 ms `busy_timeout`, then raises `database
is locked`.

### Reader Endpoints and Read-Only Connections

An Aurora cluster publishes two endpoints: a writer, and a reader serving
its replicas. `connect()` takes a `role` naming which one to open.

```python
import database as db

# The default. Opens hostname:port and writes freely.
writer = db.connect(options)

# Opens reader_hostname:reader_port and is read-only.
reader = db.connect(options, role='reader')
```

`role` is keyword only, and accepts `'writer'` and `'reader'`. Any other
value raises `ValidationError` before a connection opens, as does passing
the role positionally, where it would otherwise land in `config` and be
dropped. A reader carries `cn.readonly == True`, and `cn.options` are the
options as passed in, so handing them back to `connect()` reopens the
same role.

#### Endpoint selection

A reader takes `reader_hostname` and `reader_port` from the options, and
falls back to `hostname` or `port` for whichever of the two is unset. Each
falls back on its own, so a cluster sharing one port needs only the
hostname:

```python
# config.py
postgresql = Setting()
postgresql.hostname = 'cluster.cluster-abc123.us-east-1.rds.amazonaws.com'
postgresql.reader_hostname = 'cluster.cluster-ro-abc123.us-east-1.rds.amazonaws.com'
postgresql.port = 5432
```

A database declaring no reader field still answers `role='reader'`. The
connection lands on the writer endpoint and stays read-only, so a partial
config remains usable.

SQLite has no reader endpoint, so `reader_hostname` and `reader_port` are
ignored there: `role='reader'` opens the same database file read-only.
A SQLite reader also leaves the file itself alone, skipping the
`journal_mode` and `synchronous` pragmas a writer sets, so it can open a
database whose file permissions deny writing.

`role='reader'` with `database=':memory:'` raises `ValidationError`. Each
connection to `':memory:'` owns a private database, so a reader would get
an empty one and report every table as missing.

#### SQLite open modes

`open_mode` sets how a SQLite reader opens the file. It is SQLite only,
defaults to `None`, and otherwise takes `'ro'` or `'immutable'`, in lower
case. Any other value raises `ValidationError` when `DatabaseOptions` is
built. Either mode on a writer raises `ValidationError` at `connect()`.
A reader opened with either mode keeps
every reader guard: `ReadOnlyError` from the library's write methods,
`PRAGMA query_only` on the session, and no `journal_mode` or
`synchronous` pragma.

```python
reader = db.connect({'drivername': 'sqlite', 'database': 'store.db',
                     'open_mode': 'ro'}, role='reader')
```

With `open_mode=None` the file opens read-write at the OS level, the
session refuses writes through `PRAGMA query_only`, and each read takes
the usual shared lock. SQLite creates a missing file as an empty
database, as it does for a writer.

Both modes raise at `connect()` on a missing file and create nothing.

`'ro'` opens the file with SQLite's `mode=ro` URI parameter. On a WAL
file `'ro'` creates the `-shm` and `-wal` files beside it when they do
not exist yet, so the directory must be writable then. With both present,
as when a writer holds the file open, a read-only directory works. `'ro'`
leaves the `-shm` and `-wal` files behind after close.

`'immutable'` opens the file with `mode=ro&immutable=1`. SQLite takes no
lock and reads no journal or `-wal` file, so the reader never waits on a
writer's lock and a writer never waits on the reader. It is safe only
when the file never changes while a connection holds it open. For a
synced file, the usual way to meet that is all three of:

- the file is replaced by rename, as a sync tool such as Dropbox does
- its writer uses a rollback-journal mode such as `delete`
- each read opens a fresh connection, with `use_pool` off

A file rewritten in place under an immutable connection returns wrong
rows or raises a corruption error.

For a reader with `open_mode` set, the library percent-encodes a path
holding `?`, `#` or a space before it reaches SQLite. With `open_mode`
`None` the path reaches SQLite unencoded and opens as it is.

#### What a reader refuses

Two layers reject a write, and both are active on every reader.

The first is the library's own write methods, which write whatever their
arguments and so need no look at any SQL:

- `insert_row`, `insert_rows`, `update_row`, `update_or_insert`,
  `upsert_rows`, `copy_from`
- `vacuum_table`, `reindex_table`, `cluster_table`,
  `reset_table_sequence`

Each raises `ReadOnlyError`, a subclass of `DatabaseError`, in process,
before a statement exists:

```python
reader = db.connect(options, role='reader')

db.select(reader, 'select count(*) from orders')   # fine

db.insert_row(reader, 'orders', ('id',), (1,))
# ReadOnlyError: insert_row is not allowed on a read-only connection
```

The second layer is the session. A PostgreSQL reader runs with
`default_transaction_read_only = on`, a SQLite reader with `PRAGMA
query_only = ON`. A statement written by hand answers to that setting,
whatever its text:

```python
db.execute(reader, 'delete from orders')
# psycopg.errors.ReadOnlySqlTransaction:
# cannot execute DELETE in a read-only transaction
```

The server refuses at statement start, before a row moves, so no write
reaches a replica. This layer also catches what no reading of the
statement text could: a write reached through a function, such as
`select setval(...)`, a write inside a `transaction()` block, and a
statement issued on the raw DBAPI connection. PostgreSQL permits
`VACUUM` and `ANALYZE` under the setting, so `vacuum_table` on a reader
is stopped by the first layer alone.

A reader may not turn that setting off. `SET default_transaction_read_only`,
`RESET default_transaction_read_only`, `RESET ALL`, and an assigning
`PRAGMA query_only` raise `ReadOnlyError`. That check reads the statement
only far enough to recognize those four forms. It does not classify
writes, and every other statement goes to the server.

Reads are untouched. `select`, `select_row`, `select_scalar`,
`select_column`, `table_data`, the schema helpers, and a `transaction()`
block that only reads all behave as they do on a writer.

`cn.dbapi_connection` and the SQLAlchemy methods reached through
attribute delegation, such as `exec_driver_sql`, are the documented
escape hatch out of the wrapper. A statement issued there answers to the
session setting alone, and a caller who reaches that far can also turn
the setting off.

#### Pooling

A reader and a writer pointed at one endpoint hold separate engines, so
the read-only session setting never reaches a writer through the pool.

### Configuration File Pattern

A module-level `config.py` holds one `Setting` object per database
environment:

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

Setting.lock()
```

A caller imports a setting and hands it to `connect`:

```python
# Import from config file
from config import postgresql, sqlite

# Connect to PostgreSQL
pg_cn = db.connect(postgresql)

# Connect to SQLite
lite_cn = db.connect(sqlite)

# A copy overrides settings for one connection
from copy import copy
temp_config = copy(postgresql)
temp_config.use_pool = True
temp_config.pool_max_connections = 10
cn_pool = db.connect(temp_config)
```

### Connection Pooling

Pooling is configured on `DatabaseOptions`, in the options dict, or on a
`Setting` object:

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

cn.close()  # Returns the connection to the pool
```

A PostgreSQL pool holds at most `pool_max_connections` connections,
with no overflow. A checkout pings the connection first and discards a
stale one. A connection returned to the pool is rolled back, so an
aborted transaction never reaches the next checkout.

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
empty_results = db.select(cn, '''
    SELECT id, name FROM users WHERE 1=0;
    SELECT email, status FROM users WHERE 1=0;
''', return_all=True)
print(type(empty_results))                 # <class 'list'>
print(len(empty_results))                  # 2 (list of empty DataFrames)
print(list(empty_results[0].columns))      # ['id', 'name'] (first result set columns)
print(list(empty_results[1].columns))      # ['email', 'status'] (second result set columns)
```

An empty DataFrame keeps the query's columns, in SELECT order, and each
empty result set of a multi-statement query keeps its own.

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

The `Column` class has static helpers for the column list a data loader
receives:

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

[Custom Data Loaders](#custom-data-loaders) shows a loader built on them.

Type information comes from each backend's own metadata:
- PostgreSQL: the native type system, by OID
- SQLite: the declared type string, mapped to a Python type

### Stored Procedures and Multiple Result Sets

`select` reads every result set of a statement that opens with `EXEC`,
`CALL` or `EXECUTE` (in any case), or of any statement under
`return_all=True`. A plain multi-statement query without `return_all`
returns its first result set, whatever `prefer_first` says.

```python
# PostgreSQL stored procedure: the result set with the most rows,
# the earlier one on a tie
result = db.select(cn, 'CALL get_users_by_status(%s)', 'active')

# The procedure's first result set, whatever its size
first_result = db.select(cn, 'CALL get_users_by_status(%s)', 'active',
                         prefer_first=True)

# Every result set, as a list, for a procedure or a plain query
result_sets = db.select(cn, """
    SELECT id, name FROM users WHERE active = true;
    SELECT COUNT(*) AS total_count FROM users;
""", return_all=True)

# A plain multi-statement query: the first result set
first = db.select(cn, """
    SELECT id, name FROM users WHERE active = true;
    SELECT COUNT(*) AS total_count FROM users;
""")
```

`return_all` wins over `prefer_first`. A result set the loader turns
into `None` is left out, and a call whose result sets all load as
`None` returns `[]`.

## Data Manipulation

### Insert Operations

#### insert

Insert a single row:

```python
db.insert(cn, 'INSERT INTO users (name, email) VALUES (%s, %s)',
         'Jane Smith', 'jane@example.com')
```

#### insert_row

Insert a row with named columns:

```python
db.insert_row(cn, 'users',
             ['name', 'email', 'age'],
             ['Jane Smith', 'jane@example.com', 30])
```

#### insert_rows

Insert multiple rows in one `executemany` and return the row count:

```python
rows = [
    {'name': 'John Doe', 'email': 'john@example.com', 'age': 25},
    {'name': 'Jane Smith', 'email': 'jane@example.com'},
    {'name': 'Bob Johnson', 'email': 'bob@example.com', 'age': 45}
]
db.insert_rows(cn, 'users', rows)   # 3; Jane's age binds null
```

`insert_rows` binds by column name. The column list is the union of
every row's keys, matched to the table without regard to case. A row
missing a key another row supplies binds null for it. Keys the table
lacks are dropped, and rows holding only unknown columns return 0 with
nothing written.

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
    'UPDATE users SET active = %s WHERE email = %s',
    'INSERT INTO users (active, email) VALUES (%s, %s)',
    True, 'new@example.com')
```

#### upsert_rows

Insert rows, updating those that hit a conflict target:

```python
rows = [
    {'id': 1, 'name': 'John Doe', 'email': 'john@example.com'},
    {'id': 2, 'name': 'Jane Smith', 'email': 'jane@example.com'},
    {'id': 3, 'name': 'New User', 'email': 'new@example.com'}
]

# Conflict on the primary key; do nothing for a row already there
db.upsert_rows(cn, 'users', rows)

# Update name on conflict
db.upsert_rows(cn, 'users', rows, update_cols_always=['name'])

# Set email on conflict only where the stored value is null
db.upsert_rows(cn, 'users', rows, update_cols_ifnull=['email'])

# Conflict on a unique column set instead of the primary key
db.upsert_rows(cn, 'users', rows, conflict_columns=['email'],
               update_cols_always=['name'])

# PostgreSQL: conflict on a named unique index or constraint
db.upsert_rows(cn, 'users', rows, constraint_name='users_email_key',
               update_cols_always=['name'])

# Reset sequence after operation (for auto-increment columns)
db.upsert_rows(cn, 'users', rows, reset_sequence=True)
```

Both backends use `INSERT ... ON CONFLICT`. With both update lists
`None` the conflict action is `DO NOTHING`. Key columns are dropped from
the update lists, except under `constraint_name`. `constraint_name` and
`conflict_columns` are mutually exclusive and raise `ValidationError`
together. `constraint_name` is PostgreSQL only and ignored on SQLite.
Its conflict target is read from the index or constraint definition: a
partial unique index keeps its `WHERE` predicate, and `INCLUDE`,
`NULLS NOT DISTINCT` and `WITH` clauses are dropped. With no usable
target (no primary key in the rows and no `conflict_columns` or
`constraint_name`) the rows go to `insert_rows`. On SQLite with
`use_primary_key=False`, rows that omit the primary key fall back to
the first unique index whose columns they all supply.

### Delete Operations

#### delete

Delete records:

```python
db.delete(cn, 'DELETE FROM users WHERE active = %s', False)
```

### Multiple-Row Operations

`insert_rows` and `upsert_rows` take a sequence of dict rows and send
them in `executemany` batches, `batch_size` rows (default 500) per
batch for `upsert_rows`:

```python
users = [
    {'name': 'User 1', 'email': 'user1@example.com'},
    {'name': 'User 2', 'email': 'user2@example.com'},
    # ... potentially thousands of rows
]
db.insert_rows(cn, 'users', users)

db.upsert_rows(cn, 'products', products,
               conflict_columns=['product_code'],     # Identify records by product_code
               update_cols_always=['name', 'price'],  # Always update these fields
               update_cols_ifnull=['description'])    # Only update if target is NULL
```

## Transaction Management

### Using Transactions

The `transaction` context manager runs a block as one transaction:

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

A thread holds at most one open `transaction` per connection; a nested
one raises `RuntimeError`. Auto-commit is on after the block, even
where it was off before it.

### Commits Outside a Transaction

Outside a `transaction` block every `execute`, `executemany` and write
method commits the DBAPI connection as soon as it finishes. `cn.commit()`
and `cn.rollback()` act on the DBAPI connection as well as the
SQLAlchemy connection, so they reach work done on a raw cursor.
`cn.close()` commits first unless a transaction is open, then closes;
an error in either step is logged at WARNING and never raised.

### Isolation Levels

`transaction(cn)` takes no isolation level. A block runs at the
driver's default: `READ COMMITTED` on PostgreSQL, a `DEFERRED`
transaction on SQLite. On PostgreSQL a block that needs another level
sets it with its first statement:

```python
with db.transaction(cn) as tx:
    tx.execute('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE')
    tx.execute('UPDATE accounts SET balance = balance - %s WHERE id = %s', 100, 1)
    tx.execute('UPDATE accounts SET balance = balance + %s WHERE id = %s', 100, 2)
```

## Type System

Each value is converted once in each direction:

- **Database to Python**: the database driver and its registered adapters
- **Python to database**: `TypeConverter`, during parameter binding

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

1. **Single conversion point**: a value is converted once, where it
   crosses the database boundary
   - Going in (Python to database): `TypeConverter` during parameter binding
   - Coming out (database to Python): the database driver and the
     adapters `types.py` registers with it

2. **Separate responsibilities**:
   - **Database adapters**: convert values
   - **Column class**: type metadata only, no conversion
   - **Row adapters**: row structure (dict/row format) only, no conversion
   - **Type maps**: database type codes to Python types

3. **Flow of data**:
   ```
   [Python values] -> TypeConverter -> database driver -> database
   database -> database driver -> registered adapters -> [Python values]
   ```

## Schema Operations

Operations on tables and sequences.

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
4. Works across PostgreSQL and SQLite with database-specific implementations

### Table Maintenance Operations

#### vacuum_table

Reclaim space and optimize a table:

```python
db.vacuum_table(cn, 'users')
```

Different behavior by database:
- **PostgreSQL**: Executes `VACUUM users`
- **SQLite**: Executes `VACUUM` on the entire database; `table` is
  required and ignored

#### reindex_table

Rebuild indexes for a table:

```python
db.reindex_table(cn, 'users')
```

Different behavior by database:
- **PostgreSQL**: Executes `REINDEX TABLE users`
- **SQLite**: Rebuilds all indexes on the table

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
primary_keys = cn.get_table_primary_keys('users')  # ['id']

# Get all column names, in declaration order
columns = cn.get_table_columns('users')  # ['id', 'name', ...]
```

`get_table_columns` and `get_table_primary_keys` return the cached list
itself; a caller must not mutate it. The list lives in `Cache`'s schema
cache, keyed by table name and engine, for 600 seconds.
`Cache.get_instance().clear_for_table('users')` drops it, so a caller
that alters a table clears it before the next `insert_rows` or
`upsert_rows`. `cn.get_table_columns('users', bypass_cache=True)` reads
the catalog again.

#### SQLite Schema Operations

```python
# SQLite VACUUM (operates on entire database; the table is ignored)
db.vacuum_table(cn, 'users')

# Get table information
columns = cn.get_table_columns('users')

# Schema introspection (SQLite only; PostgreSQL raises NotImplementedError)
cn.list_tables()                 # ['orders', 'users'], internal tables excluded
cn.table_exists('users')         # True
cn.describe_columns('users')     # [ColumnInfo(name='id', type='INTEGER',
                                 #   notnull=False, default=None, primary_key=True), ...]
cn.get_unique_indexes('users')   # [['email']], primary-key indexes included
cn.table_ddl('users')            # 'CREATE TABLE users (...)'

# Reset AUTOINCREMENT sequence
db.reset_table_sequence(cn, 'users')
```

## Advanced Features

### SQL Query Helpers

`database.sql` exports the identifier quoting and placeholder rewriting
the connection methods use:

```python
from database.sql import prepare_query, quote_identifier

# Quote identifiers for different databases (schema-qualified names
# split on unquoted dots and quote each segment)
table_name = quote_identifier('my_table', 'postgresql')     # '"my_table"'
qualified = quote_identifier('public.users', 'postgresql')  # '"public"."users"'

# IN-clause expansion is built into prepare_query: a sequence at an
# IN placeholder expands to one marker per item.
sql, args = prepare_query(
    'SELECT * FROM users WHERE status IN %s',
    [('active', 'pending', 'new')],
    'postgresql',
)
# sql becomes 'SELECT * FROM users WHERE status IN (%s, %s, %s)'
# args becomes ('active', 'pending', 'new')
```

### Custom Data Loaders

`data_loader` sets the form query results take:

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

# A custom data loader
def my_custom_loader(data, columns, **kwargs):
    """Rows as dicts keyed by column name.

    Parameters
    ----------
    data : iterable
        Raw rows from the database.
    columns : list[Column]
        Column objects with names and types.
    **kwargs
        The select() keyword arguments the connection did not consume.
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

cn = db.connect({
    'drivername': 'postgres',
    'database': 'your_database',
    'data_loader': my_custom_loader,
})
```

### Caching

`database.cache.Cache` is a singleton registry of named TTL caches. The
strategies' `get_primary_keys`, `get_columns` and `get_sequence_columns`
results live in it:

```python
from database.cache import Cache
import cachetools

# Get the singleton cache manager instance
cache_manager = Cache.get_instance()

# Create or get a TTL cache
cache = cache_manager.get_cache('my_cache', maxsize=100, ttl=300)

# Use with cachetools decorators
@cachetools.cached(cache=cache)
def fetch_user(user_id):
    return db.select_row(cn, "SELECT * FROM users WHERE id = %s", user_id)

# Clear all caches
cache_manager.clear_all()

# Clear cache entries for a specific table
cache_manager.clear_for_table('users')
```

`clear_for_table` matches the table part of each key exactly, by its
last dotted segment, unquoted and lower-cased: `'order'` drops
`order:...`, `public.order:...` and `other.order:...`, and keeps
`orders:...`. An empty name logs a warning and drops nothing. The
schema cache behind `get_table_columns` and `get_table_primary_keys`
lives in the same registry, so `clear_for_table` and `clear_all` reach
it too.

Each key holds the table name in the case the caller passed and a
number unique to the engine. PostgreSQL reads `"MixedCase"` and
`mixedcase` as two tables, and two databases may each hold a `users`
table, so neither pair shares an entry. An unqualified PostgreSQL name
keeps the result its `search_path` gave. A caller that changes
`search_path` calls `clear_for_table`.

### SQL Parameter Handling

`prepare_query` rewrites placeholders for the connection's dialect and
handles `IN` lists, `LIKE` patterns, and `NULL` comparisons. The SQL
keywords `NULL`, `IN`, and `LIKE` match in any case.

#### LIKE Clauses and Percent Signs

The table applies to PostgreSQL. SQLite's driver never reads `%`, so
no `%` is doubled for SQLite.

| Statement as written                                                                         | Statement the driver receives                                                  |
| -------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| `db.select(cn, "SELECT * FROM users WHERE name LIKE 'test%'")`                               | `"SELECT * FROM users WHERE name LIKE 'test%'"`                                |
| `db.select(cn, "SELECT * FROM users WHERE code LIKE '%%CODE' AND id = %s", 1)`               | `"SELECT * FROM users WHERE code LIKE '%%CODE' AND id = %s", 1`                |
| `db.select(cn, "SELECT * FROM users WHERE name LIKE %s", "test%")`                           | `"SELECT * FROM users WHERE name LIKE %s", "test%"`                            |
| `db.select(cn, "SELECT * FROM products WHERE code LIKE 'PRD-%' AND name LIKE %s", "Chair%")` | `"SELECT * FROM products WHERE code LIKE 'PRD-%%' AND name LIKE %s", "Chair%"` |
| `db.select(cn, "SELECT '%s' AS tag, name FROM users WHERE id = %s", 1)`                      | `"SELECT '%%s' AS tag, name FROM users WHERE id = %s", 1`                      |

On PostgreSQL a `%%` in the statement means one `%`, whether or not
the call binds args. The alias `"SI %% Float"` in libtc reads back as
`SI % Float` either way. A lone `%` means `%` as well.

With bound args psycopg reads every `%` as a format directive, so
`prepare_query` doubles each lone `%` that is not a placeholder: in a
string literal, a quoted identifier, a comment, a dollar-quoted body,
or a modulo operator. A `%%` the caller writes stays `%%`, and psycopg
sends it as one `%`. A `%` in a parameter value is never touched.

A call that binds no parameters runs without them, after the cursor
turns each `%%` into `%`. That covers a call with no args, a call
whose args all inline into the text (`is %s` with `None`, `in %s` with
an empty list), and a statement with no placeholder in a
multi-statement call. PL/pgSQL `raise` and `format()` read `%%` as one
literal percent, so their text takes `%%%%` through this library. Only
`cn.cursor().execute(sql)` with no args sends a `%%` unchanged.

SQLite rewrites no `%`, so a `%%` reaches it as two characters. A lone
`%`, in a literal or as modulo, means the same on both dialects.

#### IS NULL / IS NOT NULL Handling

| Statement as written                                                                           | Statement the driver receives                                                   |
| ---------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| `db.select(cn, "SELECT * FROM users WHERE last_login IS NULL")`                                | `"SELECT * FROM users WHERE last_login IS NULL"`                                |
| `db.select(cn, "SELECT * FROM users WHERE email IS NOT NULL")`                                 | `"SELECT * FROM users WHERE email IS NOT NULL"`                                 |
| `db.select(cn, "SELECT * FROM users WHERE last_login IS %s", None)`                            | `"SELECT * FROM users WHERE last_login IS NULL"`                                |
| `db.select(cn, "SELECT * FROM users WHERE email IS NOT %s", None)`                             | `"SELECT * FROM users WHERE email IS NOT NULL"`                                 |
| `db.select(cn, "SELECT * FROM orders WHERE date > %s AND tracking_number IS NULL", some_date)` | `"SELECT * FROM orders WHERE date > %s AND tracking_number IS NULL", some_date` |

`None` at `IS %s` or `IS NOT %s` is inlined as `NULL` on both backends.

#### IN Clause Parameter Handling

| Statement as written                                                                                                                                    | Statement the driver receives                                                                            |
| ------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `db.select(cn, "SELECT * FROM users WHERE id IN %s", [1, 2, 3])`                                                                                        | `"SELECT * FROM users WHERE id IN (%s, %s, %s)", 1, 2, 3`                                                |
| `db.select(cn, "SELECT * FROM users WHERE status IN %s", ['active'])`                                                                                   | `"SELECT * FROM users WHERE status IN (%s)", "active"`                                                   |
| `db.select(cn, "SELECT * FROM users WHERE id IN %s", ([1, 2, 3],))`                                                                                     | `"SELECT * FROM users WHERE id IN (%s, %s, %s)", 1, 2, 3`                                                |
| `db.select(cn, "SELECT * FROM users WHERE id IN %(ids)s", {'ids': [1, 2, 3]})`                                                                          | `"SELECT * FROM users WHERE id IN (%s, %s, %s)", 1, 2, 3`                                                |
| `db.select(cn, """SELECT * FROM products WHERE category IN %(cat)s AND status IN %(status)s""", {'cat': ['electronics'], 'status': ['active', 'new']})` | `"SELECT * FROM products WHERE category IN (%s) AND status IN (%s, %s)", "electronics", "active", "new"` |

An empty sequence at an `IN` placeholder becomes `(null)`, which
matches no row. With one placeholder in the statement, a lone sequence
is the `IN` list itself; with several, a lone sequence fills the
placeholders in order. A dict binds by name only when the SQL names a
placeholder (`%(name)s`, or `:name` on SQLite); under `%s` or `?` alone
a dict is one value, for a JSON column.

### Common Table Expressions (CTEs)

Placeholders inside a CTE bind as anywhere else:

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

# Recursive CTE (PostgreSQL)
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

## Database-Specific Features

### PostgreSQL Features

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

```python
# SQLite VACUUM (operates on entire database; the table is ignored)
db.vacuum_table(cn, 'users')

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

## API Reference

The public functions of `database` and the methods they wrap.

### Core Functions

| Function                                                    | Description                        | Parameters                                                                                                                                                                                                                                                        | Returns                            |
| ----------------------------------------------------------- | ---------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------- |
| `connect(options, config=None, *, role='writer', **kwargs)` | Create database connection         | `options`: `DatabaseOptions`, dict, `Setting` object, dotted config path, or `None`<br>`config`: Configuration object a dotted path resolves against<br>`role`: `'writer'` or `'reader'`, keyword only<br>`**kwargs`: Options, read only when `options` is `None` | `ConnectionWrapper`                |
| `execute(cn, sql, *args)`                                   | Execute SQL statement              | `cn`: Database connection<br>`sql`: SQL statement<br>`*args`: Query parameters                                                                                                                                                                                    | Row count or specified return data |
| `transaction(cn)`                                           | Create transaction context manager | `cn`: Database connection                                                                                                                                                                                                                                         | `Transaction` context manager      |

### Query Operations

| Function                                | Description                                    | Parameters                                                                                                          | Returns                             |
| --------------------------------------- | ---------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- | ----------------------------------- |
| `select(cn, sql, *args, **kwargs)`      | Execute SELECT query or stored procedure       | `cn`: Database connection<br>`sql`: SELECT statement<br>`*args`: Query parameters<br>`**kwargs`: Additional options | DataFrame or list                   |
| `select_row(cn, sql, *args)`            | Execute query, return single row               | `cn`: Database connection<br>`sql`: SELECT statement<br>`*args`: Query parameters                                   | Row as attribute dictionary         |
| `select_row_or_none(cn, sql, *args)`    | Like select_row but returns None if no rows    | `cn`: Database connection<br>`sql`: SELECT statement<br>`*args`: Query parameters                                   | Row as attribute dictionary or None |
| `select_scalar(cn, sql, *args)`         | Execute query, return single value             | `cn`: Database connection<br>`sql`: SELECT statement<br>`*args`: Query parameters                                   | Single value                        |
| `select_scalar_or_none(cn, sql, *args)` | Like select_scalar but returns None if no rows | `cn`: Database connection<br>`sql`: SELECT statement<br>`*args`: Query parameters                                   | Single value or None                |
| `select_column(cn, sql, *args)`         | Execute query, return single column            | `cn`: Database connection<br>`sql`: SELECT statement<br>`*args`: Query parameters                                   | List of values                      |

### Data Operations

| Function                                                                                                                                                                                   | Description                                              | Parameters                                                                                                                                                                                                   | Returns   |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------- |
| `insert(cn, sql, *args)`                                                                                                                                                                   | Execute INSERT statement                                 | `cn`: Database connection<br>`sql`: INSERT statement<br>`*args`: Query parameters                                                                                                                            | Row count |
| `update(cn, sql, *args)`                                                                                                                                                                   | Execute UPDATE statement                                 | `cn`: Database connection<br>`sql`: UPDATE statement<br>`*args`: Query parameters                                                                                                                            | Row count |
| `delete(cn, sql, *args)`                                                                                                                                                                   | Execute DELETE statement                                 | `cn`: Database connection<br>`sql`: DELETE statement<br>`*args`: Query parameters                                                                                                                            | Row count |
| `insert_row(cn, table, fields, values)`                                                                                                                                                    | Insert single row with named fields                      | `cn`: Database connection<br>`table`: Table name<br>`fields`: List of column names<br>`values`: List of values                                                                                               | Row count |
| `insert_rows(cn, table, rows)`                                                                                                                                                             | Insert multiple rows                                     | `cn`: Database connection<br>`table`: Table name<br>`rows`: List of dictionaries                                                                                                                             | Row count |
| `update_row(cn, table, keyfields, keyvalues, datafields, datavalues)`                                                                                                                      | Update rows matching the key fields                      | `cn`: Database connection<br>`table`: Table name<br>`keyfields`: List of key column names<br>`keyvalues`: List of key values<br>`datafields`: List of data column names<br>`datavalues`: List of data values | Row count |
| `update_or_insert(cn, update_sql, insert_sql, *args)`                                                                                                                                      | Try update, insert if no row matched                     | `cn`: Database connection<br>`update_sql`: UPDATE statement<br>`insert_sql`: INSERT statement<br>`*args`: Query parameters                                                                                   | Row count |
| `upsert_rows(cn, table, rows, constraint_name=None, conflict_columns=None, update_cols_always=None, update_cols_ifnull=None, reset_sequence=False, batch_size=500, use_primary_key=False)` | Insert rows, updating those that hit the conflict target | `cn`: Database connection<br>`table`: Table name<br>`rows`: List of dictionaries<br>See [upsert_rows](#upsert_rows) for the rest                                                                             | Row count |
| `copy_from(cn, table, file, columns=None)`                                                                                                                                                 | Bulk load CSV text with PostgreSQL `COPY`                | `cn`: Database connection<br>`table`: Table name<br>`file`: Text file object<br>`columns`: Optional list of column names                                                                                     | Row count |

### Schema Operations

| Function                                         | Description                            | Parameters                                                                                    | Returns |
| ------------------------------------------------ | -------------------------------------- | --------------------------------------------------------------------------------------------- | ------- |
| `reset_table_sequence(cn, table, identity=None)` | Reset table's auto-increment sequence  | `cn`: Database connection<br>`table`: Table name<br>`identity`: Optional identity column name | None    |
| `vacuum_table(cn, table)`                        | Optimize table, reclaiming space       | `cn`: Database connection<br>`table`: Table name                                              | None    |
| `reindex_table(cn, table)`                       | Rebuild table indexes                  | `cn`: Database connection<br>`table`: Table name                                              | None    |
| `cluster_table(cn, table, index=None)`           | Order table data according to an index | `cn`: Database connection<br>`table`: Table name<br>`index`: Optional index name              | None    |

The schema readers are methods on the connection, with a `bypass_cache`
keyword that rereads the catalog:

| Method                               | Description                       | Parameters                                                      | Returns                      |
| ------------------------------------ | --------------------------------- | --------------------------------------------------------------- | ---------------------------- |
| `cn.get_table_columns(table)`        | Column names in declaration order | `table`: Table name                                             | List of column names, cached |
| `cn.get_table_primary_keys(table)`   | Primary key columns               | `table`: Table name                                             | List of column names, cached |
| `cn.get_sequence_columns(table)`     | Sequence/identity columns         | `table`: Table name                                             | List of column names         |
| `cn.table_fields(table)`             | Same as `get_table_columns`       | `table`: Table name                                             | List of column names, cached |
| `cn.table_data(table, columns=None)` | Every row, through `select()`     | `table`: Table name<br>`columns`: Optional list of column names | The data loader's result     |

### Exception Types

| Exception                 | Description                         |
| ------------------------- | ----------------------------------- |
| `DatabaseError`           | Base class for all database errors  |
| `DbConnectionError`       | Connection issues                   |
| `IntegrityError`          | Constraint violations               |
| `ProgrammingError`        | SQL syntax errors                   |
| `OperationalError`        | Database operational issues         |
| `UniqueViolation`         | Unique constraint violations        |
| `ConnectionFailure`       | Custom connection errors            |
| `IntegrityViolationError` | Custom constraint errors            |
| `QueryError`              | Query execution errors              |
| `TypeConversionError`     | Type conversion errors              |
| `ValidationError`         | Bad input: options, role, row count |
| `ReadOnlyError`           | Write on a read-only connection     |

`DbConnectionError`, `IntegrityError`, `ProgrammingError`,
`OperationalError` and `UniqueViolation` are tuples of driver and
library classes, for `except` clauses.

`database.exceptions.is_retryable_error(exc)` says whether a retry may
succeed. An error carrying `connection_invalidated` is retryable. A
driver error with a SQLSTATE is retryable when the code starts with an
entry of `RETRYABLE_SQLSTATES` (`08`, `25P03`, `53300`, `57P`); a
statement timeout or a lock timeout is not. A `sqlite3` error is never
retryable. An error with no SQLSTATE is retryable when its message,
double-quoted names removed, matches `RETRYABLE_PATTERNS`.
