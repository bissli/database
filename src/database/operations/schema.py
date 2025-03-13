"""
Schema and maintenance operations for database tables.

This module provides high-level functions for working with database table schemas
and performing maintenance operations. It uses the strategy pattern to delegate
database-specific implementations while providing a consistent interface.

Functions in this module handle:
- Schema information retrieval (columns, primary keys, sequences)
- Table maintenance (vacuum, reindex, cluster)
- Sequence/identity column management
- Table data access with intelligent column selection

All schema information functions support caching with a bypass_cache parameter
for when fresh information is required.
"""
import logging

from database.strategy import get_db_strategy
from database.utils.connection_utils import is_psycopg_connection
from database.utils.connection_utils import is_pyodbc_connection
from database.utils.connection_utils import is_sqlite3_connection

logger = logging.getLogger(__name__)


def reset_table_sequence(cn, table, identity=None):
    """Reset a table's sequence/identity column to the max value + 1

    Args:
        cn: Database connection
        table: Table name
        identity: Identity column name (auto-detected if None)
    """
    strategy = get_db_strategy(cn)
    strategy.reset_sequence(cn, table, identity)


def vacuum_table(cn, table):
    """Optimize a table by reclaiming space

    This operation varies by database type:
    - PostgreSQL: VACUUM (FULL, ANALYZE)
    - SQLite: VACUUM (entire database)
    - SQL Server: Rebuilds all indexes
    """
    strategy = get_db_strategy(cn)
    strategy.vacuum_table(cn, table)


def reindex_table(cn, table):
    """Rebuild indexes for a table

    This operation varies by database type:
    - PostgreSQL: REINDEX TABLE
    - SQLite: REINDEX
    - SQL Server: ALTER INDEX ALL ... REBUILD
    """
    strategy = get_db_strategy(cn)
    strategy.reindex_table(cn, table)


def cluster_table(cn, table, index: str = None):
    """Order table data according to an index

    This operation is primarily for PostgreSQL:
    - PostgreSQL: CLUSTER table [USING index]
    - Other databases: Not supported (warning logged)
    """
    strategy = get_db_strategy(cn)
    strategy.cluster_table(cn, table, index)


def get_table_columns(cn, table, bypass_cache=False):
    """Get all column names for a table
    
    Args:
        cn: Database connection
        table: Table name to get columns for
        bypass_cache: If True, bypass cache and query database directly, by default False
        
    Returns:
        list: List of column names for the specified table
    """
    strategy = get_db_strategy(cn)
    return strategy.get_columns(cn, table, bypass_cache=bypass_cache)


def get_table_primary_keys(cn, table, bypass_cache=False):
    """Get primary key columns for a table

    Args:
        cn: Database connection
        table: Table name to get primary keys for
        bypass_cache: If True, bypass cache and query database directly, by default False

    Returns:
        list: List of primary key column names
    """
    strategy = get_db_strategy(cn)
    return strategy.get_primary_keys(cn, table, bypass_cache=bypass_cache)


def get_sequence_columns(cn, table, bypass_cache=False):
    """Identify columns that are likely to be sequence/identity columns

    Args:
        cn: Database connection
        table: Table name to get sequence columns for
        bypass_cache: If True, bypass cache and query database directly, by default False

    Returns:
        list: List of sequence/identity column names for the specified table
    """
    strategy = get_db_strategy(cn)
    return strategy.get_sequence_columns(cn, table, bypass_cache=bypass_cache)


def find_sequence_column(cn, table, bypass_cache=False):
    """Find the best column to reset sequence for.

    Intelligently determines the sequence column using these priorities:
    1. Columns that are both primary key and sequence columns
    2. Columns with 'id' in the name that are primary keys or sequence columns
    3. Any primary key or sequence column
    4. Default to 'id' as a last resort
    
    Args:
        cn: Database connection
        table: Table name to analyze for sequence columns
        bypass_cache: If True, bypass cache and query database directly, by default False
        
    Returns:
        str: Column name best suited for sequence resetting
    """
    # Use the strategy's implementation directly rather than duplicating logic
    strategy = get_db_strategy(cn)
    return strategy._find_sequence_column(cn, table, bypass_cache=bypass_cache)


def table_fields(cn, table, bypass_cache=False):
    """Get all column names for a table ordered by their position
    
    Args:
        cn: Database connection
        table: Table name to get fields for
        bypass_cache: If True, bypass cache and query database directly, by default False
        
    Returns:
        list: List of column names ordered by position
    """
    # For most cases, we can just use the cacheable get_table_columns method
    # This takes advantage of the caching system but maintains backward compatibility
    strategy = get_db_strategy(cn)
    
    # If a specific order is needed, use direct database queries
    from database.operations.query import select_column

    if is_psycopg_connection(cn):
        flds = select_column(cn, """
select
t.column_name
from information_schema.columns t
where
t.table_name = %s
order by
t.ordinal_position
""", table)
    elif is_sqlite3_connection(cn):
        flds = select_column(cn, """
select name from pragma_table_info('%s')
order by cid
""", table)
    elif is_pyodbc_connection(cn):
        flds = select_column(cn, """
select c.name
from sys.columns c
join sys.tables t on c.object_id = t.object_id
where t.name = ?
order by c.column_id
""", table)
    else:
        # For unsupported types, use cached columns(defaults to alphabetical order)
        flds = strategy.get_columns(cn, table, bypass_cache=bypass_cache)

    return flds


def table_data(cn, table, columns=[], bypass_cache=False):
    """Get table data by columns
    
    Args:
        cn: Database connection
        table: Table name to get data from
        columns: List of columns to retrieve (if empty, auto-detects)
        bypass_cache: If True, bypass cache when auto-detecting columns, by default False
        
    Returns:
        list or DataFrame: Table data for the specified columns
    """
    from database.operations.query import select, select_column

    from libb import peel

    if not columns:
        if is_psycopg_connection(cn):
            columns = select_column(cn, """
select
t.column_name
from information_schema.columns t
where
t.table_name = %s
and t.data_type in ('character', 'character varying', 'boolean',
    'text', 'double precision', 'real', 'integer', 'date',
    'time without time zone', 'timestamp without time zone')
order by
t.ordinal_position
""", table)
        elif is_sqlite3_connection(cn):
            columns = select_column(cn, """
SELECT name FROM pragma_table_info('%s')
""", table)
        elif is_pyodbc_connection(cn):
            columns = select_column(cn, """
SELECT c.name
FROM sys.columns c
JOIN sys.tables t ON c.object_id = t.object_id
JOIN sys.types ty ON c.system_type_id = ty.system_type_id
WHERE t.name = ?
AND ty.name IN ('char', 'varchar', 'nvarchar', 'text', 'ntext', 'bit',
    'tinyint', 'smallint', 'int', 'bigint', 'decimal', 'numeric',
    'float', 'real', 'date', 'time', 'datetime', 'datetime2')
ORDER BY c.column_id
""", table)
        else:
            # For unsupported types, use cached columns
            columns = get_table_columns(cn, table, bypass_cache=bypass_cache)

    columns = [f'{col} as {alias}' for col, alias in peel(columns)]
    return select(cn, f"select {','.join(columns)} from {table}")
