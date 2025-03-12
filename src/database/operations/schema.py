"""
Schema and maintenance operations for database tables.
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


def get_table_columns(cn, table):
    """Get all column names for a table"""
    strategy = get_db_strategy(cn)
    return strategy.get_columns(cn, table)


def get_table_primary_keys(cn, table, _=None):
    """Get primary key columns for a table

    Args:
        cn: Database connection
        table: Table name
        _: Extra parameter for database switching (to bypass cache)

    Returns
        List of primary key column names
    """
    from database.utils.cache import CacheManager

    # Use the extra parameter to bypass cache if needed
    if _ is not None:
        strategy = get_db_strategy(cn)
        return strategy.get_primary_keys(cn, table)

    # Get cache instance
    cache_manager = CacheManager.get_instance()
    cache = cache_manager.get_cache('table_primary_keys', maxsize=50, ttl=300)

    # Create cache key (ignore connection object)
    import hashlib
    cache_key = hashlib.md5(str(table).encode()).hexdigest()

    # Check cache
    if cache_key in cache:
        return cache[cache_key]

    # Get primary keys and cache the result
    strategy = get_db_strategy(cn)
    keys = strategy.get_primary_keys(cn, table)
    cache[cache_key] = keys
    return keys


def get_sequence_columns(cn, table):
    """Identify columns that are likely to be sequence/identity columns

    Args:
        cn: Database connection
        table: Table name

    Returns
        List of sequence/identity column names
    """
    strategy = get_db_strategy(cn)
    return strategy.get_sequence_columns(cn, table)


def find_sequence_column(cn, table):
    """Find the best column to reset sequence for.

    Intelligently determines the sequence column using these priorities:
    1. Columns that are both primary key and sequence columns
    2. Columns with 'id' in the name that are primary keys or sequence columns
    3. Any primary key or sequence column
    4. Default to 'id' as a last resort
    """
    sequence_cols = get_sequence_columns(cn, table)
    primary_keys = get_table_primary_keys(cn, table)

    # Find columns that are both PK and sequence columns
    pk_sequence_cols = [col for col in sequence_cols if col in primary_keys]

    if pk_sequence_cols:
        # Among PK sequence columns, prefer ones with 'id' in the name
        id_cols = [col for col in pk_sequence_cols if 'id' in col.lower()]
        if id_cols:
            return id_cols[0]
        return pk_sequence_cols[0]

    # If no PK sequence columns, try sequence columns
    if sequence_cols:
        # Among sequence columns, prefer ones with 'id' in the name
        id_cols = [col for col in sequence_cols if 'id' in col.lower()]
        if id_cols:
            return id_cols[0]
        return sequence_cols[0]

    # If no sequence columns, try primary keys
    if primary_keys:
        # Among primary keys, prefer ones with 'id' in the name
        id_cols = [col for col in primary_keys if 'id' in col.lower()]
        if id_cols:
            return id_cols[0]
        return primary_keys[0]

    # Default fallback
    return 'id'


def table_fields(cn, table):
    """Get all column names for a table ordered by their position"""
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
        raise ValueError('Unsupported database type for table_fields')

    return flds


def table_data(cn, table, columns=[]):
    """Get table data by columns
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
            raise ValueError('Unsupported database type for table_data')

    columns = [f'{col} as {alias}' for col, alias in peel(columns)]
    return select(cn, f"select {','.join(columns)} from {table}")
