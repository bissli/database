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

    Returns
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

    Returns
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

    Returns
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

    Returns
        str: Column name best suited for sequence resetting
    """
    # Use the strategy's implementation directly rather than duplicating logic
    strategy = get_db_strategy(cn)
    return strategy.find_sequence_column(cn, table, bypass_cache=bypass_cache)


def table_fields(cn, table, bypass_cache=False):
    """Get all column names for a table ordered by their position

    Args:
        cn: Database connection
        table: Table name to get fields for
        bypass_cache: If True, bypass cache and query database directly, by default False

    Returns
        list: List of column names ordered by position
    """
    strategy = get_db_strategy(cn)
    return strategy.get_ordered_columns(cn, table, bypass_cache=bypass_cache)
