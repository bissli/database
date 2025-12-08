"""
Schema and maintenance operations for database tables.

This module provides high-level functions for working with database table schemas
and performing maintenance operations. It uses SQLAlchemy Inspector for standard
schema queries and delegates database-specific operations to strategies.

Functions in this module handle:
- Schema information retrieval (columns, primary keys, sequences)
- Table maintenance (vacuum, reindex, cluster)
- Sequence/identity column management
- Table data access with intelligent column selection

All schema information functions support caching with a bypass_cache parameter
for when fresh information is required.
"""
import logging
from typing import TYPE_CHECKING

from database.strategy import get_db_strategy
from sqlalchemy import inspect

if TYPE_CHECKING:
    from database.connection import ConnectionWrapper

logger = logging.getLogger(__name__)

# Simple TTL-less cache for schema info (cleared on bypass_cache=True)
_schema_cache: dict[tuple, list[str]] = {}


def reset_table_sequence(cn: 'ConnectionWrapper', table: str,
                         identity: str | None = None) -> None:
    """Reset a table's sequence/identity column to the max value + 1.
    """
    strategy = get_db_strategy(cn)
    strategy.reset_sequence(cn, table, identity)


def vacuum_table(cn: 'ConnectionWrapper', table: str) -> None:
    """Optimize a table by reclaiming space.

    This operation varies by database type:
    - PostgreSQL: VACUUM (FULL, ANALYZE)
    - SQLite: VACUUM (entire database)
    """
    strategy = get_db_strategy(cn)
    strategy.vacuum_table(cn, table)


def reindex_table(cn: 'ConnectionWrapper', table: str) -> None:
    """Rebuild indexes for a table.

    This operation varies by database type:
    - PostgreSQL: REINDEX TABLE
    - SQLite: REINDEX
    """
    strategy = get_db_strategy(cn)
    strategy.reindex_table(cn, table)


def cluster_table(cn: 'ConnectionWrapper', table: str,
                  index: str | None = None) -> None:
    """Order table data according to an index.

    This operation is primarily for PostgreSQL:
    - PostgreSQL: CLUSTER table [USING index]
    - Other databases: Not supported (warning logged)
    """
    strategy = get_db_strategy(cn)
    strategy.cluster_table(cn, table, index)


def get_table_columns(cn: 'ConnectionWrapper', table: str,
                      bypass_cache: bool = False) -> list[str]:
    """Get all column names for a table using SQLAlchemy Inspector.
    """
    cache_key = ('columns', id(cn.engine), table)
    if not bypass_cache and cache_key in _schema_cache:
        return _schema_cache[cache_key]

    # Use connection to see uncommitted schema changes within transactions
    inspector = inspect(cn.sa_connection)
    columns = [col['name'] for col in inspector.get_columns(table)]
    _schema_cache[cache_key] = columns
    return columns


def get_table_primary_keys(cn: 'ConnectionWrapper', table: str,
                           bypass_cache: bool = False) -> list[str]:
    """Get primary key columns for a table using SQLAlchemy Inspector.
    """
    cache_key = ('primary_keys', id(cn.engine), table)
    if not bypass_cache and cache_key in _schema_cache:
        return _schema_cache[cache_key]

    # Use connection to see uncommitted schema changes within transactions
    inspector = inspect(cn.sa_connection)
    pk_constraint = inspector.get_pk_constraint(table)
    primary_keys = pk_constraint.get('constrained_columns', [])
    _schema_cache[cache_key] = primary_keys
    return primary_keys


def get_sequence_columns(cn: 'ConnectionWrapper', table: str,
                         bypass_cache: bool = False) -> list[str]:
    """Identify columns that are likely to be sequence/identity columns.
    """
    strategy = get_db_strategy(cn)
    return strategy.get_sequence_columns(cn, table, bypass_cache=bypass_cache)


def find_sequence_column(cn: 'ConnectionWrapper', table: str,
                         bypass_cache: bool = False) -> str:
    """Find the best column to reset sequence for.

    Intelligently determines the sequence column using these priorities:
    1. Columns that are both primary key and sequence columns
    2. Columns with 'id' in the name that are primary keys or sequence columns
    3. Any primary key or sequence column
    4. Default to 'id' as a last resort
    """
    strategy = get_db_strategy(cn)
    return strategy.find_sequence_column(cn, table, bypass_cache=bypass_cache)


def table_fields(cn: 'ConnectionWrapper', table: str,
                 bypass_cache: bool = False) -> list[str]:
    """Get all column names for a table ordered by their position.

    Uses SQLAlchemy Inspector which returns columns in ordinal position.
    """
    # Inspector.get_columns returns columns in ordinal position order
    return get_table_columns(cn, table, bypass_cache=bypass_cache)
