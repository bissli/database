"""
PostgreSQL-specific strategy implementation.

This module implements the DatabaseStrategy interface with PostgreSQL-specific operations.
It handles PostgreSQL's unique features such as:
- VACUUM and ANALYZE for table optimization
- REINDEX for index rebuilding
- CLUSTER for physical reordering of table data
- Sequence management for auto-increment columns
- Metadata retrieval using PostgreSQL system catalogs
"""
import logging

from database.core.transaction import Transaction
from database.strategy.base import DatabaseStrategy

logger = logging.getLogger(__name__)


class PostgresStrategy(DatabaseStrategy):
    """PostgreSQL-specific operations"""

    def vacuum_table(self, cn, table):
        """Optimize a table with VACUUM"""
        # Save current autocommit state
        original_autocommit = cn.connection.autocommit
        try:
            # Set autocommit to True for VACUUM
            cn.connection.autocommit = True
            quoted_table = self.quote_identifier(table)
            from database.operations.query import execute_with_context
            execute_with_context(cn, f'vacuum (full, analyze) {quoted_table}')
        finally:
            # Restore original autocommit state
            cn.connection.autocommit = original_autocommit

    def reindex_table(self, cn, table):
        """Rebuild indexes for a table"""
        # Save current autocommit state
        original_autocommit = cn.connection.autocommit
        try:
            # Set autocommit to True for REINDEX
            cn.connection.autocommit = True
            quoted_table = self.quote_identifier(table)
            from database.operations.query import execute_with_context
            execute_with_context(cn, f'reindex table {quoted_table}')
        finally:
            # Restore original autocommit state
            cn.connection.autocommit = original_autocommit

    def cluster_table(self, cn, table, index=None):
        """Order table data according to an index"""
        # Save current autocommit state
        original_autocommit = cn.connection.autocommit
        try:
            # Set autocommit to True for CLUSTER
            cn.connection.autocommit = True
            quoted_table = self.quote_identifier(table)

            if index is None:
                from database.operations.query import execute_with_context
                execute_with_context(cn, f'cluster {quoted_table}')
            else:
                quoted_index = self.quote_identifier(index)
                from database.operations.query import execute_with_context
                execute_with_context(cn, f'cluster {quoted_table} using {quoted_index}')
        finally:
            # Restore original autocommit state
            cn.connection.autocommit = original_autocommit

    def reset_sequence(self, cn, table, identity=None):
        """Reset the sequence for a table"""
        if identity is None:
            identity = self._find_sequence_column(cn, table)

        quoted_table = self.quote_identifier(table)
        quoted_identity = self.quote_identifier(identity)

        sql = f"""
select
    setval(pg_get_serial_sequence('{table}', '{identity}'), coalesce(max({quoted_identity}),0)+1, false)
from
{quoted_table}
"""
        if isinstance(cn, Transaction):
            cn.execute(sql)
        else:
            from database.operations.query import execute_with_context
            execute_with_context(cn, sql)

        logger.debug(f'Reset sequence for {table=} using {identity=}')

    def get_primary_keys(self, cn, table, bypass_cache=False):
        """Get primary key columns for a table

        Args:
            cn: Database connection object
            table: Table name to get primary keys for
            bypass_cache: If True, bypass cache and query database directly, by default False

        Returns
            list: List of primary key column names
        """
        sql = """
select a.attname as column
from pg_index i
join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey)
where i.indrelid = %s::regclass and i.indisprimary
"""
        from database.operations.query import select_column
        return select_column(cn, sql, table)

    def get_columns(self, cn, table, bypass_cache=False):
        """Get all columns for a table

        Args:
            cn: Database connection object
            table: Table name to get columns for
            bypass_cache: If True, bypass cache and query database directly, by default False

        Returns
            list: List of column names for the specified table
        """
        # This PostgreSQL-specific query gives all columns
        sql = f"""
select skeys(hstore(null::{table})) as column
    """
        from database.operations.query import select_column
        return select_column(cn, sql)

    def get_sequence_columns(self, cn, table, bypass_cache=False):
        """Get columns with sequences

        Args:
            cn: Database connection object
            table: Table name to get sequence columns for
            bypass_cache: If True, bypass cache and query database directly, by default False

        Returns
            list: List of sequence/identity column names for the specified table
        """
        sql = """
        SELECT column_name as column
        FROM information_schema.columns
        WHERE table_name = %s
        AND column_default LIKE 'nextval%%'
        """
        from database.operations.query import select_column
        return select_column(cn, sql, table)

    def configure_connection(self, conn):
        """Configure connection settings for PostgreSQL"""
        from database.utils.auto_commit import enable_auto_commit

        # Set auto-commit for PostgreSQL connections
        enable_auto_commit(conn)

    def quote_identifier(self, identifier):
        """Quote an identifier for PostgreSQL"""
        return '"' + identifier.replace('"', '""') + '"'

    def _find_sequence_column(self, cn, table, bypass_cache=False):
        """Find the best column to reset sequence for

        Args:
            cn: Database connection object
            table: Table name to analyze for sequence columns
            bypass_cache: If True, bypass cache and query database directly, by default False

        Returns
            str: Column name best suited for sequence resetting
        """
        # Use the common implementation from base class
        return super()._find_sequence_column(cn, table, bypass_cache=bypass_cache)
