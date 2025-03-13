"""
SQLite-specific strategy implementation.

This module implements the DatabaseStrategy interface with SQLite-specific operations.
It handles SQLite's unique features and limitations such as:
- Database-wide VACUUM (no table-specific optimization)
- REINDEX for index rebuilding
- Lack of CLUSTER support
- Automatic rowid management (no explicit sequence resetting needed)
- Metadata retrieval using SQLite PRAGMA statements
"""
import logging

from database.strategy.base import DatabaseStrategy

logger = logging.getLogger(__name__)


class SQLiteStrategy(DatabaseStrategy):
    """SQLite-specific operations"""

    def vacuum_table(self, cn, table):
        """Optimize a table with VACUUM"""
        from database.operations.query import execute_with_context

        # SQLite only supports database-wide VACUUM
        execute_with_context(cn, 'VACUUM')
        logger.info('Executed VACUUM on entire SQLite database (table-specific vacuum not supported)')

    def reindex_table(self, cn, table):
        """Rebuild indexes for a table"""
        quoted_table = self.quote_identifier(table)
        from database.operations.query import execute_with_context
        execute_with_context(cn, f'REINDEX {quoted_table}')

    def cluster_table(self, cn, table, index=None):
        """SQLite doesn't support CLUSTER"""
        logger.warning('CLUSTER operation not supported in SQLite')

    def reset_sequence(self, cn, table, identity=None):
        """SQLite doesn't need explicit sequence resetting"""
        # SQLite automatically reuses rowids, but we can
        # implement a manual sequence update if needed

    def get_primary_keys(self, cn, table, bypass_cache=False):
        """Get primary key columns for a table

        Args:
            cn: Database connection object
            table: Table name to get primary keys for
            bypass_cache: If True, bypass cache and query database directly, by default False

        Returns
            list: List of primary key column names
        """
        # SQLite pragmas don't use parameter binding
        sql = f"""
select l.name as column from pragma_table_info('{table}') as l where l.pk <> 0
"""
        from database.operations.query import select_column
        return select_column(cn, sql)

    def get_columns(self, cn, table, bypass_cache=False):
        """Get all columns for a table

        Args:
            cn: Database connection object
            table: Table name to get columns for
            bypass_cache: If True, bypass cache and query database directly, by default False

        Returns
            list: List of column names for the specified table
        """
        sql = f"""
select name as column from pragma_table_info('{table}')
    """
        from database.operations.query import select_column
        return select_column(cn, sql)

    def get_sequence_columns(self, cn, table, bypass_cache=False):
        """SQLite uses rowid but reports primary keys as sequence columns

        Args:
            cn: Database connection object
            table: Table name to get sequence columns for
            bypass_cache: If True, bypass cache and query database directly, by default False

        Returns
            list: List of sequence/identity column names for the specified table
        """
        return self.get_primary_keys(cn, table, bypass_cache=bypass_cache)

    def configure_connection(self, conn):
        """Configure connection settings for SQLite"""
        # Enable foreign keys by default
        conn.execute('PRAGMA foreign_keys = ON')

        # Set auto-commit for SQLite connections
        from database.utils.auto_commit import enable_auto_commit
        enable_auto_commit(conn)

    def quote_identifier(self, identifier):
        """Quote an identifier for SQLite"""
        return '"' + identifier.replace('"', '""') + '"'
