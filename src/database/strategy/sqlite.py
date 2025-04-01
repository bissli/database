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

from database.core.query import execute
from database.operations.query import select, select_column
from database.strategy.base import DatabaseStrategy
from database.utils.auto_commit import enable_auto_commit

logger = logging.getLogger(__name__)


class SQLiteStrategy(DatabaseStrategy):
    """SQLite-specific operations"""

    def vacuum_table(self, cn, table):
        """Optimize a table with VACUUM"""

        # SQLite only supports database-wide VACUUM
        execute(cn, 'VACUUM')
        logger.info('Executed VACUUM on entire SQLite database (table-specific vacuum not supported)')

    def reindex_table(self, cn, table):
        """Rebuild indexes for a table"""
        quoted_table = self.quote_identifier(table)
        execute(cn, f'REINDEX {quoted_table}')

    def cluster_table(self, cn, table, index=None):
        """SQLite doesn't support CLUSTER"""
        logger.warning('CLUSTER operation not supported in SQLite')

    def reset_sequence(self, cn, table, identity=None):
        """SQLite doesn't need explicit sequence resetting"""
        # SQLite automatically reuses rowids, but we can
        # implement a manual sequence update if needed
        if identity is None:
            identity = self.find_sequence_column(cn, table)

        # SQLite handles sequence resetting automatically

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
        enable_auto_commit(conn)

    def quote_identifier(self, identifier):
        """Quote an identifier for SQLite"""
        return '"' + identifier.replace('"', '""') + '"'

    def get_constraint_definition(self, cn, table, constraint_name):
        """Get the definition of a constraint by name (SQLite implementation)

        Args:
            cn: Database connection object
            table: The table containing the constraint
            constraint_name: Name of the constraint

        Returns
            dict: Constraint information
        """
        logger.warning("SQLite doesn't fully support constraint definition retrieval")
        # SQLite doesn't offer the same rich constraint info as PostgreSQL
        # but we can get basic indices info
        sql = f"PRAGMA index_info('{constraint_name}')"
        result = select(cn, sql)

        if not result:
            raise ValueError(f"Constraint '{constraint_name}' not found on table '{table}'")

        columns = [row['name'] for row in result]
        return {
            'name': constraint_name,
            'definition': f"UNIQUE ({', '.join(columns)})",
            'columns': columns
        }
