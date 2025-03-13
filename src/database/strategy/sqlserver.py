"""
SQL Server-specific strategy implementation.

This module implements the DatabaseStrategy interface with SQL Server-specific operations.
It handles SQL Server's unique features such as:
- Index rebuilding as an alternative to VACUUM
- DBCC CHECKIDENT for identity column resetting
- Metadata retrieval using SQL Server system catalogs
- Proper quoting of identifiers with square brackets
"""
import logging

from database.core.transaction import Transaction
from database.strategy.base import DatabaseStrategy

logger = logging.getLogger(__name__)


class SQLServerStrategy(DatabaseStrategy):
    """SQL Server-specific operations"""

    def vacuum_table(self, cn, table):
        """Optimize a table with compression and rebuild"""
        quoted_table = self.quote_identifier(table)
        # Closest equivalent is rebuilding the clustered index or table
        from database.operations.query import execute_with_context
        execute_with_context(cn, f'ALTER INDEX ALL ON {quoted_table} REBUILD')
        logger.info(f'Rebuilt all indexes on {table} (SQL Server equivalent of VACUUM)')

    def reindex_table(self, cn, table):
        """Rebuild indexes for a table"""
        quoted_table = self.quote_identifier(table)
        from database.operations.query import execute_with_context
        execute_with_context(cn, f'ALTER INDEX ALL ON {quoted_table} REBUILD')

    def cluster_table(self, cn, table, index=None):
        """SQL Server doesn't support CLUSTER directly"""
        logger.warning('CLUSTER operation not supported in SQL Server')

    def reset_sequence(self, cn, table, identity=None):
        """Reset the identity seed for a table"""
        if identity is None:
            identity = self._find_sequence_column(cn, table)

        quoted_table = self.quote_identifier(table)
        quoted_identity = self.quote_identifier(identity)

        sql = f"""
DECLARE @max int;
SELECT @max = ISNULL(MAX({quoted_identity}), 0) FROM {quoted_table};
DBCC CHECKIDENT ('{table}', RESEED, @max);
"""
        if isinstance(cn, Transaction):
            cn.execute(sql)
        else:
            from database.operations.query import execute_with_context
            execute_with_context(cn, sql)

        logger.debug(f'Reset identity seed for {table=} using {identity=}')

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
SELECT c.name as column
FROM sys.indexes i
JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE i.is_primary_key = 1
AND OBJECT_NAME(i.object_id) = ?
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
        sql = """
select c.name as column
from sys.columns c
join sys.tables t on c.object_id = t.object_id
where t.name = ?
    """
        from database.operations.query import select_column
        return select_column(cn, sql, table)

    def get_sequence_columns(self, cn, table, bypass_cache=False):
        """Get identity columns

        Args:
            cn: Database connection object
            table: Table name to get sequence columns for
            bypass_cache: If True, bypass cache and query database directly, by default False

        Returns
            list: List of sequence/identity column names for the specified table
        """
        sql = """
        SELECT c.name as column
        FROM sys.columns c
        JOIN sys.tables t ON c.object_id = t.object_id
        WHERE t.name = ? AND c.is_identity = 1
        """
        from database.operations.query import select_column
        return select_column(cn, sql, table)

    def configure_connection(self, conn):
        """Configure connection settings for SQL Server"""
        from database.utils.auto_commit import enable_auto_commit

        # Set auto-commit for SQL Server connections
        enable_auto_commit(conn)

    def quote_identifier(self, identifier):
        """Quote an identifier for SQL Server"""
        return f"[{identifier.replace(']', ']]')}]"

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
