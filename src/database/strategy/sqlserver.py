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
            identity = self.find_sequence_column(cn, table)

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
SELECT c.name as column_name
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
select c.name as column_name
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
        SELECT c.name as column_name
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

    def get_constraint_definition(self, cn, table, constraint_name):
        """Get the definition of a constraint by name (SQL Server implementation)

        Args:
            cn: Database connection object
            table: The table containing the constraint
            constraint_name: Name of the constraint

        Returns
            dict: Constraint information
        """
        # SQL Server doesn't support using constraint definitions directly in MERGE
        # but we can retrieve the columns for filtering
        sql = """
        SELECT
            col.name as column_name
        FROM
            sys.indexes idx
            JOIN sys.index_columns idxcol ON idx.object_id = idxcol.object_id AND idx.index_id = idxcol.index_id
            JOIN sys.columns col ON idxcol.object_id = col.object_id AND idxcol.column_id = col.column_id
        WHERE
            idx.name = ?
            AND OBJECT_NAME(idx.object_id) = ?
        ORDER BY
            idxcol.index_column_id
        """
        from database.operations.query import select

        # Get just the table name, removing any schema prefix
        table_name = table.split('.')[-1].strip('"[]')

        result = select(cn, sql, constraint_name, table_name)

        if not result:
            raise ValueError(f"Constraint '{constraint_name}' not found on table '{table}'")

        columns = [row['column_name'] for row in result]
        return {
            'name': constraint_name,
            'definition': f"UNIQUE ({', '.join(columns)})",
            'columns': columns
        }

    def get_default_columns(self, cn, table, bypass_cache=False):
        """Get columns suitable for general data display

        Args:
            cn: Database connection object
            table: Table name to get default columns for
            bypass_cache: If True, bypass cache and query database directly, by default False

        Returns
            list: List of column names suitable for general data representation
        """
        sql = """
SELECT c.name
FROM sys.columns c
JOIN sys.tables t ON c.object_id = t.object_id
JOIN sys.types ty ON c.system_type_id = ty.system_type_id
WHERE t.name = ?
AND ty.name IN ('char', 'varchar', 'nvarchar', 'text', 'ntext', 'bit',
    'tinyint', 'smallint', 'int', 'bigint', 'decimal', 'numeric',
    'float', 'real', 'date', 'time', 'datetime', 'datetime2')
ORDER BY c.column_id
"""
        from database.operations.query import select_column
        return select_column(cn, sql, table)

    def get_ordered_columns(self, cn, table, bypass_cache=False):
        """Get all column names for a table ordered by their position

        Args:
            cn: Database connection object
            table: Table name to get columns for
            bypass_cache: If True, bypass cache and query database directly, by default False

        Returns
            list: List of column names ordered by position
        """
        sql = """
SELECT c.name
FROM sys.columns c
JOIN sys.tables t ON c.object_id = t.object_id
WHERE t.name = ?
ORDER BY c.column_id
"""
        from database.operations.query import select_column
        return select_column(cn, sql, table)

    def find_sequence_column(self, cn, table, bypass_cache=False):
        """Find the best column to reset sequence for

        Args:
            cn: Database connection object
            table: Table name to analyze for sequence columns
            bypass_cache: If True, bypass cache and query database directly, by default False

        Returns
            str: Column name best suited for sequence resetting
        """
        # Use the common implementation from base class
        return self._find_sequence_column_impl(cn, table, bypass_cache=bypass_cache)
