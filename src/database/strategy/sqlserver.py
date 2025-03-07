"""
SQL Server-specific strategy implementation.
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

    def get_primary_keys(self, cn, table):
        """Get primary key columns for a table"""
        sql = """
SELECT c.name as column
FROM sys.indexes i
JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE i.is_primary_key = 1
AND OBJECT_NAME(i.object_id) = %s
"""
        from database.operations.query import select_column
        return select_column(cn, sql, table)

    def get_columns(self, cn, table):
        """Get all columns for a table"""
        sql = f"""
select c.name as column
from sys.columns c
join sys.tables t on c.object_id = t.object_id
where t.name = '{table}'
    """
        from database.operations.query import select_column
        return select_column(cn, sql)

    def get_sequence_columns(self, cn, table):
        """Get identity columns"""
        sql = """
        SELECT c.name as column
        FROM sys.columns c
        JOIN sys.tables t ON c.object_id = t.object_id
        WHERE t.name = %s AND c.is_identity = 1
        """
        from database.operations.query import select_column
        return select_column(cn, sql, table)

    def configure_connection(self, conn):
        """Configure connection settings for SQL Server"""
        # No specific configuration needed

    def quote_identifier(self, identifier):
        """Quote an identifier for SQL Server"""
        return f"[{identifier.replace(']', ']]')}]"

    def _find_sequence_column(self, cn, table):
        """Find the best column to reset sequence for"""
        # Get identity columns (SQL Server's version of sequences)
        identity_cols = self.get_sequence_columns(cn, table)

        # Get primary keys
        primary_keys = self.get_primary_keys(cn, table)

        # Find columns that are both PK and identity columns
        pk_identity_cols = [col for col in identity_cols if col in primary_keys]

        if pk_identity_cols:
            # Among PK identity columns, prefer ones with 'id' in the name
            id_cols = [col for col in pk_identity_cols if 'id' in col.lower()]
            if id_cols:
                return id_cols[0]
            return pk_identity_cols[0]

        # If no PK identity columns, try identity columns
        if identity_cols:
            # Among identity columns, prefer ones with 'id' in the name
            id_cols = [col for col in identity_cols if 'id' in col.lower()]
            if id_cols:
                return id_cols[0]
            return identity_cols[0]

        # If no identity columns, try primary keys
        if primary_keys:
            # Among primary keys, prefer ones with 'id' in the name
            id_cols = [col for col in primary_keys if 'id' in col.lower()]
            if id_cols:
                return id_cols[0]
            return primary_keys[0]

        # Default fallback
        return 'id'
