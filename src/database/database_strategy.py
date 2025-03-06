"""
Database-specific strategy implementations for core operations.
Provides a consistent interface across different database types.
"""

import logging
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class DatabaseStrategy(ABC):
    """Base class for database-specific operations"""

    @abstractmethod
    def vacuum_table(self, cn, table):
        """Optimize a table by reclaiming space"""

    @abstractmethod
    def reindex_table(self, cn, table):
        """Rebuild indexes for a table"""

    @abstractmethod
    def cluster_table(self, cn, table, index=None):
        """Order table data according to an index"""

    @abstractmethod
    def reset_sequence(self, cn, table, identity=None):
        """Reset the sequence for a table"""

    @abstractmethod
    def get_primary_keys(self, cn, table):
        """Get primary key columns for a table"""

    @abstractmethod
    def get_columns(self, cn, table):
        """Get all columns for a table"""

    @abstractmethod
    def get_sequence_columns(self, cn, table):
        """Get columns with sequences/identities"""

    @abstractmethod
    def configure_connection(self, conn):
        """Configure connection settings"""

    @abstractmethod
    def quote_identifier(self, identifier):
        """Quote a database identifier"""


class PostgresStrategy(DatabaseStrategy):
    """PostgreSQL-specific operations"""

    def vacuum_table(self, cn, table):
        """Optimize a table with VACUUM"""
        from . import client
        cn.connection.set_session(autocommit=True)
        quoted_table = self.quote_identifier(table)
        client.execute(cn, f'vacuum (full, analyze) {quoted_table}')
        cn.connection.set_session(autocommit=False)

    def reindex_table(self, cn, table):
        """Rebuild indexes for a table"""
        from . import client
        cn.connection.set_session(autocommit=True)
        quoted_table = self.quote_identifier(table)
        client.execute(cn, f'reindex table {quoted_table}')
        cn.connection.set_session(autocommit=False)

    def cluster_table(self, cn, table, index=None):
        """Order table data according to an index"""
        from . import client
        cn.connection.set_session(autocommit=True)
        quoted_table = self.quote_identifier(table)

        if index is None:
            client.execute(cn, f'cluster {quoted_table}')
        else:
            quoted_index = self.quote_identifier(index)
            client.execute(cn, f'cluster {quoted_table} using {quoted_index}')

        cn.connection.set_session(autocommit=False)

    def reset_sequence(self, cn, table, identity=None):
        """Reset the sequence for a table"""
        from . import client

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
        if isinstance(cn, client.transaction):
            cn.execute(sql)
        else:
            client.execute(cn, sql)

        logger.debug(f'Reset sequence for {table=} using {identity=}')

    def get_primary_keys(self, cn, table):
        """Get primary key columns for a table"""
        from . import client
        sql = """
select a.attname as column
from pg_index i
join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey)
where i.indrelid = %s::regclass and i.indisprimary
"""
        return client.select_column(cn, sql, table)

    def get_columns(self, cn, table):
        """Get all columns for a table"""
        from . import client

        # This PostgreSQL-specific query gives all columns
        sql = f"""
select skeys(hstore(null::{table})) as column
    """
        return client.select_column(cn, sql)

    def get_sequence_columns(self, cn, table):
        """Get columns with sequences"""
        from . import client
        sql = """
        SELECT column_name as column
        FROM information_schema.columns
        WHERE table_name = %s
        AND column_default LIKE 'nextval%%'
        """
        return client.select_column(cn, sql, table)

    def configure_connection(self, conn):
        """Configure connection settings for PostgreSQL"""
        # No specific configuration needed

    def quote_identifier(self, identifier):
        """Quote an identifier for PostgreSQL"""
        return '"' + identifier.replace('"', '""') + '"'

    def _find_sequence_column(self, cn, table):
        """Find the best column to reset sequence for"""
        sequence_cols = self.get_sequence_columns(cn, table)
        primary_keys = self.get_primary_keys(cn, table)

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


class SQLiteStrategy(DatabaseStrategy):
    """SQLite-specific operations"""

    def vacuum_table(self, cn, table):
        """Optimize a table with VACUUM"""
        from . import client

        # SQLite only supports database-wide VACUUM
        client.execute(cn, 'VACUUM')
        logger.info('Executed VACUUM on entire SQLite database (table-specific vacuum not supported)')

    def reindex_table(self, cn, table):
        """Rebuild indexes for a table"""
        from . import client
        quoted_table = self.quote_identifier(table)
        client.execute(cn, f'REINDEX {quoted_table}')

    def cluster_table(self, cn, table, index=None):
        """SQLite doesn't support CLUSTER"""
        logger.warning('CLUSTER operation not supported in SQLite')

    def reset_sequence(self, cn, table, identity=None):
        """SQLite doesn't need explicit sequence resetting"""
        # SQLite automatically reuses rowids, but we can
        # implement a manual sequence update if needed

    def get_primary_keys(self, cn, table):
        """Get primary key columns for a table"""
        from . import client
        sql = """
select l.name as column from pragma_table_info("%s") as l where l.pk <> 0
"""
        return client.select_column(cn, sql, table)

    def get_columns(self, cn, table):
        """Get all columns for a table"""
        from . import client
        sql = f"""
select name as column from pragma_table_info('{table}')
    """
        return client.select_column(cn, sql)

    def get_sequence_columns(self, cn, table):
        """SQLite uses rowid but reports primary keys as sequence columns"""
        return self.get_primary_keys(cn, table)

    def configure_connection(self, conn):
        """Configure connection settings for SQLite"""
        # Enable foreign keys by default
        conn.execute('PRAGMA foreign_keys = ON')

    def quote_identifier(self, identifier):
        """Quote an identifier for SQLite"""
        return '"' + identifier.replace('"', '""') + '"'


class SQLServerStrategy(DatabaseStrategy):
    """SQL Server-specific operations"""

    def vacuum_table(self, cn, table):
        """Optimize a table with compression and rebuild"""
        from . import client
        quoted_table = self.quote_identifier(table)
        # Closest equivalent is rebuilding the clustered index or table
        client.execute(cn, f'ALTER INDEX ALL ON {quoted_table} REBUILD')
        logger.info(f'Rebuilt all indexes on {table} (SQL Server equivalent of VACUUM)')

    def reindex_table(self, cn, table):
        """Rebuild indexes for a table"""
        from . import client
        quoted_table = self.quote_identifier(table)
        client.execute(cn, f'ALTER INDEX ALL ON {quoted_table} REBUILD')

    def cluster_table(self, cn, table, index=None):
        """SQL Server doesn't support CLUSTER directly"""
        logger.warning('CLUSTER operation not supported in SQL Server')

    def reset_sequence(self, cn, table, identity=None):
        """Reset the identity seed for a table"""
        from . import client

        if identity is None:
            identity = self._find_sequence_column(cn, table)

        quoted_table = self.quote_identifier(table)
        quoted_identity = self.quote_identifier(identity)

        sql = f"""
DECLARE @max int;
SELECT @max = ISNULL(MAX({quoted_identity}), 0) FROM {quoted_table};
DBCC CHECKIDENT ('{table}', RESEED, @max);
"""
        if isinstance(cn, client.transaction):
            cn.execute(sql)
        else:
            client.execute(cn, sql)

        logger.debug(f'Reset identity seed for {table=} using {identity=}')

    def get_primary_keys(self, cn, table):
        """Get primary key columns for a table"""
        from . import client
        sql = """
SELECT c.name as column
FROM sys.indexes i
JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE i.is_primary_key = 1
AND OBJECT_NAME(i.object_id) = %s
"""
        return client.select_column(cn, sql, table)

    def get_columns(self, cn, table):
        """Get all columns for a table"""
        from . import client
        sql = f"""
select c.name as column
from sys.columns c
join sys.tables t on c.object_id = t.object_id
where t.name = '{table}'
    """
        return client.select_column(cn, sql)

    def get_sequence_columns(self, cn, table):
        """Get identity columns"""
        from . import client
        sql = """
        SELECT c.name as column
        FROM sys.columns c
        JOIN sys.tables t ON c.object_id = t.object_id
        WHERE t.name = %s AND c.is_identity = 1
        """
        return client.select_column(cn, sql, table)

    def configure_connection(self, conn):
        """Configure connection settings for SQL Server"""
        # No specific configuration needed

    def quote_identifier(self, identifier):
        """Quote an identifier for SQL Server"""
        return f"[{identifier.replace(']', ']]')}]"

    def _find_sequence_column(self, cn, table):
        """Find the best column to reset sequence for"""
        identity_cols = self.get_sequence_columns(cn, table)
        if identity_cols:
            return identity_cols[0]

        # If no identity column found, check primary keys
        primary_keys = self.get_primary_keys(cn, table)
        if primary_keys:
            return primary_keys[0]

        # Default fallback
        return 'id'


def get_db_strategy(cn):
    """Get database strategy for the connection"""
    from . import client

    if client.is_psycopg_connection(cn):
        return PostgresStrategy()
    elif client.is_sqlite3_connection(cn):
        return SQLiteStrategy()
    elif client.is_pymssql_connection(cn):
        return SQLServerStrategy()
    else:
        raise ValueError(f'Unsupported connection type: {type(cn)}')
