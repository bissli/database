"""
Base strategy interface for database operations.

Defines the abstract base class that all database-specific strategy implementations
must inherit from. The strategy pattern allows for encapsulating database-specific
behaviors while presenting a consistent interface to the rest of the application.

Each concrete strategy implements operations with database-specific SQL and techniques,
but clients can work with any database through this consistent interface.
"""
from abc import ABC, abstractmethod

from database.strategy.decorators import cacheable_strategy


class DatabaseStrategy(ABC):
    """Base class for database-specific operations"""

    @abstractmethod
    def vacuum_table(self, cn, table):
        """Optimize a table by reclaiming space

        Args:
            cn: Database connection object
            table: Name of the table to vacuum
        """

    @abstractmethod
    def reindex_table(self, cn, table):
        """Rebuild indexes for a table

        Args:
            cn: Database connection object
            table: Name of the table to reindex
        """

    @abstractmethod
    def cluster_table(self, cn, table, index=None):
        """Order table data according to an index

        Args:
            cn: Database connection object
            table: Name of the table to cluster
            index: Name of the index to cluster by, by default None
        """

    @abstractmethod
    def reset_sequence(self, cn, table, identity=None):
        """Reset the sequence for a table

        Args:
            cn: Database connection object
            table: Name of the table with the sequence
            identity: Name of the identity column, by default None which triggers auto-detection
        """

    @abstractmethod
    @cacheable_strategy('primary_keys', ttl=300, maxsize=50)
    def get_primary_keys(self, cn, table, bypass_cache=False):
        """Get primary key columns for a table

        Args:
            cn: Database connection object
            table: Table name to get primary keys for
            bypass_cache: If True, bypass cache and query database directly, by default False

        Returns
            list: List of primary key column names
        """

    @abstractmethod
    @cacheable_strategy('table_columns', ttl=300, maxsize=50)
    def get_columns(self, cn, table, bypass_cache=False):
        """Get all columns for a table

        Args:
            cn: Database connection object
            table: Table name to get columns for
            bypass_cache: If True, bypass cache and query database directly, by default False

        Returns
            list: List of column names for the specified table
        """

    @abstractmethod
    @cacheable_strategy('sequence_columns', ttl=300, maxsize=50)
    def get_sequence_columns(self, cn, table, bypass_cache=False):
        """Get columns with sequences/identities

        Args:
            cn: Database connection object
            table: Table name to get sequence columns for
            bypass_cache: If True, bypass cache and query database directly, by default False

        Returns
            list: List of sequence/identity column names for the specified table
        """

    @abstractmethod
    def configure_connection(self, conn):
        """Configure connection settings

        Args:
            conn: Database connection to configure with database-specific settings
        """

    @abstractmethod
    def quote_identifier(self, identifier):
        """Quote a database identifier

        Args:
            identifier: Database identifier to be quoted (table name, column name, etc.)

        Returns
            str: Properly quoted identifier according to database-specific rules
        """

    # Common implementation shared by all strategies
    @cacheable_strategy('sequence_column_finder', ttl=300, maxsize=50)
    def _find_sequence_column(self, cn, table, bypass_cache=False):
        """Find the best column to reset sequence for.

        Common implementation shared by all database strategies that determines the
        most appropriate column for sequence operations based on heuristic rules.

        Args:
            cn: Database connection object
            table: Table name to analyze for sequence columns
            bypass_cache: If True, bypass cache and query database directly, by default False

        Returns
            str: Column name best suited for sequence resetting based on priority:
                1. Columns that are both primary key and sequence columns
                2. Primary key or sequence columns with 'id' in their name
                3. Any primary key or sequence column
                4. Fallback to 'id' if no suitable column found
        """
        sequence_cols = self.get_sequence_columns(cn, table, bypass_cache=bypass_cache)
        primary_keys = self.get_primary_keys(cn, table, bypass_cache=bypass_cache)

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
