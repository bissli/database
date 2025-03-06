"""
Base strategy interface for database operations.
"""
from abc import ABC, abstractmethod


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
