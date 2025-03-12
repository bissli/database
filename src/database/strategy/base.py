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
        
    # Common implementation shared by all strategies
    def _find_sequence_column(self, cn, table):
        """Find the best column to reset sequence for.
        
        Common implementation shared by all database strategies.
        """
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
