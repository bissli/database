"""
Row adapters to provide consistent interfaces across different database backends.
"""

from database.utils.connection_utils import is_psycopg_connection
from database.utils.connection_utils import is_pymssql_connection
from database.utils.connection_utils import is_sqlite3_connection

from libb import attrdict


class DatabaseRowAdapter:
    """Base adapter for database row objects providing a consistent interface"""

    @staticmethod
    def create(connection, row):
        """Factory method to create the appropriate adapter for the connection type"""
        if is_sqlite3_connection(connection):
            return SQLiteRowAdapter(row)
        if is_psycopg_connection(connection):
            return PostgreSQLRowAdapter(row)
        if is_pymssql_connection(connection):
            return SQLServerRowAdapter(row)

        return GenericRowAdapter(row)

    @staticmethod
    def create_empty_dict(cols):
        """Create an empty dictionary with null values for the given columns"""
        return {col: None for col in cols}
    
    @staticmethod
    def create_attrdict_from_cols(cols):
        """Create an attrdict with null values for all columns"""
        return attrdict(DatabaseRowAdapter.create_empty_dict(cols))

    def __init__(self, row):
        self.row = row

    def to_dict(self):
        """Convert row to a dictionary"""
        raise NotImplementedError('Subclasses must implement to_dict method')

    def get_value(self, key=None):
        """Get a single value from the row

        If key is provided, returns the value for that key.
        If key is None, returns the first value in the row.
        """
        raise NotImplementedError('Subclasses must implement get_value method')

    def to_attrdict(self):
        """Convert row to an attrdict"""
        return attrdict(self.to_dict())


class SQLiteRowAdapter(DatabaseRowAdapter):
    """Adapter for SQLite Row objects"""

    def to_dict(self):
        """Convert SQLite Row to a dictionary"""
        if hasattr(self.row, 'keys') and callable(self.row.keys):
            return {key: self.row[key] for key in self.row}
        elif isinstance(self.row, dict):
            return self.row
        elif hasattr(self.row, '__getitem__') and isinstance(self.row, list | tuple):
            # Handle tuple results - need column names from caller
            return self.row
        return self.row

    def get_value(self, key=None):
        """Get value from SQLite Row"""
        if key is not None:
            return self.row[key]

        # Get first value
        if hasattr(self.row, 'keys') and callable(self.row.keys):
            keys = list(self.row.keys())
            if keys:
                return self.row[keys[0]]

        # Try to access first element
        if hasattr(self.row, '__getitem__'):
            return self.row[0]

        return self.row


class PostgreSQLRowAdapter(DatabaseRowAdapter):
    """Adapter for PostgreSQL row objects"""

    def to_dict(self):
        """PostgreSQL rows are already dictionaries"""
        return self.row

    def get_value(self, key=None):
        """Get value from PostgreSQL row"""
        if key is not None:
            return self.row[key]

        # Get first value
        if isinstance(self.row, dict) and self.row:
            return tuple(self.row.values())[0]

        # Try to access first element
        if hasattr(self.row, '__getitem__'):
            return self.row[0]

        return self.row


class SQLServerRowAdapter(DatabaseRowAdapter):
    """Adapter for SQL Server row objects"""

    def to_dict(self):
        """SQL Server rows should be dictionaries already"""
        return self.row

    def get_value(self, key=None):
        """Get value from SQL Server row"""
        if key is not None:
            return self.row[key]

        # Get first value
        if isinstance(self.row, dict) and self.row:
            return tuple(self.row.values())[0]

        # Try to access first element
        if hasattr(self.row, '__getitem__'):
            return self.row[0]

        return self.row


class GenericRowAdapter(DatabaseRowAdapter):
    """Adapter for unknown row types, attempting reasonable behavior"""

    def to_dict(self):
        """Try to convert to dictionary using common patterns"""
        if isinstance(self.row, dict):
            return self.row
        if hasattr(self.row, '_asdict'):  # namedtuple support
            return self.row._asdict()
        if hasattr(self.row, '__dict__'):  # object with attributes
            return self.row.__dict__
        return self.row

    def get_value(self, key=None):
        """Attempt to get a value using multiple approaches"""
        if key is not None:
            # Try dict access
            if hasattr(self.row, '__getitem__'):
                try:
                    return self.row[key]
                except (KeyError, TypeError, IndexError):
                    pass

            # Try attribute access
            if hasattr(self.row, key):
                return getattr(self.row, key)

        # Try to get the first value
        if isinstance(self.row, dict) and self.row:
            return next(iter(self.row.values()))

        # Try tuple/list behavior
        if hasattr(self.row, '__getitem__'):
            try:
                return self.row[0]
            except (IndexError, TypeError):
                pass

        return self.row
