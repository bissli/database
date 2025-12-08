"""
Row and result structure adapters to provide consistent interfaces across different database backends.

These adapters handle ONLY the structure of database results (mapping to dictionaries,
accessing by name/index, etc). They do NOT perform any type conversion, which is
handled exclusively by the database drivers and their registered adapters.
"""
from typing import Any

from libb import attrdict


def create_row_adapter(connection, row):
    """
    Simplified factory function for creating row adapters.
    This consolidates the old DatabaseRowAdapter.create pattern.

    Args:
        connection: Database connection
        row: Database row to adapt

    Returns
        Appropriate row adapter instance
    """
    return RowStructureAdapter.create(connection, row)


class RowStructureAdapter:
    """Base adapter for database row objects providing a consistent interface"""

    @staticmethod
    def create(connection, row):
        """Factory method to create the appropriate adapter for the connection type"""
        if connection.dialect == 'postgresql':
            return PostgreSQLRowAdapter(row)
        if connection.dialect == 'sqlite':
            return SQLiteRowAdapter(row)
        return GenericRowAdapter(row)

    @staticmethod
    def create_empty_dict(cols: list[str]) -> dict[str, None]:
        """Create an empty dictionary with null values for the given columns"""
        return dict.fromkeys(cols)

    @staticmethod
    def create_attrdict_from_cols(cols: list[str]) -> attrdict:
        """Create an attrdict with null values for all columns"""
        return attrdict(RowStructureAdapter.create_empty_dict(cols))

    def __init__(self, row: Any):
        self.row = row
        self.cursor = None

    def to_dict(self) -> dict[str, Any]:
        """Convert row to a dictionary"""
        raise NotImplementedError('Subclasses must implement to_dict method')

    def get_value(self, key: str | None = None) -> Any:
        """Get a single value from the row

        If key is provided, returns the value for that key.
        If key is None, returns the first value in the row.
        """
        raise NotImplementedError('Subclasses must implement get_value method')

    def to_attrdict(self) -> attrdict:
        """Convert row to an attrdict"""
        return attrdict(self.to_dict())

    def _get_first_value_from_dict(self, data) -> Any:
        """Helper to safely get the first value from a dictionary"""
        if not data:
            return None
        return next(iter(data.values()))


class SQLiteRowAdapter(RowStructureAdapter):
    """Adapter for SQLite Row objects - handles structure only, not type conversion

    SQLite's Row objects are dictionary and sequence-like objects that can be accessed
    by name or index. This adapter provides a consistent interface for accessing
    values and mapping to dictionaries.

    No type conversion is performed by this adapter - types come directly from SQLite's
    built-in adapters and converters registered with the SQLite connection.
    """

    def to_dict(self) -> dict[str, Any]:
        """Convert SQLite Row to a dictionary without type conversion"""
        if hasattr(self.row, 'keys') and callable(self.row.keys):
            # sqlite3.Row has keys() method - use it to get column names
            # Note: iterating over Row yields values, not keys
            return {key: self.row[key] for key in self.row.keys()}  # noqa: SIM118
        elif isinstance(self.row, dict):
            return self.row
        elif hasattr(self.row, '__getitem__') and isinstance(self.row, list | tuple):
            # Handle tuple results - need column names from caller
            return self.row
        return self.row

    def get_value(self, key=None) -> Any:
        """Get value from SQLite Row without type conversion"""
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


class PostgreSQLRowAdapter(RowStructureAdapter):
    """Adapter for PostgreSQL row objects - handles structure only, not type conversion

    PostgreSQL rows via psycopg are already dictionary-like objects. This adapter
    provides a consistent interface for accessing values and ensuring they maintain
    the same format as other database adapters.

    No type conversion is performed by this adapter - types come directly from
    psycopg's type system and registered type adapters.
    """

    def to_dict(self) -> dict[str, Any]:
        """PostgreSQL rows are already dictionaries"""
        return self.row

    def get_value(self, key=None) -> Any:
        """Get value from PostgreSQL row without type conversion"""
        if key is not None:
            return self.row[key]

        # Get first value from dictionary
        return self._get_first_value_from_dict(self.row) if isinstance(self.row, dict) else self.row


class GenericRowAdapter(RowStructureAdapter):
    """Adapter for unknown row types, attempting reasonable behavior"""

    def to_dict(self) -> dict[str, Any]:
        """Try to convert to dictionary using common patterns"""
        # Check for different dictionary-like conversions in order of preference
        if isinstance(self.row, dict):
            return self.row
        if hasattr(self.row, '_asdict'):  # namedtuple support
            return self.row._asdict()
        if hasattr(self.row, '__dict__'):  # object with attributes
            return self.row.__dict__
        if hasattr(self.row, '__getitem__') and hasattr(self.row, '__len__'):
            # Convert sequence-like objects to dictionaries
            return {f'column_{i}': v for i, v in enumerate(self.row)}

        # Return the row itself if no conversion is possible
        return self.row

    def get_value(self, key=None) -> Any:
        """Attempt to get a value using multiple approaches"""
        if key is not None:
            # Try attribute access (for objects)
            if hasattr(self.row, key):
                return getattr(self.row, key)

            # Try dictionary access
            if hasattr(self.row, '__getitem__'):
                try:
                    return self.row[key]
                except (KeyError, TypeError, IndexError):
                    pass

        # No key or key not found - try to get the first value
        if isinstance(self.row, dict):
            return self._get_first_value_from_dict(self.row)

        # Try sequence-like access
        if hasattr(self.row, '__getitem__') and hasattr(self.row, '__len__') and len(self.row) > 0:
            return self.row[0]

        # Return the row itself as a last resort
        return self.row


class ResultStructureAdapter:
    """Adapter for sets of database rows providing consistent interface"""

    def __init__(self, connection, data):
        self.connection = connection
        self.data = data

    def to_dict_list(self):
        """Convert all rows to dictionaries"""
        return [RowStructureAdapter.create(self.connection, row).to_dict() for row in self.data]

    def get_first_value(self):
        """Get the first value from the first row"""
        if not self.data:
            return None
        return RowStructureAdapter.create(self.connection, self.data[0]).get_value()

    def get_first_row_dict(self):
        """Get the first row as a dictionary"""
        if not self.data:
            return None
        return RowStructureAdapter.create(self.connection, self.data[0]).to_dict()

    def get_column_values(self, column_index=0):
        """Get values from a specific column across all rows"""
        result = []
        for row in self.data:
            adapter = RowStructureAdapter.create(self.connection, row)
            if isinstance(column_index, int):
                # Get by position
                try:
                    if hasattr(row, '__getitem__'):
                        result.append(row[column_index])
                    else:
                        # Can't get by index, try first value
                        result.append(adapter.get_value())
                except (IndexError, KeyError):
                    result.append(None)
            else:
                # Get by column name
                try:
                    result.append(adapter.get_value(column_index))
                except (IndexError, KeyError):
                    result.append(None)
        return result
