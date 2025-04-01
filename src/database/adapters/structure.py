"""
Row and result structure adapters to provide consistent interfaces across different database backends.

These adapters handle ONLY the structure of database results (mapping to dictionaries,
accessing by name/index, etc). They do NOT perform any type conversion, which is
handled exclusively by the database drivers and their registered adapters.
"""
from typing import Any

from database.utils.connection_utils import get_dialect_name

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
        dialect = get_dialect_name(connection)
        if dialect == 'postgresql':
            return PostgreSQLRowAdapter(row)
        if dialect == 'sqlite':
            return SQLiteRowAdapter(row)
        if dialect == 'mssql':
            return SQLServerRowAdapter(row, connection)
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
            return {key: self.row[key] for key in self.row}
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


class SQLServerRowAdapter(RowStructureAdapter):
    """Adapter for SQL Server row objects using pyodbc with ODBC Driver 18+

    With ODBC Driver 18+, SQL Server rows via pyodbc preserve full column names
    without truncation. This adapter provides a thin wrapper that:
    - Handles structure mapping only, not type conversion
    - Takes advantage of column name preservation in Driver 18+
    - Provides direct attribute access in most cases
    - Falls back to indexing for rare edge cases

    The values returned are the exact values provided by the pyodbc driver
    without any additional type conversion.

    IMPORTANT: This adapter requires ODBC Driver 18+ for SQL Server. With older
    drivers, column names may be truncated and data access unreliable.
    """

    def __init__(self, row: Any, connection=None):
        """Initialize with row and optional connection for type conversion"""
        super().__init__(row)
        self.connection = connection

    def to_dict(self) -> dict[str, Any]:
        """Convert SQL Server row to dictionary

        With ODBC Driver 18+, column names are preserved correctly, so this
        function is much simpler than before.
        """
        # Quick handling of special cases
        if isinstance(self.row, dict):
            return self.row

        if self.row is None:
            return {}

        # Priority 1: Use cursor description when available (most reliable source of column names)
        if hasattr(self, 'cursor') and self.cursor and hasattr(self.cursor, 'description') and self.cursor.description:
            # If we have a row with direct index access and a cursor
            if hasattr(self.row, '__getitem__') and hasattr(self.row, '__len__'):
                result = {}
                for i, desc in enumerate(self.cursor.description):
                    if i < len(self.row):
                        # Get column name directly from cursor description
                        col_name = desc[0]
                        if not col_name:  # Skip null column names
                            continue
                        result[col_name] = self.row[i]
                if result:
                    return result

        # Priority 2: pyodbc Row objects with __members__ (common case with Driver 18+)
        if hasattr(self.row, '__members__') and self.row.__members__:
            # Verify __members__ contains valid column names
            if all(isinstance(m, str) and m for m in self.row.__members__):
                result = {}
                for member in self.row.__members__:
                    # Clean null bytes from member names - needed for some SQL Server versions
                    clean_member = member.replace('\x00', '')
                    result[clean_member] = getattr(self.row, member)
                return result

        # Priority 3: Try row dictionary-like access with keys
        if hasattr(self.row, 'keys') and callable(self.row.keys):
            try:
                keys = self.row.keys()
                return {key: self.row[key] for key in keys}
            except Exception:
                pass

        # Last resort: tuple/list results with no column information
        if isinstance(self.row, tuple | list):
            # Try to use expected_column_names from cursor if available
            if hasattr(self, 'cursor') and hasattr(self.cursor, 'expected_column_names'):
                names = self.cursor.expected_column_names
                if len(names) == len(self.row):
                    return dict(zip(names, self.row))

            # Otherwise use generic column names
            return {f'column_{i}': value for i, value in enumerate(self.row)}

        # Return original object if no known conversion
        return self.row

    def get_value(self, key=None) -> Any:
        """Get value from SQL Server row

        With ODBC Driver 18+, column access is much simpler as names are preserved.
        """
        if self.row is None:
            return None

        # Special case: no key requested - return first value
        if key is None:
            # First try dictionary access for dict-like objects
            if isinstance(self.row, dict):
                return self._get_first_value_from_dict(self.row)

            # For pyodbc Row objects with __members__, get first member
            if hasattr(self.row, '__members__') and self.row.__members__:
                return getattr(self.row, self.row.__members__[0])

            # Get first element from sequence
            if hasattr(self.row, '__getitem__') and hasattr(self.row, '__len__') and len(self.row) > 0:
                return self.row[0]

            # Return the row itself as last resort
            return self.row

        # With Driver 18+, we can usually use direct attribute access
        if hasattr(self.row, '__members__') and key in self.row.__members__:
            return getattr(self.row, key)

        # For dict-like objects
        if isinstance(self.row, dict) and key in self.row:
            return self.row[key]

        # For indexable objects with numeric keys
        if isinstance(key, int) and hasattr(self.row, '__getitem__') and hasattr(self.row, '__len__'):
            try:
                if key < len(self.row):
                    return self.row[key]
            except (IndexError, TypeError):
                pass

        # Last resort: try case-insensitive match
        if hasattr(self.row, '__members__') and isinstance(key, str):
            key_lower = key.lower()
            for member in self.row.__members__:
                if isinstance(member, str) and member.lower() == key_lower:
                    return getattr(self.row, member)

        # Key not found
        raise KeyError(f"Column '{key}' not found in row")


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
