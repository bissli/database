"""
Row adapters to provide consistent interfaces across different database backends.
"""
import datetime
from typing import Any

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
            return SQLServerRowAdapter(row, connection)

        return GenericRowAdapter(row)

    @staticmethod
    def create_empty_dict(cols: list[str]) -> dict[str, None]:
        """Create an empty dictionary with null values for the given columns"""
        return dict.fromkeys(cols)

    @staticmethod
    def create_attrdict_from_cols(cols: list[str]) -> attrdict:
        """Create an attrdict with null values for all columns"""
        return attrdict(DatabaseRowAdapter.create_empty_dict(cols))

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

    def _get_first_value_from_dict(self, data: dict[str, Any]) -> Any:
        """Helper to safely get the first value from a dictionary"""
        if not data:
            return None
        return next(iter(data.values()))


class SQLiteRowAdapter(DatabaseRowAdapter):
    """Adapter for SQLite Row objects"""

    def to_dict(self) -> dict[str, Any]:
        """Convert SQLite Row to a dictionary"""
        if hasattr(self.row, 'keys') and callable(self.row.keys):
            return {key: self.row[key] for key in self.row}
        elif isinstance(self.row, dict):
            return self.row
        elif hasattr(self.row, '__getitem__') and isinstance(self.row, list | tuple):
            # Handle tuple results - need column names from caller
            return self.row
        return self.row

    def get_value(self, key=None) -> Any:
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

    def to_dict(self) -> dict[str, Any]:
        """PostgreSQL rows are already dictionaries"""
        return self.row

    def get_value(self, key=None) -> Any:
        """Get value from PostgreSQL row"""
        if key is not None:
            return self.row[key]

        # Get first value from dictionary
        return self._get_first_value_from_dict(self.row) if isinstance(self.row, dict) else self.row


class SQLServerRowAdapter(DatabaseRowAdapter):
    """Adapter for SQL Server row objects"""

    def __init__(self, row: Any, connection=None):
        """Initialize with row and optional connection for type conversion"""
        super().__init__(row)
        self.connection = connection

    def _get_cursor_description(self) -> list[tuple] | None:
        """Get cursor description from row or adapter's cursor"""
        if hasattr(self.row, '_cursor') and hasattr(self.row._cursor, 'description'):
            return self.row._cursor.description
        elif hasattr(self.cursor, 'description'):
            return self.cursor.description
        return None

    def _convert_value(self, value: Any, python_type: type, col_name: str) -> Any:
        """Convert a value to the appropriate Python type"""
        if value is None:
            return None

        # Handle datetime/date conversion first with special date column detection
        if python_type == datetime.date and isinstance(value, datetime.datetime):
            # If it's explicitly typed as date but we got a datetime, convert to date
            return value.date()
        elif python_type == datetime.datetime:
            # For datetime values, ensure they're timezone-naive
            from database.utils.sqlserver_utils import \
                ensure_timezone_naive_datetime
            value = ensure_timezone_naive_datetime(value)

            # Be much more conservative with datetime to date conversions
            # Only convert explicitly marked date columns
            if col_name.lower() == 'date_col':
                from database.utils.sqlserver_utils import \
                    convert_to_date_if_no_time
                value = convert_to_date_if_no_time(value)
            return value

        # Standard type conversions
        try:
            if python_type == int:
                return int(value)
            elif python_type == float:
                return float(value)
            elif python_type == bool:
                return bool(value)
            elif python_type == str:
                return str(value)
        except (ValueError, TypeError):
            pass

        # Apply name-based conversions if standard conversion failed
        try:
            # Boolean type detection
            if (col_name.lower() == 'bit_col' or
                'bool' in col_name.lower() or
                col_name.lower().endswith('_bit') or
                col_name.lower().startswith('is_') or
                    col_name.lower().startswith('has_')):
                return bool(value)

            # Numeric type detection
            elif (col_name.lower() == 'decimal_col' or
                  'numeric' in col_name.lower() or
                  'money' in col_name.lower() or
                  'price' in col_name.lower() or
                  'float' in col_name.lower()):
                return float(value)

            # Integer type detection
            elif ('id_' in col_name.lower() or
                  '_id' in col_name.lower() or
                  'int' in col_name.lower()):
                return int(value)
        except (ValueError, TypeError):
            pass  # Keep original value if all conversions fail

        return value

    def to_dict(self) -> dict[str, Any]:
        """Convert SQL Server row to dictionary"""
        # If already a dictionary, return as is
        if isinstance(self.row, dict):
            return self.row

        # Special empty case
        if self.row is None:
            return {}

        # Handle tuple/list results from non-dictionary cursor
        if isinstance(self.row, tuple | list):
            description = self._get_cursor_description()

            # If we have description, use it for column names
            if description:
                # Create a dictionary with column names
                result = {}
                for i, value in enumerate(self.row):
                    if i < len(description):
                        col_name = description[i][0] or f'column_{i}'

                        # Get type information if available
                        if hasattr(description[i], 'type_code') and description[i].type_code:
                            type_code = description[i].type_code
                            # Apply type conversion based on SQL Server type
                            from database.adapters.type_adapters import \
                                mssql_type_codes
                            if type_code in mssql_type_codes:
                                python_type = mssql_type_codes.get(type_code)
                                value = self._convert_value(value, python_type, col_name)

                        # Special handling for date/time values
                        if isinstance(value, datetime.datetime):
                            # 1. Only convert to DATE if this is explicitly a date column
                            if col_name.lower() == 'date_col' or (col_name.lower().endswith('_date') and not
                                                                  any(term in col_name.lower() for term in ['datetime', 'smalldatetime', 'datetimeoffset'])):
                                # Convert to pure date object only for explicit date columns
                                value = value.date()
                            # 2. For normal datetime columns - ensure they're timezone-naive
                            elif col_name != 'datetimeoffset_col':
                                # Remove timezone if present
                                if value.tzinfo is not None:
                                    value = value.replace(tzinfo=None)

                                # Be more conservative about converting datetime to date
                                # Only do it for columns explicitly named as date (not datetime)
                                if (value.hour == 0 and value.minute == 0 and
                                    value.second == 0 and value.microsecond == 0 and
                                        col_name.lower() == 'date_col'):
                                    value = value.date()

                        elif isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
                            # Ensure date object is proper date without timezone info
                            if hasattr(value, 'tzinfo') and value.tzinfo is not None:
                                value = value.replace(tzinfo=None)

                        result[col_name] = value
                return result

            # No description, return dictionary with numeric keys
            return {f'column_{i}': value for i, value in enumerate(self.row)}

        # Handle other types
        return self.row

    def get_value(self, key=None) -> Any:
        """Get value from SQL Server row"""
        # Handle None row case
        if self.row is None:
            return None

        # If key specified and we have a dictionary
        if key is not None and isinstance(self.row, dict):
            try:
                return self.row[key]
            except KeyError:
                # Try case-insensitive match for SQL Server
                for k in self.row:
                    if k and key and k.lower() == key.lower():
                        return self.row[k]
                # Try column aliases that may have been added
                if key + '_col' in self.row:
                    return self.row[key + '_col']
                # Not found after all attempts
                raise KeyError(f"Column '{key}' not found in row")

        # If key specified and we have a non-dictionary object
        if key is not None and hasattr(self.row, '__getitem__'):
            if isinstance(key, int) or (isinstance(key, str) and key.isdigit()):
                return self.row[int(key)]
            # Try to find the column by name in the row
            description = self._get_cursor_description()
            if description:
                for i, col in enumerate(description):
                    if col[0] and key and col[0].lower() == key.lower():
                        return self.row[i]
            # Key not found
            raise KeyError(f"Column '{key}' not found in row")

        # Get first value if no key specified
        return self._get_first_value_from_dict(self.row) if isinstance(self.row, dict) else (
            self.row[0] if hasattr(self.row, '__getitem__') and len(self.row) > 0 else self.row
        )


class GenericRowAdapter(DatabaseRowAdapter):
    """Adapter for unknown row types, attempting reasonable behavior"""

    def to_dict(self) -> dict[str, Any]:
        """Try to convert to dictionary using common patterns"""
        if isinstance(self.row, dict):
            return self.row
        if hasattr(self.row, '_asdict'):  # namedtuple support
            return self.row._asdict()
        if hasattr(self.row, '__dict__'):  # object with attributes
            return self.row.__dict__
        if hasattr(self.row, '__getitem__') and hasattr(self.row, '__len__'):
            # Try to convert sequence-like objects to dictionaries
            return {f'column_{i}': v for i, v in enumerate(self.row)}
        return self.row

    def get_value(self, key=None) -> Any:
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
        return self._get_first_value_from_dict(self.row) if isinstance(self.row, dict) else (
            self.row[0] if hasattr(self.row, '__getitem__') and
            hasattr(self.row, '__len__') and
            len(self.row) > 0
            else self.row
        )
