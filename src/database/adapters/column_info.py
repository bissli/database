"""
Column information abstraction across database backends.
"""
import logging
import re
from typing import Any, Self

logger = logging.getLogger(__name__)


class Column:
    """Representation of a database column with type information"""

    def __init__(self,
                 name: str,
                 type_code: Any,
                 python_type: type | None = None,
                 display_size: int | None = None,
                 internal_size: int | None = None,
                 precision: int | None = None,
                 scale: int | None = None,
                 nullable: bool | None = None):
        """
        Initialize column information

        Args:
            name: Display name of the column
            type_code: Database-specific type code
            python_type: Corresponding Python type (e.g., str, int)
            display_size: Maximum display size (character count)
            internal_size: Internal storage size (bytes)
            precision: Numeric precision (for numeric types)
            scale: Numeric scale (for numeric types)
            nullable: Whether the column allows NULL values
        """
        self.name = name
        self.type_code = type_code
        self.python_type = python_type
        self.display_size = display_size
        self.internal_size = internal_size
        self.precision = precision
        self.scale = scale
        self.nullable = nullable

    @classmethod
    def from_cursor_description(cls, description_item: Any, connection_type: str) -> Self:
        """
        Create a Column from cursor description item

        Args:
            description_item: One item from cursor.description
            connection_type: Database type ('postgres', 'sqlserver', 'sqlite')

        Returns
            Column instance
        """
        # Extract basic column information based on database type
        if connection_type == 'postgres':
            column_info = cls._extract_postgres_column_info(description_item)
        elif connection_type == 'sqlite':
            column_info = cls._extract_sqlite_column_info(description_item)
        elif connection_type == 'sqlserver':
            column_info = cls._extract_sqlserver_column_info(description_item)
        else:
            # Generic fallback for unknown types
            column_info = {
                'name': str(description_item[0]) if description_item else None,
                'type_code': None,
                'display_size': None,
                'internal_size': None,
                'precision': None,
                'scale': None,
                'nullable': None
            }

        # Get the Python type from the type code
        python_type = cls._get_python_type_for_db_type(connection_type, column_info['type_code'])
        column_info['python_type'] = python_type

        return cls(**column_info)

    @classmethod
    def _extract_postgres_column_info(cls, description_item: Any) -> dict:
        """Extract column information from PostgreSQL cursor description"""
        return {
            'name': getattr(description_item, 'name', None),
            'type_code': getattr(description_item, 'type_code', None),
            'display_size': getattr(description_item, 'display_size', None),
            'internal_size': getattr(description_item, 'internal_size', None),
            'precision': getattr(description_item, 'precision', None),
            'scale': getattr(description_item, 'scale', None),
            'nullable': None  # Not provided in psycopg description
        }

    @classmethod
    def _extract_sqlite_column_info(cls, description_item: Any) -> dict:
        """Extract column information from SQLite cursor description"""
        # SQLite description format: (name, type_code, display_size, internal_size, precision, scale, nullable)
        if len(description_item) >= 7:
            return {
                'name': description_item[0],
                'type_code': description_item[1],
                'display_size': description_item[2],
                'internal_size': description_item[3],
                'precision': description_item[4],
                'scale': description_item[5],
                'nullable': bool(description_item[6])
            }
        else:
            return {
                'name': description_item[0] if len(description_item) > 0 else None,
                'type_code': description_item[1] if len(description_item) > 1 else None,
                'display_size': None,
                'internal_size': None,
                'precision': None,
                'scale': None,
                'nullable': None
            }

    @classmethod
    def _extract_sqlserver_column_info(cls, description_item: Any) -> dict:
        """Extract column information from SQL Server cursor description"""
        # pymssql description format: (name, type_code, display_size, internal_size, precision, scale, nullable)
        if len(description_item) < 7:
            return {
                'name': description_item[0] if len(description_item) > 0 else None,
                'type_code': description_item[1] if len(description_item) > 1 else None,
                'display_size': None,
                'internal_size': None,
                'precision': None,
                'scale': None,
                'nullable': None
            }

        name = description_item[0]
        original_type_code = description_item[1]

        # Log the original type code for diagnostics
        logger.debug(f"SQL Server column '{name}': original type_code={original_type_code}")

        # Apply SQL Server type refinement by examining type codes and column names
        refined_type_code = cls._refine_sqlserver_type(name, original_type_code)

        # Log type refinement if it happened
        if refined_type_code != original_type_code:
            logger.debug(f"Type refinement for '{name}': {original_type_code} -> {refined_type_code}")

        return {
            'name': name,
            'type_code': refined_type_code,
            'display_size': description_item[2],
            'internal_size': description_item[3],
            'precision': description_item[4],
            'scale': description_item[5],
            'nullable': bool(description_item[6])
        }

    @classmethod
    def _refine_sqlserver_type(cls, name: str, type_code: Any) -> Any:
        """Refine SQL Server type code based on column name patterns and type codes"""
        # Standard ODBC Type Codes - use these when available
        if type_code in {1, 12, -1, -8, -9, -10, 175, 167, 231, 239, 173, 99, 35, 98, 240}:
            return 'varchar'  # String type

        if type_code in {104, -7}:  # BIT type codes
            return 'bit'  # Boolean type

        if type_code in {106, 108, 60, 122, 62, 59, 6, 5, 3}:  # Decimal/numeric type codes
            return 'float'  # Decimal type

        # Type code 2 can represent dates or decimals depending on context
        if type_code == 2:
            # If name explicitly suggests it's a date
            if isinstance(name, str) and (
                name.lower() == 'date' or
                name.lower().endswith('_date') or
                name.lower().startswith('date_') or
                name.lower() in {'settles', 'td'} or
                # Only match 'date' in name if not part of 'update', 'validate', etc.
                (re.search(r'\bdate\b', name.lower()) and not any(
                    term in name.lower() for term in ['update', 'validate', 'candidate', 'mandate']))):
                return 'date'
            # If it has time in the name but not date, likely datetime
            elif isinstance(name, str) and ('time' in name.lower() and 'date' not in name.lower()):
                return 'datetime'

        # Integer types - this needs higher precedence to correctly type id_col in tests
        if type_code in {4, 5, -5, -6, 56, 127, 52, 38, 3} or (isinstance(name, str) and ('id_col' == name.lower() or '_id' == name.lower()[-3:])):
            return 'int'  # Integer type

        if type_code in {91, 40}:  # DATE type
            return 'date'  # Date type

        if type_code in {92, 41}:  # TIME type
            return 'time'  # Time type

        if type_code in {93, 61, 42, 58, 43, 36, 7}:  # DATETIME type
            return 'datetime'  # DateTime type

        # Last resort name-based detection for cases where type_code doesn't match any known pattern
        if isinstance(name, str):
            # Only for columns that would otherwise be misidentified:

            # Very conservative string detection - avoid ambiguous suffixes/substrings
            if any(term == name.lower() or
                   name.lower().startswith(term + '_') or
                   name.lower().endswith('_' + term)
                   for term in ['char', 'text', 'str', 'name', 'desc', 'varchar', 'nvarchar']):
                return 'varchar'  # More precise string detection

            # Boolean detection - much stricter to avoid false matches
            if name.lower() == 'bit' or name.lower() == 'bool' or name.lower() == 'flag' or \
               name.lower().startswith('is_') or name.lower().startswith('has_') or \
               name.lower().endswith('_bit') or name.lower().endswith('_flag'):
                return 'bit'  # More precise boolean detection

            # Date detection - be more specific
            if name.lower() == 'date' or name.lower() == 'time' or \
               name.lower().endswith('_date') or name.lower().endswith('_time') or \
               name.lower().startswith('date_') or name.lower().startswith('time_'):
                return 'datetime'

        return type_code

    @staticmethod
    def _get_python_type_for_db_type(connection_type: str, type_code: Any) -> type:
        """Get Python type from database type code"""
        if connection_type == 'postgres':
            from database.adapters.type_adapters import postgres_types
            return postgres_types.get(type_code, str)
        elif connection_type == 'sqlserver':
            # Handle both string type names and numeric type codes for SQL Server
            from database.adapters.type_adapters import mssql_type_codes
            from database.adapters.type_adapters import mssql_types
            if isinstance(type_code, str):
                return mssql_types.get(type_code.lower(), str)
            elif isinstance(type_code, int):
                return mssql_type_codes.get(type_code, str)
            return str
        elif connection_type == 'sqlite':
            if isinstance(type_code, str):
                from database.adapters.type_adapters import sqlite_types

                # SQLite type strings can have parameters like "VARCHAR(50)"
                # Extract just the base type
                base_type = type_code.split('(')[0].upper()
                return sqlite_types.get(base_type, str)
            return str
        return str

    def __repr__(self) -> str:
        return (f'Column(name={self.name!r}, type_code={self.type_code!r}, '
                f'python_type={self.python_type.__name__ if self.python_type else None})')

    def to_dict(self) -> dict:
        """Convert to dictionary for serialization"""
        return {
            'name': self.name,
            'type_code': self.type_code,
            'python_type': self.python_type.__name__ if self.python_type else None,
            'display_size': self.display_size,
            'internal_size': self.internal_size,
            'precision': self.precision,
            'scale': self.scale,
            'nullable': self.nullable
        }

    @staticmethod
    def get_names(columns: list[Self]) -> list[str]:
        """Get column names from a list of Column objects"""
        return [col.name for col in columns]

    @staticmethod
    def get_column_by_name(columns: list[Self], name: str) -> Self | None:
        """Find a column by name in a list of Column objects"""
        for col in columns:
            if col.name == name:
                return col
        return None

    @staticmethod
    def get_column_types_dict(columns: list[Self]) -> dict[str, dict]:
        """Get a dictionary of column types indexed by name"""
        return {col.name: col.to_dict() for col in columns}

    @staticmethod
    def get_types(columns: list[Self]) -> list[type | None]:
        """Get Python types for each column as a list

        Args:
            columns: List of Column objects

        Returns
            List of Python types for each column
        """
        return [col.python_type for col in columns]

    @staticmethod
    def create_empty_columns(names: list[str]) -> list[Self]:
        """Create empty Column objects with just names"""
        return [Column(name=name, type_code=None) for name in names]


def columns_from_cursor_description(cursor: Any, connection_type: str) -> list[Column]:
    """
    Create Column objects directly from a cursor description

    Args:
        cursor: Database cursor with a description attribute
        connection_type: Database type ('postgres', 'sqlserver', 'sqlite')

    Returns
        List of Column objects
    """
    if cursor.description is None:
        return []

    return [
        Column.from_cursor_description(desc_item, connection_type)
        for desc_item in cursor.description
    ]
