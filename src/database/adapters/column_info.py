"""
Column information abstraction across database backends.
"""
import logging
from typing import Any

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
    def from_cursor_description(cls, description_item: Any, connection_type: str) -> 'Column':
        """
        Create a Column from cursor description item

        Args:
            description_item: One item from cursor.description
            connection_type: Database type ('postgres', 'sqlserver', 'sqlite')

        Returns
            Column instance
        """
        if connection_type == 'postgres':
            # psycopg3 description format
            name = getattr(description_item, 'name', None)
            type_code = getattr(description_item, 'type_code', None)
            display_size = getattr(description_item, 'display_size', None)
            internal_size = getattr(description_item, 'internal_size', None)
            precision = getattr(description_item, 'precision', None)
            scale = getattr(description_item, 'scale', None)
            nullable = None  # Not provided in psycopg description

        elif connection_type == 'sqlite':
            # SQLite description format: (name, type_code, display_size, internal_size, precision, scale, nullable)
            if len(description_item) >= 7:
                name, type_code, display_size, internal_size, precision, scale, nullable = (
                    description_item[0], description_item[1], description_item[2],
                    description_item[3], description_item[4], description_item[5],
                    bool(description_item[6])
                )
            else:
                name = description_item[0] if len(description_item) > 0 else None
                type_code = description_item[1] if len(description_item) > 1 else None
                display_size = internal_size = precision = scale = nullable = None

        elif connection_type == 'sqlserver':
            # pymssql description format varies but typically:
            # (name, type_code, display_size, internal_size, precision, scale, nullable)
            if len(description_item) >= 7:
                name, type_code, display_size, internal_size, precision, scale, nullable = (
                    description_item[0], description_item[1], description_item[2],
                    description_item[3], description_item[4], description_item[5],
                    bool(description_item[6])
                )
            else:
                name = description_item[0] if len(description_item) > 0 else None
                type_code = description_item[1] if len(description_item) > 1 else None
                display_size = internal_size = precision = scale = nullable = None
        else:
            name = str(description_item[0]) if description_item else None
            type_code = display_size = internal_size = precision = scale = nullable = None

        # Get the Python type from the type code
        python_type = cls._get_python_type_for_db_type(connection_type, type_code)

        return cls(
            name=name,
            type_code=type_code,
            python_type=python_type,
            display_size=display_size,
            internal_size=internal_size,
            precision=precision,
            scale=scale,
            nullable=nullable
        )

    @staticmethod
    def _get_python_type_for_db_type(connection_type: str, type_code: Any) -> type:
        """Get Python type from database type code"""
        if connection_type == 'postgres':
            from database.adapters.type_adapters import postgres_types
            return postgres_types.get(type_code, str)
        elif connection_type == 'sqlserver':
            if isinstance(type_code, str):
                from database.adapters.type_adapters import mssql_types
                return mssql_types.get(type_code.lower(), str)
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
    def get_names(columns: list['Column']) -> list[str]:
        """Get column names from a list of Column objects"""
        return [col.name for col in columns]

    @staticmethod
    def get_column_by_name(columns: list['Column'], name: str) -> 'Column | None':
        """Find a column by name in a list of Column objects"""
        for col in columns:
            if col.name == name:
                return col
        return None

    @staticmethod
    def get_column_types_dict(columns: list['Column']) -> dict[str, dict]:
        """Get a dictionary of column types indexed by name"""
        return {col.name: col.to_dict() for col in columns}

    @staticmethod
    def get_types(columns: list['Column']) -> list[type | None]:
        """Get Python types for each column as a list

        Args:
            columns: List of Column objects

        Returns
            List of Python types for each column
        """
        return [col.python_type for col in columns]

    @staticmethod
    def create_empty_columns(names: list[str]) -> list['Column']:
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
