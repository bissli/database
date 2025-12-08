"""
Column information abstraction across database backends.
"""
import logging
from typing import Any, Self

from database.adapters.type_mapping import resolve_type

logger = logging.getLogger(__name__)


class Column:
    """Representation of a database column with type information and metadata

    Technical implementation details:
    - Encapsulates database-specific column metadata (type_code, precision, scale, etc.)
    - Maps type_code from cursor.description to Python types via TypeResolver
    - Stores display_size and internal_size for rendering and memory allocation optimization
    - Provides nullable information for validation and constraint checking
    - Includes static factory methods for creation from cursor.description

    Internal architecture:
    - Uses _type_resolver singleton to perform database-specific type resolution
    - Maintains separate Python type from database type_code for type safety
    - Supports different extraction strategies for each database backend
    - Provides static utility methods for handling column collections

    Database compatibility:
    - PostgreSQL: Uses native OIDs and type system
    - SQLite: Extracts type information from schema pragmas

    Performance considerations:
    - Efficiently extracted from cursor.description to minimize overhead
    - Used with SchemaCache to reduce repeated metadata queries
    - Provides column type dictionaries for dataframe type annotations
    """

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
    def from_cursor_description(cls, description_item: Any, connection_type: str,
                                table_name: str | None = None,
                                connection: Any | None = None) -> Self:
        """Create a Column from cursor description item.

        Args:
            description_item: One item from cursor.description
            connection_type: Database type ('postgresql', 'sqlite')
            table_name: Optional table name for metadata lookup
            connection: Optional connection for metadata lookup

        Returns
            Column instance
        """
        if connection_type == 'postgresql':
            column_info = cls._extract_postgres_column_info(description_item, table_name, connection)
        elif connection_type == 'sqlite':
            column_info = cls._extract_sqlite_column_info(description_item, table_name, connection)
        else:
            column_info = {
                'name': str(description_item[0]) if description_item else None,
                'type_code': None,
                'display_size': None,
                'internal_size': None,
                'precision': None,
                'scale': None,
                'nullable': None,
                }

        python_type = cls._get_python_type_for_db_type(
            connection_type,
            column_info['type_code'],
            column_name=column_info['name'],
            display_size=column_info['display_size'],
            precision=column_info['precision'],
            scale=column_info['scale'],
            table_name=table_name
        )
        column_info['python_type'] = python_type

        return cls(**column_info)

    @classmethod
    def _extract_postgres_column_info(cls, description_item: Any,
                                      table_name: str | None = None,
                                      connection: Any | None = None) -> dict:
        """Extract column information from PostgreSQL cursor description.
        """
        return {
            'name': getattr(description_item, 'name', None),
            'type_code': getattr(description_item, 'type_code', None),
            'display_size': getattr(description_item, 'display_size', None),
            'internal_size': getattr(description_item, 'internal_size', None),
            'precision': getattr(description_item, 'precision', None),
            'scale': getattr(description_item, 'scale', None),
            'nullable': None,
            }

    @classmethod
    def _extract_sqlite_column_info(cls, description_item: Any,
                                    table_name: str | None = None,
                                    connection: Any | None = None) -> dict:
        """Extract column information from SQLite cursor description.
        """
        if len(description_item) >= 7:
            return {
                'name': description_item[0],
                'type_code': description_item[1],
                'display_size': description_item[2],
                'internal_size': description_item[3],
                'precision': description_item[4],
                'scale': description_item[5],
                'nullable': bool(description_item[6]),
                }
        else:
            return {
                'name': description_item[0] if len(description_item) > 0 else None,
                'type_code': description_item[1] if len(description_item) > 1 else None,
                'display_size': None,
                'internal_size': None,
                'precision': None,
                'scale': None,
                'nullable': None,
                }

    @classmethod
    def _get_python_type_for_db_type(cls, connection_type: str, type_code: Any,
                                     column_name: str | None = None,
                                     display_size: int | None = None,
                                     precision: int | None = None,
                                     scale: int | None = None,
                                     table_name: str | None = None) -> type:
        """Get Python type from database type code using type resolver.
        """
        return resolve_type(
            connection_type, type_code, column_name,
            table_name=table_name, column_size=display_size,
            precision=precision, scale=scale
        )

    def __repr__(self) -> str:
        return (f'Column(name={self.name!r}, type_code={self.type_code!r}, '
                f'python_type={self.python_type.__name__ if self.python_type else None})')

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization.
        """
        return {
            'name': self.name,
            'type_code': self.type_code,
            'python_type': self.python_type.__name__ if self.python_type else None,
            'display_size': self.display_size,
            'internal_size': self.internal_size,
            'precision': self.precision,
            'scale': self.scale,
            'nullable': self.nullable,
            }

    @staticmethod
    def get_names(columns: list[Self]) -> list[str]:
        """Get column names from a list of Column objects.
        """
        return [col.name for col in columns]

    @staticmethod
    def get_column_by_name(columns: list[Self], name: str) -> Self | None:
        """Find a column by name in a list of Column objects.
        """
        for col in columns:
            if col.name == name:
                return col
        return None

    @staticmethod
    def get_column_types_dict(columns: list[Self]) -> dict[str, dict[str, Any]]:
        """Get a dictionary of column types indexed by name.
        """
        return {col.name: col.to_dict() for col in columns}

    @staticmethod
    def get_types(columns: list[Self]) -> list[type | None]:
        """Get Python types for each column as a list.

        Args:
            columns: List of Column objects

        Returns
            List of Python types for each column
        """
        return [col.python_type for col in columns]

    @staticmethod
    def create_empty_columns(names: list[str]) -> list[Self]:
        """Create empty Column objects with just names.
        """
        return [Column(name=name, type_code=None) for name in names]


def columns_from_cursor_description(cursor: Any, connection_type: str,
                                    table_name: str | None = None,
                                    connection: Any | None = None) -> list[Column]:
    """Create Column objects directly from a cursor description.

    Args:
        cursor: Database cursor with a description attribute
        connection_type: Database type ('postgresql', 'sqlite')
        table_name: Optional table name for metadata lookup
        connection: Optional connection for metadata lookup

    Returns
        List of Column objects
    """
    if cursor.description is None:
        return []

    result = []
    for desc_item in cursor.description:
        col = Column.from_cursor_description(desc_item, connection_type, table_name, connection)
        result.append(col)

    return result
