"""
Type resolution system for database columns.

This module provides a centralized system for resolving database column types
to appropriate Python types across different database backends. It combines
information from multiple sources:

1. Database-specific type codes
2. Column name patterns
3. Configuration-based overrides
4. Type handler registry

The module focuses solely on type identification, not conversion.
"""
import datetime
import logging
from typing import Any

from database.config.type_mapping import TypeMappingConfig

logger = logging.getLogger(__name__)


class TypeHandler:
    """Base class for database type handlers.
    """

    def __init__(self, python_type: type) -> None:
        self.python_type = python_type

    def handles_type(self, type_code: Any, type_name: str | None = None) -> bool:
        """Check if this handler can handle the given type code/name.
        """
        return False

    def convert_value(self, value: Any) -> Any:
        """Return the database value without conversion

        The TypeHandler is for identifying types only, not for conversion.
        Actual conversion happens at the database driver/adapter level.
        """
        return value


def create_simple_handler(name: str, python_type: type,
                          type_codes: set,
                          type_names: set | None = None) -> TypeHandler:
    """Factory function for creating simple type handlers.

    This reduces duplication by creating type handler classes with consistent behavior.

    Args:
        name: Handler name (used for the class name)
        python_type: Python type this handler returns
        type_codes: Set of database type codes this handler recognizes
        type_names: Optional set of type names this handler recognizes

    Returns
        A TypeHandler instance
    """
    class SimpleHandler(TypeHandler):
        def __init__(self):
            super().__init__(python_type=python_type)
            self.type_codes = type_codes
            self.type_names = type_names or set()

        def handles_type(self, type_code: Any, type_name: str | None = None) -> bool:
            if type_name and type_name.lower() in self.type_names:
                return True
            return type_code in self.type_codes

    SimpleHandler.__name__ = f'{name}Handler'
    return SimpleHandler()


class TypeHandlerRegistry:
    """Registry for database type handlers

    This registry maintains handlers that identify the appropriate Python type
    for database-specific type codes. It does not perform conversions.
    """

    _instance = None

    @classmethod
    def get_instance(cls) -> 'TypeHandlerRegistry':
        """Get singleton instance.
        """
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self) -> None:
        self._handlers: dict[str, list[TypeHandler]] = {
            'postgresql': [],
            'sqlite': [],
            }

    def get_type_from_type_code(self, db_type: str, type_code: Any) -> type | None:
        """Find a Python type based strictly on the database type code.

        This method only uses handlers that can determine the type based solely
        on the type_code, without considering column names or other contextual
        information. This ensures we trust the database's type system.

        Args:
            db_type: Database engine type ('postgresql', 'sqlite')
            type_code: Database-specific type code

        Returns
            Python type or None if no handler matches just the type code
        """
        if isinstance(type_code, type):
            return type_code

        if db_type not in self._handlers:
            return None

        for handler in self._handlers.get(db_type, []):
            if handler.handles_type(type_code, None):
                return handler.python_type

        return None

    def register_handler(self, db_type: str, handler: TypeHandler) -> None:
        """Register a new type handler.
        """
        if db_type not in self._handlers:
            self._handlers[db_type] = []
        self._handlers[db_type].append(handler)

    def get_python_type(self, db_type: str, type_code: Any,
                        type_name: str | None = None) -> type:
        """Get Python type for a database type

        This method only identifies the appropriate Python type for a database type.
        It does not perform any conversion of values.
        """
        if isinstance(type_code, type):
            return type_code

        handlers = self._handlers.get(db_type, [])

        for handler in handlers:
            if handler.handles_type(type_code, type_name):
                return handler.python_type

        return str

    def convert_value(self, db_type: str, value: Any,
                      type_code: Any, type_name: str | None = None) -> Any:
        """Return the database value without conversion

        This method exists for backward compatibility but simply returns the value
        without any conversion. Type conversion should happen solely at the database
        driver/adapter level.
        """
        if value is None:
            return None

        return value


from psycopg.postgres import types

oid = lambda x: types.get(x).oid
aoid = lambda x: types.get(x).array_oid

postgres_types = {}
for v in [
    oid('"char"'),
    oid('bpchar'),
    oid('character varying'),
    oid('character'),
    oid('json'),
    oid('name'),
    oid('text'),
    oid('uuid'),
    oid('varchar'),
]:
    postgres_types[v] = str
for v in [
    oid('bigint'),
    oid('int2'),
    oid('int4'),
    oid('int8'),
    oid('integer'),
]:
    postgres_types[v] = int
for v in [
    oid('float4'),
    oid('float8'),
    oid('double precision'),
    oid('numeric'),
]:
    postgres_types[v] = float
for v in [oid('date')]:
    postgres_types[v] = datetime.date
for v in [
    oid('time'),
    oid('time with time zone'),
    oid('time without time zone'),
    oid('timestamp with time zone'),
    oid('timestamp without time zone'),
    oid('timestamptz'),
    oid('timetz'),
    oid('timestamp'),
]:
    postgres_types[v] = datetime.datetime
for v in [oid('bool'), oid('boolean')]:
    postgres_types[v] = bool
for v in [oid('bytea'), oid('jsonb')]:
    postgres_types[v] = bytes
postgres_types[aoid('int2vector')] = tuple
for k in tuple(postgres_types):
    postgres_types[aoid(k)] = tuple


sqlite_types = {
    'INTEGER': int,
    'REAL': float,
    'TEXT': str,
    'BLOB': bytes,
    'NUMERIC': float,
    'BOOLEAN': bool,
    'DATE': datetime.date,
    'DATETIME': datetime.datetime,
    'TIME': datetime.time,
    }


class TypeResolver:
    """
    Resolves database column types across different database backends.

    This class is responsible ONLY for determining appropriate Python types
    for database column types. It does NOT perform any value conversion -
    that is handled by the database drivers and registered adapters.

    The resolver uses multiple sources of type information in priority order:
    1. Direct database type code mapping (highest priority)
    2. Type handler registry (customizable type mapping)
    3. Configuration-based type overrides
    4. Column name pattern matching
    5. Built-in type maps for each database
    """

    def __init__(self) -> None:
        """Initialize the type resolver with empty type maps.
        """
        self._type_maps: dict[str, dict[Any, type]] = {}
        self._registry = TypeHandlerRegistry.get_instance()
        self._initialize_type_maps()

    def _is_python_type(self, type_code: Any) -> bool:
        """Check if the type_code is already a Python type.
        """
        return isinstance(type_code, type)

    def _use_type_directly_if_python_type(self, type_code: Any) -> type | None:
        """Return type_code if it's already a Python type, otherwise None.
        """
        return type_code if self._is_python_type(type_code) else None

    def get_type_from_type_code(self, db_type: str, type_code: Any) -> type | None:
        """Resolve type based only on database type code.

        This method prioritizes the database's own type information
        without considering column names or patterns. It strictly uses
        the type code provided by the database.

        Args:
            db_type: Database type ('postgresql', 'sqlite')
            type_code: Database-specific type code

        Returns
            Python type or None if unmappable
        """
        direct_type = self._use_type_directly_if_python_type(type_code)
        if direct_type:
            return direct_type

        if db_type in self._type_maps and type_code in self._type_maps[db_type]:
            return self._type_maps[db_type][type_code]

        return self._registry.get_type_from_type_code(db_type, type_code)

    def _initialize_type_maps(self) -> None:
        """Load type mapping dictionaries for supported database systems.
        """
        self._type_maps = {
            'postgresql': postgres_types,
            'sqlite': sqlite_types,
            }

        for type_code, python_type in self._type_maps['postgresql'].items():
            postgres_handler = create_simple_handler(
                f'PostgreSQL_{type_code}',
                python_type,
                {type_code},
                None,
                )
            self._registry.register_handler('postgresql', postgres_handler)

    def _resolve_type_with_context(
        self,
        db_type: str,
        type_code: Any,
        column_name: str | None,
        table_name: str | None,
        type_map: dict[Any, type] | None = None
    ) -> type:
        """Common type resolution logic across database types.

        This centralizes the common resolution pattern to reduce duplication.

        Args:
            db_type: Database type identifier
            type_code: Database-specific type code
            column_name: Optional column name for context
            table_name: Optional table name for configuration lookup
            type_map: Optional type map for direct lookups

        Returns
            Python type
        """
        if column_name:
            config = TypeMappingConfig.get_instance()
            config_type = config.get_type_for_column(db_type, table_name, column_name)
            if config_type:
                python_type = self._map_config_type_to_python(config_type)
                if python_type:
                    return python_type

        if column_name:
            python_type = self._resolve_by_column_name(column_name, type_code, table_name, db_type)
            if python_type:
                return python_type

        if type_map and type_code in type_map:
            return type_map[type_code]

        return str

    def resolve_python_type(
        self,
        db_type: str,
        type_code: Any,
        column_name: str | None = None,
        column_size: int | None = None,
        precision: int | None = None,
        scale: int | None = None,
        table_name: str | None = None
    ) -> type:
        """
        Resolve database type to Python type.

        This is the main entry point for type resolution. It delegates to database-specific
        methods based on the db_type parameter.

        Args:
            db_type: Database type ('postgresql', 'sqlite')
            type_code: Database-specific type code
            column_name: Optional column name for context
            column_size: Optional display size
            precision: Optional numeric precision
            scale: Optional numeric scale
            table_name: Optional table name for configuration lookup

        Returns
            Python type (e.g., int, str, datetime.date)
        """
        direct_type = self._use_type_directly_if_python_type(type_code)
        if direct_type:
            return direct_type

        python_type = self._registry.get_python_type(db_type, type_code, None)
        if python_type != str:
            return python_type

        if db_type == 'postgresql':
            return self._resolve_postgresql_type(type_code, column_name, column_size, precision, scale, table_name)
        elif db_type == 'sqlite':
            return self._resolve_sqlite_type(type_code, column_name, column_size, precision, scale, table_name)

        logger.warning(f'Unknown database type: {db_type}')
        return str

    def _resolve_postgresql_type(
        self,
        type_code: Any,
        column_name: str | None = None,
        column_size: int | None = None,
        precision: int | None = None,
        scale: int | None = None,
        table_name: str | None = None
    ) -> type:
        """
        Resolve PostgreSQL type to Python type.

        Args:
            type_code: PostgreSQL type OID
            column_name: Optional column name for context
            column_size: Optional display size
            precision: Optional numeric precision
            scale: Optional numeric scale
            table_name: Optional table name for config lookup

        Returns
            Python type
        """
        return self._resolve_type_with_context(
            'postgresql', type_code, column_name, table_name,
            self._type_maps['postgresql']
        )

    def _resolve_sqlite_type(
        self,
        type_code: Any,
        column_name: str | None = None,
        column_size: int | None = None,
        precision: int | None = None,
        scale: int | None = None,
        table_name: str | None = None
    ) -> type:
        """
        Resolve SQLite type to Python type.

        Args:
            type_code: SQLite type string (e.g., 'INTEGER', 'TEXT') or code
            column_name: Optional column name for context
            column_size: Optional display size
            precision: Optional numeric precision
            scale: Optional numeric scale
            table_name: Optional table name for config lookup

        Returns
            Python type
        """
        if isinstance(type_code, str):
            base_type = type_code.split('(')[0].upper()
            if base_type in self._type_maps['sqlite']:
                return self._type_maps['sqlite'][base_type]

        return self._resolve_type_with_context(
            'sqlite', type_code, column_name, table_name,
            self._type_maps['sqlite']
        )

    def _map_config_type_to_python(self, config_type: str) -> type | None:
        """
        Map a configuration type string to a Python type.

        Args:
            config_type: Type name from configuration (e.g., 'int', 'varchar')

        Returns
            Python type or None if no mapping found
        """
        type_map = {
            'int': int,
            'integer': int,
            'bigint': int,
            'smallint': int,
            'tinyint': int,
            'float': float,
            'real': float,
            'double': float,
            'decimal': float,
            'numeric': float,
            'money': float,
            'bit': bool,
            'boolean': bool,
            'date': datetime.date,
            'datetime': datetime.datetime,
            'timestamp': datetime.datetime,
            'time': datetime.time,
            'varchar': str,
            'char': str,
            'text': str,
            'nvarchar': str,
            'nchar': str,
            'ntext': str,
            'binary': bytes,
            'varbinary': bytes,
            'blob': bytes,
            }
        return type_map.get(config_type.lower())

    def _resolve_by_column_name(
        self,
        column_name: str,
        type_code: Any,
        table_name: str | None = None,
        db_type: str = 'postgresql'
    ) -> type | None:
        """
        Resolve type based on column name patterns.

        This method uses both configuration and common naming conventions
        to determine the most likely type for a column.

        Args:
            column_name: Column name to analyze
            type_code: Original type code (for context)
            table_name: Optional table name for configuration lookup
            db_type: Database type for configuration lookup

        Returns
            Python type or None if no match found
        """
        config = TypeMappingConfig.get_instance()
        config_type = config.get_type_for_column(db_type, table_name, column_name)

        if config_type:
            mapped_type = self._map_config_type_to_python(config_type)
            if mapped_type:
                return mapped_type

        name_lower = column_name.lower()

        patterns = {
            'id_pattern': (lambda n: n.endswith('_id') or n == 'id', int),
            'date_pattern': (lambda n: n.endswith('_date') or n == 'date', datetime.date),
            'datetime_pattern': (
                lambda n: (n.endswith(('_datetime', '_at', '_timestamp')) or n == 'timestamp'),
                datetime.datetime,
                ),
            'time_pattern': (lambda n: n.endswith('_time') or n == 'time', datetime.time),
            'bool_pattern': (
                lambda n: (n.startswith('is_') or n.endswith('_flag') or
                           n in {'active', 'enabled', 'disabled', 'is_deleted'}),
                bool,
                ),
            'money_pattern': (
                lambda n: (n.endswith(('_price', '_cost', '_amount')) or
                           n.startswith(('price_', 'cost_', 'amount_'))),
                float,
                ),
            }

        for (matcher, python_type) in patterns.values():
            if matcher(name_lower):
                return python_type

        return None


# Global function for direct type resolution
def resolve_type(
    db_type: str,
    type_code: Any,
    column_name: str | None = None,
    table_name: str | None = None,
    **kwargs
) -> type:
    """
    Central function for type resolution across the codebase.

    This function provides a simplified interface to the TypeResolver, creating
    a singleton instance if needed and delegating to its resolve_python_type method.

    The resolution process prioritizes the database's type information over
    name-based pattern matching, ensuring more accurate type identification.

    Args:
        db_type: Database type ('postgresql', 'sqlite')
        type_code: Database-specific type code
        column_name: Optional column name for name-based resolution
        table_name: Optional table name for context
        **kwargs: Additional type information (precision, scale, etc)

    Returns
        Python type (e.g., int, str, datetime.date)
    """
    global _global_resolver
    if '_global_resolver' not in globals() or _global_resolver is None:
        _global_resolver = TypeResolver()

    if isinstance(type_code, type):
        return type_code

    python_type = _global_resolver.get_type_from_type_code(db_type, type_code)
    if python_type is not None:
        return python_type

    return _global_resolver.resolve_python_type(
        db_type,
        type_code,
        column_name,
        column_size=kwargs.get('column_size'),
        precision=kwargs.get('precision'),
        scale=kwargs.get('scale'),
        table_name=table_name
    )
