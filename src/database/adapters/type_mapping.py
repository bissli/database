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
    """Base class for database type handlers"""

    def __init__(self, python_type: type):
        self.python_type = python_type

    def handles_type(self, type_code: Any, type_name: str | None = None) -> bool:
        """Check if this handler can handle the given type code/name"""
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


# Create SQL Server handlers using the factory function
try:
    import pyodbc

    # SQL Server integer handler
    SqlServerIntegerHandler = create_simple_handler(
        'SqlServerInteger',
        int,
        {pyodbc.SQL_TINYINT, pyodbc.SQL_SMALLINT, pyodbc.SQL_INTEGER, pyodbc.SQL_BIGINT,
         -6, 5, 4, -5, 38, 48, 52, 56, 127},
        {'tinyint', 'smallint', 'int', 'bigint'}
    )

    # SQL Server numeric handler
    SqlServerNumericHandler = create_simple_handler(
        'SqlServerNumeric',
        float,
        {pyodbc.SQL_DECIMAL, pyodbc.SQL_NUMERIC, pyodbc.SQL_FLOAT,
         pyodbc.SQL_REAL, pyodbc.SQL_DOUBLE, 2, 3, 6, 7, 8,
         106, 108, 59, 60, 62, 122},
        {'decimal', 'numeric', 'money', 'smallmoney', 'float', 'real'}
    )

    # SQL Server boolean handler
    SqlServerBooleanHandler = create_simple_handler(
        'SqlServerBoolean',
        bool,
        {pyodbc.SQL_BIT, -7, 104},
        {'bit'}
    )

    # SQL Server string handler
    SqlServerStringHandler = create_simple_handler(
        'SqlServerString',
        str,
        {pyodbc.SQL_CHAR, pyodbc.SQL_VARCHAR, pyodbc.SQL_LONGVARCHAR,
         pyodbc.SQL_WCHAR, pyodbc.SQL_WVARCHAR, pyodbc.SQL_WLONGVARCHAR,
         1, 12, -1, -8, -9, -10, 167, 175, 231, 239, 241, 99, 173, -11},
        {'char', 'varchar', 'text', 'nchar', 'nvarchar', 'ntext', 'xml', 'uniqueidentifier'}
    )

    # SQL Server date handler
    SqlServerDateHandler = create_simple_handler(
        'SqlServerDate',
        datetime.date,
        {pyodbc.SQL_TYPE_DATE, 91, 40},
        {'date'}
    )

    # SQL Server time handler
    SqlServerTimeHandler = create_simple_handler(
        'SqlServerTime',
        datetime.time,
        {pyodbc.SQL_TYPE_TIME, 92, 41, -154},
        {'time'}
    )

    # SQL Server datetime handler
    SqlServerDateTimeHandler = create_simple_handler(
        'SqlServerDateTime',
        datetime.datetime,
        {pyodbc.SQL_TYPE_TIMESTAMP, 93, 36, 9, 61, 58, 42},
        {'datetime', 'smalldatetime', 'datetime2'}
    )

    # SQL Server binary handler
    SqlServerBinaryHandler = create_simple_handler(
        'SqlServerBinary',
        bytes,
        {pyodbc.SQL_BINARY, pyodbc.SQL_VARBINARY, pyodbc.SQL_LONGVARBINARY,
         -2, -3, -4, 34, 35, 165},
        {'binary', 'varbinary', 'image'}
    )

    # SQL Server guid handler
    SqlServerGuidHandler = create_simple_handler(
        'SqlServerGuid',
        str,
        {-11, 36},
        {'uniqueidentifier'}
    )

except ImportError:
    # Fallbacks when pyodbc not available - use literal type codes
    SqlServerIntegerHandler = create_simple_handler(
        'SqlServerInteger',
        int,
        {-6, 5, 4, -5, 38, 48, 52, 56, 127},
        {'tinyint', 'smallint', 'int', 'bigint'}
    )

    SqlServerNumericHandler = create_simple_handler(
        'SqlServerNumeric',
        float,
        {2, 3, 6, 7, 8, 106, 108, 59, 60, 62, 122},
        {'decimal', 'numeric', 'money', 'smallmoney', 'float', 'real'}
    )

    SqlServerBooleanHandler = create_simple_handler(
        'SqlServerBoolean',
        bool,
        {-7, 104},
        {'bit'}
    )

    SqlServerStringHandler = create_simple_handler(
        'SqlServerString',
        str,
        {1, 12, -1, -8, -9, -10, 167, 175, 231, 239, 241, 99, 173, -11},
        {'char', 'varchar', 'text', 'nchar', 'nvarchar', 'ntext', 'xml', 'uniqueidentifier'}
    )

    SqlServerDateHandler = create_simple_handler(
        'SqlServerDate',
        datetime.date,
        {91, 40},
        {'date'}
    )

    SqlServerTimeHandler = create_simple_handler(
        'SqlServerTime',
        datetime.time,
        {92, 41, -154},
        {'time'}
    )

    SqlServerDateTimeHandler = create_simple_handler(
        'SqlServerDateTime',
        datetime.datetime,
        {93, 36, 9, 61, 58, 42},
        {'datetime', 'smalldatetime', 'datetime2'}
    )

    SqlServerBinaryHandler = create_simple_handler(
        'SqlServerBinary',
        bytes,
        {-2, -3, -4, 34, 35, 165},
        {'binary', 'varbinary', 'image'}
    )

    SqlServerGuidHandler = create_simple_handler(
        'SqlServerGuid',
        str,
        {-11, 36},
        {'uniqueidentifier'}
    )


# Define the SQL Server DATETIMEOFFSET handler separately as it needs custom handling
class SqlServerDateTimeOffsetHandler(TypeHandler):
    """Handles SQL Server DATETIMEOFFSET type"""

    def __init__(self):
        super().__init__(python_type=datetime.datetime)
        # Include all known SQL Server DATETIMEOFFSET type codes
        self.type_codes = {
            -155,  # SQL_SS_TIMESTAMPOFFSET
            43,    # datetimeoffset variant
            36,    # Can sometimes be reported with this code
            2013,  # Another possible code
        }
        self.type_names = {'datetimeoffset'}

    def handles_type(self, type_code: Any, type_name: str | None = None) -> bool:
        if type_name and type_name.lower() in self.type_names:
            return True
        if isinstance(type_code, int) and type_code in self.type_codes:
            return True
        # Also handle case where type code is a string
        return bool(isinstance(type_code, str) and type_code.lower() in self.type_names)


class TypeHandlerRegistry:
    """Registry for database type handlers

    This registry maintains handlers that identify the appropriate Python type
    for database-specific type codes. It does not perform conversions.
    """

    _instance = None

    @classmethod
    def get_instance(cls):
        """Get singleton instance"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        self._handlers: dict[str, list[TypeHandler]] = {
            'mssql': [
                SqlServerIntegerHandler,
                SqlServerNumericHandler,
                SqlServerBooleanHandler,
                SqlServerStringHandler,
                SqlServerDateHandler,
                SqlServerTimeHandler,
                SqlServerDateTimeHandler,
                SqlServerDateTimeOffsetHandler(),  # Instance needed for special handling
                SqlServerBinaryHandler,
                SqlServerGuidHandler
            ],
            'postgresql': [],
            'sqlite': []
        }

        # Default handlers for all databases
        self._default_handler = SqlServerStringHandler  # Uses str type

    def get_type_from_type_code(self, db_type: str, type_code: Any) -> type | None:
        """Find a Python type based strictly on the database type code.

        This method only uses handlers that can determine the type based solely
        on the type_code, without considering column names or other contextual
        information. This ensures we trust the database's type system.

        Args:
            db_type: Database engine type ('postgresql', 'mssql', 'sqlite')
            type_code: Database-specific type code

        Returns
            Python type or None if no handler matches just the type code
        """
        # Fast path for already-Python types
        if isinstance(type_code, type):
            return type_code

        if db_type not in self._handlers:
            return None

        for handler in self._handlers.get(db_type, []):
            # Call handles_type with type_name=None to test if the handler
            # can determine the type based solely on the type_code
            if handler.handles_type(type_code, None):
                return handler.python_type

        return None

    def register_handler(self, db_type: str, handler: TypeHandler):
        """Register a new type handler"""
        if db_type not in self._handlers:
            self._handlers[db_type] = []
        self._handlers[db_type].append(handler)

    def get_python_type(self, db_type: str, type_code: Any,
                        type_name: str | None = None) -> type:
        """Get Python type for a database type

        This method only identifies the appropriate Python type for a database type.
        It does not perform any conversion of values.
        """
        # Fast path for already-Python types
        if isinstance(type_code, type):
            return type_code

        handlers = self._handlers.get(db_type, [])

        # Try each handler in order
        for handler in handlers:
            if handler.handles_type(type_code, type_name):
                return handler.python_type

        # Use default handler if no specific handler found
        return str  # Default to string for unknown types

    def convert_value(self, db_type: str, value: Any,
                      type_code: Any, type_name: str | None = None) -> Any:
        """Return the database value without conversion

        This method exists for backward compatibility but simply returns the value
        without any conversion. Type conversion should happen solely at the database
        driver/adapter level.
        """
        if value is None:
            return None

        # Pass-through the value without conversion
        return value


# Import database type mappings
from psycopg.postgres import types

# PostgreSQL type mapping
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


# SQLite type mappings
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

# SQL Server type mappings (by name)
mssql_types = {
    # Integer types
    'int': int,
    'bigint': int,
    'smallint': int,
    'tinyint': int,

    # Boolean type
    'bit': bool,

    # Decimal/numeric types
    'decimal': float,
    'numeric': float,
    'money': float,
    'smallmoney': float,
    'float': float,
    'real': float,

    # Date and time types
    'datetime': datetime.datetime,
    'datetime2': datetime.datetime,
    'smalldatetime': datetime.datetime,
    'date': datetime.date,
    'time': datetime.time,
    'datetimeoffset': datetime.datetime,

    # String types
    'char': str,
    'varchar': str,
    'nchar': str,
    'nvarchar': str,
    'text': str,
    'ntext': str,

    # Binary types
    'binary': bytes,
    'varbinary': bytes,
    'image': bytes,

    # Other types
    'uniqueidentifier': str,  # GUIDs are represented as strings
    'xml': str,
}

# Import SQL Server type codes carefully with fallbacks
try:
    import pyodbc

    # SQL Server type code mappings
    mssql_type_codes = {
        # Integer types
        pyodbc.SQL_BIT: bool,
        pyodbc.SQL_TINYINT: int,
        pyodbc.SQL_SMALLINT: int,
        pyodbc.SQL_INTEGER: int,
        pyodbc.SQL_BIGINT: int,
        -7: bool,      # SQL_BIT
        -6: int,       # SQL_TINYINT
        5: int,        # SQL_SMALLINT
        4: int,        # SQL_INTEGER
        -5: int,       # SQL_BIGINT
        38: int,       # int variant
        48: int,       # tinyint variant
        52: int,       # smallint variant
        56: int,       # int variant
        127: int,      # bigint variant
        104: bool,     # bit variant

        # Decimal/numeric types
        pyodbc.SQL_NUMERIC: float,
        pyodbc.SQL_DECIMAL: float,
        pyodbc.SQL_REAL: float,
        pyodbc.SQL_FLOAT: float,
        pyodbc.SQL_DOUBLE: float,
        2: float,      # SQL_NUMERIC
        3: float,      # SQL_DECIMAL
        6: float,      # SQL_FLOAT
        7: float,      # SQL_REAL
        8: float,      # SQL_DOUBLE
        106: float,    # decimal variant
        108: float,    # numeric variant
        60: float,     # money variant
        122: float,    # smallmoney variant
        62: float,     # float variant
        59: float,     # real variant

        # String types
        pyodbc.SQL_CHAR: str,
        pyodbc.SQL_VARCHAR: str,
        pyodbc.SQL_LONGVARCHAR: str,
        pyodbc.SQL_WCHAR: str,
        pyodbc.SQL_WVARCHAR: str,
        pyodbc.SQL_WLONGVARCHAR: str,
        pyodbc.SQL_GUID: str,
        1: str,        # SQL_CHAR
        12: str,       # SQL_VARCHAR
        -1: str,       # SQL_LONGVARCHAR
        -8: str,       # SQL_WCHAR
        -9: str,       # SQL_WVARCHAR
        -10: str,      # SQL_WLONGVARCHAR
        -11: str,      # SQL_GUID (uniqueidentifier)
        175: str,      # char variant
        167: str,      # varchar variant
        239: str,      # nchar variant
        231: str,      # nvarchar variant
        173: str,      # ntext variant
        99: str,       # ntext variant
        240: str,      # generic string
        241: str,      # xml
        36: str,       # uniqueidentifier variant
        98: str,       # sql_variant

        # Date and time types
        pyodbc.SQL_TYPE_DATE: datetime.date,
        pyodbc.SQL_TYPE_TIME: datetime.time,
        pyodbc.SQL_TYPE_TIMESTAMP: datetime.datetime,
        91: datetime.date,      # SQL_TYPE_DATE
        92: datetime.time,      # SQL_TYPE_TIME
        93: datetime.datetime,  # SQL_TYPE_TIMESTAMP
        9: datetime.datetime,   # SQL_DATETIME (ODBC 2.x)
        40: datetime.date,      # date variant
        41: datetime.time,      # time variant
        -154: datetime.time,    # SQL_SS_TIME2
        61: datetime.datetime,  # datetime variant
        58: datetime.datetime,  # smalldatetime variant
        42: datetime.datetime,  # datetime2 variant
        -155: datetime.datetime,  # SQL_SS_TIMESTAMPOFFSET
        43: datetime.datetime,  # datetimeoffset variant

        # Binary types
        pyodbc.SQL_BINARY: bytes,
        pyodbc.SQL_VARBINARY: bytes,
        pyodbc.SQL_LONGVARBINARY: bytes,
        -2: bytes,     # SQL_BINARY
        -3: bytes,     # SQL_VARBINARY
        -4: bytes,     # SQL_LONGVARBINARY
        165: bytes,    # varbinary variant
        35: bytes,     # varbinary variant
        34: bytes,     # image variant

        # Fallbacks
        0: str,        # SQL_UNKNOWN_TYPE
        -150: str,     # SQL Server XML
        -151: str,     # SQL_VARIANT
        -152: str,     # SQL Server spatial types
    }

    # Add SQL Server specific types
    if hasattr(pyodbc, 'SQL_SS_TIME2'):
        mssql_type_codes[pyodbc.SQL_SS_TIME2] = datetime.time
    if hasattr(pyodbc, 'SQL_SS_TIMESTAMPOFFSET'):
        mssql_type_codes[pyodbc.SQL_SS_TIMESTAMPOFFSET] = datetime.datetime

except ImportError:
    # Create a minimal fallback for environments without pyodbc
    logger.warning('pyodbc not available, using minimal SQL Server type codes mapping')
    mssql_type_codes = {
        -7: bool,      # SQL_BIT
        -6: int,       # SQL_TINYINT
        5: int,        # SQL_SMALLINT
        4: int,        # SQL_INTEGER
        -5: int,       # SQL_BIGINT
        2: float,      # SQL_NUMERIC
        3: float,      # SQL_DECIMAL
        6: float,      # SQL_FLOAT
        7: float,      # SQL_REAL
        8: float,      # SQL_DOUBLE
        1: str,        # SQL_CHAR
        12: str,       # SQL_VARCHAR
        -1: str,       # SQL_LONGVARCHAR
        -8: str,       # SQL_WCHAR
        -9: str,       # SQL_WVARCHAR
        -10: str,      # SQL_WLONGVARCHAR
        -2: bytes,     # SQL_BINARY
        -3: bytes,     # SQL_VARBINARY
        -4: bytes,     # SQL_LONGVARBINARY
        91: datetime.date,      # SQL_TYPE_DATE
        92: datetime.time,      # SQL_TYPE_TIME
        93: datetime.datetime,  # SQL_TYPE_TIMESTAMP
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

    def __init__(self):
        """Initialize the type resolver with empty type maps."""
        self._type_maps: dict[str, dict[Any, type]] = {}
        self._registry = TypeHandlerRegistry.get_instance()
        self._initialize_type_maps()

    def _is_python_type(self, type_code: Any) -> bool:
        """Check if the type_code is already a Python type."""
        return isinstance(type_code, type)

    def _use_type_directly_if_python_type(self, type_code: Any) -> type | None:
        """Return type_code if it's already a Python type, otherwise None."""
        return type_code if self._is_python_type(type_code) else None

    def get_type_from_type_code(self, db_type: str, type_code: Any) -> type | None:
        """Resolve type based only on database type code.

        This method prioritizes the database's own type information
        without considering column names or patterns. It strictly uses
        the type code provided by the database.

        Args:
            db_type: Database type ('postgresql', 'mssql', 'sqlite')
            type_code: Database-specific type code

        Returns
            Python type or None if unmappable
        """
        # Fast path: If type_code is already a Python type, use it directly
        direct_type = self._use_type_directly_if_python_type(type_code)
        if direct_type:
            return direct_type

        # Check direct mappings from type maps first (fastest lookup)
        if db_type in self._type_maps and type_code in self._type_maps[db_type]:
            return self._type_maps[db_type][type_code]

        # Fall back to type handler registry but only use handlers
        # that make decisions purely on type_code
        return self._registry.get_type_from_type_code(db_type, type_code)

    def _initialize_type_maps(self):
        """Load type mapping dictionaries for supported database systems."""
        self._type_maps = {
            'postgresql': postgres_types,
            'sqlite': sqlite_types,
            'mssql_names': mssql_types,
            'mssql_codes': mssql_type_codes
        }

        # Register handlers for PostgreSQL based on type maps
        for type_code, python_type in self._type_maps['postgresql'].items():
            # Use the factory to create PostgreSQL handlers
            postgres_handler = create_simple_handler(
                f'PostgreSQL_{type_code}',
                python_type,
                {type_code},
                None
            )
            # Register with registry
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
        # Check configuration first if column_name available
        if column_name:
            config = TypeMappingConfig.get_instance()
            config_type = config.get_type_for_column(db_type, table_name, column_name)
            if config_type:
                python_type = self._map_config_type_to_python(config_type)
                if python_type:
                    return python_type

        # Check column name patterns
        if column_name:
            python_type = self._resolve_by_column_name(column_name, type_code, table_name, db_type)
            if python_type:
                return python_type

        # Use type map if provided
        if type_map and type_code in type_map:
            return type_map[type_code]

        # Default fallback
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
            db_type: Database type ('postgresql', 'mssql', 'sqlite')
            type_code: Database-specific type code
            column_name: Optional column name for context
            column_size: Optional display size
            precision: Optional numeric precision
            scale: Optional numeric scale
            table_name: Optional table name for configuration lookup

        Returns
            Python type (e.g., int, str, datetime.date)
        """
        # Fast path: If type_code is already a Python type, use it directly
        direct_type = self._use_type_directly_if_python_type(type_code)
        if direct_type:
            return direct_type

        # Try the handler registry first
        # Handle SQL Server string type codes specially
        if db_type == 'mssql' and isinstance(type_code, str):
            python_type = self._registry.get_python_type(db_type, None, type_code)
            if python_type != str:  # If not default
                return python_type
        else:
            # Try with type code directly
            python_type = self._registry.get_python_type(db_type, type_code, None)
            if python_type != str:  # If not default
                return python_type

        # Delegate to database-specific resolvers
        if db_type == 'mssql':
            return self._resolve_mssql_type(type_code, column_name, column_size, precision, scale, table_name)
        elif db_type == 'postgresql':
            return self._resolve_postgresql_type(type_code, column_name, column_size, precision, scale, table_name)
        elif db_type == 'sqlite':
            return self._resolve_sqlite_type(type_code, column_name, column_size, precision, scale, table_name)

        # Unknown database type
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
        # Handle string type codes with parameters before using common resolution logic
        if isinstance(type_code, str):
            base_type = type_code.split('(')[0].upper()
            if base_type in self._type_maps['sqlite']:
                return self._type_maps['sqlite'][base_type]

        return self._resolve_type_with_context(
            'sqlite', type_code, column_name, table_name,
            self._type_maps['sqlite']
        )

    def _resolve_mssql_type(
        self,
        type_code: Any,
        column_name: str | None = None,
        column_size: int | None = None,
        precision: int | None = None,
        scale: int | None = None,
        table_name: str | None = None
    ) -> type:
        """
        Resolve SQL Server type to Python type.

        Args:
            type_code: SQL Server type code or name
            column_name: Optional column name for context
            column_size: Optional display size
            precision: Optional numeric precision
            scale: Optional numeric scale
            table_name: Optional table name for config lookup

        Returns
            Python type
        """
        # Try direct name match if type_code is a string
        if isinstance(type_code, str):
            type_name = type_code.lower()
            python_type = self._type_maps['mssql_names'].get(type_name)
            if python_type:
                return python_type

        # Try numeric type code lookup
        if isinstance(type_code, int):
            python_type = self._type_maps['mssql_codes'].get(type_code)
            if python_type:
                return python_type

        # Use the common resolution logic as fallback
        return self._resolve_type_with_context(
            'mssql', type_code, column_name, table_name
        )

    def _map_config_type_to_python(self, config_type: str) -> type | None:
        """
        Map a configuration type string to a Python type.

        Args:
            config_type: Type name from configuration (e.g., 'int', 'varchar')

        Returns
            Python type or None if no mapping found
        """
        # This map is centralized here to avoid duplication
        type_map = {
            # Integer types
            'int': int,
            'integer': int,
            'bigint': int,
            'smallint': int,
            'tinyint': int,

            # Floating point types
            'float': float,
            'real': float,
            'double': float,
            'decimal': float,
            'numeric': float,
            'money': float,

            # Boolean type
            'bit': bool,
            'boolean': bool,

            # Date/time types
            'date': datetime.date,
            'datetime': datetime.datetime,
            'timestamp': datetime.datetime,
            'time': datetime.time,

            # String types
            'varchar': str,
            'char': str,
            'text': str,
            'nvarchar': str,
            'nchar': str,
            'ntext': str,

            # Binary types
            'binary': bytes,
            'varbinary': bytes,
            'blob': bytes
        }
        return type_map.get(config_type.lower())

    def _resolve_by_column_name(
        self,
        column_name: str,
        type_code: Any,
        table_name: str | None = None,
        db_type: str = 'mssql'
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
        # First check configuration
        config = TypeMappingConfig.get_instance()
        config_type = config.get_type_for_column(db_type, table_name, column_name)

        if config_type:
            mapped_type = self._map_config_type_to_python(config_type)
            if mapped_type:
                return mapped_type

        # Fall back to hard-coded patterns for column names
        name_lower = column_name.lower()

        # Column name pattern matching - centralized here to reduce duplication
        patterns = {
            # ID columns are typically integers
            'id_pattern': (lambda n: n.endswith('_id') or n == 'id', int),

            # Date columns
            'date_pattern': (lambda n: n.endswith('_date') or n == 'date', datetime.date),

            # DateTime columns
            'datetime_pattern': (
                lambda n: (n.endswith(('_datetime', '_at', '_timestamp')) or n == 'timestamp'),
                datetime.datetime
            ),

            # Time columns
            'time_pattern': (lambda n: n.endswith('_time') or n == 'time', datetime.time),

            # Boolean indicators
            'bool_pattern': (
                lambda n: (n.startswith('is_') or n.endswith('_flag') or
                           n in {'active', 'enabled', 'disabled', 'is_deleted'}),
                bool
            ),

            # Money/currency columns
            'money_pattern': (
                lambda n: (n.endswith(('_price', '_cost', '_amount')) or
                           n.startswith(('price_', 'cost_', 'amount_'))),
                float
            )
        }

        # Check each pattern
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
        db_type: Database type ('postgresql', 'mssql', 'sqlite')
        type_code: Database-specific type code
        column_name: Optional column name for name-based resolution
        table_name: Optional table name for context
        **kwargs: Additional type information (precision, scale, etc)

    Returns
        Python type (e.g., int, str, datetime.date)
    """
    # Use a global instance of the resolver
    global _global_resolver
    if '_global_resolver' not in globals() or _global_resolver is None:
        _global_resolver = TypeResolver()

    # First check if type_code is already a Python type (fastest path)
    if isinstance(type_code, type):
        return type_code

    # Try to resolve based strictly on the database type code
    # This ensures we trust the database's type system first
    python_type = _global_resolver.get_type_from_type_code(db_type, type_code)
    if python_type is not None:
        return python_type

    # Fall back to the full resolution process if type code-only resolution fails
    return _global_resolver.resolve_python_type(
        db_type,
        type_code,
        column_name,
        column_size=kwargs.get('column_size'),
        precision=kwargs.get('precision'),
        scale=kwargs.get('scale'),
        table_name=table_name
    )
