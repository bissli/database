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


class SqlServerIntegerHandler(TypeHandler):
    """Handles SQL Server integer types (TINYINT, SMALLINT, INT, BIGINT)"""

    def __init__(self):
        import pyodbc
        super().__init__(python_type=int)
        self.type_codes = {
            pyodbc.SQL_TINYINT, pyodbc.SQL_SMALLINT,
            pyodbc.SQL_INTEGER, pyodbc.SQL_BIGINT,
            -6, 5, 4, -5,  # Standard ODBC codes
            38, 48, 52, 56, 127  # SQL Server specific type codes
        }
        self.type_names = {'tinyint', 'smallint', 'int', 'bigint'}

    def handles_type(self, type_code: Any, type_name: str | None = None) -> bool:
        if type_name and type_name.lower() in self.type_names:
            return True
        return type_code in self.type_codes


class SqlServerNumericHandler(TypeHandler):
    """Handles SQL Server numeric types (DECIMAL, NUMERIC, MONEY, FLOAT, REAL)"""

    def __init__(self):
        super().__init__(python_type=float)
        import pyodbc
        self.type_codes = {
            pyodbc.SQL_DECIMAL, pyodbc.SQL_NUMERIC,
            pyodbc.SQL_FLOAT, pyodbc.SQL_REAL, pyodbc.SQL_DOUBLE,
            2, 3, 6, 7, 8,  # Standard ODBC codes
            106, 108, 59, 60, 62, 122  # SQL Server specific codes
        }
        self.type_names = {'decimal', 'numeric', 'money', 'smallmoney', 'float', 'real'}

    def handles_type(self, type_code: Any, type_name: str | None = None) -> bool:
        if type_name and type_name.lower() in self.type_names:
            return True
        return type_code in self.type_codes


class SqlServerBooleanHandler(TypeHandler):
    """Handles SQL Server BIT type"""

    def __init__(self):
        super().__init__(python_type=bool)
        import pyodbc
        self.type_codes = {pyodbc.SQL_BIT, -7, 104}  # ODBC + SQL Server specific
        self.type_names = {'bit'}

    def handles_type(self, type_code: Any, type_name: str | None = None) -> bool:
        if type_name and type_name.lower() in self.type_names:
            return True
        return type_code in self.type_codes


class SqlServerStringHandler(TypeHandler):
    """Handles SQL Server string types (CHAR, VARCHAR, TEXT, NVARCHAR, etc.)"""

    def __init__(self):
        super().__init__(python_type=str)
        import pyodbc
        self.type_codes = {
            pyodbc.SQL_CHAR, pyodbc.SQL_VARCHAR, pyodbc.SQL_LONGVARCHAR,
            pyodbc.SQL_WCHAR, pyodbc.SQL_WVARCHAR, pyodbc.SQL_WLONGVARCHAR,
            1, 12, -1, -8, -9, -10,  # Standard ODBC codes
            167, 175, 231, 239, 241, 99, 173, -11  # SQL Server specific
        }
        self.type_names = {'char', 'varchar', 'text', 'nchar', 'nvarchar',
                           'ntext', 'xml', 'uniqueidentifier'}

    def handles_type(self, type_code: Any, type_name: str | None = None) -> bool:
        if type_name and type_name.lower() in self.type_names:
            return True
        return type_code in self.type_codes


class SqlServerDateHandler(TypeHandler):
    """Handles SQL Server DATE type"""

    def __init__(self):
        super().__init__(python_type=datetime.date)
        import pyodbc
        self.type_codes = {pyodbc.SQL_TYPE_DATE, 91, 40}  # ODBC + SQL Server specific
        self.type_names = {'date'}

    def handles_type(self, type_code: Any, type_name: str | None = None) -> bool:
        if type_name and type_name.lower() in self.type_names:
            return True
        return type_code in self.type_codes


class SqlServerTimeHandler(TypeHandler):
    """Handles SQL Server TIME type"""

    def __init__(self):
        super().__init__(python_type=datetime.time)
        import pyodbc
        self.type_codes = {pyodbc.SQL_TYPE_TIME, 92, 41, -154}  # ODBC + SQL Server specific
        self.type_names = {'time'}

    def handles_type(self, type_code: Any, type_name: str | None = None) -> bool:
        if type_name and type_name.lower() in self.type_names:
            return True
        return type_code in self.type_codes


class SqlServerDateTimeHandler(TypeHandler):
    """Handles SQL Server DATETIME types (DATETIME, SMALLDATETIME, DATETIME2)"""

    def __init__(self):
        super().__init__(python_type=datetime.datetime)
        import pyodbc
        self.type_codes = {
            pyodbc.SQL_TYPE_TIMESTAMP, 93, 36,
            9,  # ODBC 2.x datetime
            61, 58, 42  # SQL Server specific
        }
        self.type_names = {'datetime', 'smalldatetime', 'datetime2'}

    def handles_type(self, type_code: Any, type_name: str | None = None) -> bool:
        if type_name and type_name.lower() in self.type_names:
            return True
        return type_code in self.type_codes


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


class SqlServerBinaryHandler(TypeHandler):
    """Handles SQL Server binary types (BINARY, VARBINARY, IMAGE)"""

    def __init__(self):
        super().__init__(python_type=bytes)
        import pyodbc
        self.type_codes = {
            pyodbc.SQL_BINARY, pyodbc.SQL_VARBINARY, pyodbc.SQL_LONGVARBINARY,
            -2, -3, -4,  # Standard ODBC codes
            34, 35, 165  # SQL Server specific
        }
        self.type_names = {'binary', 'varbinary', 'image'}

    def handles_type(self, type_code: Any, type_name: str | None = None) -> bool:
        if type_name and type_name.lower() in self.type_names:
            return True
        return type_code in self.type_codes


class SqlServerGuidHandler(TypeHandler):
    """Handles SQL Server UNIQUEIDENTIFIER type"""

    def __init__(self):
        super().__init__(python_type=str)  # Use string as Python type for compatibility
        self.type_codes = {-11, 36}  # ODBC + SQL Server specific
        self.type_names = {'uniqueidentifier'}

    def handles_type(self, type_code: Any, type_name: str | None = None) -> bool:
        if type_name and type_name.lower() in self.type_names:
            return True
        return type_code in self.type_codes


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
                SqlServerIntegerHandler(),
                SqlServerNumericHandler(),
                SqlServerBooleanHandler(),
                SqlServerStringHandler(),
                SqlServerDateHandler(),
                SqlServerTimeHandler(),
                SqlServerDateTimeHandler(),
                SqlServerDateTimeOffsetHandler(),
                SqlServerBinaryHandler(),
                SqlServerGuidHandler()
            ],
            'postgresql': [],
            'sqlite': []
        }

        # Default handlers for all databases
        self._default_handler = SqlServerStringHandler()  # Uses str type

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
    1. Type handler registry (customizable type mapping)
    2. Configuration-based type overrides
    3. Column name pattern matching
    4. Built-in type maps for each database
    """

    def __init__(self):
        """Initialize the type resolver with empty type maps."""
        self._type_maps: dict[str, dict[Any, type]] = {}
        self._registry = TypeHandlerRegistry.get_instance()
        self._initialize_type_maps()

    def _initialize_type_maps(self):
        """Load type mapping dictionaries for supported database systems."""
        self._type_maps = {
            'postgresql': postgres_types,
            'sqlite': sqlite_types,
            'mssql_names': mssql_types,
            'mssql_codes': mssql_type_codes
        }

        # Register missing handlers for PostgreSQL based on type maps
        for type_code, python_type in self._type_maps['postgresql'].items():
            # Create a simple handler that just returns the mapped Python type
            class SimplePostgresHandler(TypeHandler):
                def __init__(self, code, py_type):
                    super().__init__(py_type)
                    self.code = code

                def handles_type(self, type_code, type_name=None):
                    return type_code == self.code

            # Register with registry
            self._registry.register_handler('postgresql', SimplePostgresHandler(type_code, python_type))

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
        if isinstance(type_code, type):
            return type_code

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
        # First check configuration
        if column_name:
            config = TypeMappingConfig.get_instance()
            config_type = config.get_type_for_column('postgresql', table_name, column_name)
            if config_type:
                python_type = self._map_config_type_to_python(config_type)
                if python_type:
                    return python_type

        # Check for column name patterns
        if column_name:
            python_type = self._resolve_by_column_name(column_name, type_code, table_name, 'postgresql')
            if python_type:
                return python_type

        # Direct lookup from type maps
        return self._type_maps['postgresql'].get(type_code, str)

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
        # First check configuration
        if column_name:
            config = TypeMappingConfig.get_instance()
            config_type = config.get_type_for_column('sqlite', table_name, column_name)
            if config_type:
                python_type = self._map_config_type_to_python(config_type)
                if python_type:
                    return python_type

        # Check for column name patterns
        if column_name:
            python_type = self._resolve_by_column_name(column_name, type_code, table_name, 'sqlite')
            if python_type:
                return python_type

        # Handle string type codes with parameters (e.g., "NUMERIC(10,2)")
        if isinstance(type_code, str):
            base_type = type_code.split('(')[0].upper()
            return self._type_maps['sqlite'].get(base_type, str)

        # Default fallback
        return str

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
        # Fast path: If type_code is already a Python type, use it directly
        if isinstance(type_code, type):
            return type_code

        # First check configuration - highest priority
        if column_name:
            config = TypeMappingConfig.get_instance()
            config_type = config.get_type_for_column('mssql', table_name, column_name)
            if config_type:
                python_type = self._map_config_type_to_python(config_type)
                if python_type:
                    return python_type

        # Check for column name patterns - especially for VARCHAR types
        if column_name and (type_code == 12 or isinstance(type_code, str) and type_code.lower() in {'varchar', 'nvarchar'}):
            python_type = self._resolve_by_column_name(column_name, type_code, table_name, 'mssql')
            if python_type:
                return python_type

        # Try direct name match if type_code is a string
        if isinstance(type_code, str):
            type_name = type_code.lower()
            return self._type_maps['mssql_names'].get(type_name, str)

        # Try numeric type code lookup
        if isinstance(type_code, int):
            python_type = self._type_maps['mssql_codes'].get(type_code)
            if python_type:
                return python_type

        # Apply name-based refinement for any other cases not handled above
        if column_name:
            python_type = self._resolve_by_column_name(column_name, type_code, table_name, 'mssql')
            if python_type:
                return python_type

        # Default to string if no match
        return str

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

        # ID columns are typically integers
        if name_lower.endswith('_id') or name_lower == 'id':
            return int

        # Date columns
        if name_lower.endswith('_date') or name_lower == 'date':
            return datetime.date

        # DateTime columns
        if (name_lower.endswith(('_datetime', '_at', '_timestamp')) or
                name_lower == 'timestamp'):
            return datetime.datetime

        # Time columns
        if name_lower.endswith('_time') or name_lower == 'time':
            return datetime.time

        # Boolean indicators
        if (name_lower.startswith('is_') or name_lower.endswith('_flag') or
                name_lower in {'active', 'enabled', 'disabled', 'is_deleted'}):
            return bool

        # Money/currency columns
        if (name_lower.endswith(('_price', '_cost', '_amount')) or
                name_lower.startswith(('price_', 'cost_', 'amount_'))):
            return float

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

    Args:
        db_type: Database type ('postgresql', 'mssql', 'sqlite')
        type_code: Database-specific type code
        column_name: Optional column name for name-based resolution
        table_name: Optional table name for context
        **kwargs: Additional type information (precision, scale, etc)

    Returns
        Python type (e.g., int, str, datetime.date)
    """
    # Fast path: If type_code is already a Python type, use it directly
    if isinstance(type_code, type):
        return type_code

    # Use a global instance of the resolver
    global _global_resolver
    if '_global_resolver' not in globals() or _global_resolver is None:
        _global_resolver = TypeResolver()

    return _global_resolver.resolve_python_type(
        db_type,
        type_code,
        column_name,
        column_size=kwargs.get('column_size'),
        precision=kwargs.get('precision'),
        scale=kwargs.get('scale'),
        table_name=table_name
    )
