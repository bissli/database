"""
Base strategy interface for database operations.

Defines the abstract base class that all database-specific strategy implementations
must inherit from. The strategy pattern allows for encapsulating database-specific
behaviors while presenting a consistent interface to the rest of the application.

Each concrete strategy implements operations with database-specific SQL and techniques,
but clients can work with any database through this consistent interface.
"""
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from database.cache import cacheable_strategy
from database.sql import quote_identifier as sql_quote_identifier

if TYPE_CHECKING:
    from database.connection import ConnectionWrapper
    from database.options import DatabaseOptions

# Registry of dialect name -> strategy class
# Defined here to avoid circular imports (concrete strategies import from base)
_STRATEGY_REGISTRY: dict[str, type['DatabaseStrategy']] = {}


def register_strategy(dialect: str):
    """Decorator to register a strategy class for a dialect.

    Usage:
        @register_strategy('postgresql')
        class PostgresStrategy(DatabaseStrategy):
            ...
    """
    def decorator(cls: type['DatabaseStrategy']) -> type['DatabaseStrategy']:
        _STRATEGY_REGISTRY[dialect] = cls
        return cls
    return decorator


class DatabaseStrategy(ABC):
    """Base class for database-specific operations.
    """

    @contextmanager
    def _cursor(self, cn: 'ConnectionWrapper', sql: str, params: tuple | None = None):
        """Context manager for cursor lifecycle with SQL standardization.

        Handles cursor creation, SQL execution, and cleanup.
        """
        sql = self.standardize_sql(sql)
        cursor = cn.dbapi_connection.cursor()
        try:
            cursor.execute(sql, params or ())
            yield cursor
        finally:
            cursor.close()

    def _execute_raw(self, cn: 'ConnectionWrapper', sql: str,
                     params: tuple | None = None) -> int:
        """Execute SQL and return rowcount without importing query.py.

        Used internally by strategy methods for DDL/DML operations.
        """
        with self._cursor(cn, sql, params) as cursor:
            return cursor.rowcount

    def _select_raw(self, cn: 'ConnectionWrapper', sql: str,
                    params: tuple | None = None) -> list[dict]:
        """Execute SQL and return results as list of dicts without importing query.py.

        Used internally by strategy methods for queries returning multiple columns.
        """
        with self._cursor(cn, sql, params) as cursor:
            if cursor.description is None:
                return []
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def _select_column_raw(self, cn: 'ConnectionWrapper', sql: str,
                           params: tuple | None = None) -> list:
        """Execute SQL and return first column as list without importing query.py.

        Used internally by strategy methods for single-column queries.
        """
        with self._cursor(cn, sql, params) as cursor:
            return [row[0] for row in cursor.fetchall()]

    @abstractmethod
    def vacuum_table(self, cn: 'ConnectionWrapper', table: str) -> None:
        """Optimize a table by reclaiming space

        Args:
            cn: Database connection object
            table: Name of the table to vacuum
        """

    @abstractmethod
    def reindex_table(self, cn: 'ConnectionWrapper', table: str) -> None:
        """Rebuild indexes for a table.

        Args:
            cn: Database connection object
            table: Name of the table to reindex
        """

    @abstractmethod
    def cluster_table(self, cn: 'ConnectionWrapper', table: str,
                      index: str | None = None) -> None:
        """Order table data according to an index.

        Args:
            cn: Database connection object
            table: Name of the table to cluster
            index: Name of the index to cluster by, by default None
        """

    @abstractmethod
    def reset_sequence(self, cn: 'ConnectionWrapper', table: str,
                       identity: str | None = None) -> None:
        """Reset the sequence for a table.

        Args:
            cn: Database connection object
            table: Name of the table with the sequence
            identity: Name of the identity column, by default None
        """

    @abstractmethod
    @cacheable_strategy('primary_keys', ttl=300, maxsize=50)
    def get_primary_keys(self, cn: 'ConnectionWrapper', table: str,
                         bypass_cache: bool = False) -> list[str]:
        """Get primary key columns for a table

        Args:
            cn: Database connection object
            table: Table name to get primary keys for
            bypass_cache: If True, bypass cache and query database directly, by default False

        Returns
            list: List of primary key column names
        """

    @abstractmethod
    @cacheable_strategy('table_columns', ttl=300, maxsize=50)
    def get_columns(self, cn: 'ConnectionWrapper', table: str,
                    bypass_cache: bool = False) -> list[str]:
        """Get all columns for a table.

        Args:
            cn: Database connection object
            table: Table name to get columns for
            bypass_cache: If True, bypass cache and query database directly

        Returns
            list: List of column names for the specified table
        """

    @abstractmethod
    @cacheable_strategy('sequence_columns', ttl=300, maxsize=50)
    def get_sequence_columns(self, cn: 'ConnectionWrapper', table: str,
                             bypass_cache: bool = False) -> list[str]:
        """Get columns with sequences/identities.

        Args:
            cn: Database connection object
            table: Table name to get sequence columns for
            bypass_cache: If True, bypass cache and query database directly

        Returns
            list: List of sequence/identity column names for the specified table
        """

    @abstractmethod
    def configure_connection(self, conn: Any) -> None:
        """Configure connection settings.

        Args:
            conn: Database connection to configure with database-specific settings
        """

    @abstractmethod
    def enable_autocommit(self, raw_conn: Any) -> None:
        """Enable auto-commit mode on a raw database connection.

        Args:
            raw_conn: The raw DBAPI connection (not wrapped)
        """

    @abstractmethod
    def disable_autocommit(self, raw_conn: Any) -> None:
        """Disable auto-commit mode on a raw database connection.

        Args:
            raw_conn: The raw DBAPI connection (not wrapped)
        """

    @property
    @abstractmethod
    def dialect_name(self) -> str:
        """Return the dialect identifier (e.g., 'postgresql', 'sqlite')."""

    @abstractmethod
    def build_connection_url(self, options: 'DatabaseOptions') -> str:
        """Build the database connection URL for this dialect.

        Args:
            options: DatabaseOptions containing connection parameters

        Returns
            Connection URL string suitable for SQLAlchemy
        """

    @abstractmethod
    def get_engine_kwargs(self, options: 'DatabaseOptions') -> dict[str, Any]:
        """Return SQLAlchemy create_engine kwargs for this dialect.

        Args:
            options: DatabaseOptions containing connection parameters

        Returns
            Dictionary of keyword arguments for create_engine
        """

    @abstractmethod
    def register_type_adapters(self, connection: Any) -> None:
        """Register dialect-specific type adapters.

        Args:
            connection: Database connection to register adapters on
        """

    @abstractmethod
    def create_dict_cursor(self, raw_conn: Any) -> Any:
        """Create a cursor that returns rows as dictionaries.

        Args:
            raw_conn: Raw DBAPI connection

        Returns
            Cursor configured to return dict-like rows
        """

    @abstractmethod
    def get_type_map(self) -> dict:
        """Return mapping of database type codes to Python types.

        Returns
            Dictionary mapping type codes (int for PostgreSQL, str for SQLite)
            to Python types
        """

    @classmethod
    @abstractmethod
    def get_required_options(cls) -> list[str]:
        """Return list of required option field names for this dialect.

        Returns
            List of field names that must have non-None/non-zero values
        """

    @classmethod
    def validate_options(cls, options: 'DatabaseOptions') -> None:
        """Validate options for this dialect.

        Args:
            options: DatabaseOptions to validate

        Raises
            ValueError: If any required field is None or 0
        """
        for field in cls.get_required_options():
            if not getattr(options, field):
                raise ValueError(f'field {field} cannot be None or 0')

    def quote_identifier(self, identifier: str) -> str:
        """Quote a database identifier.

        Default implementation uses standard SQL double-quote escaping.
        Override in subclasses if database requires different quoting.

        Args:
            identifier: Database identifier to be quoted

        Returns
            str: Properly quoted identifier according to database-specific rules
        """
        return sql_quote_identifier(identifier)

    @abstractmethod
    def get_constraint_definition(self, cn: 'ConnectionWrapper', table: str,
                                  constraint_name: str) -> dict[str, Any] | str:
        """Get the definition of a constraint by name.

        Args:
            cn: Database connection object
            table: The table containing the constraint
            constraint_name: Name of the constraint

        Returns
            Constraint information including columns and definition
        """

    @abstractmethod
    def get_default_columns(self, cn: 'ConnectionWrapper', table: str,
                            bypass_cache: bool = False) -> list[str]:
        """Get columns suitable for general data display.

        Args:
            cn: Database connection object
            table: Table name to get default columns for
            bypass_cache: If True, bypass cache and query database directly

        Returns
            list: List of column names suitable for general data representation
        """

    @abstractmethod
    def get_ordered_columns(self, cn: 'ConnectionWrapper', table: str,
                            bypass_cache: bool = False) -> list[str]:
        """Get all column names for a table ordered by their position.

        Args:
            cn: Database connection object
            table: Table name to get columns for
            bypass_cache: If True, bypass cache and query database directly

        Returns
            list: List of column names ordered by position
        """

    @abstractmethod
    def find_sequence_column(self, cn: 'ConnectionWrapper', table: str,
                             bypass_cache: bool = False) -> str:
        """Find the best column to reset sequence for.

        Args:
            cn: Database connection object
            table: Table name to analyze for sequence columns
            bypass_cache: If True, bypass cache and query database directly

        Returns
            str: Column name best suited for sequence resetting
        """

    @abstractmethod
    def build_upsert_sql(
        self,
        table: str,
        columns: list[str],
        key_columns: list[str],
        constraint_expr: str | None = None,
        update_cols_always: list[str] | None = None,
        update_cols_ifnull: list[str] | None = None,
    ) -> str:
        """Generate dialect-specific upsert SQL.

        Args:
            table: Target table name
            columns: All columns to insert
            key_columns: Columns for conflict detection
            constraint_expr: Pre-resolved constraint expression (PostgreSQL only)
            update_cols_always: Columns to always update on conflict
            update_cols_ifnull: Columns to update only if target is NULL

        Returns
            str: Complete upsert SQL statement
        """

    def get_placeholder_style(self) -> str:
        """Return the placeholder marker for this database.

        Returns
            str: '%s' for PostgreSQL-style, '?' for SQLite-style
        """
        return '%s'

    def standardize_sql(self, sql: str) -> str:
        """Convert placeholders to this dialect's style.

        Default implementation is a no-op. Override in strategies that need
        placeholder conversion (e.g., SQLite converts %s to ?).

        Args:
            sql: SQL string potentially containing placeholders

        Returns
            str: SQL string with placeholders converted to this dialect's style
        """
        return sql

    @cacheable_strategy('sequence_column_finder', ttl=300, maxsize=50)
    def _find_sequence_column_impl(self, cn: 'ConnectionWrapper', table: str,
                                   bypass_cache: bool = False) -> str:
        """Find the best column to reset sequence for.

        Common implementation shared by all database strategies that determines the
        most appropriate column for sequence operations based on heuristic rules.

        Args:
            cn: Database connection object
            table: Table name to analyze for sequence columns
            bypass_cache: If True, bypass cache and query database directly, by default False

        Returns
            str: Column name best suited for sequence resetting based on priority:
                1. Columns that are both primary key and sequence columns
                2. Primary key or sequence columns with 'id' in their name
                3. Any primary key or sequence column
                4. Fallback to 'id' if no suitable column found
        """
        sequence_cols = self.get_sequence_columns(cn, table, bypass_cache=bypass_cache)
        primary_keys = self.get_primary_keys(cn, table, bypass_cache=bypass_cache)

        pk_sequence_cols = [col for col in sequence_cols if col in primary_keys]

        if pk_sequence_cols:
            id_cols = [col for col in pk_sequence_cols if 'id' in col.lower()]
            if id_cols:
                return id_cols[0]
            return pk_sequence_cols[0]

        if sequence_cols:
            id_cols = [col for col in sequence_cols if 'id' in col.lower()]
            if id_cols:
                return id_cols[0]
            return sequence_cols[0]

        if primary_keys:
            id_cols = [col for col in primary_keys if 'id' in col.lower()]
            if id_cols:
                return id_cols[0]
            return primary_keys[0]

        return 'id'

    def _build_update_exprs(
        self,
        table: str,
        update_cols_always: list[str] | None,
        update_cols_ifnull: list[str] | None,
    ) -> list[str]:
        """Build UPDATE SET expressions for upsert operations.

        Shared helper used by both PostgreSQL and SQLite strategies.

        Args:
            table: Target table name (for COALESCE expressions)
            update_cols_always: Columns to always update
            update_cols_ifnull: Columns to update only if target is NULL

        Returns
            List of SET expressions like 'col = excluded.col'
        """
        quoted_table = self.quote_identifier(table)
        update_exprs = []

        if update_cols_always:
            for col in update_cols_always:
                qc = self.quote_identifier(col)
                update_exprs.append(f'{qc} = excluded.{qc}')

        if update_cols_ifnull:
            for col in update_cols_ifnull:
                qc = self.quote_identifier(col)
                update_exprs.append(f'{qc} = COALESCE({quoted_table}.{qc}, excluded.{qc})')

        return update_exprs
