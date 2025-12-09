"""
SQLite-specific strategy implementation.

This module implements the DatabaseStrategy interface with SQLite-specific operations.
It handles SQLite's unique features and limitations such as:
- Database-wide VACUUM (no table-specific optimization)
- REINDEX for index rebuilding
- Lack of CLUSTER support
- Automatic rowid management (no explicit sequence resetting needed)
- Metadata retrieval using SQLite PRAGMA statements
"""
import json
import logging
import sqlite3
from typing import TYPE_CHECKING, Any

from database.sql import make_placeholders, quote_identifier
from database.sql import standardize_placeholders
from database.strategy.base import DatabaseStrategy, register_strategy
from database.types import convert_date, convert_datetime, sqlite_types

if TYPE_CHECKING:
    from database.connection import ConnectionWrapper
    from database.options import DatabaseOptions

logger = logging.getLogger(__name__)


@register_strategy('sqlite')
class SQLiteStrategy(DatabaseStrategy):
    """SQLite-specific operations.
    """

    @property
    def dialect_name(self) -> str:
        """Return the dialect identifier for SQLite."""
        return 'sqlite'

    def build_connection_url(self, options: 'DatabaseOptions') -> str:
        """Build the SQLAlchemy connection URL for SQLite."""
        return f'sqlite:///{options.database}'

    def get_engine_kwargs(self, options: 'DatabaseOptions') -> dict[str, Any]:
        """Return SQLAlchemy create_engine kwargs for SQLite."""
        return {
            'connect_args': {
                'detect_types': sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES
            }
        }

    def register_type_adapters(self, connection: Any) -> None:
        """Register dialect-specific type adapters and converters for SQLite.

        SQLite needs adapters to handle complex types like dict and list,
        and converters to handle date/datetime coming from the database.
        """
        # Adapters (Python -> SQLite)
        sqlite3.register_adapter(dict, json.dumps)
        sqlite3.register_adapter(list, json.dumps)

        # Converters (SQLite -> Python)
        connection.execute('SELECT 1')
        sqlite3.register_converter('date', convert_date)
        sqlite3.register_converter('datetime', convert_datetime)

    def create_dict_cursor(self, raw_conn: Any) -> Any:
        """Create a cursor that returns rows as dictionaries.

        Uses sqlite3.Row for SQLite connections.
        """
        sqlite_conn = raw_conn
        if hasattr(raw_conn, 'dbapi_connection'):
            sqlite_conn = raw_conn.dbapi_connection
        sqlite_conn.row_factory = sqlite3.Row
        return sqlite_conn.cursor()

    def get_type_map(self) -> dict[str, type]:
        """Return mapping of SQLite type names to Python types."""
        return sqlite_types

    @classmethod
    def get_required_options(cls) -> list[str]:
        """Return required options for SQLite connections."""
        return ['database']

    def vacuum_table(self, cn: 'ConnectionWrapper', table: str) -> None:
        """Optimize a table with VACUUM.
        """
        self._execute_raw(cn, 'VACUUM')
        logger.info('Executed VACUUM on entire SQLite database (table-specific vacuum not supported)')

    def reindex_table(self, cn: 'ConnectionWrapper', table: str) -> None:
        """Rebuild indexes for a table.
        """
        quoted_table = self.quote_identifier(table)
        self._execute_raw(cn, f'REINDEX {quoted_table}')

    def cluster_table(self, cn: 'ConnectionWrapper', table: str,
                      index: str | None = None) -> None:
        """SQLite doesn't support CLUSTER.
        """
        logger.warning('CLUSTER operation not supported in SQLite')

    def reset_sequence(self, cn: 'ConnectionWrapper', table: str,
                       identity: str | None = None) -> None:
        """SQLite doesn't need explicit sequence resetting.
        """
        if identity is None:
            identity = self.find_sequence_column(cn, table)

    def get_primary_keys(self, cn: 'ConnectionWrapper', table: str,
                         bypass_cache: bool = False) -> list[str]:
        """Get primary key columns for a table.
        """
        quoted_table = quote_identifier(table, 'sqlite')
        sql = f"""
select l.name as column from pragma_table_info({quoted_table}) as l where l.pk <> 0
"""
        return self._select_column_raw(cn, sql)

    def get_columns(self, cn: 'ConnectionWrapper', table: str,
                    bypass_cache: bool = False) -> list[str]:
        """Get all columns for a table.
        """
        quoted_table = quote_identifier(table, 'sqlite')
        sql = f"""
select name as column from pragma_table_info({quoted_table})
    """
        return self._select_column_raw(cn, sql)

    def get_sequence_columns(self, cn: 'ConnectionWrapper', table: str,
                             bypass_cache: bool = False) -> list[str]:
        """SQLite uses rowid but reports primary keys as sequence columns.
        """
        return self.get_primary_keys(cn, table, bypass_cache=bypass_cache)

    def configure_connection(self, conn: Any) -> None:
        """Configure connection settings for SQLite.
        """
        sqlite_conn = conn
        if hasattr(conn, 'dbapi_connection'):
            sqlite_conn = conn.dbapi_connection

        sqlite_conn.execute('PRAGMA foreign_keys = ON')
        sqlite_conn.row_factory = sqlite3.Row
        self.enable_autocommit(sqlite_conn)

    def enable_autocommit(self, raw_conn: Any) -> None:
        """Enable auto-commit mode for SQLite.
        """
        raw_conn.isolation_level = None

    def disable_autocommit(self, raw_conn: Any) -> None:
        """Disable auto-commit mode for SQLite.
        """
        raw_conn.isolation_level = 'DEFERRED'

    def get_placeholder_style(self) -> str:
        """Return SQLite's placeholder marker.
        """
        return '?'

    def standardize_sql(self, sql: str) -> str:
        """Convert PostgreSQL-style placeholders (%s) to SQLite-style (?).
        """
        return standardize_placeholders(sql, dialect='sqlite')

    def get_constraint_definition(self, cn: 'ConnectionWrapper', table: str,
                                  constraint_name: str) -> dict[str, Any] | str:
        """Get the definition of a constraint by name (SQLite implementation).
        """
        logger.warning("SQLite doesn't fully support constraint definition retrieval")
        quoted_constraint = quote_identifier(constraint_name, 'sqlite')
        sql = f'PRAGMA index_info({quoted_constraint})'
        result = self._select_raw(cn, sql)

        if not result:
            raise ValueError(f"Constraint '{constraint_name}' not found on table '{table}'")

        columns = [row['name'] for row in result]
        return {
            'name': constraint_name,
            'definition': f"UNIQUE ({', '.join(columns)})",
            'columns': columns,
        }

    def get_default_columns(self, cn: 'ConnectionWrapper', table: str,
                            bypass_cache: bool = False) -> list[str]:
        """Get columns suitable for general data display.
        """
        quoted_table = quote_identifier(table, 'sqlite')
        sql = f"""
SELECT name FROM pragma_table_info({quoted_table})
ORDER BY cid
"""
        return self._select_column_raw(cn, sql)

    def get_ordered_columns(self, cn: 'ConnectionWrapper', table: str,
                            bypass_cache: bool = False) -> list[str]:
        """Get all column names for a table ordered by their position.
        """
        quoted_table = quote_identifier(table, 'sqlite')
        sql = f"""
SELECT name FROM pragma_table_info({quoted_table})
ORDER BY cid
"""
        return self._select_column_raw(cn, sql)

    def find_sequence_column(self, cn: 'ConnectionWrapper', table: str,
                             bypass_cache: bool = False) -> str:
        """Find the best column to reset sequence for.
        """
        return self._find_sequence_column_impl(cn, table, bypass_cache=bypass_cache)

    def get_unique_columns(self, cn: 'ConnectionWrapper', table: str,
                           bypass_cache: bool = False) -> list[list[str]]:
        """Get columns that have UNIQUE constraints (excluding primary key).
        """
        quoted_table = quote_identifier(table, 'sqlite')
        sql = f'SELECT name FROM pragma_index_list({quoted_table}) WHERE "unique" = 1'
        index_names = self._select_column_raw(cn, sql)

        unique_columns = []
        primary_keys = set(self.get_primary_keys(cn, table, bypass_cache=bypass_cache))

        for idx_name in index_names:
            quoted_idx = quote_identifier(idx_name, 'sqlite')
            col_sql = f'SELECT name FROM pragma_index_info({quoted_idx})'
            cols = self._select_column_raw(cn, col_sql)

            if cols and set(cols) != primary_keys:
                unique_columns.append(cols)

        return unique_columns

    def build_upsert_sql(
        self,
        table: str,
        columns: list[str],
        key_columns: list[str],
        constraint_expr: str | None = None,
        update_cols_always: list[str] | None = None,
        update_cols_ifnull: list[str] | None = None,
    ) -> str:
        """Generate SQLite upsert SQL using INSERT ... ON CONFLICT.

        Note: constraint_expr is ignored for SQLite (only PostgreSQL supports named constraints).
        """
        quoted_table = self.quote_identifier(table)
        quoted_columns = [self.quote_identifier(col) for col in columns]
        placeholders = make_placeholders(len(columns), 'sqlite')

        insert_sql = f"INSERT INTO {quoted_table} ({', '.join(quoted_columns)}) VALUES ({placeholders})"

        quoted_keys = [self.quote_identifier(k) for k in key_columns]
        conflict_sql = f"ON CONFLICT ({', '.join(quoted_keys)})"

        if not (update_cols_always or update_cols_ifnull):
            return f'{insert_sql} {conflict_sql} DO NOTHING'

        update_exprs = self._build_update_exprs(table, update_cols_always, update_cols_ifnull)
        return f"{insert_sql} {conflict_sql} DO UPDATE SET {', '.join(update_exprs)}"
