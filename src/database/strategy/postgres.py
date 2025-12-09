"""
PostgreSQL-specific strategy implementation.

This module implements the DatabaseStrategy interface with PostgreSQL-specific operations.
It handles PostgreSQL's unique features such as:
- VACUUM and ANALYZE for table optimization
- REINDEX for index rebuilding
- CLUSTER for physical reordering of table data
- Sequence management for auto-increment columns
- Metadata retrieval using PostgreSQL system catalogs
"""
import logging
import re
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from database.row import DictRowFactory
from database.sql import make_placeholders
from database.strategy.base import DatabaseStrategy, register_strategy
from database.types import postgres_types

logger = logging.getLogger(__name__)


@contextmanager
def temporary_autocommit(connection):
    """Context manager to temporarily enable autocommit on a connection.

    Saves current autocommit state, enables autocommit, executes the block,
    then restores the original state.
    """
    original = connection.autocommit
    try:
        connection.autocommit = True
        yield
    finally:
        connection.autocommit = original


def _escape_string_literal(s: str) -> str:
    """Escape a string for use as a PostgreSQL string literal."""
    return s.replace("'", "''")


if TYPE_CHECKING:
    from database.connection import ConnectionWrapper
    from database.options import DatabaseOptions


@register_strategy('postgresql')
class PostgresStrategy(DatabaseStrategy):
    """PostgreSQL-specific operations.
    """

    @property
    def dialect_name(self) -> str:
        """Return the dialect identifier for PostgreSQL."""
        return 'postgresql'

    def build_connection_url(self, options: 'DatabaseOptions') -> str:
        """Build the SQLAlchemy connection URL for PostgreSQL."""
        query_parts = []
        if options.timeout:
            query_parts.append(f'connect_timeout={options.timeout}')

        url = (f'postgresql+psycopg://{options.username}:{options.password}'
               f'@{options.hostname}:{options.port}/{options.database}')

        if query_parts:
            url += '?' + '&'.join(query_parts)

        return url

    def get_engine_kwargs(self, options: 'DatabaseOptions') -> dict[str, Any]:
        """Return SQLAlchemy create_engine kwargs for PostgreSQL."""
        return {}

    def register_type_adapters(self, connection: Any) -> None:
        """Register dialect-specific type adapters for PostgreSQL.

        PostgreSQL with psycopg doesn't need special adapters.
        """

    def create_dict_cursor(self, raw_conn: Any) -> Any:
        """Create a cursor that returns rows as dictionaries.

        Uses DictRowFactory for PostgreSQL connections.
        """
        return raw_conn.cursor(row_factory=DictRowFactory)

    def get_type_map(self) -> dict[int, type]:
        """Return mapping of PostgreSQL type codes to Python types."""
        return postgres_types

    @classmethod
    def get_required_options(cls) -> list[str]:
        """Return required options for PostgreSQL connections."""
        return ['hostname', 'username', 'password', 'database', 'port', 'timeout']

    def vacuum_table(self, cn: 'ConnectionWrapper', table: str) -> None:
        """Optimize a table with VACUUM.
        """
        with temporary_autocommit(cn.connection):
            quoted_table = self.quote_identifier(table)
            self._execute_raw(cn, f'vacuum (full, analyze) {quoted_table}')

    def reindex_table(self, cn: 'ConnectionWrapper', table: str) -> None:
        """Rebuild indexes for a table.
        """
        with temporary_autocommit(cn.connection):
            quoted_table = self.quote_identifier(table)
            self._execute_raw(cn, f'reindex table {quoted_table}')

    def cluster_table(self, cn: 'ConnectionWrapper', table: str,
                      index: str | None = None) -> None:
        """Order table data according to an index.
        """
        with temporary_autocommit(cn.connection):
            quoted_table = self.quote_identifier(table)
            if index is None:
                self._execute_raw(cn, f'cluster {quoted_table}')
            else:
                quoted_index = self.quote_identifier(index)
                self._execute_raw(cn, f'cluster {quoted_table} using {quoted_index}')

    def reset_sequence(self, cn: 'ConnectionWrapper', table: str,
                       identity: str | None = None) -> None:
        """Reset the sequence for a table.
        """
        if identity is None:
            identity = self.find_sequence_column(cn, table)

        quoted_table = self.quote_identifier(table)
        quoted_identity = self.quote_identifier(identity)
        escaped_table = _escape_string_literal(table)
        escaped_identity = _escape_string_literal(identity)

        sql = f"""
select
    setval(pg_get_serial_sequence('{escaped_table}', '{escaped_identity}'), coalesce(max({quoted_identity}),0)+1, false)
from
{quoted_table}
"""
        # Handle both ConnectionWrapper and Transaction (which wraps a connection)
        conn = getattr(cn, 'cn', cn)
        self._select_raw(conn, sql)

        logger.debug(f'Reset sequence for {table=} using {identity=}')

    def get_primary_keys(self, cn: 'ConnectionWrapper', table: str,
                         bypass_cache: bool = False) -> list[str]:
        """Get primary key columns for a table.
        """
        sql = """
select a.attname as column
from pg_index i
join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey)
where i.indrelid = %s::regclass and i.indisprimary
"""
        return self._select_column_raw(cn, sql, (table,))

    def get_columns(self, cn: 'ConnectionWrapper', table: str,
                    bypass_cache: bool = False) -> list[str]:
        """Get all columns for a table.
        """
        quoted_table = self.quote_identifier(table)
        sql = f"""
select skeys(hstore(null::{quoted_table})) as column
    """
        return self._select_column_raw(cn, sql)

    def get_sequence_columns(self, cn: 'ConnectionWrapper', table: str,
                             bypass_cache: bool = False) -> list[str]:
        """Get columns with sequences.
        """
        sql = """
        SELECT column_name as column
        FROM information_schema.columns
        WHERE table_name = %s
        AND column_default LIKE 'nextval%%'
        """
        return self._select_column_raw(cn, sql, (table,))

    def configure_connection(self, conn: Any) -> None:
        """Configure connection settings for PostgreSQL.
        """
        raw_conn = conn
        if hasattr(conn, 'driver_connection'):
            raw_conn = conn.driver_connection
        self.enable_autocommit(raw_conn)

    def enable_autocommit(self, raw_conn: Any) -> None:
        """Enable auto-commit mode for PostgreSQL.
        """
        raw_conn.autocommit = True

    def disable_autocommit(self, raw_conn: Any) -> None:
        """Disable auto-commit mode for PostgreSQL.
        """
        raw_conn.autocommit = False

    def get_constraint_definition(self, cn: 'ConnectionWrapper', table: str,
                                  constraint_name: str) -> dict[str, Any] | str:
        """Get the definition of a constraint or unique index by name.
        """
        parts = table.split('.')
        table_name = parts[-1].strip('"')

        union_query = """
        SELECT
            indexdef AS definition,
            'index' AS source
        FROM
            pg_indexes
        WHERE
            indexname = %s
            AND tablename = %s
            AND indexdef ~ 'CREATE UNIQUE INDEX'

        UNION ALL

        SELECT
            pg_get_constraintdef(c.oid) AS definition,
            'constraint' AS source
        FROM
            pg_constraint c
            JOIN pg_class tbl ON c.conrelid = tbl.oid
            JOIN pg_namespace n ON tbl.relnamespace = n.oid
        WHERE
            c.conname = %s
            AND tbl.relname = %s
            AND c.contype IN ('c', 'p', 'u')

        """

        result = self._select_raw(cn, union_query, (constraint_name, table_name, constraint_name, table_name))

        if not result:
            raise ValueError(f"Constraint or unique index '{constraint_name}' not found on table '{table}'.")

        definition = result[0]['definition']
        source = result[0]['source']

        definition = definition.strip()

        if source == 'constraint':
            match = re.search(r'(?:UNIQUE|PRIMARY KEY)\s*\(([^)]+)\)', definition, re.IGNORECASE)
            if match:
                return match.group(1)
            match = re.search(r'\(([^)]+)\)', definition)
            if match:
                return match.group(1)
        else:
            return extract_index_definition(definition)

        raise ValueError(f'Failed to extract regex from definition: {definition}')

    def get_default_columns(self, cn: 'ConnectionWrapper', table: str,
                            bypass_cache: bool = False) -> list[str]:
        """Get columns suitable for general data display.
        """
        sql = """
select
t.column_name
from information_schema.columns t
where
t.table_name = %s
and t.data_type in ('character', 'character varying', 'boolean',
    'text', 'double precision', 'real', 'integer', 'date',
    'time without time zone', 'timestamp without time zone')
order by
t.ordinal_position
"""
        return self._select_column_raw(cn, sql, (table,))

    def get_ordered_columns(self, cn: 'ConnectionWrapper', table: str,
                            bypass_cache: bool = False) -> list[str]:
        """Get all column names for a table ordered by their position.
        """
        sql = """
select
t.column_name
from information_schema.columns t
where
t.table_name = %s
order by
t.ordinal_position
"""
        return self._select_column_raw(cn, sql, (table,))

    def find_sequence_column(self, cn: 'ConnectionWrapper', table: str,
                             bypass_cache: bool = False) -> str:
        """Find the best column to reset sequence for.
        """
        return self._find_sequence_column_impl(cn, table, bypass_cache=bypass_cache)

    def build_upsert_sql(
        self,
        table: str,
        columns: list[str],
        key_columns: list[str],
        constraint_expr: str | None = None,
        update_cols_always: list[str] | None = None,
        update_cols_ifnull: list[str] | None = None,
    ) -> str:
        """Generate PostgreSQL upsert SQL using INSERT ... ON CONFLICT.

        Args:
            table: Target table name
            columns: All columns to insert
            key_columns: Columns for conflict detection (used if constraint_expr is None)
            constraint_expr: Pre-resolved constraint expression from get_constraint_definition()
            update_cols_always: Columns to always update on conflict
            update_cols_ifnull: Columns to update only if target is NULL
        """
        quoted_table = self.quote_identifier(table)
        quoted_columns = [self.quote_identifier(col) for col in columns]
        placeholders = make_placeholders(len(columns), 'postgresql')

        insert_sql = f"INSERT INTO {quoted_table} ({', '.join(quoted_columns)}) VALUES ({placeholders})"

        if constraint_expr:
            conflict_sql = f'ON CONFLICT {constraint_expr}'
        else:
            quoted_keys = [self.quote_identifier(k) for k in key_columns]
            conflict_sql = f"ON CONFLICT ({', '.join(quoted_keys)})"

        if not (update_cols_always or update_cols_ifnull):
            return f'{insert_sql} {conflict_sql} DO NOTHING RETURNING *'

        update_exprs = self._build_update_exprs(table, update_cols_always, update_cols_ifnull)
        return f"{insert_sql} {conflict_sql} DO UPDATE SET {', '.join(update_exprs)} RETURNING *"


def extract_index_definition(definition: str) -> str:
    """Extract column list and WHERE clause from a PostgreSQL unique index definition.

    This function parses PostgreSQL CREATE UNIQUE INDEX statements to extract:
    1. The column specification, which may include functions like COALESCE
    2. Any WHERE clause that makes the uniqueness conditional
    """
    pattern = r'CREATE\s+UNIQUE\s+INDEX\s+\w+\s+ON\s+(?:[a-zA-Z0-9_]+\.)?[a-zA-Z0-9_]+(?:\s+USING\s+\w+)?\s+(\(.*?\))(?:\s+NULLS\s+NOT\s+DISTINCT)?(?:\s+WHERE\s+(.*?))?$'

    match = re.search(pattern, definition)
    if match:
        column_clause = match.group(1)
        where_clause = match.group(2)
        if where_clause:
            return f'{column_clause} WHERE {where_clause}'
        else:
            return column_clause

    paren_match = re.search(r'\(([^()]*(?:\([^()]*\)[^()]*)*)\)', definition)
    if paren_match:
        column_def = paren_match.group(0)

        where_match = re.search(r'\)\s+WHERE\s+(.*?)(?:\s*$|\s+NULLS)', definition)
        if where_match:
            return f'{column_def} WHERE {where_match.group(1)}'
        return column_def

    raise ValueError(f'Failed to extract column definition from index: {definition}')
