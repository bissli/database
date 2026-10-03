"""PostgreSQL strategy: maintenance, sequences, catalog metadata, upsert SQL.
"""
import logging
import re
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, TextIO
from urllib.parse import quote, quote_plus

from database.cache import cacheable_strategy
from database.exceptions import QueryError
from database.row import DictRowFactory
from database.sql import _split_qualified_identifier, make_placeholders
from database.strategy.base import DatabaseStrategy, register_strategy
from database.types import postgres_types

if TYPE_CHECKING:
    from database.connection import ConnectionWrapper
    from database.options import DatabaseOptions

logger = logging.getLogger(__name__)


@contextmanager
def temporary_autocommit(connection: Any) -> Iterator[None]:
    """Turn autocommit on for the block, then restore the prior setting.

    Parameters
    ----------
    connection : Any
        Object with a settable autocommit attribute.
    """
    original = connection.autocommit
    try:
        connection.autocommit = True
        yield
    finally:
        connection.autocommit = original


def _escape_string_literal(s: str) -> str:
    """s with each single quote doubled, for use inside '...'.
    """
    return s.replace("'", "''")


def _split_schema_table(table: str) -> tuple[str | None, str]:
    """(schema, name) of a table, with schema None when unqualified.

    Parameters
    ----------
    table : str
        'name', 'schema.name', or the double-quoted form of either.
        Quotes are removed from both parts.
    """
    parts = _split_qualified_identifier(table)
    if len(parts) >= 2:
        return parts[-2], parts[-1]
    return None, parts[-1]


@register_strategy('postgresql')
class PostgresStrategy(DatabaseStrategy):
    """PostgreSQL-specific operations.
    """

    @property
    def dialect_name(self) -> str:
        """'postgresql'.
        """
        return 'postgresql'

    def build_connection_url(self, options: 'DatabaseOptions') -> str:
        """psycopg URL for options, with TCP keepalives always on.

        Parameters
        ----------
        options : DatabaseOptions
            timeout becomes connect_timeout in seconds, and appname becomes
            application_name.

        Returns
        -------
        str
            postgresql+psycopg URL with libpq settings in its query string.
        """
        user = quote(options.username or '', safe='')
        pwd = quote(options.password or '', safe='')
        db = quote(options.database or '', safe='')

        query_parts = [
            'keepalives=1',
            'keepalives_idle=30',
            'keepalives_interval=10',
            'keepalives_count=5',
            ]
        if options.timeout:
            query_parts.append(f'connect_timeout={options.timeout}')
        if options.appname:
            query_parts.append(f'application_name={quote_plus(options.appname)}')

        return (f'postgresql+psycopg://{user}:{pwd}'
                f'@{options.hostname}:{options.port}/{db}'
                f"?{'&'.join(query_parts)}")

    def get_engine_kwargs(self, options: 'DatabaseOptions') -> dict[str, Any]:
        """create_engine kwargs: pool settings when options.use_pool, else {}.
        """
        kwargs: dict[str, Any] = {}
        if options.use_pool:
            kwargs['max_overflow'] = 0
            kwargs['pool_pre_ping'] = True
            kwargs['pool_reset_on_return'] = 'rollback'
        return kwargs

    def register_type_adapters(self, connection: Any) -> None:
        """No-op: psycopg needs no adapters registered.
        """

    def create_dict_cursor(self, raw_conn: Any) -> Any:
        """Cursor on raw_conn whose rows come from DictRowFactory.
        """
        return raw_conn.cursor(row_factory=DictRowFactory)

    def get_type_map(self) -> dict[int, type]:
        """PostgreSQL type OID to Python type.
        """
        return postgres_types

    @classmethod
    def get_required_options(cls) -> list[str]:
        """Option fields a PostgreSQL connection must set.
        """
        return ['hostname', 'username', 'password', 'database', 'port', 'timeout']

    def vacuum_table(self, cn: 'ConnectionWrapper', table: str) -> None:
        """Run vacuum (full, analyze) on table, outside any transaction.
        """
        with temporary_autocommit(cn.connection):
            quoted_table = self.quote_identifier(table)
            self._execute_raw(cn, f'vacuum (full, analyze) {quoted_table}')

    def reindex_table(self, cn: 'ConnectionWrapper', table: str) -> None:
        """Run reindex table on table, outside any transaction.
        """
        with temporary_autocommit(cn.connection):
            quoted_table = self.quote_identifier(table)
            self._execute_raw(cn, f'reindex table {quoted_table}')

    def cluster_table(self, cn: 'ConnectionWrapper', table: str,
                      index: str | None = None) -> None:
        """Run cluster on table, outside any transaction.

        Parameters
        ----------
        cn : ConnectionWrapper
            Connection to run on.
        table : str
            Table name, optionally schema-qualified.
        index : str or None, default None
            Index to order by. None reuses the last one, and PostgreSQL
            raises when the table was never clustered.
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
        """Point table's serial sequence at max(identity) + 1, or 1 if empty.

        Parameters
        ----------
        cn : ConnectionWrapper or Transaction
            A Transaction runs on the connection it holds.
        table : str
            Table name, optionally schema-qualified.
        identity : str or None, default None
            Serial column. None picks one with find_sequence_column.
        """
        if identity is None:
            identity = self.find_sequence_column(cn, table)

        quoted_table = self.quote_identifier(table)
        quoted_identity = self.quote_identifier(identity)
        escaped_table = _escape_string_literal(table)
        escaped_identity = _escape_string_literal(identity)

        sql = f"""
select
    setval(pg_get_serial_sequence('{escaped_table}', '{escaped_identity}'),
           coalesce(max({quoted_identity}), 0) + 1, false)
from
{quoted_table}
"""
        conn = getattr(cn, 'cn', cn)
        self._select_raw(conn, sql)

        logger.debug(f'Reset sequence for {table=} using {identity=}')

    def copy_from(self, cn: 'ConnectionWrapper', table: str,
                  file: TextIO, columns: list[str] | None = None) -> int:
        """Bulk load CSV text into table with PostgreSQL copy.

        Parameters
        ----------
        cn : ConnectionWrapper
            Connection whose raw DBAPI connection runs the copy.
        table : str
            Table name, optionally schema-qualified.
        file : TextIO
            CSV text with no header row. An empty field loads as null.
        columns : list[str] or None, default None
            Target columns in file order. None means every column of table.

        Returns
        -------
        int
            Rows loaded.
        """
        quoted_table = self.quote_identifier(table)

        if columns:
            quoted_cols = ','.join(self.quote_identifier(c) for c in columns)
            sql = f"copy {quoted_table} ({quoted_cols}) from stdin with (format csv, null '')"
        else:
            sql = f"copy {quoted_table} from stdin with (format csv, null '')"

        cursor = cn.dbapi_connection.cursor()
        try:
            with cursor.copy(sql) as copy:
                while data := file.read(8192):
                    copy.write(data)
            return cursor.rowcount
        except Exception:
            logger.error('Error with copy:\nSQL:\n%s', sql, exc_info=True)
            raise
        finally:
            cursor.close()

    @cacheable_strategy('primary_keys', ttl=300, maxsize=50)
    def get_primary_keys(self, cn: 'ConnectionWrapper', table: str,
                         bypass_cache: bool = False) -> list[str]:
        """Primary key column names of table, in no set order.
        """
        sql = """
select a.attname as column
from pg_index i
join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey)
where i.indrelid = %s::regclass and i.indisprimary
"""
        return self._select_column_raw(cn, sql, (table,))

    @cacheable_strategy('table_columns', ttl=300, maxsize=50)
    def get_columns(self, cn: 'ConnectionWrapper', table: str,
                    bypass_cache: bool = False) -> list[str]:
        """Column names of table. Raises unless the hstore extension exists.
        """
        quoted_table = self.quote_identifier(table)
        sql = f"""
select skeys(hstore(null::{quoted_table})) as column
"""
        return self._select_column_raw(cn, sql)

    @cacheable_strategy('sequence_columns', ttl=300, maxsize=50)
    def get_sequence_columns(self, cn: 'ConnectionWrapper', table: str,
                             bypass_cache: bool = False) -> list[str]:
        """Columns of table whose default draws from a sequence.

        An unqualified table matches that name in every schema.
        """
        schema, name = _split_schema_table(table)
        schema_clause = 'table_schema = %s and ' if schema is not None else ''
        sql = f"""
select column_name as column
from information_schema.columns
where {schema_clause}table_name = %s
and column_default like 'nextval%%'
"""
        params = (schema, name) if schema is not None else (name,)
        return self._select_column_raw(cn, sql, params)

    def configure_connection(self, conn: Any) -> None:
        """Turn autocommit on.
        """
        raw_conn = getattr(conn, 'driver_connection', conn)
        self.enable_autocommit(raw_conn)

    def enable_autocommit(self, raw_conn: Any) -> None:
        """Set raw_conn.autocommit to True.
        """
        raw_conn.autocommit = True

    def disable_autocommit(self, raw_conn: Any) -> None:
        """Set raw_conn.autocommit to False.
        """
        raw_conn.autocommit = False

    def set_session_readonly(self, conn: Any) -> None:
        """Put a PostgreSQL session in read-only mode.

        Parameters
        ----------
        conn : Any
            Pooled or raw psycopg connection.
        """
        raw_conn = getattr(conn, 'driver_connection', conn)
        raw_conn.execute('set default_transaction_read_only = on')

    def get_constraint_definition(self, cn: 'ConnectionWrapper', table: str,
                                  constraint_name: str) -> dict[str, Any] | str:
        """Conflict target of a named unique index or constraint.

        Parameters
        ----------
        cn : ConnectionWrapper
            Connection to query.
        table : str
            Table name. Any schema prefix is ignored.
        constraint_name : str
            Index or constraint name.

        Returns
        -------
        str
            For a unique index, the parenthesized column list plus any
            where predicate. For a unique or primary key constraint, its
            column list without parentheses.

        Raises
        ------
        QueryError
            No unique index or check, primary key, or unique constraint of
            that name exists on table, or its definition has no column list.
        """
        table_name = table.split('.')[-1].strip('"')

        union_query = """
select
    indexdef as definition,
    'index' as source
from
    pg_indexes
where
    indexname = %s
    and tablename = %s
    and indexdef ~ 'CREATE UNIQUE INDEX'

union all

select
    pg_get_constraintdef(c.oid) as definition,
    'constraint' as source
from
    pg_constraint c
    join pg_class tbl on c.conrelid = tbl.oid
    join pg_namespace n on tbl.relnamespace = n.oid
where
    c.conname = %s
    and tbl.relname = %s
    and c.contype in ('c', 'p', 'u')
"""
        result = self._select_raw(
            cn, union_query,
            (constraint_name, table_name, constraint_name, table_name))

        if not result:
            raise QueryError(f"Constraint or unique index '{constraint_name}' not found on table '{table}'.")

        definition = result[0]['definition'].strip()

        if result[0]['source'] != 'constraint':
            return extract_index_definition(definition)

        match = re.search(
            r'(?:UNIQUE|PRIMARY KEY)\s*\(([^)]+)\)', definition, re.IGNORECASE)
        if match:
            return match.group(1)
        match = re.search(r'\(([^)]+)\)', definition)
        if match:
            return match.group(1)

        raise QueryError(f'Failed to extract regex from definition: {definition}')

    def get_default_columns(self, cn: 'ConnectionWrapper', table: str,
                            bypass_cache: bool = False) -> list[str]:
        """Text, boolean, numeric, date, and time columns of table, in order.

        Uncached. An unqualified table matches that name in every schema.
        """
        schema, name = _split_schema_table(table)
        schema_clause = 't.table_schema = %s and ' if schema is not None else ''
        sql = f"""
select
t.column_name
from information_schema.columns t
where
{schema_clause}t.table_name = %s
and t.data_type in ('character', 'character varying', 'boolean',
    'text', 'double precision', 'real', 'integer', 'date',
    'time without time zone', 'timestamp without time zone')
order by
t.ordinal_position
"""
        params = (schema, name) if schema is not None else (name,)
        return self._select_column_raw(cn, sql, params)

    def get_ordered_columns(self, cn: 'ConnectionWrapper', table: str,
                            bypass_cache: bool = False) -> list[str]:
        """Column names of table in declaration order.

        Uncached. An unqualified table matches that name in every schema.
        """
        schema, name = _split_schema_table(table)
        schema_clause = 't.table_schema = %s and ' if schema is not None else ''
        sql = f"""
select
t.column_name
from information_schema.columns t
where
{schema_clause}t.table_name = %s
order by
t.ordinal_position
"""
        params = (schema, name) if schema is not None else (name,)
        return self._select_column_raw(cn, sql, params)

    def find_sequence_column(self, cn: 'ConnectionWrapper', table: str,
                             bypass_cache: bool = False) -> str:
        """Column of table that reset_sequence should target.
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
        """insert ... on conflict statement with one %s per column.

        Parameters
        ----------
        table : str
            Target table, optionally schema-qualified.
        columns : list[str]
            Columns to insert, in placeholder order.
        key_columns : list[str]
            Conflict target, used only when constraint_expr is empty.
        constraint_expr : str or None, default None
            Conflict target from get_constraint_definition, inserted
            after on conflict as is.
        update_cols_always : list[str] or None, default None
            Columns overwritten on conflict.
        update_cols_ifnull : list[str] or None, default None
            Columns written on conflict only where the stored value is null.

        Returns
        -------
        str
            The statement, ending do nothing when both update lists are
            empty.
        """
        quoted_table = self.quote_identifier(table)
        quoted_columns = [self.quote_identifier(col) for col in columns]
        placeholders = make_placeholders(len(columns), 'postgresql')

        insert_sql = f"insert into {quoted_table} ({', '.join(quoted_columns)}) values ({placeholders})"

        if constraint_expr:
            conflict_sql = f'on conflict {constraint_expr}'
        else:
            quoted_keys = [self.quote_identifier(k) for k in key_columns]
            conflict_sql = f"on conflict ({', '.join(quoted_keys)})"

        if not (update_cols_always or update_cols_ifnull):
            return f'{insert_sql} {conflict_sql} do nothing'

        update_exprs = self._build_update_exprs(table, update_cols_always, update_cols_ifnull)
        return f"{insert_sql} {conflict_sql} do update set {', '.join(update_exprs)}"


def extract_index_definition(definition: str) -> str:
    """on conflict target read from a CREATE UNIQUE INDEX statement.

    Parameters
    ----------
    definition : str
        pg_indexes.indexdef text of a unique index.

    Returns
    -------
    str
        The parenthesized column list, plus ' WHERE <predicate>' for a
        partial index. NULLS NOT DISTINCT is dropped.

    Raises
    ------
    QueryError
        definition holds no parenthesized group.
    """
    pattern = (
        r'CREATE\s+UNIQUE\s+INDEX\s+\w+'
        r'\s+ON\s+(?:[a-zA-Z0-9_]+\.)?[a-zA-Z0-9_]+'
        r'(?:\s+USING\s+\w+)?'
        r'\s+(\(.*?\))'
        r'(?:\s+NULLS\s+NOT\s+DISTINCT)?'
        r'(?:\s+WHERE\s+(.*?))?$')

    match = re.search(pattern, definition)
    if match:
        column_clause, where_clause = match.group(1), match.group(2)
        if where_clause:
            return f'{column_clause} WHERE {where_clause}'
        return column_clause

    paren_match = re.search(r'\(([^()]*(?:\([^()]*\)[^()]*)*)\)', definition)
    if paren_match:
        column_def = paren_match.group(0)
        where_match = re.search(r'\)\s+WHERE\s+(.*?)(?:\s*$|\s+NULLS)', definition)
        if where_match:
            return f'{column_def} WHERE {where_match.group(1)}'
        return column_def

    raise QueryError(f'Failed to extract column definition from index: {definition}')
