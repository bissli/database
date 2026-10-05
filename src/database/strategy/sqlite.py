"""SQLite DatabaseStrategy, registered for the 'sqlite' dialect.
"""
import datetime
import json
import logging
import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING, Any, TextIO

from database.cache import cacheable_strategy
from database.exceptions import QueryError, ValidationError
from database.sql import _split_qualified_identifier, make_placeholders
from database.sql import quote_identifier, standardize_placeholders
from database.strategy.base import DatabaseStrategy, register_strategy
from database.types import ColumnInfo, convert_date, convert_datetime
from database.types import sqlite_types

if TYPE_CHECKING:
    from database.connection import ConnectionWrapper
    from database.options import DatabaseOptions

logger = logging.getLogger(__name__)

JOURNAL_MODES = frozenset({'wal', 'delete', 'truncate', 'persist'})
OPEN_MODES = {'ro': 'mode=ro', 'immutable': 'mode=ro&immutable=1'}


def _raw_sqlite(conn: Any) -> Any:
    """The sqlite3 connection behind a pooled wrapper, or conn itself.
    """
    if hasattr(conn, 'dbapi_connection'):
        return conn.dbapi_connection
    return conn


def _pragma_target(table: str) -> tuple[str, str | None]:
    """Name and schema to bind into a pragma table-valued function.

    Parameters
    ----------
    table : str
        Table or index name in any form quote_identifier accepts: 't',
        '"t"', 'main.t'.

    Returns
    -------
    tuple[str, str | None]
        The unquoted name, and its schema or None. None searches every
        database, temp first.
    """
    parts = _split_qualified_identifier(table)
    return parts[-1], (parts[-2] if len(parts) >= 2 else None)


def _is_memory_db(sqlite_conn: Any) -> bool:
    """True when 'main' has no file: ':memory:' or the '' temporary database.
    """
    cursor = sqlite_conn.execute('pragma database_list')
    for row in cursor.fetchall():
        if row[1] == 'main':
            return not row[2]
    return False


class JsonBindingCursor(sqlite3.Cursor):
    """sqlite3 cursor that binds a dict or list parameter as JSON text.
    """

    def execute(self, sql: str, parameters: Any = (), /) -> 'JsonBindingCursor':
        """Run sql with each dict or list parameter bound as JSON text.
        """
        return super().execute(sql, self._json_containers(parameters))

    def executemany(self, sql: str, seq_of_parameters: Any, /) -> 'JsonBindingCursor':
        """Run sql once per parameter set, dicts and lists bound as JSON text.
        """
        return super().executemany(
            sql, (self._json_containers(params) for params in seq_of_parameters))

    @staticmethod
    def _json_containers(parameters: Any) -> Any:
        """Parameters with each dict or list value replaced by json.dumps.

        Parameters
        ----------
        parameters : dict or sequence
            Named parameters (dict) or positional parameters.

        Returns
        -------
        dict or tuple
            A dict for named parameters, else a tuple. A dict or list
            subclass, such as attrdict, passes through unchanged.
        """
        if isinstance(parameters, dict):
            return {
                name: json.dumps(value) if type(value) in {dict, list} else value
                for name, value in parameters.items()
                }
        return tuple(
            json.dumps(value) if type(value) in {dict, list} else value
            for value in parameters)


@register_strategy('sqlite')
class SQLiteStrategy(DatabaseStrategy):
    """SQLite-specific operations.
    """

    @property
    def dialect_name(self) -> str:
        """'sqlite'.
        """
        return 'sqlite'

    def build_connection_url(self, options: 'DatabaseOptions') -> str:
        """SQLAlchemy URL for options.database, as a URI when open_mode is set.

        Parameters
        ----------
        options : DatabaseOptions
            database names the file. open_mode None gives a plain path;
            'ro' or 'immutable' gives a file: URI carrying that parameter.

        Returns
        -------
        str
            The URL.
        """
        if options.open_mode is None:
            return f'sqlite:///{options.database}'
        file_uri = Path(options.database).absolute().as_uri()
        return f'sqlite:///{file_uri}?{OPEN_MODES[options.open_mode]}&uri=true'

    def get_engine_kwargs(self, options: 'DatabaseOptions') -> dict[str, Any]:
        """create_engine kwargs that turn on sqlite3's type converters.
        """
        return {
            'connect_args': {
                'detect_types': sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
                },
            }

    def register_type_adapters(self, connection: Any) -> None:
        """Register the sqlite3 date and datetime adapters and converters.

        Parameters
        ----------
        connection : Any
            Raw sqlite3 connection. The registrations are global to the
            sqlite3 module.
        """
        sqlite3.register_adapter(datetime.date, datetime.date.isoformat)
        # The space keeps new rows sortable as text against rows the
        # stdlib adapter wrote.
        sqlite3.register_adapter(
            datetime.datetime,
            lambda value: value.isoformat(' '))

        connection.execute('select 1')
        sqlite3.register_converter('date', convert_date)
        sqlite3.register_converter('datetime', convert_datetime)
        sqlite3.register_converter('timestamp', convert_datetime)

    def create_dict_cursor(self, raw_conn: Any) -> Any:
        """Cursor whose rows are sqlite3.Row and which binds JSON.

        Parameters
        ----------
        raw_conn : Any
            Pooled or raw sqlite3 connection. Its row_factory is set to
            sqlite3.Row, so every later cursor on it returns Row too.

        Returns
        -------
        JsonBindingCursor
            Binds a dict or list parameter as JSON text.
        """
        sqlite_conn = _raw_sqlite(raw_conn)
        sqlite_conn.row_factory = sqlite3.Row
        return sqlite_conn.cursor(factory=JsonBindingCursor)

    def get_type_map(self) -> dict[str, type]:
        """SQLite declared type name to Python type.
        """
        return sqlite_types

    @classmethod
    def get_required_options(cls) -> list[str]:
        """['database'].
        """
        return ['database']

    @classmethod
    def validate_options(cls, options: 'DatabaseOptions') -> None:
        """Check the required fields, the journal mode and the open mode.

        Parameters
        ----------
        options : DatabaseOptions
            Options to validate.

        Raises
        ------
        ValidationError
            When database is unset, journal_mode is not one of
            JOURNAL_MODES, or open_mode is neither None nor a key of
            OPEN_MODES, each matched in exact lower case.
        """
        super().validate_options(options)
        if options.journal_mode not in JOURNAL_MODES:
            raise ValidationError(
                f'journal_mode must be one of {sorted(JOURNAL_MODES)}, '
                f'got {options.journal_mode!r}')
        if options.open_mode is not None and options.open_mode not in OPEN_MODES:
            raise ValidationError(
                f'open_mode must be None or one of {sorted(OPEN_MODES)}, '
                f'got {options.open_mode!r}')

    def vacuum_table(self, cn: 'ConnectionWrapper', table: str) -> None:
        """Vacuum the whole database; table is ignored.
        """
        self._execute_raw(cn, 'vacuum')
        logger.info('Executed VACUUM on entire SQLite database (table-specific vacuum not supported)')

    def reindex_table(self, cn: 'ConnectionWrapper', table: str) -> None:
        """Rebuild every index on a table.
        """
        quoted_table = self.quote_identifier(table)
        self._execute_raw(cn, f'reindex {quoted_table}')

    def cluster_table(self, cn: 'ConnectionWrapper', table: str,
                      index: str | None = None) -> None:
        """Log a warning and do nothing.
        """
        logger.warning('CLUSTER operation not supported in SQLite')

    def reset_sequence(self, cn: 'ConnectionWrapper', table: str,
                       identity: str | None = None) -> None:
        """Run nothing, since SQLite assigns rowids without a sequence.
        """

    def copy_from(self, cn: 'ConnectionWrapper', table: str,
                  file: TextIO, columns: list[str] | None = None) -> int:
        """Log a warning and return 0, loading nothing.
        """
        logger.warning('COPY operation not supported in SQLite, use insert_rows instead')
        return 0

    @cacheable_strategy('primary_keys', ttl=300, maxsize=50)
    def get_primary_keys(self, cn: 'ConnectionWrapper', table: str,
                         bypass_cache: bool = False) -> list[str]:
        """Primary key columns of a table, in column order.

        Parameters
        ----------
        cn : ConnectionWrapper
            Connection to the database.
        table : str
            Table name in any form _pragma_target accepts. A missing table
            returns [].
        bypass_cache : bool, default False
            Read the database and skip the strategy cache.

        Returns
        -------
        list[str]
            Column names in column-position order: 'primary key (b, a)'
            over columns a, b returns ['a', 'b'].
        """
        sql = """
select l.name as column from pragma_table_info(?, ?) as l where l.pk <> 0
"""
        return self._select_column_raw(cn, sql, _pragma_target(table))

    @cacheable_strategy('table_columns', ttl=300, maxsize=50)
    def get_columns(self, cn: 'ConnectionWrapper', table: str,
                    bypass_cache: bool = False) -> list[str]:
        """Column names of a table, or [] for a missing table.
        """
        sql = """
select name as column from pragma_table_info(?, ?)
"""
        return self._select_column_raw(cn, sql, _pragma_target(table))

    @cacheable_strategy('sequence_columns', ttl=300, maxsize=50)
    def get_sequence_columns(self, cn: 'ConnectionWrapper', table: str,
                             bypass_cache: bool = False) -> list[str]:
        """The primary key columns, which stand in for SQLite's rowid.
        """
        return self.get_primary_keys(cn, table, bypass_cache=bypass_cache)

    def configure_connection(self, conn: Any) -> None:
        """Apply the SQLite session settings every connection takes.

        Parameters
        ----------
        conn : Any
            Pooled or raw sqlite3 connection.
        """
        sqlite_conn = _raw_sqlite(conn)
        sqlite_conn.execute('pragma foreign_keys = on')
        sqlite_conn.execute('pragma busy_timeout = 5000')
        sqlite_conn.row_factory = sqlite3.Row
        self.enable_autocommit(sqlite_conn)

    def configure_writer_connection(self, conn: Any,
                                    options: 'DatabaseOptions') -> None:
        """Set the journal mode and the synchronous level it needs.

        Parameters
        ----------
        conn : Any
            Pooled or raw sqlite3 connection. A connection with no file
            behind it, such as ':memory:', is left untouched.
        options : DatabaseOptions
            journal_mode names the mode. 'wal' takes synchronous
            normal, every other mode full.

        Raises
        ------
        sqlite3.OperationalError
            'database is locked', at once despite busy_timeout, when
            switching a WAL file to another mode while another connection
            has read it.
        """
        sqlite_conn = _raw_sqlite(conn)
        if _is_memory_db(sqlite_conn):
            return
        synchronous = 'normal' if options.journal_mode == 'wal' else 'full'
        sqlite_conn.execute(f'pragma journal_mode = {options.journal_mode}')
        sqlite_conn.execute(f'pragma synchronous = {synchronous}')

    def enable_autocommit(self, raw_conn: Any) -> None:
        """Enable auto-commit mode for SQLite.
        """
        raw_conn.isolation_level = None

    def disable_autocommit(self, raw_conn: Any) -> None:
        """Open a transaction that holds every statement, DDL included.

        Parameters
        ----------
        raw_conn : Any
            sqlite3 connection with no transaction open. The caller ends
            the transaction with commit or rollback.

        Raises
        ------
        sqlite3.OperationalError
            A transaction is already open on raw_conn.
        """
        raw_conn.isolation_level = 'DEFERRED'
        raw_conn.execute('begin')

    def set_session_readonly(self, conn: Any) -> None:
        """Make a connection refuse every write, DDL included.

        Parameters
        ----------
        conn : Any
            Pooled or raw sqlite3 connection.
        """
        _raw_sqlite(conn).execute('pragma query_only = on')

    def get_placeholder_style(self) -> str:
        """Return SQLite's placeholder marker.
        """
        return '?'

    def standardize_sql(self, sql: str) -> str:
        """SQL with '%s' rewritten to '?' and '%(name)s' to ':name'.
        """
        return standardize_placeholders(sql, dialect='sqlite')

    def get_constraint_definition(self, cn: 'ConnectionWrapper', table: str,
                                  constraint_name: str) -> dict[str, Any]:
        """Columns of the index named constraint_name, read as unique.

        Parameters
        ----------
        cn : ConnectionWrapper
            Connection to the database.
        table : str
            Used only in the error message. The index is found by name
            alone.
        constraint_name : str
            Index name.

        Returns
        -------
        dict[str, Any]
            'name', 'definition' as 'unique (col, ...)', and 'columns'.

        Raises
        ------
        QueryError
            When no index has that name.
        """
        logger.warning("SQLite doesn't fully support constraint definition retrieval")
        quoted_constraint = quote_identifier(constraint_name, 'sqlite')
        sql = f'pragma index_info({quoted_constraint})'
        result = self._select_raw(cn, sql)

        if not result:
            raise QueryError(f"Constraint '{constraint_name}' not found on table '{table}'")

        columns = [row['name'] for row in result]
        return {
            'name': constraint_name,
            'definition': f"unique ({', '.join(columns)})",
            'columns': columns,
            }

    def get_default_columns(self, cn: 'ConnectionWrapper', table: str,
                            bypass_cache: bool = False) -> list[str]:
        """Every column, as get_ordered_columns returns them.
        """
        return self.get_ordered_columns(cn, table)

    def get_ordered_columns(self, cn: 'ConnectionWrapper', table: str,
                            bypass_cache: bool = False) -> list[str]:
        """Column names of a table in position order, never cached.
        """
        sql = """
select name from pragma_table_info(?, ?)
order by cid
"""
        return self._select_column_raw(cn, sql, _pragma_target(table))

    def find_sequence_column(self, cn: 'ConnectionWrapper', table: str,
                             bypass_cache: bool = False) -> str:
        """Find the best column to reset sequence for.
        """
        return self._find_sequence_column_impl(cn, table, bypass_cache=bypass_cache)

    def get_unique_columns(self, cn: 'ConnectionWrapper', table: str,
                           bypass_cache: bool = False) -> list[list[str]]:
        """Columns of each unique index other than the primary key's.

        Parameters
        ----------
        cn : ConnectionWrapper
            Connection to the database.
        table : str
            Table name in any form _pragma_target accepts.
        bypass_cache : bool, default False
            Passed to get_primary_keys.

        Returns
        -------
        list[list[str]]
            Each index's columns in index order.
        """
        name, schema = _pragma_target(table)
        sql = 'select name from pragma_index_list(?, ?) where "unique" = 1'
        index_names = self._select_column_raw(cn, sql, (name, schema))

        unique_columns = []
        primary_keys = set(self.get_primary_keys(cn, table, bypass_cache=bypass_cache))

        for index_name in index_names:
            col_sql = 'select name from pragma_index_info(?, ?)'
            index_columns = self._select_column_raw(cn, col_sql, (index_name, schema))

            if index_columns and set(index_columns) != primary_keys:
                unique_columns.append(index_columns)

        return unique_columns

    def list_tables(self, cn: 'ConnectionWrapper') -> list[str]:
        """User table names in the main database, ordered by name.

        Returns
        -------
        list[str]
            Every table except SQLite's internal 'sqlite_' tables. Views
            and temporary tables are left out.
        """
        sql = """
select name from sqlite_master
where type = 'table' and name not glob 'sqlite_*'
order by name
"""
        return self._select_column_raw(cn, sql)

    def table_exists(self, cn: 'ConnectionWrapper', table: str) -> bool:
        """True when the main database holds a table of that name.

        Parameters
        ----------
        table : str
            Table name, matched without regard to ASCII case, as SQLite
            matches identifiers. A view does not count.
        """
        sql = """
select count(*) from sqlite_master
where type = 'table' and name = ? collate nocase
"""
        return bool(self._select_column_raw(cn, sql, (table,))[0])

    def describe_columns(self, cn: 'ConnectionWrapper',
                         table: str) -> list[ColumnInfo]:
        """Each column of a table as declared, in declaration order.

        Parameters
        ----------
        table : str
            Table or view name in the main database, matched without regard
            to ASCII case.

        Returns
        -------
        list[ColumnInfo]
            One record per column. Generated columns are left out.

        Raises
        ------
        ValidationError
            When no table or view has that name.
        """
        sql = """
select name, type, "notnull", dflt_value, pk
from pragma_table_info(?, 'main')
order by cid
"""
        rows = self._select_raw(cn, sql, (table,))
        if not rows:
            raise ValidationError(f'No table or view named {table}')
        return [
            ColumnInfo(
                name=row['name'],
                type=row['type'],
                notnull=bool(row['notnull']),
                default=row['dflt_value'],
                primary_key=bool(row['pk']))
            for row in rows
            ]

    def get_unique_indexes(self, cn: 'ConnectionWrapper',
                           table: str) -> list[list[str | None]]:
        """Columns of each unique index on a table, ordered by index name.

        Parameters
        ----------
        table : str
            Table name in the main database, matched without regard to
            ASCII case. A missing table has no indexes and returns [].

        Returns
        -------
        list[list[str | None]]
            Each index's columns in index order, primary-key and partial
            indexes included. An expression column appears as None. An
            integer primary key is the rowid and has no index, so it does
            not appear.
        """
        sql = """
select il.name as index_name, ii.name as column_name
from pragma_index_list(?, 'main') as il
join pragma_index_info(il.name, 'main') as ii
where il."unique" = 1
order by il.name, ii.seqno
"""
        columns_by_index: dict[str, list[str | None]] = {}
        for row in self._select_raw(cn, sql, (table,)):
            columns_by_index.setdefault(
                row['index_name'], []).append(row['column_name'])
        return list(columns_by_index.values())

    def table_ddl(self, cn: 'ConnectionWrapper', table: str) -> str:
        """The create table statement as SQLite stored it.

        Parameters
        ----------
        table : str
            Table name, matched without regard to ASCII case.

        Returns
        -------
        str
            The statement as written, with any later alter table applied.
            It leaves out the table's indexes and triggers.

        Raises
        ------
        ValidationError
            When the main database holds no table of that name.
        """
        sql = """
select sql from sqlite_master
where type = 'table' and name = ? collate nocase
"""
        ddl = self._select_column_raw(cn, sql, (table,))
        if not ddl:
            raise ValidationError(f'No table named {table}')
        return ddl[0]

    def index_ddl(self, cn: 'ConnectionWrapper') -> list[str]:
        """Create index statements of the main database, as SQLite stored them.

        Returns
        -------
        list[str]
            One statement per index, ordered by index name. An index SQLite
            builds for a primary-key or unique constraint has no statement
            and does not appear.
        """
        sql = """
select sql from sqlite_master
where type = 'index' and sql is not null
order by name
"""
        return self._select_column_raw(cn, sql)

    def foreign_key_violations(
            self, cn: 'ConnectionWrapper') -> list[tuple[str, int | None, str, int]]:
        """Rows of the main database whose foreign key names no parent row.

        Returns
        -------
        list[tuple[str, int | None, str, int]]
            One (table, rowid, parent table, foreign-key id) per violating
            row, empty when every key resolves. rowid is None for a without
            rowid table. A key whose parent table is missing fails on every
            row with a non-null key. The check ignores the connection's
            foreign_keys setting.

        Raises
        ------
        sqlite3.OperationalError
            When any key's parent columns carry no unique index ('foreign
            key mismatch'). No row of any table is reported then.
        """
        # The table-valued pragma_foreign_key_check takes no schema
        # argument before SQLite 3.33.0.
        sql = 'pragma main.foreign_key_check'
        return [tuple(row.values()) for row in self._select_raw(cn, sql)]

    def build_upsert_sql(
        self,
        table: str,
        columns: list[str],
        key_columns: list[str],
        constraint_expr: str | None = None,
        update_cols_always: list[str] | None = None,
        update_cols_ifnull: list[str] | None = None,
    ) -> str:
        """An 'insert ... on conflict (key_columns)' statement for one row.

        Parameters
        ----------
        table : str
            Target table.
        columns : list[str]
            Inserted columns, one '?' placeholder each.
        key_columns : list[str]
            Conflict target.
        constraint_expr : str or None, default None
            Ignored.
        update_cols_always : list[str] or None, default None
            Columns set to the incoming value on conflict.
        update_cols_ifnull : list[str] or None, default None
            Columns set to the incoming value on conflict only where the
            stored value is null.

        Returns
        -------
        str
            Ends in 'do nothing' when both update lists are empty or None.
        """
        quoted_table = self.quote_identifier(table)
        quoted_columns = [self.quote_identifier(col) for col in columns]
        placeholders = make_placeholders(len(columns), 'sqlite')

        column_list = ', '.join(quoted_columns)
        insert_sql = (
            f'insert into {quoted_table} ({column_list}) '
            f'values ({placeholders})')

        quoted_keys = [self.quote_identifier(k) for k in key_columns]
        conflict_sql = f"on conflict ({', '.join(quoted_keys)})"

        if not (update_cols_always or update_cols_ifnull):
            return f'{insert_sql} {conflict_sql} do nothing'

        update_exprs = self._build_update_exprs(
            table, update_cols_always, update_cols_ifnull)
        return f"{insert_sql} {conflict_sql} do update set {', '.join(update_exprs)}"
