"""Abstract base for the per-dialect database strategies, and their registry.
"""
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, TextIO

from database.cache import cacheable_strategy
from database.exceptions import ValidationError
from database.sql import quote_identifier as sql_quote_identifier
from database.sql import raise_on_readonly_disarm
from database.types import ColumnInfo

if TYPE_CHECKING:
    from database.connection import ConnectionWrapper
    from database.options import DatabaseOptions

logger = logging.getLogger(__name__)

_STRATEGY_REGISTRY: dict[str, type['DatabaseStrategy']] = {}


def register_strategy(
        dialect: str) -> Callable[[type['DatabaseStrategy']], type['DatabaseStrategy']]:
    """Class decorator that registers a strategy class under a dialect.

    Parameters
    ----------
    dialect : str
        Name get_dialect_name reports, e.g. 'postgresql'. A later
        registration under the same name replaces the earlier one.

    Returns
    -------
    Callable
        Decorator that records the class and returns it unchanged.
    """
    def decorator(cls: type['DatabaseStrategy']) -> type['DatabaseStrategy']:
        _STRATEGY_REGISTRY[dialect] = cls
        return cls
    return decorator


class DatabaseStrategy(ABC):
    """Dialect-specific SQL, connection setup, and schema lookups.
    """

    @contextmanager
    def _cursor(self, cn: 'ConnectionWrapper', sql: str,
                params: tuple | None = None) -> Iterator[Any]:
        """Run sql on a raw DBAPI cursor, yield the cursor, then close it.

        Parameters
        ----------
        cn : ConnectionWrapper
            Connection to run on.
        sql : str
            Statement text, standardized for the dialect before it runs.
        params : tuple or None, default None
            Positional parameters; None runs the statement with none.

        Yields
        ------
        Any
            The raw DBAPI cursor, after execute.

        Raises
        ------
        ReadOnlyError
            sql would turn off a reader's read-only session setting.
        """
        raise_on_readonly_disarm(cn, sql)
        sql = self.standardize_sql(sql)
        cursor = cn.dbapi_connection.cursor()
        try:
            cursor.execute(sql, params or ())
            yield cursor
        except Exception:
            logger.error(
                'Error with query:\nSQL:\n%s\nparams: %s',
                sql,
                params,
                exc_info=True)
            raise
        finally:
            cursor.close()

    def _execute_raw(self, cn: 'ConnectionWrapper', sql: str,
                     params: tuple | None = None) -> int:
        """Run sql through _cursor and return the driver's rowcount.
        """
        with self._cursor(cn, sql, params) as cursor:
            return cursor.rowcount

    def _select_raw(self, cn: 'ConnectionWrapper', sql: str,
                    params: tuple | None = None) -> list[dict]:
        """Run sql through _cursor and return its rows as dicts.

        Returns
        -------
        list[dict]
            One dict per row, keyed by column name. Empty when the
            statement returns no result set.
        """
        with self._cursor(cn, sql, params) as cursor:
            if cursor.description is None:
                return []
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def _select_column_raw(self, cn: 'ConnectionWrapper', sql: str,
                           params: tuple | None = None) -> list:
        """Run sql through _cursor and return the first value of each row.
        """
        with self._cursor(cn, sql, params) as cursor:
            return [row[0] for row in cursor.fetchall()]

    @abstractmethod
    def vacuum_table(self, cn: 'ConnectionWrapper', table: str) -> None:
        """Reclaim the space dead rows hold.

        Parameters
        ----------
        cn : ConnectionWrapper
            Connection to run on.
        table : str
            Table to vacuum. A dialect that can only vacuum the whole
            database may ignore it.
        """

    @abstractmethod
    def reindex_table(self, cn: 'ConnectionWrapper', table: str) -> None:
        """Rebuild every index on a table.

        Parameters
        ----------
        cn : ConnectionWrapper
            Connection to run on.
        table : str
            Table whose indexes to rebuild.
        """

    @abstractmethod
    def cluster_table(self, cn: 'ConnectionWrapper', table: str,
                      index: str | None = None) -> None:
        """Reorder a table's rows on disk to follow an index.

        Parameters
        ----------
        cn : ConnectionWrapper
            Connection to run on.
        table : str
            Table to reorder.
        index : str or None, default None
            Index to follow; None lets the dialect choose, e.g. the index
            the table was last clustered on.
        """

    @abstractmethod
    def reset_sequence(self, cn: 'ConnectionWrapper', table: str,
                       identity: str | None = None) -> None:
        """Set a table's sequence so the next value follows the column max.

        Parameters
        ----------
        cn : ConnectionWrapper
            Connection to run on.
        table : str
            Table that owns the sequence.
        identity : str or None, default None
            Sequence-backed column; None picks one with
            find_sequence_column.
        """

    @abstractmethod
    def copy_from(self, cn: 'ConnectionWrapper', table: str,
                  file: TextIO, columns: list[str] | None = None) -> int:
        """Bulk load CSV text from file into table; return rows loaded.
        """

    @abstractmethod
    def get_primary_keys(self, cn: 'ConnectionWrapper', table: str,
                         bypass_cache: bool = False) -> list[str]:
        """Primary key column names of a table.

        Parameters
        ----------
        cn : ConnectionWrapper
            Connection to query.
        table : str
            Table to inspect.
        bypass_cache : bool, default False
            True queries the database and leaves the cached entry alone.

        Returns
        -------
        list[str]
            Primary key columns; empty when the table has none.
        """

    @abstractmethod
    def get_columns(self, cn: 'ConnectionWrapper', table: str,
                    bypass_cache: bool = False) -> list[str]:
        """Column names of a table.

        Parameters
        ----------
        cn : ConnectionWrapper
            Connection to query.
        table : str
            Table to inspect.
        bypass_cache : bool, default False
            True queries the database and leaves the cached entry alone.

        Returns
        -------
        list[str]
            Every column of the table.
        """

    @abstractmethod
    def get_sequence_columns(self, cn: 'ConnectionWrapper', table: str,
                             bypass_cache: bool = False) -> list[str]:
        """Names of a table's sequence or identity columns.

        Parameters
        ----------
        cn : ConnectionWrapper
            Connection to query.
        table : str
            Table to inspect.
        bypass_cache : bool, default False
            True queries the database and leaves the cached entry alone.

        Returns
        -------
        list[str]
            Sequence-backed columns; empty when the table has none.
        """

    @abstractmethod
    def configure_connection(self, conn: Any) -> None:
        """Apply the session settings every connection takes.

        Parameters
        ----------
        conn : Any
            Pooled or raw DBAPI connection to configure.
        """

    def configure_writer_connection(self, conn: Any,
                                    options: 'DatabaseOptions') -> None:
        """Apply the settings only a writer may set.

        Parameters
        ----------
        conn : Any
            Pooled or raw DBAPI connection to configure.
        options : DatabaseOptions
            Options the connection was opened with.
        """

    @abstractmethod
    def enable_autocommit(self, raw_conn: Any) -> None:
        """Turn auto-commit on for a raw DBAPI connection, never a wrapper.
        """

    @abstractmethod
    def disable_autocommit(self, raw_conn: Any) -> None:
        """Turn auto-commit off for a raw DBAPI connection, never a wrapper.
        """

    def set_session_readonly(self, conn: Any) -> None:
        """Put a session in read-only mode for the rest of its life.

        Parameters
        ----------
        conn : Any
            Pooled or raw DBAPI connection for this dialect.

        Raises
        ------
        NotImplementedError
            Always, for a dialect that has not overridden this.
        """
        raise NotImplementedError(
            f'{type(self).__name__} has no read-only session setting, so '
            f"role='reader' is not available for this dialect")

    def list_tables(self, cn: 'ConnectionWrapper') -> list[str]:
        """User table names, ordered by name.

        Raises
        ------
        NotImplementedError
            Always, for a dialect that has not overridden this.
        """
        raise NotImplementedError(f'{type(self).__name__} cannot list tables')

    def table_exists(self, cn: 'ConnectionWrapper', table: str) -> bool:
        """True when a user table of that name exists.

        Raises
        ------
        NotImplementedError
            Always, for a dialect that has not overridden this.
        """
        raise NotImplementedError(f'{type(self).__name__} cannot test a table')

    def describe_columns(self, cn: 'ConnectionWrapper',
                         table: str) -> list[ColumnInfo]:
        """Each column of a table, in declaration order.

        Raises
        ------
        NotImplementedError
            Always, for a dialect that has not overridden this.
        """
        raise NotImplementedError(f'{type(self).__name__} cannot describe a table')

    def get_unique_indexes(self, cn: 'ConnectionWrapper',
                           table: str) -> list[list[str | None]]:
        """Columns of each unique index on a table, in index order.

        Raises
        ------
        NotImplementedError
            Always, for a dialect that has not overridden this.
        """
        raise NotImplementedError(f'{type(self).__name__} cannot list indexes')

    def table_ddl(self, cn: 'ConnectionWrapper', table: str) -> str:
        """Create statement text of a table.

        Raises
        ------
        NotImplementedError
            Always, for a dialect that has not overridden this.
        """
        raise NotImplementedError(f'{type(self).__name__} cannot read table DDL')

    def index_ddl(self, cn: 'ConnectionWrapper') -> list[str]:
        """Create index statement text of every index, ordered by name.

        Raises
        ------
        NotImplementedError
            Always, for a dialect that has not overridden this.
        """
        raise NotImplementedError(f'{type(self).__name__} cannot read index DDL')

    def foreign_key_violations(
            self, cn: 'ConnectionWrapper') -> list[tuple[str, int | None, str, int]]:
        """Rows whose foreign key names no parent row.

        Raises
        ------
        NotImplementedError
            Always, for a dialect that has not overridden this.
        """
        raise NotImplementedError(f'{type(self).__name__} cannot check foreign keys')

    @property
    @abstractmethod
    def dialect_name(self) -> str:
        """Name the strategy registers under, e.g. 'postgresql'.
        """

    @abstractmethod
    def build_connection_url(self, options: 'DatabaseOptions') -> str:
        """SQLAlchemy connection URL built from options.
        """

    @abstractmethod
    def get_engine_kwargs(self, options: 'DatabaseOptions') -> dict[str, Any]:
        """Keyword arguments for SQLAlchemy create_engine, built from options.
        """

    @abstractmethod
    def register_type_adapters(self, connection: Any) -> None:
        """Register the dialect's type adapters on a DBAPI connection.
        """

    @abstractmethod
    def create_dict_cursor(self, raw_conn: Any) -> Any:
        """Cursor on a raw DBAPI connection that returns dict-like rows.
        """

    @abstractmethod
    def get_type_map(self) -> dict:
        """Python type for each driver type code.

        Returns
        -------
        dict
            Keyed by the driver's type code: an int OID for PostgreSQL, a
            declared type name for SQLite.
        """

    @classmethod
    @abstractmethod
    def get_required_options(cls) -> list[str]:
        """DatabaseOptions field names that validate_options requires.

        Returns
        -------
        list[str]
            Fields that must be truthy: None, 0, and '' all fail.
        """

    @classmethod
    def validate_options(cls, options: 'DatabaseOptions') -> None:
        """Check that every get_required_options field is truthy.

        Parameters
        ----------
        options : DatabaseOptions
            Options to check.

        Raises
        ------
        ValidationError
            A required field is falsy: None, 0, or ''.
        """
        for field in cls.get_required_options():
            if not getattr(options, field):
                raise ValidationError(f'field {field} cannot be None or 0')

    def quote_identifier(self, identifier: str) -> str:
        """Double-quoted identifier, each part of a dotted name on its own.

        Parameters
        ----------
        identifier : str
            Name to quote; 'schema.table' quotes as "schema"."table". A
            dot inside an already quoted part stays in that part.

        Returns
        -------
        str
            Quoted identifier, with an embedded double quote doubled.

        Raises
        ------
        ValidationError
            identifier holds a NUL byte.
        """
        return sql_quote_identifier(identifier)

    @abstractmethod
    def get_constraint_definition(self, cn: 'ConnectionWrapper', table: str,
                                  constraint_name: str) -> dict[str, Any] | str:
        """Conflict target of a named constraint or unique index.

        Parameters
        ----------
        cn : ConnectionWrapper
            Connection to query.
        table : str
            Table that owns the constraint.
        constraint_name : str
            Constraint or index name.

        Returns
        -------
        dict[str, Any] or str
            Conflict target that upsert_rows hands to build_upsert_sql as
            constraint_expr, in whatever shape the dialect reads there.
        """

    @abstractmethod
    def get_default_columns(self, cn: 'ConnectionWrapper', table: str,
                            bypass_cache: bool = False) -> list[str]:
        """Columns of a table to select by default, in declaration order.

        Parameters
        ----------
        cn : ConnectionWrapper
            Connection to query.
        table : str
            Table to inspect.
        bypass_cache : bool, default False
            True skips the cache, where an override keeps one.

        Returns
        -------
        list[str]
            The dialect's choice of columns; it may leave out types that
            do not load as plain values.
        """

    @abstractmethod
    def get_ordered_columns(self, cn: 'ConnectionWrapper', table: str,
                            bypass_cache: bool = False) -> list[str]:
        """Column names of a table in declaration order.

        Parameters
        ----------
        cn : ConnectionWrapper
            Connection to query.
        table : str
            Table to inspect.
        bypass_cache : bool, default False
            True skips the cache, where an override keeps one.

        Returns
        -------
        list[str]
            Every column, ordered by position.
        """

    @abstractmethod
    def find_sequence_column(self, cn: 'ConnectionWrapper', table: str,
                             bypass_cache: bool = False) -> str:
        """Column whose sequence reset_sequence should reset.

        Parameters
        ----------
        cn : ConnectionWrapper
            Connection to query.
        table : str
            Table to inspect.
        bypass_cache : bool, default False
            True queries the database and leaves the cached entry alone.

        Returns
        -------
        str
            The chosen column; _find_sequence_column_impl gives the
            shared rule.
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
        """Insert ... on conflict statement for one row of columns.

        Parameters
        ----------
        table : str
            Target table.
        columns : list[str]
            Columns to insert, in placeholder order.
        key_columns : list[str]
            Conflict target columns, used when constraint_expr is empty.
        constraint_expr : str or None, default None
            Conflict target from get_constraint_definition. SQLite
            ignores it.
        update_cols_always : list[str] or None, default None
            Columns overwritten from the new row on conflict.
        update_cols_ifnull : list[str] or None, default None
            Columns overwritten on conflict only where the stored value
            is null.

        Returns
        -------
        str
            Statement with one placeholder per column. With no update
            columns, a conflicting row is left unchanged.
        """

    def get_placeholder_style(self) -> str:
        """Positional placeholder marker: '%s' here, '?' for SQLite.
        """
        return '%s'

    def standardize_sql(self, sql: str) -> str:
        """Sql with its placeholders rewritten to this dialect's style.
        """
        return sql

    @cacheable_strategy('sequence_column_finder', ttl=300, maxsize=50)
    def _find_sequence_column_impl(self, cn: 'ConnectionWrapper', table: str,
                                   bypass_cache: bool = False) -> str:
        """Column to reset a table's sequence on, shared by every dialect.

        Parameters
        ----------
        cn : ConnectionWrapper
            Connection to query.
        table : str
            Table to inspect.
        bypass_cache : bool, default False
            True skips this cache and the two lookups' caches.

        Returns
        -------
        str
            From the first non-empty group, in order: sequence columns
            that are also primary keys, sequence columns, primary keys.
            Within the group, the first name holding 'id' in any case,
            else the group's first column. 'id' when every group is empty.
        """
        sequence_cols = self.get_sequence_columns(cn, table, bypass_cache=bypass_cache)
        primary_keys = self.get_primary_keys(cn, table, bypass_cache=bypass_cache)
        pk_sequence_cols = [col for col in sequence_cols if col in primary_keys]

        for candidates in (pk_sequence_cols, sequence_cols, primary_keys):
            if candidates:
                return next(
                    (col for col in candidates if 'id' in col.lower()),
                    candidates[0])
        return 'id'

    def _build_update_exprs(
        self,
        table: str,
        update_cols_always: list[str] | None,
        update_cols_ifnull: list[str] | None,
    ) -> list[str]:
        """SET expressions for the conflict branch of an upsert.

        Parameters
        ----------
        table : str
            Target table, which qualifies the stored value.
        update_cols_always : list[str] or None
            Columns set to the new row's value.
        update_cols_ifnull : list[str] or None
            Columns set to the new row's value only where the stored value
            is null.

        Returns
        -------
        list[str]
            The always columns first, then the if-null columns.
        """
        quoted_table = self.quote_identifier(table)
        update_exprs = []

        for col in update_cols_always or ():
            quoted_col = self.quote_identifier(col)
            update_exprs.append(f'{quoted_col} = excluded.{quoted_col}')

        for col in update_cols_ifnull or ():
            quoted_col = self.quote_identifier(col)
            update_exprs.append(
                f'{quoted_col} = coalesce({quoted_table}.{quoted_col}, '
                f'excluded.{quoted_col})')

        return update_exprs
