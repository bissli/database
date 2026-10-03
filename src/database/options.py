from collections.abc import Callable, Iterable
from dataclasses import dataclass
from functools import wraps
from typing import Any

import pandas as pd
from database.exceptions import ValidationError
from database.strategy import get_available_dialects, get_strategy_class
from database.strategy import is_supported_dialect
from database.types import Column

from libb import ConfigOptions, scriptname

__all__ = [
    'DatabaseOptions',
    'pandas_numpy_data_loader',
    'pandas_pyarrow_data_loader',
    'iterdict_data_loader',
    'use_iterdict_data_loader',
]


def use_iterdict_data_loader(func: Callable[..., Any]) -> Callable[..., Any]:
    """Wrap func to run with iterdict_data_loader as the connection's loader.

    Parameters
    ----------
    func : Callable[..., Any]
        Callable whose first argument is a connection, or a Transaction.

    Returns
    -------
    Callable[..., Any]
        Wrapper that swaps the loader for the call only.
    """

    @wraps(func)
    def inner(*args: Any, **kwargs: Any) -> Any:
        cn = args[0]

        if hasattr(cn, 'connection') and not hasattr(cn, 'options'):
            cn = cn.connection

        original_data_loader = cn.options.data_loader
        cn.options.data_loader = iterdict_data_loader

        try:
            return func(*args, **kwargs)
        finally:
            cn.options.data_loader = original_data_loader

    return inner


def iterdict_data_loader(data: Iterable[dict] | None, column_info: list[Column],
                         **kwargs: Any) -> list[dict]:
    """Rows as a new list of dicts, every key kept.

    Parameters
    ----------
    data : Iterable[dict] | None
        Result rows.
    column_info : list[Column]
        Unused.
    **kwargs : Any
        Unused.

    Returns
    -------
    list[dict]
        A new list, [] for None or empty input.
    """
    if not data:
        return []
    return list(data)


def _empty_dataframe(columns: list[Column], dtype: Any = None) -> pd.DataFrame:
    """Zero-row frame with the metadata's columns and attrs['column_types'].
    """
    df = pd.DataFrame(columns=Column.get_names(columns), dtype=dtype)
    df.attrs['column_types'] = Column.get_column_types_dict(columns)
    return df


def pandas_numpy_data_loader(data: Iterable[dict] | None, columns: list[Column],
                             **kwargs: Any) -> pd.DataFrame:
    """NumPy-backed DataFrame of the rows, in the metadata's column order.

    Parameters
    ----------
    data : Iterable[dict] | None
        Result rows. A key outside the metadata is dropped, and a missing
        key reads as null.
    columns : list[Column]
        Sets the frame's columns and their order.
    **kwargs : Any
        Unused.

    Returns
    -------
    pd.DataFrame
        Zero rows for empty input. attrs['column_types'] holds
        Column.get_column_types_dict(columns).
    """
    if not data:
        return _empty_dataframe(columns)

    df = pd.DataFrame.from_records(list(data), columns=Column.get_names(columns))
    df.attrs['column_types'] = Column.get_column_types_dict(columns)
    return df


def pandas_pyarrow_data_loader(data: Iterable[dict] | None, columns: list[Column],
                               **kwargs: Any) -> pd.DataFrame:
    """Arrow-backed DataFrame of the rows, in the metadata's column order.

    Parameters
    ----------
    data : Iterable[dict] | None
        Result rows. A key outside the metadata is dropped, and a missing
        key reads as null.
    columns : list[Column]
        Sets the frame's columns and their order.
    **kwargs : Any
        Unused.

    Returns
    -------
    pd.DataFrame
        Every column a pd.ArrowDtype, pa.null() for empty input.
        attrs['column_types'] holds Column.get_column_types_dict(columns).
    """
    # Deferred import: pyarrow is optional.
    import pyarrow as pa

    if not data:
        return _empty_dataframe(columns, dtype=pd.ArrowDtype(pa.null()))

    column_names = Column.get_names(columns)
    columns_data = [[row.get(col) for row in data] for col in column_names]
    arrow_table = pa.table(columns_data, names=column_names)
    df = arrow_table.to_pandas(types_mapper=pd.ArrowDtype)
    df.attrs['column_types'] = Column.get_column_types_dict(columns)
    return df


@dataclass
class DatabaseOptions(ConfigOptions):
    """Connection settings for one PostgreSQL or SQLite database.

    Parameters
    ----------
    drivername : str, default 'postgresql'
        'postgresql' or 'sqlite'.
    hostname : str, default None
        Required for PostgreSQL.
    username : str, default None
        Required for PostgreSQL.
    password : str, default None
        Required for PostgreSQL. repr shows '***'.
    database : str, default None
        Database name, or a SQLite file path or ':memory:'. Required.
    port : int, default 0
        Required for PostgreSQL, so 0 raises.
    timeout : int, default 0
        PostgreSQL connect timeout in seconds. Required, so 0 raises.
    appname : str, default None
        PostgreSQL application_name. None takes the script name, else
        'python_console'.
    data_loader : Callable[..., Any] | None, default None
        Called as data_loader(rows, columns, **kwargs) to shape a select
        result. None takes pandas_numpy_data_loader.
    reader_hostname : str, default None
        Host for `connect(..., role='reader')`. None falls back to
        hostname. SQLite ignores it.
    reader_port : int, default 0
        0 falls back to port, independent of reader_hostname. SQLite
        ignores it.
    use_pool : bool, default False
        False opens a fresh connection each time, except for SQLite
        ':memory:'.
    pool_max_connections : int, default 5
        SQLAlchemy pool_size. A hard ceiling on PostgreSQL.
    pool_max_idle_time : int, default 300
        Seconds before a pooled connection is replaced.
    pool_wait_timeout : int, default 30
        Seconds a caller waits for a pooled connection.
    journal_mode : str, default 'wal'
        SQLite only: 'wal', 'delete', 'truncate' or 'persist'.
    open_mode : str | None, default None
        SQLite only: None, 'ro' or 'immutable'. Requires
        `connect(..., role='reader')`. 'immutable' needs a file that
        never changes while a connection holds it.

    Raises
    ------
    ValidationError
        At construction, for an unregistered drivername, a missing
        required field, or a journal_mode or open_mode outside its set.
    """
    drivername: str = 'postgresql'
    hostname: str = None
    username: str = None
    password: str = None
    database: str = None
    port: int = 0
    timeout: int = 0
    appname: str = None
    data_loader: Callable[..., Any] | None = None
    reader_hostname: str = None
    reader_port: int = 0
    use_pool: bool = False
    pool_max_connections: int = 5
    pool_max_idle_time: int = 300
    pool_wait_timeout: int = 30
    journal_mode: str = 'wal'
    open_mode: str | None = None

    def __post_init__(self) -> None:
        """Validate the fields and fill appname and data_loader when unset.
        """
        if not is_supported_dialect(self.drivername):
            available = get_available_dialects()
            raise ValidationError(f'drivername must be one of: {available}')
        self.appname = self.appname or scriptname() or 'python_console'
        strategy_cls = get_strategy_class(self.drivername)
        strategy_cls.validate_options(self)
        if self.data_loader is None:
            self.data_loader = pandas_numpy_data_loader

    def __repr__(self) -> str:
        masked = None if self.password is None else '***'
        return (
            f'DatabaseOptions(drivername={self.drivername!r}, '
            f'hostname={self.hostname!r}, username={self.username!r}, '
            f'password={masked!r}, database={self.database!r}, '
            f'port={self.port!r}, appname={self.appname!r})'
        )
