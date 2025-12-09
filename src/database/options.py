from collections.abc import Callable
from dataclasses import dataclass
from functools import wraps
from typing import Any

import pandas as pd
import pyarrow as pa
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


def use_iterdict_data_loader(func):
    """Temporarily use default dict loader over user-specified loader"""

    @wraps(func)
    def inner(*args, **kwargs):
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


def iterdict_data_loader(data, column_info, **kwargs) -> list[dict]:
    """Minimal data loader.

    Accepts additional keyword arguments (like table_name) for compatibility
    with other data loaders, but doesn't use them.
    """
    if not data:
        return []
    return list(data)


def _empty_dataframe(columns) -> pd.DataFrame:
    """Create empty DataFrame with column metadata."""
    df = pd.DataFrame(columns=Column.get_names(columns))
    df.attrs['column_types'] = Column.get_column_types_dict(columns)
    return df


def pandas_numpy_data_loader(data, columns, **kwargs) -> pd.DataFrame:
    """Standard pandas DataFrame loader using NumPy.

    Always returns a DataFrame, never None, with columns preserved for empty results.
    Includes type information in the DataFrame.attrs attribute.
    """
    if not data:
        return _empty_dataframe(columns)

    df = pd.DataFrame.from_records(list(data), columns=Column.get_names(columns))
    df.attrs['column_types'] = Column.get_column_types_dict(columns)
    return df


def pandas_pyarrow_data_loader(data, columns, **kwargs) -> pd.DataFrame:
    """PyArrow-based pandas DataFrame loader.

    Always returns a DataFrame, never None, with columns preserved for empty results.
    """
    if not data:
        return _empty_dataframe(columns)

    column_names = Column.get_names(columns)
    columns_data = [[row[col] for row in data] for col in column_names]
    df = pa.table(columns_data, names=column_names).to_pandas(types_mapper=pd.ArrowDtype)
    df.attrs['column_types'] = Column.get_column_types_dict(columns)
    return df


@dataclass
class DatabaseOptions(ConfigOptions):
    """Options

    supported driver names: `postgresql`, `sqlite`

    Connection pooling options:
    - use_pool: Whether to use connection pooling (default: False)
    - pool_max_connections: Maximum connections in pool (default: 5)
    - pool_max_idle_time: Maximum seconds a connection can be idle (default: 300)
    - pool_wait_timeout: Maximum seconds to wait for a connection (default: 30)
    """
    drivername: str = 'postgresql'
    hostname: str = None
    username: str = None
    password: str = None
    database: str = None
    port: int = 0
    timeout: int = 0
    appname: str = None
    cleanup: bool = True
    check_connection: bool = True
    data_loader: Callable[..., Any] | None = None
    # Connection pooling parameters
    use_pool: bool = False
    pool_max_connections: int = 5
    pool_max_idle_time: int = 300
    pool_wait_timeout: int = 30

    def __post_init__(self):
        if not is_supported_dialect(self.drivername):
            available = get_available_dialects()
            raise ValueError(f'drivername must be one of: {available}')
        self.appname = self.appname or scriptname() or 'python_console'
        strategy_cls = get_strategy_class(self.drivername)
        strategy_cls.validate_options(self)
        if self.data_loader is None:
            self.data_loader = pandas_numpy_data_loader
