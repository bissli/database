from dataclasses import dataclass
from functools import wraps

import pandas as pd
import pyarrow as pa

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

        # For Transaction objects, get options from the connection
        if hasattr(cn, 'connection') and not hasattr(cn, 'options'):
            cn = cn.connection

        # Save original data loader and temporarily switch to iterdict_data_loader
        original_data_loader = cn.options.data_loader
        cn.options.data_loader = iterdict_data_loader

        try:
            return func(*args, **kwargs)
        finally:
            # Restore the original data loader
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


def pandas_numpy_data_loader(data, columns, **kwargs) -> pd.DataFrame:
    """
    Standard pandas DataFrame loader using NumPy.

    Always returns a DataFrame, never None, with columns preserved for empty results.
    Includes type information in the DataFrame.attrs attribute.
    """
    from database.adapters.column_info import Column

    if not data:
        df = pd.DataFrame(columns=Column.get_names(columns))
        # Save column type information in DataFrame attributes
        df.attrs['column_types'] = Column.get_column_types_dict(columns)
        return df

    df = pd.DataFrame.from_records(list(data), columns=Column.get_names(columns))

    # Save column type information in DataFrame attributes
    df.attrs['column_types'] = Column.get_column_types_dict(columns)

    return df


def pandas_pyarrow_data_loader(data, columns, **kwargs) -> pd.DataFrame:
    """PyArrow-based pandas DataFrame loader.

    Always returns a DataFrame, never None, with columns preserved for empty results.
    """
    from database.adapters.column_info import Column

    if not data:
        df = pd.DataFrame(columns=Column.get_names(columns))
        # Save column type information
        df.attrs['column_types'] = Column.get_column_types_dict(columns)
        return df

    column_names = Column.get_names(columns)
    dataT = [[row[col] for row in data] for col in column_names]  # list of cols
    df = pa.table(dataT, names=column_names).to_pandas(types_mapper=pd.ArrowDtype)

    # Save column type information
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
    data_loader: callable = None
    # Connection pooling parameters
    use_pool: bool = False
    pool_max_connections: int = 5
    pool_max_idle_time: int = 300
    pool_wait_timeout: int = 30

    def __post_init__(self):
        assert self.drivername in {'postgresql', 'sqlite'}, \
            'drivername must be `postgresql` or `sqlite`'
        self.appname = self.appname or scriptname() or 'python_console'
        if self.drivername == 'postgresql':
            for field in ('hostname', 'username', 'password', 'database',
                          'port', 'timeout'):
                assert getattr(self, field), f'field {field} cannot be None or 0'
        if self.drivername == 'sqlite':
            assert self.database, 'field database cannot be None'
        if self.data_loader is None:
            self.data_loader = pandas_numpy_data_loader


if __name__ == '__main__':
    options = DatabaseOptions(hostname='hostname', username='username',
                              password='password', database='database',
                              port=1234, timeout=30)
    print(options.data_loader([{'name': 'foo'}], ['name']))
    print(str(options))
