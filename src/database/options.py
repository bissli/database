from dataclasses import dataclass

import pandas as pd
import pyarrow as pa

from libb import ConfigOptions, scriptname

__all__ = [
    'DatabaseOptions',
    'pandas_numpy_data_loader',
    'pandas_pyarrow_data_loader',
    'iterdict_data_loader',
]


def iterdict_data_loader(data, column_info) -> list[dict]:
    """Minimal data loader.
    """
    if not data:
        return []
    return list(data)


def pandas_numpy_data_loader(data, columns) -> pd.DataFrame:
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


def pandas_pyarrow_data_loader(data, columns) -> pd.DataFrame:
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

    supported driver names: `postgres`, `sqlserver`, `sqlite`

    """
    drivername: str = 'postgres'
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

    def __post_init__(self):
        assert self.drivername in {'postgres', 'sqlserver', 'sqlite'}, \
            'drivername must be `postgres`, `sqlserver`, or `sqlite`'
        self.appname = self.appname or scriptname() or 'python_console'
        if self.drivername in {'postgres', 'sqlserver'}:
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
