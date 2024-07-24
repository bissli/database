from dataclasses import dataclass, field

import pandas as pd
import pyarrow as pa

from libb import ConfigOptions, scriptname

__all__ = [
    'DatabaseOptions',
    'pandas_numpy_data_loader',
    'pandas_pyarrow_data_loader',
    'iterdict_data_loader',
]


def iterdict_data_loader(data, cols) -> list[dict]:
    """Minimal data loader.
    """
    return list(data)


def pandas_numpy_data_loader(data, cols) -> pd.DataFrame:
    return pd.DataFrame.from_records(list(data), columns=cols)


def pandas_pyarrow_data_loader(data, cols) -> pd.DataFrame:
    dataT = [[row[col] for row in data] for col in cols]  # list of cols
    return pa.table(dataT, names=cols).to_pandas(types_mapper=pd.ArrowDtype)


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
