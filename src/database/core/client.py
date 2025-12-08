"""
Testing notes:

Prefer supplying a fake client when unit-testing business logic so the
test suite remains fast and deterministic.

**Simplest approach – hand-rolled stub**

    class FakeDB:
        def select(self, *_, **__):
            return [{'id': 1, 'name': 'stub'}]

        # OR

        def select(self, sql, *_, **__):
            if 'foobar' in sql:
                return return [{'id': 1, 'name': 'stub'}]
            if 'barbaz' in sql:
                return return [{'id': 2, 'name': 'stub'}]

    service_under_test(db=FakeDB())

**Recording fake – inherit from DBClient**

    class RecordingDB(DBClient):
        def __init__(self):
            # Skip real connection
            self._cn = None
            self.recorded_sql = []

        def execute(self, sql: str, *args, **kw):
            self.recorded_sql.append(sql)
            return []  # or raise if unexpected
"""
import logging
from collections.abc import Callable
from functools import wraps
from typing import Any

from database.core.connection import connect
from database.options import DatabaseOptions

from libb import load_options

logger = logging.getLogger(__name__)


def _bind(op_name: str) -> Callable[..., Any]:
    """Create a method that forwards to the real database function.

    Parameters
        op_name: The database operation name in the `database` package

    Returns
        A bound method that supplies the stored connection as first argument
    """
    op = getattr(_database, op_name)
    if not callable(op):
        raise TypeError(f'{op_name!r} is not callable in the database package')

    @wraps(op)
    def _method(self, *args, **kwargs):
        return op(self._cn, *args, **kwargs)

    return _method


class DBClient:
    """Light-weight facade around a database connection.

    Provides a single object that
    - Stores the real connection internally (``_cn``)
    - Exposes all high-level database verbs as instance methods
    - Delegates unknown attributes to the wrapped connection
    """

    __slots__ = ('_cn',)

    __CLIENT_METHODS = tuple(
        name
        for name in __all__
        if name not in {'connect', 'client', 'DBClient', 'insert_dataset', 'insert_identity'}
        and callable(getattr(_database, name, None))
    )

    for _name in __CLIENT_METHODS:
        locals()[_name] = _bind(_name)
    del _name

    del __CLIENT_METHODS

    def __init__(self, cn) -> None:
        """Wrap an existing database connection.

        Parameters
            cn: A live connection returned by `tc.db.connect`
        """
        self._cn = cn

    def __getattr__(self, item) -> Any:
        """Delegate unknown attributes to the wrapped connection."""
        try:
            return getattr(self._cn, item)
        except AttributeError as exc:
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{item}'")

    def __enter__(self):
        if hasattr(self._cn, '__enter__'):
            self._cn.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        if hasattr(self._cn, '__exit__'):
            return self._cn.__exit__(exc_type, exc, tb)
        return False

    @property
    def connection(self) -> Any:
        """Expose the underlying connection object"""
        return self._cn


@load_options(cls=DatabaseOptions)
def client(
    options: DatabaseOptions | dict[str, Any] | str,
    config: Any | None = None,
    **kw: Any
) -> DBClient:
    """Return a fully initialised DBClient.

    Returns
        DBClient instance ready for use

    """
    return DBClient(connect(options, config, **kw))


if __name__ == '__main__':
    __import__('doctest').testmod(optionflags=4 | 8 | 32)
