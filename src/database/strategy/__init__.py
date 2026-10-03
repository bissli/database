"""Strategy lookup by dialect name or by connection.
"""
from functools import lru_cache
from typing import Any

from database.exceptions import DatabaseError
from database.strategy.base import _STRATEGY_REGISTRY
from database.strategy.base import DatabaseStrategy as DatabaseStrategy
from database.strategy.base import register_strategy as register_strategy
from database.strategy.postgres import PostgresStrategy as PostgresStrategy
from database.strategy.sqlite import SQLiteStrategy as SQLiteStrategy
from database.utils import get_dialect_name


def _validate_dialect(dialect: str) -> None:
    """Raise DatabaseError, listing the registered dialects, for any other.
    """
    if dialect not in _STRATEGY_REGISTRY:
        available = list(_STRATEGY_REGISTRY.keys())
        raise DatabaseError(
            f'Unsupported dialect: {dialect}. Available: {available}')


@lru_cache(maxsize=8)
def _get_strategy(dialect: str) -> DatabaseStrategy:
    """The one shared strategy instance for a registered dialect.
    """
    _validate_dialect(dialect)
    return _STRATEGY_REGISTRY[dialect]()


def get_strategy(dialect: str) -> DatabaseStrategy:
    """Shared strategy instance for a dialect name.

    Parameters
    ----------
    dialect : str
        Registered dialect name, e.g. 'postgresql' or 'sqlite'.

    Returns
    -------
    DatabaseStrategy
        The same instance on every call for a dialect.

    Raises
    ------
    DatabaseError
        dialect is not registered.
    """
    return _get_strategy(dialect)


def get_db_strategy(cn: Any) -> DatabaseStrategy:
    """Shared strategy instance for the dialect of a connection.

    Parameters
    ----------
    cn : Any
        Any connection get_dialect_name accepts.

    Returns
    -------
    DatabaseStrategy
        The instance get_strategy returns for that dialect.

    Raises
    ------
    DatabaseError
        The connection's dialect is not registered.
    AttributeError
        get_dialect_name finds no dialect on cn.
    """
    dialect = get_dialect_name(cn)
    return _get_strategy(dialect)


def get_available_dialects() -> list[str]:
    """Registered dialect names, in registration order.
    """
    return list(_STRATEGY_REGISTRY.keys())


def is_supported_dialect(dialect: str) -> bool:
    """True when a strategy is registered under dialect.
    """
    return dialect in _STRATEGY_REGISTRY


def get_strategy_class(dialect: str) -> type[DatabaseStrategy]:
    """Strategy class registered under dialect, not instantiated.

    Raises
    ------
    DatabaseError
        dialect is not registered.
    """
    _validate_dialect(dialect)
    return _STRATEGY_REGISTRY[dialect]
