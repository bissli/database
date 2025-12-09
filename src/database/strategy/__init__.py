"""
Database strategy factory for database-specific operations.
"""
from functools import lru_cache

from database.strategy.base import _STRATEGY_REGISTRY
from database.strategy.base import DatabaseStrategy as DatabaseStrategy
from database.strategy.base import register_strategy as register_strategy
from database.strategy.postgres import PostgresStrategy as PostgresStrategy
from database.strategy.sqlite import SQLiteStrategy as SQLiteStrategy
from database.utils import get_dialect_name


def _validate_dialect(dialect: str) -> None:
    """Raise ValueError if dialect is not registered."""
    if dialect not in _STRATEGY_REGISTRY:
        available = list(_STRATEGY_REGISTRY.keys())
        raise ValueError(f'Unsupported dialect: {dialect}. Available: {available}')


@lru_cache(maxsize=8)
def _get_strategy(dialect: str) -> DatabaseStrategy:
    """Get cached strategy instance for a dialect."""
    _validate_dialect(dialect)
    return _STRATEGY_REGISTRY[dialect]()


def get_strategy(dialect: str) -> DatabaseStrategy:
    """Get strategy instance for a dialect name.

    This is the public interface for getting a strategy when you have a dialect
    name string but not a connection object.
    """
    return _get_strategy(dialect)


def get_db_strategy(cn) -> DatabaseStrategy:
    """Get database strategy for the connection."""
    dialect = get_dialect_name(cn)
    return _get_strategy(dialect)


def get_available_dialects() -> list[str]:
    """Return list of registered dialect names."""
    return list(_STRATEGY_REGISTRY.keys())


def is_supported_dialect(dialect: str) -> bool:
    """Check if a dialect is supported."""
    return dialect in _STRATEGY_REGISTRY


def get_strategy_class(dialect: str) -> type['DatabaseStrategy']:
    """Get the strategy class for a dialect without instantiating."""
    _validate_dialect(dialect)
    return _STRATEGY_REGISTRY[dialect]
