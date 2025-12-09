"""Row factory implementations for dictionary-like cursor results."""
from numbers import Number
from typing import Any

from database.types import postgres_types


class DictRowFactory:
    """Row factory for psycopg that returns dictionary-like rows.

    This factory is used with PostgreSQL connections to convert cursor results
    into dictionaries, with automatic type casting for numeric values based
    on PostgreSQL type codes.
    """

    def __init__(self, cursor: Any) -> None:
        """Initialize with cursor to extract column metadata.

        Args:
            cursor: Database cursor with description attribute
        """
        self.fields = [
            (c.name, postgres_types.get(c.type_code))
            for c in (cursor.description or [])
        ]

    def __call__(self, values: tuple) -> dict:
        """Convert a row tuple to a dictionary.

        Args:
            values: Tuple of column values from cursor

        Returns
            Dictionary mapping column names to values
        """
        return {
            name: cast(value) if isinstance(value, Number) and cast is not None else value
            for (name, cast), value in zip(self.fields, values)
        }
