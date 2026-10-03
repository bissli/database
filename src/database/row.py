"""psycopg row factory returning dict rows."""
from numbers import Number
from typing import Any

from database.types import postgres_types


class DictRowFactory:
    """psycopg row factory that returns each row as a dict.

    Parameters
    ----------
    cursor : Any
        A psycopg cursor. A None description yields empty rows.
    """

    def __init__(self, cursor: Any) -> None:
        self.fields = [
            (c.name, postgres_types.get(c.type_code))
            for c in (cursor.description or [])
        ]

    def __call__(self, values: tuple) -> dict:
        """Row dict keyed by column name, in cursor order.

        Parameters
        ----------
        values : tuple
            Column values in cursor order.

        Returns
        -------
        dict
            A Number cast to its column's postgres_types entry, so a
            numeric Decimal arrives as float. Other values, None included,
            and columns with no entry pass through uncast.
        """
        return {
            name: cast(value)
            if isinstance(value, Number) and cast is not None else value
            for (name, cast), value in zip(self.fields, values)
        }
