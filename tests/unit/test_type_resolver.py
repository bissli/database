"""Tests for resolve_type and the built-in type-code maps."""
import datetime
import decimal

import database.types
from database.types import _build_postgres_types, postgres_types
from database.types import resolve_type, sqlite_types
from psycopg.postgres import types as pg_types

# Literal pg_type OIDs keep the oracle independent of psycopg's
# registry, which _build_postgres_types reads.
BOOL_OID = 16
BOOL_ARRAY_OID = 1000
INT2VECTOR_OID = 22
INT4_OID = 23
TEXT_OID = 25
JSON_OID = 114
NUMERIC_OID = 1700
DATE_OID = 1082
JSONB_OID = 3802
INT2VECTOR_ARRAY_OID = 1006
INT4_ARRAY_OID = 1007
TEXT_ARRAY_OID = 1009
DATE_ARRAY_OID = 1182
JSONB_ARRAY_OID = 3807
UNKNOWN_OID = 99999


def test_resolve_type_walks_code_then_name_then_str():
    """Verify the ladder: type code first, then column name, then str.

    Mutation: returning str right after a failed type-code lookup.
    Oracle: hand-computed type for 'user_id' under each type code.
    """
    assert resolve_type('postgresql', INT4_OID, 'user_id') is int
    assert resolve_type('postgresql', UNKNOWN_OID, 'user_id') is int
    assert resolve_type('postgresql', TEXT_OID, 'user_id') is str
    assert resolve_type('postgresql', UNKNOWN_OID, 'nothing_matches') is str
    assert resolve_type('sqlite', 'REAL', 'user_id') is float
    assert resolve_type('sqlite', 'MYSTERY', 'user_id') is int
    assert resolve_type('sqlite', None, 'user_id') is int


def test_type_code_beats_a_conflicting_column_name():
    """Verify a known type code wins over a column name that disagrees.

    Mutation: the column-name block moved above the type-code lookup.
    Oracle: date and bool codes paired with names that disagree.
    """
    assert resolve_type('postgresql', DATE_OID, 'created_at') is datetime.date
    assert resolve_type('postgresql', BOOL_OID, 'total_price') is bool
    assert resolve_type('sqlite', 'BOOLEAN', 'total_price') is bool
    assert resolve_type('sqlite', 'TEXT', 'user_id') is str


def test_unknown_code_and_foreign_dialect_fall_back_to_str():
    """Verify the str fallback covers unknown codes and unknown dialects.

    Mutation: dropping the `db_type == 'postgresql'` guard.
    Oracle: OID 23 is int under 'postgresql' and str under 'mysql'.
    """
    assert resolve_type('postgresql', UNKNOWN_OID) is str
    assert resolve_type('sqlite', 'CUSTOM_TYPE') is str
    assert resolve_type('postgresql', INT4_OID) is int
    assert resolve_type('mysql', INT4_OID) is str
    assert resolve_type('mysql', 'INTEGER') is str
    assert resolve_type('mysql', INT4_OID, 'user_id') is int


def test_sqlite_type_names_pin_the_python_class():
    """Verify every declared SQLite type name resolves to its own class.

    Mutation: retyping 'BOOLEAN' as int or 'BLOB' as str.
    Oracle: hand-written class per name, and the full key set.
    """
    name_cases = [
        ('INTEGER', int),
        ('REAL', float),
        ('NUMERIC', float),
        ('TEXT', str),
        ('BLOB', bytes),
        ('BOOLEAN', bool),
        ('DATE', datetime.date),
        ('DATETIME', datetime.datetime),
        ('TIME', datetime.time),
        ]

    resolved = [resolve_type('sqlite', name) for name, _ in name_cases]

    assert resolved == [expected for _, expected in name_cases]
    assert set(sqlite_types) == {name for name, _ in name_cases}


def test_sqlite_code_is_case_folded_and_stripped_of_its_size():
    """Verify a declared type is upper-cased and cut at the first paren.

    Mutation: split('(')[-1] for split('(')[0], or dropping .upper().
    Oracle: 'NUMERIC(10,2)' -> float; INT and VARCHAR(255) stay str.
    """
    assert resolve_type('sqlite', 'NUMERIC(10,2)') is float
    assert resolve_type('sqlite', 'Numeric(10, 2)') is float
    assert resolve_type('sqlite', 'integer') is int
    assert resolve_type('sqlite', 'datetime(6)') is datetime.datetime
    assert resolve_type('sqlite', 'Blob') is bytes
    assert resolve_type('sqlite', 'INT') is str
    assert resolve_type('sqlite', 'VARCHAR(255)') is str


def test_supplied_type_map_replaces_the_builtin_map():
    """Verify type_map, once passed, is the only code map consulted.

    Mutation: `if type_map:` in place of `if type_map is not None:`.
    Oracle: int4 is int under the builtin map; bytes and str differ.
    """
    assert resolve_type('postgresql', INT4_OID, type_map={INT4_OID: bytes}) is bytes
    assert resolve_type('postgresql', INT4_OID, type_map={}) is str
    assert resolve_type('sqlite', 'INTEGER', type_map={'TEXT': bytes}) is str
    assert resolve_type('sqlite', 'TEXT', type_map={'TEXT': bytes}) is bytes


def test_type_map_miss_still_falls_through_to_the_column_name():
    """Verify a type_map miss reaches the column-name patterns, not str.

    Mutation: returning str on a type_map miss, or dropping the
        isinstance(type_code, str) guard.
    Oracle: 'user_id' resolves to int; a None code does not raise.
    """
    assert resolve_type('sqlite', 'MYSTERY', 'user_id', type_map={'TEXT': str}) is int
    assert resolve_type('postgresql', UNKNOWN_OID, 'created_at', type_map={}) \
        is datetime.datetime
    assert resolve_type('sqlite', 'MYSTERY', 'note', type_map={'TEXT': str}) is str
    assert resolve_type('sqlite', None, 'note', type_map={'TEXT': str}) is str


def test_type_map_base_type_retry_is_sqlite_only():
    """Verify only sqlite retries a type_map lookup on the base type name.

    Mutation: dropping the `db_type == 'sqlite'` guard on the retry.
    Oracle: one code and map: float under sqlite, str under postgresql.
    """
    type_map = {'NUMERIC': float}

    assert resolve_type('sqlite', 'NUMERIC(10,2)', type_map=type_map) is float
    assert resolve_type('sqlite', 'numeric(10,2)', type_map=type_map) is float
    assert resolve_type('postgresql', 'NUMERIC(10,2)', type_map=type_map) is str


def test_python_type_code_short_circuits_every_other_rule():
    """Verify a type passed as the code is returned before any lookup runs.

    Mutation: the isinstance(type_code, type) check moved below a lookup.
    Oracle: each type paired with a name or map that disagrees.
    """
    assert resolve_type('postgresql', decimal.Decimal, 'user_id') is decimal.Decimal
    assert resolve_type('postgresql', str, 'user_id') is str
    assert resolve_type('postgresql', datetime.date, 'is_active') is datetime.date
    assert resolve_type('sqlite', int, 'created_at', type_map={int: bytes}) is int
    assert resolve_type('sqlite', bool) is bool


def test_resolve_by_column_name():
    """Verify each column-name family resolves to its own Python type.

    Mutation: dropping a pattern branch, or swapping two families' types.
    Oracle: hand-computed type per name for each rule form.
    """
    name_cases = [
        ('user_id', int),
        ('id', int),
        ('created_at', datetime.datetime),
        ('event_datetime', datetime.datetime),
        ('updated_timestamp', datetime.datetime),
        ('timestamp', datetime.datetime),
        ('start_date', datetime.date),
        ('date', datetime.date),
        ('start_time', datetime.time),
        ('time', datetime.time),
        ('is_active', bool),
        ('active_flag', bool),
        ('active', bool),
        ('enabled', bool),
        ('disabled', bool),
        ('total_price', float),
        ('unit_cost', float),
        ('total_amount', float),
        ('amount_paid', float),
        ('price_usd', float),
        ('cost_center', float),
        ('unknown_column', str),
        ]

    resolved = [resolve_type('postgresql', None, name) for name, _ in name_cases]

    assert resolved == [expected for _, expected in name_cases]


def test_column_name_patterns_reject_near_misses():
    """Verify the patterns anchor at a word edge instead of matching anywhere.

    Mutation: a pattern losing its '_' anchor, e.g. endswith('id'), or a
        substring test in place of the exact name set.
    Oracle: real column words that hold a pattern, each str.
    """
    near_misses = [
        'valid',
        'paid',
        'uuid',
        'this_is_a_column',
        'disabled_reason',
        'flagged',
        'costume',
        'pricing',
        'amounts',
        'price',
        'datetime',
        'format',
        'flag',
        'cost',
        'costs',
        'update',
        'runtime',
        'inactive',
        'issue_count',
        ]

    resolved = [resolve_type('postgresql', None, name) for name in near_misses]

    assert resolved == [str] * len(near_misses)


def test_float_branch_loses_to_id_and_timestamp_branches():
    """Verify the float rung yields to the id and timestamp rungs above it.

    Mutation: the float branch hoisted above the _id and _at branches.
    Oracle: 'price_id' is int and 'amount_at' is datetime.
    """
    assert resolve_type('postgresql', None, 'price_id') is int
    assert resolve_type('postgresql', None, 'amount_at') is datetime.datetime


def test_column_name_patterns_are_case_folded():
    """Verify pattern matching lower-cases the name before testing it.

    Mutation: testing column_name without .lower().
    Oracle: upper- and mixed-case names, typed as their lower case.
    """
    assert resolve_type('postgresql', None, 'USER_ID') is int
    assert resolve_type('postgresql', None, 'Created_At') is datetime.datetime
    assert resolve_type('postgresql', None, 'IS_ACTIVE') is bool
    assert resolve_type('postgresql', None, 'Total_Price') is float


def test_array_oids_resolve_to_tuple_not_the_element_type():
    """Verify an array OID resolves to tuple while its element OID does not.

    Mutation: py_type for tuple on array OIDs, or no array loop.
    Oracle: catalog array OIDs and their element OIDs.
    """
    array_pairs = [
        (BOOL_ARRAY_OID, BOOL_OID, bool),
        (INT4_ARRAY_OID, INT4_OID, int),
        (TEXT_ARRAY_OID, TEXT_OID, str),
        (DATE_ARRAY_OID, DATE_OID, datetime.date),
        (JSONB_ARRAY_OID, JSONB_OID, dict),
        ]

    for array_oid, element_oid, element_type in array_pairs:
        assert resolve_type('postgresql', array_oid) is tuple
        assert resolve_type('postgresql', element_oid) is element_type


def test_int2vector_contributes_only_its_array_oid():
    """Verify int2vector is mapped through its array OID, not its own.

    Mutation: `.oid` in place of `.array_oid` on int2vector.
    Oracle: catalog OIDs 22 and 1006; only 1006 is mapped.
    """
    assert resolve_type('postgresql', INT2VECTOR_ARRAY_OID) is tuple
    assert resolve_type('postgresql', INT2VECTOR_OID) is str
    assert postgres_types.get(INT2VECTOR_ARRAY_OID) is tuple
    assert INT2VECTOR_OID not in postgres_types


def test_build_skips_a_type_name_psycopg_does_not_register(monkeypatch):
    """Verify a listed name missing from psycopg's registry is skipped.

    Mutation: dropping the `if type_info` guard, which raises.
    Oracle: a registry stub without jsonb; json stays mapped.
    """
    class RegistryWithoutJsonb:
        """psycopg's type registry with jsonb removed.
        """

        def get(self, key):
            if key in {'jsonb', JSONB_OID}:
                return None
            return pg_types.get(key)

    monkeypatch.setattr(database.types, 'pg_types', RegistryWithoutJsonb())

    built = _build_postgres_types()

    assert JSONB_OID not in built
    assert JSONB_ARRAY_OID not in built
    assert built[JSON_OID] is dict
    assert built[INT2VECTOR_ARRAY_OID] is tuple


def test_cursor_metadata_kwargs_are_accepted_and_ignored():
    """Verify the extra kwargs Column passes neither raise nor change the type.

    Mutation: dropping `**_` from the resolve_type signature.
    Oracle: the numeric OID is float with and without the metadata.
    """
    with_metadata = resolve_type(
        'postgresql',
        NUMERIC_OID,
        column_name='total_amount',
        table_name='ledger',
        column_size=11,
        precision=10,
        scale=2)

    assert with_metadata is resolve_type('postgresql', NUMERIC_OID)
    assert with_metadata is float
    assert resolve_type('postgresql', UNKNOWN_OID, 'user_id', table_name='x') is int


if __name__ == '__main__':
    __import__('pytest').main([__file__])
