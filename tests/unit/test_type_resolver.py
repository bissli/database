"""
Tests for resolve_type: the type-code maps, the supplied type_map path,
the column-name patterns, and the str fallback.
"""
import datetime

from database.types import _safe_array_oid, _safe_oid, postgres_types
from database.types import resolve_type, sqlite_types

# Notes:
# - OIDs are written as literals, taken from the PostgreSQL pg_type
#   catalog, so the oracle stays independent of psycopg's registry -
#   the same registry _build_postgres_types reads.
# - 99999 is above every catalog OID a stock server hands out.
BOOL_OID = 16
BYTEA_OID = 17
INT2VECTOR_OID = 22
INT4_OID = 23
TEXT_OID = 25
JSON_OID = 114
TIMESTAMP_OID = 1114
NUMERIC_OID = 1700
DATE_OID = 1082
TIME_OID = 1083
UUID_OID = 2950
JSONB_OID = 3802
INT2VECTOR_ARRAY_OID = 1006
INT4_ARRAY_OID = 1007
TEXT_ARRAY_OID = 1009
DATE_ARRAY_OID = 1182
JSONB_ARRAY_OID = 3807
UNKNOWN_OID = 99999


def test_resolve_type_walks_code_then_name_then_str():
    """Verify the ladder: type code first, then column name, then str.

    Mutation: returning str straight after a failed type-code lookup, so
        the column-name block never runs.
    Oracle: 'user_id' resolved against int4 OID -> int, unknown OID ->
        int by name, text OID -> str despite the name; sqlite MYSTERY ->
        int by name; sqlite None (real sqlite3 type_code) -> int by name.
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

    Mutation: moving the `if column_name:` block above the type-code
        lookup in resolve_type.
    Oracle: catalog OIDs 1082 (date) and 16 (bool) paired with names the
        pattern rules would resolve to datetime and float.
    """
    assert resolve_type('postgresql', DATE_OID, 'created_at') is datetime.date
    assert resolve_type('postgresql', BOOL_OID, 'total_price') is bool
    assert resolve_type('sqlite', 'BOOLEAN', 'total_price') is bool
    assert resolve_type('sqlite', 'TEXT', 'user_id') is str


def test_postgres_oid_map_pins_the_python_class():
    """Verify each core OID resolves to the exact class, not a near neighbor.

    Mutation: mapping 'bytea' to str, 'json' to str, or 'time' to
        datetime.datetime in _build_postgres_types.
    Oracle: hand-written class per catalog OID, each chosen to differ from
        the str fallback; the date/timestamp pair also pins that the two
        stay distinct classes rather than collapsing onto datetime.
    """
    oid_cases = [
        (BOOL_OID, bool),
        (BYTEA_OID, bytes),
        (INT4_OID, int),
        (TEXT_OID, str),
        (JSON_OID, dict),
        (JSONB_OID, dict),
        (DATE_OID, datetime.date),
        (TIME_OID, datetime.time),
        (TIMESTAMP_OID, datetime.datetime),
        (NUMERIC_OID, float),
        ]

    resolved = [resolve_type('postgresql', oid) for oid, _ in oid_cases]

    assert resolved == [expected for _, expected in oid_cases]
    assert resolve_type('postgresql', DATE_OID) is not datetime.datetime
    assert resolve_type('postgresql', UUID_OID) is str


def test_unknown_code_and_foreign_dialect_fall_back_to_str():
    """Verify the str fallback covers unknown codes and unknown dialects.

    Mutation: dropping the `db_type == 'postgresql'` guard, so any dialect
        reads postgres_types.
    Oracle: OID 23 is int4 - it must resolve to int under 'postgresql' and
        to str under 'mysql', where the map is not the library's to use.
    """
    assert resolve_type('postgresql', UNKNOWN_OID) is str
    assert resolve_type('sqlite', 'CUSTOM_TYPE') is str
    assert resolve_type('postgresql', INT4_OID) is int
    assert resolve_type('mysql', INT4_OID) is str
    assert resolve_type('mysql', 'INTEGER') is str
    assert resolve_type('mysql', INT4_OID, 'user_id') is int


def test_sqlite_type_names_pin_the_python_class():
    """Verify every declared SQLite type name resolves to its own class.

    Mutation: retyping 'BOOLEAN' as int or 'BLOB' as str in sqlite_types,
        either of which the storage classes would excuse.
    Oracle: hand-written class per name, plus a key-set comparison that
        catches a name silently added or dropped.
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

    Mutation: split('(')[-1] in place of split('(')[0], or dropping the
        .upper() call, in resolve_type's sqlite branch.
    Oracle: hand-computed 'NUMERIC(10,2)' -> 'NUMERIC' -> float; the
        lowercase spellings fail the moment .upper() goes.
    """
    assert resolve_type('sqlite', 'NUMERIC(10,2)') is float
    assert resolve_type('sqlite', 'integer') is int
    assert resolve_type('sqlite', 'datetime(6)') is datetime.datetime
    assert resolve_type('sqlite', 'Blob') is bytes
    assert resolve_type('sqlite', 'INT') is str


def test_supplied_type_map_replaces_the_builtin_map():
    """Verify type_map, once passed, is the only code map consulted.

    Mutation: `if type_map:` in place of `if type_map is not None:`, which
        sends an empty map back to the builtin dialect lookup.
    Oracle: OID 23 is int4 - int under the builtin map, so bytes and str
        prove which map answered.
    """
    assert resolve_type('postgresql', INT4_OID, type_map={INT4_OID: bytes}) is bytes
    assert resolve_type('postgresql', INT4_OID, type_map={}) is str
    assert resolve_type('sqlite', 'INTEGER', type_map={'TEXT': bytes}) is str
    assert resolve_type('sqlite', 'TEXT', type_map={'TEXT': bytes}) is bytes


def test_type_map_miss_still_falls_through_to_the_column_name():
    """Verify a type_map miss reaches the column-name patterns, not str.

    Mutation: returning str inside the `type_map is not None` branch when
        the lookup misses, or dropping `isinstance(type_code, str)` so a
        None type_code raises AttributeError on .split('(').
    Oracle: 'user_id' resolves to int by pattern; str would mean the name
        block was skipped; None type_code with a map must not raise.
    """
    assert resolve_type('sqlite', 'MYSTERY', 'user_id', type_map={'TEXT': str}) is int
    assert resolve_type('postgresql', UNKNOWN_OID, 'created_at', type_map={}) \
        is datetime.datetime
    assert resolve_type('sqlite', 'MYSTERY', 'note', type_map={'TEXT': str}) is str
    assert resolve_type('sqlite', None, 'note', type_map={'TEXT': str}) is str


def test_type_map_base_type_retry_is_sqlite_only():
    """Verify only the sqlite dialect retries a type_map lookup on the base name.

    Mutation: dropping the `db_type == 'sqlite'` guard on the base-type
        retry, so a PostgreSQL code is split on '(' as well.
    Oracle: the same 'NUMERIC(10,2)' code and the same map, resolving to
        float under sqlite and to the str fallback under postgresql.
    """
    type_map = {'NUMERIC': float}

    assert resolve_type('sqlite', 'NUMERIC(10,2)', type_map=type_map) is float
    assert resolve_type('sqlite', 'numeric(10,2)', type_map=type_map) is float
    assert resolve_type('postgresql', 'NUMERIC(10,2)', type_map=type_map) is str


def test_python_type_code_short_circuits_every_other_rule():
    """Verify a type passed as the code is returned before any lookup runs.

    Mutation: moving `if isinstance(type_code, type): return type_code`
        below the type_map lookup or below the column-name block.
    Oracle: each case pairs a type with a name or map entry that resolves
        to something else, so the returned class names the branch that ran.
    """
    assert resolve_type('postgresql', str, 'user_id') is str
    assert resolve_type('postgresql', datetime.date, 'is_active') is datetime.date
    assert resolve_type('sqlite', int, 'created_at', type_map={int: bytes}) is int
    assert resolve_type('sqlite', bool) is bool


def test_resolve_by_column_name():
    """Verify each column-name family resolves to its own Python type.

    Mutation: dropping any one pattern branch from resolve_type, or
        swapping the type two families return.
    Oracle: hand-computed type per name, one name per rule form - suffix,
        prefix, and whole-name equality; 'active' pins the whole-name
        bool set.
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
        ('amount_paid', float),
        ('price_usd', float),
        ('unknown_column', str),
        ]

    resolved = [resolve_type('postgresql', None, name) for name, _ in name_cases]

    assert resolved == [expected for _, expected in name_cases]


def test_column_name_patterns_reject_near_misses():
    """Verify the patterns anchor at a word edge instead of matching anywhere.

    Mutation: endswith('id') for endswith('_id'), `'is_' in name_lower` for
        startswith('is_'), startswith('cost') for startswith('cost_'),
        endswith('at') for endswith('_at'), endswith('flag') for
        endswith('_flag'), or endswith('cost') for endswith('_cost').
    Oracle: real column words that contain a pattern without meaning it -
        'format', 'flag', 'cost', 'valid', 'this_is_a_column', 'costume' -
        each must stay str.
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
        ]

    resolved = [resolve_type('postgresql', None, name) for name in near_misses]

    assert resolved == [str] * len(near_misses)


def test_float_branch_loses_to_id_and_timestamp_branches():
    """Verify the float rung yields to the id and timestamp rungs above it.

    Mutation: hoisting the float branch above the _id and timestamp
        branches in resolve_type.
    Oracle: 'price_id' matches both _id (int, line 372) and price_
        (float, line 389) - the _id branch is listed first and wins;
        'amount_at' matches both _at (datetime, line 375) and amount_
        (float, line 389) - the _at branch wins.
    """
    assert resolve_type('postgresql', None, 'price_id') is int
    assert resolve_type('postgresql', None, 'amount_at') is datetime.datetime


def test_column_name_patterns_are_case_folded():
    """Verify pattern matching lower-cases the name before testing it.

    Mutation: dropping `name_lower = column_name.lower()` and testing
        column_name directly.
    Oracle: the same names in upper and mixed case, each hand-computed to
        the type its lowercase spelling gives.
    """
    assert resolve_type('postgresql', None, 'USER_ID') is int
    assert resolve_type('postgresql', None, 'Created_At') is datetime.datetime
    assert resolve_type('postgresql', None, 'IS_ACTIVE') is bool
    assert resolve_type('postgresql', None, 'Total_Price') is float


def test_array_oids_resolve_to_tuple_not_the_element_type():
    """Verify an array OID resolves to tuple while its element OID does not.

    Mutation: `types[type_info.array_oid] = py_type` in place of tuple in
        _build_postgres_types, or dropping the array loop.
    Oracle: catalog array OIDs (_int4 1007, _text 1009, _date 1182,
        _jsonb 3807) paired with their element OIDs, whose scalar answers
        must survive.
    """
    array_pairs = [
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

    Mutation: _safe_oid('int2vector') in place of _safe_array_oid, which
        maps OID 22 to tuple.
    Oracle: catalog OIDs 22 (int2vector) and 1006 (_int2vector) - only the
        array form is in the map, so 22 falls through to str.
    """
    assert resolve_type('postgresql', INT2VECTOR_ARRAY_OID) is tuple
    assert resolve_type('postgresql', INT2VECTOR_OID) is str
    assert postgres_types.get(INT2VECTOR_ARRAY_OID) is tuple
    assert INT2VECTOR_OID not in postgres_types


def test_safe_oid_helpers_report_a_missing_type_as_none():
    """Verify the OID helpers return None rather than raising or crossing over.

    Mutation: dropping the `if type_info else None` guard (AttributeError
        on an unknown name), or returning type_info.oid from
        _safe_array_oid.
    Oracle: catalog OIDs for int4 - 23 for the type, 1007 for its array -
        and a type name no catalog carries.
    """
    assert _safe_oid('int4') == INT4_OID
    assert _safe_array_oid('int4') == INT4_ARRAY_OID
    assert _safe_oid('no_such_type_xyz') is None
    assert _safe_array_oid('no_such_type_xyz') is None
    assert None not in postgres_types


def test_cursor_metadata_kwargs_are_accepted_and_ignored():
    """Verify the extra kwargs Column passes neither raise nor change the type.

    Mutation: dropping `**_` from the resolve_type signature, which breaks
        Column.from_cursor_description's call with column_size, precision,
        and scale.
    Oracle: the same numeric OID resolved with and without the metadata -
        float either way, since scale=2 must not steer the answer.
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
