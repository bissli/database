"""
Tests for the type mapping layer.

Covers TypeConverter inbound parameter conversion and the PostgreSQL
and SQLite type resolution maps.
"""
import datetime
import decimal
import math
import time

import numpy as np
import pandas as pd
import psycopg
import pyarrow as pa
import pytest
from database.types import TypeConverter, postgres_types, resolve_type
from psycopg.adapt import Transformer
from psycopg.pq import Format


def get_pg_oid(type_name):
    """Return the OID psycopg registers for a PostgreSQL type name."""
    return psycopg.postgres.types.get(type_name).oid


def get_pg_array_oid(type_name):
    """Return the array OID psycopg registers for a type name."""
    return psycopg.postgres.types.get(type_name).array_oid


@pytest.fixture
def eastern_timezone(monkeypatch):
    """Run the test body under a local timezone that is not UTC."""
    if not hasattr(time, 'tzset'):
        pytest.fail('time.tzset unavailable on this platform')
    monkeypatch.setenv('TZ', 'America/New_York')
    time.tzset()
    if datetime.datetime.fromtimestamp(0) == datetime.datetime.utcfromtimestamp(0):
        pytest.fail('tz database missing - the UTC oracle is disarmed')
    yield
    monkeypatch.undo()
    time.tzset()


class TestPostgresTypeMapping:
    """Tests for PostgreSQL type resolution"""

    @pytest.mark.parametrize(
        ('pg_name', 'expected_type'),
        [
            ('"char"', str),
            ('bpchar', str),
            ('character varying', str),
            ('character', str),
            ('name', str),
            ('text', str),
            ('uuid', str),
            ('varchar', str),
            ('bigint', int),
            ('int2', int),
            ('int4', int),
            ('int8', int),
            ('integer', int),
            ('float4', float),
            ('float8', float),
            ('double precision', float),
            ('numeric', float),
            ('date', datetime.date),
            ('time', datetime.time),
            ('time with time zone', datetime.time),
            ('time without time zone', datetime.time),
            ('timetz', datetime.time),
            ('timestamp', datetime.datetime),
            ('timestamp with time zone', datetime.datetime),
            ('timestamp without time zone', datetime.datetime),
            ('timestamptz', datetime.datetime),
            ('bool', bool),
            ('boolean', bool),
            ('bytea', bytes),
            ('json', dict),
            ('jsonb', dict),
            ])
    def test_every_declared_pg_name_resolves(self, pg_name, expected_type):
        """Verify each name declared in the map resolves to its type.

        Mutation: a name dropped or misspelled in the `type_mappings`
            table of `_build_postgres_types`, for a name that is the
            sole holder of its OID (numeric, name, '"char"', uuid, bytea,
            json, jsonb, date, int2, float4); the ten alias pairs each
            share an OID and pin only the alternate spelling.
        Oracle: psycopg's own type registry supplies the OID; the
            expected Python type is hand-written per name.
        """
        assert resolve_type('postgresql', get_pg_oid(pg_name)) is expected_type

    @pytest.mark.parametrize(
        ('pg_name', 'wire_value'),
        [
            ('date', b'2024-01-15'),
            ('time', b'17:00:00'),
            ('timetz', b'17:00:00+00'),
            ('timestamp', b'2024-01-15 17:00:00'),
            ('timestamptz', b'2024-01-15 17:00:00+00'),
            ])
    def test_temporal_type_matches_what_psycopg_loads(self, pg_name, wire_value):
        """Verify a temporal OID resolves to the class psycopg loads it as.

        Mutation: mapping time or timetz to datetime.datetime, the way
            the SQL spelling suggests.
        Oracle: psycopg's own registered loader for the same OID, which
            decides the class a cursor actually hands back.
        """
        oid = get_pg_oid(pg_name)
        loaded = Transformer().get_loader(oid, Format.TEXT).load(wire_value)
        assert resolve_type('postgresql', oid) is type(loaded)

    def test_numeric_and_uuid_coerce_past_the_psycopg_loader(self):
        """Verify numeric maps to float and uuid to str, diverging from psycopg's loaders.

        Mutation: aligning either entry with psycopg's loader - numeric
            to Decimal, uuid to UUID - which the map's own Notes invite.
        Oracle: psycopg's registered loaders, which return Decimal and
            UUID, so the divergence is visible rather than assumed.
        """
        numeric_oid = get_pg_oid('numeric')
        uuid_oid = get_pg_oid('uuid')
        numeric_loaded = Transformer().get_loader(numeric_oid, Format.TEXT).load(b'1.5')
        uuid_wire = b'12345678-1234-5678-1234-567812345678'
        uuid_loaded = Transformer().get_loader(uuid_oid, Format.TEXT).load(uuid_wire)

        assert type(numeric_loaded) is decimal.Decimal
        assert type(uuid_loaded) is not str
        assert resolve_type('postgresql', numeric_oid) is float
        assert resolve_type('postgresql', uuid_oid) is str

    @pytest.mark.parametrize(
        ('pg_name', 'array_oid'),
        [
            ('int4', 1007),
            ('text', 1009),
            ('bool', 1000),
            ('timestamptz', 1185),
            ('int2vector', 1006),
            ])
    def test_array_oids_resolve_to_tuple(self, pg_name, array_oid):
        """Verify the array OID of a mapped type resolves to tuple.

        Mutation: dropping the array pass in `_build_postgres_types`,
            or the explicit `int2vector` array entry, which leaves array
            OIDs unmapped and falling through to str.
        Oracle: array OIDs hand-written from pg_type.h, cross-checked
            against psycopg's registry.
        """
        assert get_pg_array_oid(pg_name) == array_oid
        assert resolve_type('postgresql', array_oid) is tuple

    def test_scalar_and_array_oids_stay_distinct(self):
        """Verify an element OID keeps its own Python type.

        Mutation: `_safe_oid` returning `array_oid`, or the array pass
            overwriting `types[type_info.oid]` instead of the array OID,
            which would type every int4 column as tuple.
        Oracle: hand-written pair - int4 OID 23 is int, int4[] OID 1007
            is tuple.
        """
        assert postgres_types[23] is int
        assert postgres_types[1007] is tuple


class TestSqliteTypeMapping:
    """Tests for SQLite type resolution"""

    @pytest.mark.parametrize(
        ('sqlite_type', 'expected_type'),
        [
            ('INTEGER', int),
            ('REAL', float),
            ('TEXT', str),
            ('BLOB', bytes),
            ('NUMERIC', float),
            ('BOOLEAN', bool),
            ('DATE', datetime.date),
            ('DATETIME', datetime.datetime),
            ('TIME', datetime.time),
            ])
    def test_sqlite_type_resolution(self, sqlite_type, expected_type):
        """Verify each SQLite declared type resolves to its Python type.

        Mutation: an entry dropped from `sqlite_types`, or NUMERIC
            mapped to int, or BOOLEAN mapped to int.
        Oracle: hand-written expected type per SQLite storage class.
        """
        assert resolve_type('sqlite', sqlite_type) is expected_type

    @pytest.mark.parametrize(
        ('declared_type', 'expected_type'),
        [
            ('integer', int),
            ('Numeric(10, 2)', float),
            ('datetime', datetime.datetime),
            ('varchar(255)', str),
            ])
    def test_sqlite_lookup_upcases_and_strips_parameters(
        self, declared_type,
        expected_type):
        """Verify a declared type is upcased and its parameters dropped.

        Mutation: dropping `.upper()` or the `split('(')[0]` on the
            SQLite base type, either of which sends a lowercase or
            parameterized declaration to the str fallback.
        Oracle: hand-written declarations in the spellings sqlite3
            reports, paired with their expected types; varchar(255) is
            the near miss that has no entry either way.
        """
        assert resolve_type('sqlite', declared_type) is expected_type


class TestTypeMapParameter:
    """Tests for the caller-supplied type_map argument"""

    def test_supplied_type_map_replaces_the_dialect_map(self):
        """Verify type_map wins and suppresses the dialect fallback.

        Mutation: consulting `postgres_types` when the supplied map
            misses, or checking the dialect map before the supplied one.
        Oracle: OID 23 is int in `postgres_types`, so bytes proves the
            supplied map won and str proves no fallback ran.
        """
        assert resolve_type('postgresql', 23, type_map={23: bytes}) is bytes
        assert resolve_type('postgresql', 23, type_map={}) is str

    def test_base_type_split_applies_only_to_sqlite(self):
        """Verify the parameterized-type split is gated on the dialect.

        Mutation: dropping the `db_type == 'sqlite'` guard on the base
            type split, which would strip parameters for every dialect.
        Oracle: the same type_map and type code under two dialects -
            int for sqlite, str for postgresql.
        """
        type_map = {'INTEGER': int}
        assert resolve_type('sqlite', 'integer(10)', type_map=type_map) is int
        assert resolve_type('postgresql', 'INTEGER(10)', type_map=type_map) is str

    def test_type_map_miss_falls_through_to_column_name(self):
        """Verify a type_map miss still reaches the name patterns.

        Mutation: returning str as soon as a supplied type_map misses,
            skipping the column name rules.
        Oracle: hand-written pair - user_id is int, note is str.
        """
        assert resolve_type(
            'postgresql', 99999, type_map={},
            column_name='user_id') is int
        assert resolve_type(
            'postgresql', 99999, type_map={},
            column_name='note') is str


class TestColumnNamePatterns:
    """Tests for column name-based type resolution"""

    @pytest.mark.parametrize(
        ('dialect', 'column_name', 'expected_type'),
        [
            ('postgresql', 'user_id', int),
            ('sqlite', 'id', int),
            ('postgresql', 'created_at', datetime.datetime),
            ('postgresql', 'timestamp', datetime.datetime),
            ('sqlite', 'event_timestamp', datetime.datetime),
            ('postgresql', 'birth_date', datetime.date),
            ('sqlite', 'date', datetime.date),
            ('postgresql', 'start_time', datetime.time),
            ('postgresql', 'is_active', bool),
            ('sqlite', 'enabled_flag', bool),
            ('postgresql', 'disabled', bool),
            ('postgresql', 'total_amount', float),
            ('sqlite', 'unit_price', float),
            ('postgresql', 'cost_center', float),
            ('postgresql', 'unit_cost', float),
            ('sqlite', 'price_usd', float),
            ('postgresql', 'amount_paid', float),
            ('sqlite', 'time', datetime.time),
            ('postgresql', 'active', bool),
            ('sqlite', 'enabled', bool),
            ('postgresql', 'USER_ID', int),
            ('postgresql', 'description', str),
            ])
    def test_column_name_type_resolution(
        self, dialect, column_name,
        expected_type):
        """Verify each column name pattern resolves to its Python type.

        Mutation: a pattern dropped from the name ladder, or a suffix
            swapped between rungs (`_amount` typed as int, `_flag` as
            str), or the `cost_` prefix rung removed, or the lower()
            call dropped so mixed-case names fall to str.
        Oracle: hand-written expected type per name, with description as
            the unmatched control that must stay str.
        """
        result = resolve_type(dialect, None, column_name=column_name)
        assert result is expected_type

    @pytest.mark.parametrize('column_name', [
        'valid',
        'paid',
        'update',
        'runtime',
        'inactive',
        'issue_count',
        'costs',
        ])
    def test_patterns_require_their_separator(self, column_name):
        """Verify a name that only contains a pattern is not matched.

        Mutation: loosening a rung - `endswith('id')`, `endswith('date')`,
            `endswith('time')`, `startswith('is')`, or a substring test
            in place of the exact `{'active', 'enabled', ...}` set.
        Oracle: near-miss names hand-picked one character off each rung;
            every one must fall through to str.
        """
        assert resolve_type('postgresql', None, column_name=column_name) is str

    def test_datetime_suffix_resolves_to_datetime(self):
        """Verify a '_datetime' column name resolves to datetime, not time.

        Mutation: dropping '_datetime' from the datetime suffix tuple in
            resolve_type, which sends updated_datetime past the datetime
            rung to the str fallback.
        Oracle: updated_datetime resolves to datetime while updated_time
            resolves to time, keeping the two suffixes distinct.
        """
        assert resolve_type(
            'postgresql', None,
            column_name='updated_datetime') is datetime.datetime
        assert resolve_type(
            'postgresql', None,
            column_name='updated_time') is datetime.time

    def test_type_code_beats_column_name(self):
        """Verify a known type code outranks the column name patterns.

        Mutation: running the column name ladder before the type code
            lookup, which would type a text `user_id` column as int.
        Oracle: hand-written triple - a text OID under an id name stays
            str, while the same name under an unknown code becomes int.
        """
        assert resolve_type('postgresql', get_pg_oid('text'),
                            column_name='user_id') is str
        assert resolve_type('sqlite', 'TEXT', column_name='created_at') is str
        assert resolve_type('postgresql', 99999, column_name='user_id') is int


class TestUnknownTypes:
    """Tests for unknown type handling"""

    @pytest.mark.parametrize(('dialect', 'type_code'), [
        ('postgresql', 99999),
        ('sqlite', 'UNKNOWN'),
        ('sqlite', 'VARCHAR(255)'),
        ('unknown_db', 'type'),
        ('unknown_db', 23),
        ])
    def test_unknown_types_default_to_str(self, dialect, type_code):
        """Verify an unrecognized type code falls back to str.

        Mutation: dropping the `db_type` guard so any dialect consults
            `postgres_types`, which would type OID 23 as int under
            unknown_db; or a fallback that returns None instead of str.
        Oracle: hand-written str contract, with OID 23 under the wrong
            dialect as the case that separates the guard from the map.
        """
        assert resolve_type(dialect, type_code) is str


class TestPythonTypePassthrough:
    """Tests for Python type passthrough behavior"""

    @pytest.mark.parametrize(('dialect', 'python_type'), [
        ('postgresql', decimal.Decimal),
        ('sqlite', bool),
        ('postgresql', datetime.datetime),
        ])
    def test_python_type_returns_unchanged(self, dialect, python_type):
        """Verify a type code that is already a type is returned as is.

        Mutation: moving the `isinstance(type_code, type)` check below
            the column name ladder, or narrowing it to types the maps
            know, which would drop Decimal.
        Oracle: a column name that would otherwise force int, so only a
            genuine passthrough returns the type handed in.
        """
        assert resolve_type(
            dialect, python_type,
            column_name='user_id') is python_type


class TestTypeConverterScalars:
    """Tests for TypeConverter.convert_value"""

    def test_non_finite_floats_become_null(self):
        """Verify NaN and both infinities convert to None.

        Mutation: dropping `math.isinf(value)` from the float branch of
            convert_value, which sends inf to a numeric column.
        Oracle: hand-picked pair straddling the finite boundary - the
            largest float64 survives, inf does not.
        """
        assert TypeConverter.convert_value(math.nan) is None
        assert TypeConverter.convert_value(math.inf) is None
        assert TypeConverter.convert_value(-math.inf) is None
        largest_float = 1.7976931348623157e308
        assert TypeConverter.convert_value(largest_float) == largest_float
        assert TypeConverter.convert_value(0.0) == 0.0

    @pytest.mark.parametrize('value', [
        '',
        'null',
        'NULL',
        'Null',
        'nan',
        'NaN',
        'none',
        'None',
        'na',
        'NA',
        'nat',
        'NaT',
        ])
    def test_special_null_strings_are_case_insensitive(self, value):
        """Verify every spelling of a null-ish string becomes None.

        Mutation: dropping `.lower()` in `_check_special_string`, which
            lets 'NULL' reach the column as text, or removing a member
            of SPECIAL_STRINGS.
        Oracle: hand-written spellings, each in a case the source never
            stores literally.
        """
        assert TypeConverter.convert_value(value) is None

    @pytest.mark.parametrize('value', [
        ' ',
        'n/a',
        'nulls',
        'nan ',
        'null_value',
        '0',
        ])
    def test_strings_that_only_resemble_null_survive(self, value):
        """Verify a near-miss string is passed through untouched.

        Mutation: `.strip()` added before the SPECIAL_STRINGS lookup, or
            a `startswith` / substring test in place of set membership,
            either of which would blank real column data.
        Oracle: hand-picked near misses one character off a member.
        """
        assert TypeConverter.convert_value(value) == value

    def test_pandas_nat_is_not_mistaken_for_a_datetime(self):
        """Verify pd.NaT converts to None despite subclassing datetime.

        Mutation: `isinstance(value, datetime.datetime)` in place of the
            exact `value_type is datetime.datetime` hot-path check,
            which returns pd.NaT before the NaT guard can run.
        Oracle: pd.NaT's own class tree - it is a datetime.datetime
            subclass, so only the exact type check keeps it out.
        """
        assert isinstance(pd.NaT, datetime.datetime)
        assert TypeConverter.convert_value(pd.NaT) is None
        assert TypeConverter.convert_value(np.datetime64('NaT')) is None

    def test_missing_scalars_become_null_and_present_ones_do_not(self):
        """Verify the pandas isna branch nulls only missing values.

        Mutation: `or` in place of `and` in the
            `is_scalar(value) and pd.isna(value)` guard, which nulls
            every scalar the hot path did not already return.
        Oracle: a pair through the same branch - Decimal('NaN') is None,
            Decimal('1.5') is unchanged.
        """
        assert TypeConverter.convert_value(pd.NA) is None
        present = decimal.Decimal('1.5')
        assert TypeConverter.convert_value(decimal.Decimal('NaN')) is None
        assert TypeConverter.convert_value(present) == present

    def test_numpy_scalars_become_python_builtins(self):
        """Verify numpy scalars are unwrapped, not merely returned.

        Mutation: `return val` in place of `return val.item()` in
            `_convert_numpy_value`, which hands the driver a numpy
            scalar that equals the right number but is not an int.
        Oracle: exact type identity plus the hand-written uint64
            maximum, which no Python int narrowing could survive.
        """
        converted_int = TypeConverter.convert_value(np.int64(42))
        converted_uint = TypeConverter.convert_value(np.uint64(18446744073709551615))
        converted_float = TypeConverter.convert_value(np.float32(0.5))

        assert type(converted_int) is int
        assert converted_int == 42
        assert type(converted_uint) is int
        assert converted_uint == 18446744073709551615
        assert type(converted_float) is float
        assert converted_float == 0.5

    def test_non_finite_float32_nulls_like_a_builtin_float(self):
        """Verify np.float32 infinity nulls the way float infinity does.

        Mutation: dropping `or np.isinf(val)` from the np.floating guard
            in `_convert_numpy_value`, which lets a float32 infinity
            reach the driver while float and np.float64 both become
            None.
        Oracle: the same infinity as a builtin float and as np.float64,
            both nulled by convert_value's earlier isinf branch, plus
            the hand-written float32 maximum as the boundary that must
            survive.
        """
        # np.float32 does not subclass float, so it misses the isinf
        # branch that catches float and np.float64.
        assert not isinstance(np.float32(1.0), float)
        assert TypeConverter.convert_value(float('inf')) is None
        assert TypeConverter.convert_value(np.float64('inf')) is None

        assert TypeConverter.convert_value(np.float32('inf')) is None
        assert TypeConverter.convert_value(np.float32('-inf')) is None
        assert TypeConverter.convert_value(np.float32('nan')) is None

        converted_max = TypeConverter.convert_value(np.float32(3.4028235e38))
        assert type(converted_max) is float
        assert converted_max == 3.4028234663852886e38

    def test_numpy_bool_unboxes_to_a_builtin_bool(self):
        """Verify np.bool_ reaches the driver as a builtin bool.

        Mutation: dropping NUMPY_BOOL_TYPES from the isinstance gate in
            convert_value, or np.bool_ from the unboxing isinstance in
            `_convert_numpy_value`; either hands back np.True_.
        Oracle: exact type identity against bool - np.True_ equals True
            and is not a bool subclass, so equality alone cannot tell
            an unboxed value from a numpy one.
        """
        builtin_true = True
        assert np.True_ == builtin_true
        assert not isinstance(np.True_, bool)

        converted_true = TypeConverter.convert_value(np.True_)
        converted_false = TypeConverter.convert_value(np.bool_(False))

        assert type(converted_true) is bool
        assert converted_true is True
        assert type(converted_false) is bool
        assert converted_false is False

    def test_pyarrow_scalars_unwrap_and_null_out(self):
        """Verify a PyArrow scalar is unwrapped and still null-checked.

        Mutation: dropping `_normalize_special_string` around the
            `as_py()` result in `_convert_pyarrow_value` (a PyArrow
            'null' string reaches the column as text), or dropping the
            `pa.Scalar` clause in the second pyarrow branch of
            convert_value (BooleanScalar and Date32Scalar bypass
            conversion entirely).
        Oracle: the plain-Python conversion of the same values - 'null'
            is None, 'kept' survives, 1.5 is a float, True is a bool,
            and a date stays a date with exact type identity.
        """
        assert TypeConverter.convert_value(pa.scalar('null')) is None
        assert TypeConverter.convert_value(pa.scalar('kept')) == 'kept'
        assert TypeConverter.convert_value(pa.scalar(None, type=pa.float64())) is None

        unwrapped = TypeConverter.convert_value(pa.scalar(1.5))
        assert type(unwrapped) is float
        assert unwrapped == 1.5

        converted_bool = TypeConverter.convert_value(pa.scalar(True))
        assert type(converted_bool) is bool
        assert converted_bool is True

        converted_date = TypeConverter.convert_value(pa.scalar(datetime.date(2023, 1, 15)))
        assert type(converted_date) is datetime.date
        assert converted_date == datetime.date(2023, 1, 15)

    def test_datetime64_converts_as_utc_at_second_scale(self, eastern_timezone):
        """Verify datetime64 converts through the UTC epoch in seconds.

        Mutation: `fromtimestamp` in place of `utcfromtimestamp` in
            `_convert_numpy_value`, or dropping the
            `astype('datetime64[s]')` rescale so a millisecond count is
            read as seconds.
        Oracle: hand-computed wall clock 12:34:56 UTC, asserted while
            the local timezone is deliberately America/New_York.
        """
        converted = TypeConverter.convert_value(
            np.datetime64('2023-01-15T12:34:56.789'))

        assert converted == datetime.datetime(2023, 1, 15, 12, 34, 56)
        assert converted.tzinfo is None


class TestTypeConverterParams:
    """Tests for TypeConverter.convert_params"""

    def test_batch_rows_are_converted_element_by_element(self):
        """Verify a sequence of rows converts inside each row.

        Mutation: `convert_value` in place of `convert_params` in the
            batch recursion (returns rows untouched), or narrowing the
            batch guard to `tuple` only (silently skips list-of-lists
            rows).
        Oracle: hand-written expected batches with both null spellings
            resolved, for tuple rows and list rows.
        """
        rows = [(1, 'null'), (2, '')]
        assert TypeConverter.convert_params(rows) == [(1, None), (2, None)]
        assert TypeConverter.convert_params([[1, 'null'], [2, '']]) == [[1, None], [2, None]]

    def test_sequence_container_type_is_preserved(self):
        """Verify the container class survives conversion, inside and out.

        Mutation: `list(...)` in place of `type(params)(...)`, which
            turns an executemany tuple of tuples into lists.
        Oracle: type identity on the outer and inner containers, not
            just equality, since a list equals no tuple.
        """
        converted = TypeConverter.convert_params(((1, 'null'), (2, 'nan')))

        assert type(converted) is tuple
        assert type(converted[0]) is tuple
        assert converted == ((1, None), (2, None))

        flat = TypeConverter.convert_params(['null', 3])
        assert type(flat) is list
        assert flat == [None, 3]

        list_batch = TypeConverter.convert_params([[1, 'null']])
        assert type(list_batch[0]) is list

    def test_mixed_sequence_is_one_row_not_a_batch(self):
        """Verify a row holding a tuple parameter is left intact.

        Mutation: `any` in place of `all` in the batch guard, which
            treats a single row carrying a tuple parameter as a batch
            and rewrites inside that tuple.
        Oracle: hand-written expectation - the tuple parameter reaches
            the driver as given, while the sibling string is nulled.
        """
        params = [(1, 'null'), 'null']
        assert TypeConverter.convert_params(params) == [(1, 'null'), None]

    def test_scalar_params_go_through_value_conversion(self):
        """Verify a lone parameter is converted, not returned as is.

        Mutation: `return params` in place of
            `return TypeConverter.convert_value(params)` at the tail of
            convert_params.
        Oracle: the same values through convert_value - 'nan' is None
            and np.int64(7) is a Python int.
        """
        assert TypeConverter.convert_params('nan') is None
        assert TypeConverter.convert_params(None) is None
        assert type(TypeConverter.convert_params(np.int64(7))) is int

    def test_dict_params_keep_their_keys(self):
        """Verify dict parameters convert values and keep every key.

        Mutation: swapping the key and value in the dict comprehension
            of convert_params, or converting keys instead of values.
        Oracle: hand-written expected dict, with a key that is itself a
            special null string so a key conversion would erase it.
        """
        params = {'null': 'null', 'kept': np.int64(5)}
        assert TypeConverter.convert_params(params) == {'null': None, 'kept': 5}


if __name__ == '__main__':
    __import__('pytest').main([__file__])
