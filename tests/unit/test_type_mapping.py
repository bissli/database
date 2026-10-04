"""Tests for TypeConverter and the PostgreSQL map against psycopg."""
import datetime
import decimal
import math
import time

import numpy as np
import pandas as pd
import psycopg
import pyarrow as pa
import pytest
from database.types import TypeConverter, resolve_type
from psycopg.adapt import Transformer
from psycopg.pq import Format


def get_pg_oid(type_name):
    """OID psycopg registers for a PostgreSQL type name."""
    return psycopg.postgres.types.get(type_name).oid


@pytest.fixture
def eastern_timezone(monkeypatch):
    """Run the test body under a local timezone that is not UTC."""
    if not hasattr(time, 'tzset'):
        pytest.fail('time.tzset unavailable on this platform')
    monkeypatch.setenv('TZ', 'America/New_York')
    time.tzset()
    utc_epoch = datetime.datetime.fromtimestamp(0, datetime.UTC).replace(tzinfo=None)
    if datetime.datetime.fromtimestamp(0) == utc_epoch:
        pytest.fail('tz database missing - the UTC oracle is disarmed')
    yield
    monkeypatch.undo()
    time.tzset()


class TestPostgresTypeMapping:
    """PostgreSQL map entries checked against psycopg's loaders."""

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

        Mutation: dropping a name that alone holds its OID, e.g. numeric.
        Oracle: OIDs from psycopg's registry; hand-written type per name.
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

        Mutation: mapping time or timetz to datetime.datetime.
        Oracle: the class psycopg's own loader returns for the OID.
        """
        oid = get_pg_oid(pg_name)
        loaded = Transformer().get_loader(oid, Format.TEXT).load(wire_value)
        assert resolve_type('postgresql', oid) is type(loaded)

    def test_numeric_and_uuid_coerce_past_the_psycopg_loader(self):
        """Verify numeric maps to float and uuid to str, unlike psycopg.

        Mutation: numeric mapped to Decimal or uuid to UUID, as psycopg loads.
        Oracle: psycopg's loaders, which return Decimal and UUID.
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


class TestTypeConverterScalars:
    """TypeConverter.convert_value on one value."""

    def test_non_finite_floats_become_null(self):
        """Verify NaN and both infinities convert to None.

        Mutation: dropping `math.isinf(value)` from the float branch.
        Oracle: the largest float64 survives; inf does not.
        """
        assert TypeConverter.convert_value(math.nan) is None
        assert TypeConverter.convert_value(math.inf) is None
        assert TypeConverter.convert_value(-math.inf) is None
        largest_float = 1.7976931348623157e308
        assert TypeConverter.convert_value(largest_float) == largest_float
        assert TypeConverter.convert_value(0.0) == 0.0

    def test_empty_string_becomes_null(self):
        """Verify convert_value turns '' into None.

        Mutation: dropping the '' arm of the str fast path.
        Oracle: '' against a one-space string.
        """
        assert TypeConverter.convert_value('') is None
        assert TypeConverter.convert_value(' ') == ' '

    @pytest.mark.parametrize('value', [
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
    def test_spelled_null_strings_bind_as_text(self, value):
        """Verify every spelling of a null-ish word comes back unchanged.

        Mutation: a SPECIAL_STRINGS lookup on convert_value's str paths.
        Oracle: each spelling SPECIAL_STRINGS would match, in mixed case.
        """
        assert TypeConverter.convert_value(value) == value

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

        Mutation: a strip() or substring null-word test in convert_value.
        Oracle: near misses one character off a null word.
        """
        assert TypeConverter.convert_value(value) == value

    def test_pandas_nat_is_not_mistaken_for_a_datetime(self):
        """Verify pd.NaT converts to None despite subclassing datetime.

        Mutation: isinstance in place of the exact datetime type check.
        Oracle: pd.NaT subclasses datetime.datetime, asserted first.
        """
        assert isinstance(pd.NaT, datetime.datetime)
        assert TypeConverter.convert_value(pd.NaT) is None
        assert TypeConverter.convert_value(np.datetime64('NaT')) is None

    def test_missing_scalars_become_null_and_present_ones_do_not(self):
        """Verify the pandas isna branch nulls only missing values.

        Mutation: `or` for `and` in the is_scalar/isna guard.
        Oracle: Decimal('NaN') is None; Decimal('1.5') is unchanged.
        """
        assert TypeConverter.convert_value(pd.NA) is None
        present = decimal.Decimal('1.5')
        assert TypeConverter.convert_value(decimal.Decimal('NaN')) is None
        assert TypeConverter.convert_value(present) == present

    def test_numpy_scalars_become_python_builtins(self):
        """Verify numpy scalars are unwrapped, not merely returned.

        Mutation: `return val` in place of `return val.item()`.
        Oracle: exact type identity, and the uint64 maximum by value.
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
        assert type(TypeConverter.convert_value(np.int32(-7))) is int
        assert type(TypeConverter.convert_value(np.float64(0.25))) is float

    def test_non_finite_float32_nulls_like_a_builtin_float(self):
        """Verify np.float32 infinity nulls the way float infinity does.

        Mutation: dropping `np.isinf(val)` from `_convert_numpy_value`.
        Oracle: float and np.float64 infinity null; float32 max survives.
        """
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

        Mutation: dropping np.bool_ from either isinstance gate.
        Oracle: exact type identity against bool, since np.True_ == True.
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

        Mutation: dropping `_empty_string_to_none` around `as_py()`, or the
            pa.Scalar or pa.Array clause of the pyarrow branch.
        Oracle: the plain-Python values, compared by exact type.
        """
        assert TypeConverter.convert_value(pa.scalar('')) is None
        assert TypeConverter.convert_value(pa.scalar('null')) == 'null'
        assert TypeConverter.convert_value(pa.scalar('kept')) == 'kept'
        assert TypeConverter.convert_value(pa.scalar(None, type=pa.float64())) is None

        unwrapped = TypeConverter.convert_value(pa.scalar(1.5))
        assert type(unwrapped) is float
        assert unwrapped == 1.5

        converted_bool = TypeConverter.convert_value(pa.scalar(True))
        assert type(converted_bool) is bool
        assert converted_bool is True

        trade_date = datetime.date(2023, 1, 15)
        converted_date = TypeConverter.convert_value(pa.scalar(trade_date))
        assert type(converted_date) is datetime.date
        assert converted_date == trade_date

        converted_list = TypeConverter.convert_value(pa.array([1, 2, 3]))
        assert type(converted_list) is list
        assert converted_list == [1, 2, 3]

    def test_datetime64_keeps_wall_clock_and_microseconds(self, eastern_timezone):
        """Verify datetime64 keeps its wall-clock value down to the microsecond.

        Mutation: a datetime64[s] rescale, or a local-time conversion.
        Oracle: the input's own fields, 12:34:56.123456, under
            America/New_York; the nanosecond digits are cut.
        """
        converted = TypeConverter.convert_value(
            np.datetime64('2023-01-15T12:34:56.123456789'))

        assert converted == datetime.datetime(2023, 1, 15, 12, 34, 56, 123456)
        assert type(converted) is datetime.datetime
        assert converted.tzinfo is None

    def test_datetime64_outside_the_datetime_range_raises(self):
        """Verify a datetime64 past year 9999 raises instead of binding an int.

        Mutation: dropping the isinstance check after item(), which binds
            microseconds since the epoch as an integer.
        Oracle: years 10000 and 0 either side of the range, and
            9999-12-31 inside it.
        """
        with pytest.raises(ValueError, match='outside the datetime range'):
            TypeConverter.convert_value(np.datetime64('10000-01-01'))
        with pytest.raises(ValueError, match='outside the datetime range'):
            TypeConverter.convert_value(np.datetime64('0000-06-01T00:00:00'))
        assert TypeConverter.convert_value(
            np.datetime64('9999-12-31T23:59:59.999999')) == datetime.datetime(
                9999, 12, 31, 23, 59, 59, 999999)

    def test_pandas_timestamp_becomes_a_plain_datetime(self):
        """Verify a pd.Timestamp comes back as an exact datetime.datetime.

        Mutation: dropping the pd.Timestamp branch, which leaves the
            subclass that sqlite3 cannot bind.
        Oracle: the input's own fields, naive and tz-aware.
        """
        naive = TypeConverter.convert_value(
            pd.Timestamp('2023-01-15 12:34:56.123456789'))
        aware = TypeConverter.convert_value(
            pd.Timestamp('2023-01-15 12:34:56.5', tz='UTC'))

        assert type(naive) is datetime.datetime
        assert naive == datetime.datetime(2023, 1, 15, 12, 34, 56, 123456)
        assert type(aware) is datetime.datetime
        assert aware == datetime.datetime(
            2023, 1, 15, 12, 34, 56, 500000, tzinfo=datetime.UTC)

    def test_containers_skip_the_pandas_isna_branch(self):
        """Verify a list or tuple value comes back instead of raising.

        Mutation: dropping the is_scalar check before pd.isna.
        Oracle: list and tuple inputs, returned equal.
        """
        assert TypeConverter.convert_value([1, 2, 3]) == [1, 2, 3]
        assert TypeConverter.convert_value((1, 2)) == (1, 2)


class TestTypeConverterParams:
    """TypeConverter.convert_params on dicts, rows and batches."""

    def test_batch_rows_are_converted_element_by_element(self):
        """Verify a sequence of rows converts inside each row.

        Mutation: convert_value for convert_params in the batch recursion,
            or a tuple-only batch guard.
        Oracle: hand-written batches of tuple rows and list rows.
        """
        rows = [(np.int64(1), ''), (2, '')]
        converted_rows = TypeConverter.convert_params(rows)
        assert converted_rows == [(1, None), (2, None)]
        assert type(converted_rows[0][0]) is int
        list_rows = [[1, ''], [2, '']]
        assert TypeConverter.convert_params(list_rows) == [[1, None], [2, None]]

    def test_sequence_container_type_is_preserved(self):
        """Verify the container class survives conversion, inside and out.

        Mutation: `list(...)` in place of `type(params)(...)`.
        Oracle: type identity of the outer and inner containers.
        """
        converted = TypeConverter.convert_params(((1, ''), (2, '')))

        assert type(converted) is tuple
        assert type(converted[0]) is tuple
        assert converted == ((1, None), (2, None))

        flat = TypeConverter.convert_params(['', 3])
        assert type(flat) is list
        assert flat == [None, 3]

        list_batch = TypeConverter.convert_params([[1, '']])
        assert type(list_batch[0]) is list

    def test_mixed_sequence_is_one_row_not_a_batch(self):
        """Verify a row holding a tuple parameter is left intact.

        Mutation: `any` in place of `all` in the batch guard.
        Oracle: the tuple parameter comes back as given.
        """
        params = [(1, ''), '']
        assert TypeConverter.convert_params(params) == [(1, ''), None]

    def test_scalar_params_go_through_value_conversion(self):
        """Verify a lone parameter is converted, not returned as is.

        Mutation: `return params` at the tail of convert_params.
        Oracle: '' is None and np.int64(7) is a Python int.
        """
        assert TypeConverter.convert_params('') is None
        assert TypeConverter.convert_params(None) is None
        assert type(TypeConverter.convert_params(np.int64(7))) is int

    def test_dict_params_keep_their_keys(self):
        """Verify dict parameters convert values and keep every key.

        Mutation: converting keys, or swapping key and value.
        Oracle: hand-written dict whose '' key a key conversion erases.
        """
        params = {'': '', 'kept': np.int64(5), 'nan_val': float('nan')}
        assert TypeConverter.convert_params(params) == {
            '': None,
            'kept': 5,
            'nan_val': None,
            }


if __name__ == '__main__':
    __import__('pytest').main([__file__])
