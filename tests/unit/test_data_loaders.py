import datetime
from types import SimpleNamespace

import pandas as pd
import pyarrow as pa
import pytest
from database.options import iterdict_data_loader, pandas_numpy_data_loader
from database.options import pandas_pyarrow_data_loader
from database.options import use_iterdict_data_loader
from database.types import Column


def test_numpy_loader_orders_columns_by_metadata():
    """Verify column order and membership come from the Column metadata.

    Mutation: dropping the columns= argument of pd.DataFrame.from_records
        in pandas_numpy_data_loader, or dropping **kwargs from the loader
        signature (cursor.load_data always forwards kwargs to the loader).
    Oracle: hand-written ['age', 'name'] against rows whose dict order is
        the reverse and which carry an extra key; table_name kwarg exercises
        the **kwargs path.
    """
    columns = [
        Column(name='age', type_code=None),
        Column(name='name', type_code=None),
        ]
    data = [
        {'name': 'Alice', 'age': 30, 'nickname': 'Al'},
        {'name': 'Bob', 'age': 25, 'nickname': 'Bo'},
        ]

    df = pandas_numpy_data_loader(data, columns, table_name='people')

    assert list(df.columns) == ['age', 'name']
    assert df['age'].tolist() == [30, 25]
    assert df['name'].tolist() == ['Alice', 'Bob']


def test_numpy_loader_fills_missing_key_with_null():
    """Verify a row missing a column yields a null, not a shift or a raise.

    Mutation: building the frame by indexing every row for every column
        (row[col]), the way pandas_pyarrow_data_loader does.
    Oracle: hand-computed 30 in row 0 and a null in row 1, whose source
        dict has no 'age' key at all.
    """
    columns = [
        Column(name='age', type_code=None),
        Column(name='name', type_code=None),
        ]
    data = [{'name': 'Alice', 'age': 30}, {'name': 'Bob'}]

    df = pandas_numpy_data_loader(data, columns)

    assert list(df.columns) == ['age', 'name']
    assert df['name'].tolist() == ['Alice', 'Bob']
    assert df['age'].iloc[0] == 30
    assert df['age'].isna().tolist() == [False, True]


def test_pyarrow_loader_returns_arrow_backed_dtypes():
    """Verify the pyarrow loader maps every column to an arrow-backed dtype.

    Mutation: dropping types_mapper=pd.ArrowDtype from the to_pandas call
        in pandas_pyarrow_data_loader.
    Oracle: pa.int64()/pa.string()/pa.date32() named independently, plus a
        differential against pandas_numpy_data_loader, which widens the
        same nullable integer column to float64.
    """
    columns = [
        Column(name='age', type_code=None),
        Column(name='name', type_code=None),
        Column(name='born', type_code=None),
        ]
    data = [
        {'age': 30, 'name': 'Alice', 'born': datetime.date(1990, 1, 2)},
        {'age': None, 'name': 'Bob', 'born': datetime.date(1985, 6, 7)},
        ]

    df = pandas_pyarrow_data_loader(data, columns)

    assert [isinstance(dtype, pd.ArrowDtype) for dtype in df.dtypes] == [
        True,
        True,
        True,
        ]
    assert df.dtypes['age'].pyarrow_dtype == pa.int64()
    assert df.dtypes['name'].pyarrow_dtype == pa.string()
    assert df.dtypes['born'].pyarrow_dtype == pa.date32()
    assert df['age'].iloc[0] == 30
    assert df['age'].isna().tolist() == [False, True]
    assert df['born'].iloc[0] == datetime.date(1990, 1, 2)

    numpy_df = pandas_numpy_data_loader(data, columns)
    assert str(numpy_df.dtypes['age']) == 'float64'


def test_pyarrow_loader_orders_columns_by_metadata():
    """Verify each arrow column keeps the name and values its metadata gives.

    Mutation: column_names = list(data[0]) in place of
        Column.get_names(columns) in pandas_pyarrow_data_loader, or
        transposing the columns_data comprehension, or dropping **kwargs
        from the loader signature (cursor.load_data always forwards kwargs
        to the loader).
    Oracle: hand-written ['age', 'name'] and per-column value lists,
        against rows whose dict order is the reverse and which carry an
        extra key; table_name kwarg exercises the **kwargs path.
    """
    columns = [
        Column(name='age', type_code=None),
        Column(name='name', type_code=None),
        ]
    data = [
        {'name': 'Alice', 'age': 30, 'nickname': 'Al'},
        {'name': 'Bob', 'age': 25, 'nickname': 'Bo'},
        ]

    df = pandas_pyarrow_data_loader(data, columns, table_name='people')

    assert list(df.columns) == ['age', 'name']
    assert df['age'].tolist() == [30, 25]
    assert df['name'].tolist() == ['Alice', 'Bob']


def test_pyarrow_loader_nulls_a_missing_column_like_the_numpy_one():
    """Verify both loaders answer a row missing a column the same way.

    Mutation: row[col] in place of row.get(col) in
        pandas_pyarrow_data_loader, which raises KeyError where the numpy
        loader nulls the gap - so swapping data_loader would decide
        whether a sparse result set loads at all.
    Oracle: a differential against pandas_numpy_data_loader over the same
        rows, plus hand-computed [30, null] for the sparse column.
    """
    columns = [
        Column(name='age', type_code=None),
        Column(name='name', type_code=None),
        ]
    data = [{'name': 'Alice', 'age': 30}, {'name': 'Bob'}]

    arrow_df = pandas_pyarrow_data_loader(data, columns)
    numpy_df = pandas_numpy_data_loader(data, columns)

    assert arrow_df['age'].iloc[0] == 30
    assert arrow_df['age'].isna().tolist() == [False, True]
    assert arrow_df['name'].tolist() == ['Alice', 'Bob']
    assert arrow_df['age'].isna().tolist() == numpy_df['age'].isna().tolist()


@pytest.mark.parametrize(
    'loader',
    [pandas_numpy_data_loader, pandas_pyarrow_data_loader],
    ids=['numpy', 'pyarrow'])
def test_loaders_keep_columns_for_empty_input(loader):
    """Verify every falsy input returns a 0-row frame carrying the columns.

    Mutation: pd.DataFrame(columns=...) -> pd.DataFrame() in
        _empty_dataframe (both loaders), or dropping the dtype argument
        _empty_dataframe forwards, which would leave the pyarrow loader
        numpy-backed for an empty result and arrow-backed otherwise.
    Oracle: hand-written ['age', 'name'] and row count 0 for three falsy
        inputs ([], None, ()), plus the backing each loader uses on a
        populated frame, named per parametrization.
    """
    columns = [
        Column(name='age', type_code=None),
        Column(name='name', type_code=None),
        ]
    wants_arrow = loader is pandas_pyarrow_data_loader

    for empty in ([], None, ()):
        df = loader(empty, columns)
        assert list(df.columns) == ['age', 'name']
        assert len(df) == 0
        assert all(isinstance(dtype, pd.ArrowDtype)
                   for dtype in df.dtypes) is wants_arrow
        assert list(df.attrs['column_types']) == ['age', 'name']


@pytest.mark.parametrize(
    'loader',
    [pandas_numpy_data_loader, pandas_pyarrow_data_loader],
    ids=['numpy', 'pyarrow'])
def test_loaders_attach_full_column_metadata(loader):
    """Verify attrs['column_types'] carries every field of each column.

    Mutation: dropping the df.attrs assignment, or storing
        Column.get_names(columns) in place of
        Column.get_column_types_dict(columns).
    Oracle: a hand-written metadata dict for a numeric column with
        precision, scale and nullable all set.
    """
    columns = [
        Column(
            name='amount', type_code=1700, python_type=float,
            display_size=None, internal_size=8, precision=12, scale=2,
            nullable=False),
        ]

    df = loader([{'amount': 1.5}], columns)

    assert df.attrs['column_types'] == {
        'amount': {
            'name': 'amount',
            'type_code': 1700,
            'python_type': 'float',
            'display_size': None,
            'internal_size': 8,
            'precision': 12,
            'scale': 2,
            'nullable': False,
            },
        }


def test_iterdict_loader_copies_rows_into_a_new_list():
    """Verify the loader materializes rows into a list the caller owns.

    Mutation: `return data` in place of `return list(data)` in
        iterdict_data_loader.
    Oracle: a tuple input comes back as a list, a generator is drained,
        and appending to the result leaves the input list at length 2.
    """
    rows = [{'a': 1}, {'a': 2}]

    result = iterdict_data_loader(rows, [])
    result.append({'a': 3})

    assert rows == [{'a': 1}, {'a': 2}]
    assert iterdict_data_loader(({'a': 1},), []) == [{'a': 1}]
    assert iterdict_data_loader(iter(rows), []) == [{'a': 1}, {'a': 2}]


def test_iterdict_loader_returns_empty_list_for_no_rows():
    """Verify every falsy input, None included, comes back as [].

    Mutation: `return data` in place of `return []` in
        iterdict_data_loader.
    Oracle: hand-written [] for None, [] and (), where None cannot
        survive list() and () would come back as a tuple.
    """
    for empty in (None, [], ()):
        result = iterdict_data_loader(empty, [])
        assert result == []
        assert isinstance(result, list)


def test_use_iterdict_swaps_loader_for_the_call():
    """Verify the decorator installs the dict loader only for the call.

    Mutation: dropping the `cn.options.data_loader = iterdict_data_loader`
        assignment, or the restore in the finally block, of
        use_iterdict_data_loader, or reading `cn = args[-1]` instead of
        `cn = args[0]` so the loader swap targets the wrong argument.
    Oracle: a 2-arg spy recording the live loader mid-call, checked against
        iterdict_data_loader and against the original loader afterward; the
        second argument catches the args[-1] mutation.
    """
    seen = []

    @use_iterdict_data_loader
    def record(cn, sentinel):
        seen.append(cn.options.data_loader)
        return sentinel * 2

    cn = SimpleNamespace(
        options=SimpleNamespace(data_loader=pandas_numpy_data_loader))

    assert record(cn, 21) == 42
    assert seen == [iterdict_data_loader]
    assert cn.options.data_loader is pandas_numpy_data_loader


def test_use_iterdict_restores_loader_when_the_call_raises():
    """Verify a raising call still leaves the original loader in place.

    Mutation: moving the restore out of the finally block in
        use_iterdict_data_loader.
    Oracle: the loader identity after a deliberate RuntimeError, checked
        against the pyarrow loader the connection started with.
    """
    @use_iterdict_data_loader
    def blow_up(cn):
        raise RuntimeError('boom')

    cn = SimpleNamespace(
        options=SimpleNamespace(data_loader=pandas_pyarrow_data_loader))

    with pytest.raises(RuntimeError, match='boom'):
        blow_up(cn)

    assert cn.options.data_loader is pandas_pyarrow_data_loader


def test_use_iterdict_unwraps_a_transaction():
    """Verify an object holding only .connection is unwrapped to swap on it.

    Mutation: dropping the `cn = cn.connection` unwrap in
        use_iterdict_data_loader, or changing `func(*args, **kwargs)` to
        `func(cn, *args[1:], **kwargs)`, which hands func the unwrapped
        connection rather than the original caller argument.
    Oracle: spy captures its own cn parameter; must equal the transaction
        object, not the unwrapped inner connection.
    """
    inner = SimpleNamespace(
        options=SimpleNamespace(data_loader=pandas_numpy_data_loader))
    transaction = SimpleNamespace(connection=inner)
    seen = []

    @use_iterdict_data_loader
    def record(cn):
        seen.append((cn, inner.options.data_loader))

    record(transaction)

    assert seen == [(transaction, iterdict_data_loader)]
    assert inner.options.data_loader is pandas_numpy_data_loader


def test_use_iterdict_keeps_the_object_that_owns_options():
    """Verify an object holding both .connection and .options is not unwrapped.

    Mutation: dropping `and not hasattr(cn, 'options')` from the unwrap
        guard in use_iterdict_data_loader, which would then swap the
        loader on the wrapped connection instead of the wrapper.
    Oracle: a spy reading both loaders mid-call - the wrapper's must be
        the dict loader, the inner one untouched.
    """
    inner = SimpleNamespace(
        options=SimpleNamespace(data_loader=pandas_pyarrow_data_loader))
    wrapper = SimpleNamespace(
        options=SimpleNamespace(data_loader=pandas_numpy_data_loader),
        connection=inner)
    seen = []

    @use_iterdict_data_loader
    def record(cn):
        seen.append((wrapper.options.data_loader, inner.options.data_loader))

    record(wrapper)

    assert seen == [(iterdict_data_loader, pandas_pyarrow_data_loader)]
    assert wrapper.options.data_loader is pandas_numpy_data_loader


if __name__ == '__main__':
    __import__('pytest').main([__file__])
