import datetime
import threading
from types import SimpleNamespace

import database as db
import pandas as pd
import pyarrow as pa
import pytest
from database.options import DatabaseOptions, iterdict_data_loader
from database.options import pandas_numpy_data_loader
from database.options import pandas_pyarrow_data_loader
from database.options import use_iterdict_data_loader
from database.types import Column


def test_numpy_loader_orders_columns_by_metadata():
    """Verify column order and membership come from the Column metadata.

    Mutation: columns= dropped from from_records, or **kwargs from the loader.
    Oracle: ['age', 'name'] from reversed rows carrying an extra key.
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
    """Verify a row missing a column yields a null in that column.

    Mutation: every row indexed as row[col].
    Oracle: hand-computed [30, null] for the 'age' column.
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

    Mutation: types_mapper=pd.ArrowDtype dropped from to_pandas.
    Oracle: pa.int64/string/date32, and numpy's float64 for the same column.
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

    Mutation: names from data[0], columns_data transposed, or **kwargs dropped.
    Oracle: ['age', 'name'] and per-column values from reversed rows.
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

    Mutation: row[col] in place of row.get(col) in the pyarrow loader.
    Oracle: the numpy loader on the same rows, and hand-computed [30, null].
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

    Mutation: columns= dropped in _empty_dataframe, or its dtype argument.
    Oracle: ['age', 'name'], 0 rows and the loader's backing for [], None, ().
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

    Mutation: the attrs assignment dropped, or holding the column names alone.
    Oracle: hand-written metadata dict for a numeric column.
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

    Mutation: `return data` for `return list(data)`, or rows cut to columns.
    Oracle: tuple and generator inputs return lists; the input keeps 2 rows.
    """
    rows = [{'a': 1}, {'a': 2}]

    result = iterdict_data_loader(rows, [])
    result.append({'a': 3})

    assert rows == [{'a': 1}, {'a': 2}]
    assert iterdict_data_loader(({'a': 1},), []) == [{'a': 1}]
    assert iterdict_data_loader(iter(rows), []) == [{'a': 1}, {'a': 2}]


def test_iterdict_loader_returns_empty_list_for_no_rows():
    """Verify every falsy input, None included, comes back as [].

    Mutation: `return data` in place of `return []`.
    Oracle: [] for None, [] and (), checked as a list.
    """
    for empty in (None, [], ()):
        result = iterdict_data_loader(empty, [])
        assert result == []
        assert isinstance(result, list)


def test_use_iterdict_swaps_loader_for_the_call():
    """Verify the decorator installs the dict loader only for the call.

    Mutation: the swap or the restore dropped, or cn read from args[-1].
    Oracle: a two-argument spy recording the loader mid-call.
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

    Mutation: the restore moved out of the finally block.
    Oracle: the loader identity after a RuntimeError.
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

    Mutation: the unwrap dropped, or func called with the unwrapped connection.
    Oracle: a spy recording its cn and the inner loader mid-call.
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

    Mutation: `and not hasattr(cn, 'options')` dropped from the guard.
    Oracle: a spy reading both loaders mid-call.
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


def test_use_iterdict_leaves_a_shared_options_object_alone():
    """Verify a decorated call on one connection keeps its sibling's loader.

    Mutation: the decorator swapping data_loader on the options object
        the two connections share.
    Oracle: two connections on one DatabaseOptions; a barrier holds
        select_row on the first mid-query while the second selects.
    """
    options = DatabaseOptions(drivername='sqlite', database=':memory:')
    cn_held = db.connect(options)
    cn_free = db.connect(options)
    query_running = threading.Barrier(2, timeout=10)
    free_done = threading.Barrier(2, timeout=10)

    def hold():
        query_running.wait()
        free_done.wait()
        return 1

    cn_held.dbapi_connection.driver_connection.create_function('hold', 0, hold)
    held = {}
    worker = threading.Thread(
        target=lambda: held.update(
            row=cn_held.select_row('select hold() as x')))
    worker.start()
    try:
        query_running.wait()
        free_result = cn_free.select('select 1 as x')
    finally:
        free_done.wait()
        worker.join(timeout=10)
        cn_held.close()
        cn_free.close()

    assert isinstance(free_result, pd.DataFrame)
    assert held['row'].x == 1
    assert options.data_loader is pandas_numpy_data_loader


def test_use_iterdict_preserves_wrapped_function_identity():
    """Verify the decorated connection methods keep their own name and doc.

    Mutation: `@wraps(func)` dropped from the wrapper.
    Oracle: name and docstring of the undecorated function.
    """
    @use_iterdict_data_loader
    def select_row(cn):
        """Execute a query and return a single row."""

    assert select_row.__name__ == 'select_row'
    assert select_row.__doc__ == 'Execute a query and return a single row.'


if __name__ == '__main__':
    __import__('pytest').main([__file__])
