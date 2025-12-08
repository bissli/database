import pandas as pd
import pytest
from database.options import pandas_numpy_data_loader
from database.options import pandas_pyarrow_data_loader


def test_pandas_numpy_data_loader():
    """Test pandas_numpy_data_loader function"""
    data = [{'name': 'Alice', 'age': 30}, {'name': 'Bob', 'age': 25}]

    # Create mock column info
    from database.types import Column
    column_info = [Column(name='name', type_code=None), Column(name='age', type_code=None)]

    result = pandas_numpy_data_loader(data, column_info)
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == Column.get_names(column_info)
    assert len(result) == 2
    assert result.iloc[0]['name'] == 'Alice'
    assert result.iloc[1]['age'] == 25


@pytest.mark.skipif(
    not hasattr(pd, 'ArrowDtype'),
    reason='ArrowDtype not available in this pandas version'
)
def test_pandas_pyarrow_data_loader():
    """Test pandas_pyarrow_data_loader function"""
    data = [{'name': 'Alice', 'age': 30}, {'name': 'Bob', 'age': 25}]

    # Create mock column info
    from database.types import Column
    column_info = [Column(name='name', type_code=None), Column(name='age', type_code=None)]

    result = pandas_pyarrow_data_loader(data, column_info)
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == Column.get_names(column_info)
    assert len(result) == 2
    assert result.iloc[0]['name'] == 'Alice'
    assert result.iloc[1]['age'] == 25


if __name__ == '__main__':
    __import__('pytest').main([__file__])
