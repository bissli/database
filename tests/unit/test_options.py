import pandas as pd
import pytest
from database.options import DatabaseOptions, iterdict_data_loader
from database.options import pandas_numpy_data_loader
from database.options import pandas_pyarrow_data_loader


class TestDatabaseOptions:
    """Test suite for DatabaseOptions class"""

    def test_init_defaults(self):
        """Test default initialization"""
        options = DatabaseOptions(
            hostname='testhost',
            username='testuser',
            password='testpass',
            database='testdb',
            port=1234,
            timeout=30
        )

        # Check default values
        assert options.drivername == 'postgres'
        assert options.appname is not None
        assert options.cleanup is True
        assert options.check_connection is True
        assert options.data_loader == pandas_numpy_data_loader

    def test_validation(self):
        """Test validation rules"""
        # Test invalid driver name
        with pytest.raises(AssertionError):
            DatabaseOptions(
                drivername='invalid',
                hostname='testhost',
                username='testuser',
                password='testpass',
                database='testdb',
                port=1234,
                timeout=30
            )

        # Test missing required fields for postgres
        with pytest.raises(AssertionError):
            DatabaseOptions(drivername='postgres', hostname='testhost')

    def test_sqlite_options(self):
        """Test SQLite options validation"""
        # SQLite minimal config should work
        options = DatabaseOptions(
            drivername='sqlite',
            database='test.db'
        )
        assert options.drivername == 'sqlite'
        assert options.database == 'test.db'

        # SQLite without database should fail
        with pytest.raises(AssertionError):
            DatabaseOptions(drivername='sqlite')


class TestDataLoaders:
    """Test suite for data loader functions"""

    def test_iterdict_data_loader(self):
        """Test iterdict_data_loader function"""
        data = [{'name': 'Alice', 'age': 30}, {'name': 'Bob', 'age': 25}]

        # Create mock column info
        from database.adapters.column_info import Column
        column_info = [Column(name='name', type_code=None), Column(name='age', type_code=None)]

        result = iterdict_data_loader(data, column_info)
        assert result == data

    def test_pandas_numpy_data_loader(self):
        """Test pandas_numpy_data_loader function"""
        data = [{'name': 'Alice', 'age': 30}, {'name': 'Bob', 'age': 25}]

        # Create mock column info
        from database.adapters.column_info import Column
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
    def test_pandas_pyarrow_data_loader(self):
        """Test pandas_pyarrow_data_loader function"""
        data = [{'name': 'Alice', 'age': 30}, {'name': 'Bob', 'age': 25}]

        # Create mock column info
        from database.adapters.column_info import Column
        column_info = [Column(name='name', type_code=None), Column(name='age', type_code=None)]

        result = pandas_pyarrow_data_loader(data, column_info)
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == Column.get_names(column_info)
        assert len(result) == 2
        assert result.iloc[0]['name'] == 'Alice'
        assert result.iloc[1]['age'] == 25


if __name__ == '__main__':
    __import__('pytest').main([__file__])
