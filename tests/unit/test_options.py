import pytest
from database.options import DatabaseOptions, pandas_numpy_data_loader


def test_init_defaults():
    """Test default initialization"""
    options = DatabaseOptions(
        hostname='testhost',
        username='testuser',
        password='testpass',
        database='testdb',
        port=1234,
        timeout=30
    )

    assert options.drivername == 'postgresql'
    assert options.appname is not None
    assert options.cleanup is True
    assert options.check_connection is True
    assert options.data_loader == pandas_numpy_data_loader

    assert options.use_pool is False
    assert options.pool_max_connections == 5
    assert options.pool_max_idle_time == 300
    assert options.pool_wait_timeout == 30


def test_pooling_options():
    """Test connection pooling options"""
    options = DatabaseOptions(
        drivername='postgresql',
        hostname='testhost',
        username='testuser',
        password='testpass',
        database='testdb',
        port=1234,
        timeout=30,
        use_pool=True,
        pool_max_connections=10,
        pool_max_idle_time=600,
        pool_wait_timeout=60
    )

    assert options.use_pool is True
    assert options.pool_max_connections == 10
    assert options.pool_max_idle_time == 600
    assert options.pool_wait_timeout == 60


def test_validation():
    """Test validation rules"""
    with pytest.raises(ValueError):
        DatabaseOptions(
            drivername='invalid',
            hostname='testhost',
            username='testuser',
            password='testpass',
            database='testdb',
            port=1234,
            timeout=30
        )

    with pytest.raises(ValueError):
        DatabaseOptions(drivername='postgresql', hostname='testhost')


def test_sqlite_options():
    """Test SQLite options validation"""
    options = DatabaseOptions(
        drivername='sqlite',
        database='test.db'
    )
    assert options.drivername == 'sqlite'
    assert options.database == 'test.db'

    with pytest.raises(ValueError):
        DatabaseOptions(drivername='sqlite')


if __name__ == '__main__':
    __import__('pytest').main([__file__])
