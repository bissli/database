import pytest
from database.exceptions import ValidationError
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
    with pytest.raises(ValidationError):
        DatabaseOptions(
            drivername='invalid',
            hostname='testhost',
            username='testuser',
            password='testpass',
            database='testdb',
            port=1234,
            timeout=30
        )

    with pytest.raises(ValidationError):
        DatabaseOptions(drivername='postgresql', hostname='testhost')


def test_sqlite_options():
    """Test SQLite options validation"""
    options = DatabaseOptions(
        drivername='sqlite',
        database='test.db'
    )
    assert options.drivername == 'sqlite'
    assert options.database == 'test.db'

    with pytest.raises(ValidationError):
        DatabaseOptions(drivername='sqlite')


def _make_options(**overrides):
    """Build a minimal valid DatabaseOptions with defaults."""
    base = {
        'hostname': 'testhost', 'username': 'testuser',
        'password': 'secret_value_xyz', 'database': 'testdb',
        'port': 1234, 'timeout': 30,
    }
    base.update(overrides)
    return DatabaseOptions(**base)


class TestPasswordRedaction:
    """Password must never appear in repr/str output or in engine
    registry cache keys derived from str(options).
    """

    def test_repr_redacts_password(self):
        options = _make_options(password='secret_value_xyz')
        assert 'secret_value_xyz' not in repr(options)

    def test_str_redacts_password(self):
        options = _make_options(password='secret_value_xyz')
        assert 'secret_value_xyz' not in str(options)

    def test_repr_keeps_diagnostic_fields(self):
        """Redaction must not destroy debuggability — non-secret fields stay."""
        options = _make_options(password='secret_value_xyz')
        r = repr(options)
        assert 'testhost' in r
        assert 'testuser' in r
        assert 'testdb' in r
        assert 'postgresql' in r

    def test_repr_shows_redaction_marker(self):
        """A redaction marker (e.g. '***') should appear so it's obvious
        the password was scrubbed, not just missing.
        """
        options = _make_options(password='secret_value_xyz')
        assert '***' in repr(options)

    def test_repr_when_password_none(self):
        """SQLite doesn't require a password — repr should show None plainly."""
        options = DatabaseOptions(drivername='sqlite', database='test.db')
        r = repr(options)
        assert 'password=None' in r


if __name__ == '__main__':
    __import__('pytest').main([__file__])
