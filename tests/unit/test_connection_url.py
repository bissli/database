"""Unit tests for PostgreSQL connection URL construction.

The URL builder must URL-encode special characters in username, password,
appname, and database so that no character can corrupt URL parsing or
inject extra libpq parameters via the query string.
"""
import pytest
import sqlalchemy as sa
from database.options import DatabaseOptions
from database.strategy.postgres import PostgresStrategy


def _options(**overrides):
    """Build a minimal valid DatabaseOptions for URL-builder tests."""
    base = {
        'drivername': 'postgresql',
        'hostname': 'myhost', 'username': 'myuser',
        'password': 'mypass', 'database': 'mydb',
        'port': 5432, 'timeout': 30,
    }
    base.update(overrides)
    return DatabaseOptions(**base)


def _parse(url):
    """Parse a connection URL string with SQLAlchemy."""
    return sa.engine.url.make_url(url)


class TestPasswordEncoding:
    """Password characters that would corrupt URL parsing must be encoded."""

    @pytest.mark.parametrize('password', [
        'p@ss', 'p/ss', 'p?ss', 'p#ss', 'pa%ss', 'p&ss', 'p=ss', 'pa ss',
    ], ids=['at', 'slash', 'question', 'hash', 'percent', 'amp', 'eq', 'space'])
    def test_special_password_is_encoded(self, password):
        url = PostgresStrategy().build_connection_url(_options(password=password))
        parsed = _parse(url)
        assert parsed.host == 'myhost'
        assert parsed.username == 'myuser'
        assert parsed.password == password


class TestUsernameEncoding:

    def test_username_with_at_sign(self):
        url = PostgresStrategy().build_connection_url(_options(username='user@dom'))
        parsed = _parse(url)
        assert parsed.username == 'user@dom'
        assert parsed.host == 'myhost'


class TestAppnameInjection:
    """An appname containing libpq query-string separators (&) or
    key/value markers (=) must not inject extra connection parameters.
    """

    def test_appname_with_ampersand_does_not_inject_params(self):
        url = PostgresStrategy().build_connection_url(
            _options(appname='evil&sslmode=disable'))
        parsed = _parse(url)
        # The injected sslmode must NOT appear as a real query parameter
        assert 'sslmode' not in parsed.query
        # And the application_name should be the full literal we passed in
        assert parsed.query.get('application_name') == 'evil&sslmode=disable'

    def test_appname_with_equals(self):
        url = PostgresStrategy().build_connection_url(
            _options(appname='name=with=equals'))
        parsed = _parse(url)
        assert parsed.query.get('application_name') == 'name=with=equals'

    def test_appname_with_hash(self):
        url = PostgresStrategy().build_connection_url(
            _options(appname='name#frag'))
        parsed = _parse(url)
        assert parsed.query.get('application_name') == 'name#frag'


class TestCleanCredentialsBaseline:
    """A plain ASCII config still produces a parseable URL."""

    def test_no_special_chars_produces_valid_url(self):
        url = PostgresStrategy().build_connection_url(_options())
        parsed = _parse(url)
        assert parsed.host == 'myhost'
        assert parsed.username == 'myuser'
        assert parsed.password == 'mypass'
        assert parsed.database == 'mydb'
        assert parsed.port == 5432
