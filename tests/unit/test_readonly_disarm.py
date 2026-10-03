"""Unit tests for raise_on_readonly_disarm."""
import pytest
from database.exceptions import ReadOnlyError
from database.sql import raise_on_readonly_disarm


class StubConnection:
    """Connection-shaped stand-in carrying only what the guard reads."""

    def __init__(self, readonly=True, dialect='postgresql'):
        self.readonly = readonly
        self.dialect = dialect


@pytest.mark.parametrize('sql', [
    'set default_transaction_read_only = off',
    'SET SESSION default_transaction_read_only TO off',
    'set local default_transaction_read_only = false',
    'reset default_transaction_read_only',
    'RESET ALL',
    ])
def test_postgres_disarming_statement_is_refused(sql):
    """Verify every form that clears the PostgreSQL setting is refused.

    Mutation: dropping the 'reset' alternative from _DISARM_RE.
    Oracle: PostgreSQL grammar; the server accepts each form.
    """
    with pytest.raises(ReadOnlyError):
        raise_on_readonly_disarm(StubConnection(), sql)


@pytest.mark.parametrize('sql', [
    'pragma query_only = OFF',
    'PRAGMA query_only=0',
    'pragma main.query_only = off',
    ])
def test_sqlite_disarming_pragma_is_refused(sql):
    """Verify the assigning query_only pragma is refused on SQLite.

    Mutation: requiring a space before '=' in the pragma alternative.
    Oracle: SQLite accepts all three spellings, schema prefix included.
    """
    with pytest.raises(ReadOnlyError):
        raise_on_readonly_disarm(StubConnection(dialect='sqlite'), sql)


@pytest.mark.parametrize('sql', [
    'select count(*) from orders',
    'insert into orders values (1)',
    'delete from orders',
    'update orders set n = 1',
    'pragma query_only',
    'pragma table_info(orders)',
    'set search_path to public',
    "select 'reset all' as label",
    "select * from t where note = 'pragma query_only = off'",
    ])
def test_everything_else_reaches_the_server(sql):
    """Verify the guard refuses nothing but the disarming forms.

    Mutation: matching _DISARM_WORDS in place of _DISARM_RE on masked text.
    Oracle: neither server lets these statements clear its read-only setting.
    """
    raise_on_readonly_disarm(StubConnection(), sql)
    raise_on_readonly_disarm(StubConnection(dialect='sqlite'), sql)


def test_writer_is_never_checked():
    """Verify a writer may change its own session settings.

    Mutation: dropping the readonly test.
    Oracle: a writer passes 'reset all' without a raise.
    """
    raise_on_readonly_disarm(StubConnection(readonly=False), 'reset all')


def test_object_with_no_readonly_attribute_is_treated_as_a_writer():
    """Verify a raw connection-like object is left alone.

    Mutation: getattr(cn, 'readonly') without a default.
    Oracle: a bare object() passes 'reset all' without a raise.
    """
    raise_on_readonly_disarm(object(), 'reset all')


def test_unscannable_text_is_refused():
    """Verify text the mask cannot scan fails closed.

    Mutation: returning early when mask_protected_text yields None.
    Oracle: an unterminated literal, which mask_protected_text cannot scan.
    """
    sql = "select 'unterminated ; reset all"

    with pytest.raises(ReadOnlyError):
        raise_on_readonly_disarm(StubConnection(), sql)
