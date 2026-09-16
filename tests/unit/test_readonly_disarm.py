"""Unit tests for the guard that keeps a reader's backstop armed.

A reader is held read-only by a server-side session setting, and both
dialects let one statement turn that setting off. raise_on_readonly_disarm
refuses those statements and nothing else: classifying writes is the
server's job, and an earlier attempt to do it in process leaked in both
directions.
"""
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

    Mutation: dropping the 'reset' alternative from _DISARM_RE, which
        leaves 'RESET ALL' free to clear default_transaction_read_only
        and hand the caller a writable session.
    Oracle: PostgreSQL's own grammar - each form below is accepted by
        the server and takes the setting back to its default.
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

    Mutation: requiring a space before '=' in the pragma alternative,
        which lets 'PRAGMA query_only=0' through and unlocks the file
        for the rest of the connection's life.
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

    A write must reach the server and be refused there. A read naming
    the setting in a literal must run. Widening this check back into a
    write classifier is what refused 'select * from update' before.

    Mutation: matching _DISARM_WORDS directly instead of _DISARM_RE
        against the masked text, which refuses the two literals below
        and the reporting 'pragma query_only' that only reports.
    Oracle: PostgreSQL and SQLite both run every statement here under
        their read-only setting, or refuse it themselves.
    """
    raise_on_readonly_disarm(StubConnection(), sql)
    raise_on_readonly_disarm(StubConnection(dialect='sqlite'), sql)


def test_writer_is_never_checked():
    """Verify a writer may change its own session settings.

    Mutation: dropping the readonly test, which would refuse 'RESET
        ALL' on every writer in the codebase.
    Oracle: the guard exists only to hold a reader's setting in place.
    """
    raise_on_readonly_disarm(StubConnection(readonly=False), 'reset all')


def test_object_with_no_readonly_attribute_is_treated_as_a_writer():
    """Verify a raw connection-like object is left alone.

    Mutation: getattr(cn, 'readonly') without a default, raising
        AttributeError on a raw DBAPI connection handed to the guard.
    Oracle: an object that never declares itself read-only is not one.
    """
    raise_on_readonly_disarm(object(), 'reset all')


def test_unscannable_text_is_refused():
    """Verify text the mask cannot scan fails closed.

    Mutation: returning early when mask_protected_text yields None,
        which lets an unterminated literal carry a disarming statement
        past the guard at an offset nothing can place.
    Oracle: mask_protected_text returns None here, and no offset after
        an unterminated literal can be trusted.
    """
    sql = "select 'unterminated ; reset all"

    with pytest.raises(ReadOnlyError):
        raise_on_readonly_disarm(StubConnection(), sql)
