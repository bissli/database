"""Unit tests for what DatabaseStrategy demands of a subclass.

The reader endpoint is a niche feature: most deployments publish one
endpoint and never ask for a reader. Its session hook must therefore
cost a strategy that ignores it nothing, which means staying off the
abstract set.
"""
import pytest
from database.strategy.base import DatabaseStrategy


def test_session_readonly_hook_is_not_abstract():
    """Verify a strategy written before the reader role still builds.

    Mutation: restoring @abstractmethod on set_session_readonly, which
        breaks every downstream strategy at instantiation over a
        feature it never calls.
    Oracle: the abstract set itself, which must not name the hook.
    """
    assert 'set_session_readonly' not in DatabaseStrategy.__abstractmethods__


def test_a_subclass_implementing_only_the_older_contract_instantiates():
    """Verify the hook alone does not block construction.

    Mutation: restoring @abstractmethod on set_session_readonly. The
        subclass below implements every other abstract member, so it
        builds today and raises TypeError under the mutation.
    Oracle: a subclass built from the abstract set with the hook left
        out, which is exactly the downstream shape v0.1.11 broke.
    """
    members = {
        name: (lambda self, *args, **kwargs: None)
        for name in DatabaseStrategy.__abstractmethods__
        }
    members['dialect_name'] = property(lambda self: 'legacy')
    legacy_strategy = type('LegacyStrategy', (DatabaseStrategy,), members)

    assert legacy_strategy().dialect_name == 'legacy'


def test_the_unimplemented_hook_refuses_a_reader():
    """Verify a dialect with no backstop refuses rather than pretends.

    A no-op default would hand back a connection called read-only that
    nothing holds read-only, and say nothing about it.

    Mutation: replacing the base body with 'pass', which returns a
        writable session under the name of a reader.
    Oracle: NotImplementedError naming the class, raised only when a
        caller actually reaches for the reader role.
    """
    members = {
        name: (lambda self, *args, **kwargs: None)
        for name in DatabaseStrategy.__abstractmethods__
        }
    members['dialect_name'] = property(lambda self: 'legacy')
    strategy = type('LegacyStrategy', (DatabaseStrategy,), members)()

    with pytest.raises(NotImplementedError, match='LegacyStrategy'):
        strategy.set_session_readonly(object())
