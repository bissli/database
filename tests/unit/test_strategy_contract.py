"""Unit tests for what DatabaseStrategy demands of a subclass.
"""
import pytest
from database.strategy import get_strategy
from database.strategy.base import DatabaseStrategy


@pytest.fixture
def legacy_strategy():
    """Strategy implementing every abstract member but the reader hook.
    """
    members = {
        name: (lambda self, *args, **kwargs: None)
        for name in DatabaseStrategy.__abstractmethods__
        if name != 'set_session_readonly'
        }
    members['dialect_name'] = property(lambda self: 'legacy')
    return type('LegacyStrategy', (DatabaseStrategy,), members)()


def test_a_subclass_without_the_reader_hook_instantiates(legacy_strategy):
    """Verify set_session_readonly is optional for a subclass.

    Mutation: @abstractmethod on set_session_readonly.
    Oracle: a subclass implementing every abstract member but the hook.
    """
    assert legacy_strategy.dialect_name == 'legacy'


def test_the_unimplemented_hook_refuses_a_reader(legacy_strategy):
    """Verify a dialect with no read-only setting refuses the reader role.

    Mutation: the base body replaced with 'pass'.
    Oracle: NotImplementedError naming the class.
    """
    with pytest.raises(NotImplementedError, match='LegacyStrategy'):
        legacy_strategy.set_session_readonly(object())


@pytest.mark.parametrize(('method', 'args'), [
    ('list_tables', ()),
    ('table_exists', ('t',)),
    ('describe_columns', ('t',)),
    ('get_unique_indexes', ('t',)),
    ('table_ddl', ('t',)),
    ])
def test_schema_introspection_refuses_on_postgres(method, args):
    """Verify a dialect without schema introspection raises.

    Mutation: a base body returning [] or False.
    Oracle: NotImplementedError naming PostgresStrategy, given cn=None.
    """
    strategy = get_strategy('postgresql')

    with pytest.raises(NotImplementedError, match='PostgresStrategy'):
        getattr(strategy, method)(None, *args)
