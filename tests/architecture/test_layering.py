"""Architecture invariants for the database library.

Each test encodes a structural rule that must hold permanently.
Violations indicate someone broke a layering contract.
"""
import ast
import pathlib

_SRC = pathlib.Path(__file__).parent.parent.parent / 'src' / 'database'
_STRATEGY_SRC = _SRC / 'strategy'
_UNIT_TESTS = pathlib.Path(__file__).parent.parent / 'unit'


def _build_parent_map(tree: ast.AST) -> dict[int, ast.AST]:
    """Return {id(child): parent_node} for every node in tree."""
    parent_map: dict[int, ast.AST] = {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            parent_map[id(child)] = node
    return parent_map


def _is_inside_type_checking(node: ast.AST,
                             parent_map: dict[int, ast.AST]) -> bool:
    """Return True if node is nested inside an 'if TYPE_CHECKING:' block."""
    parent = parent_map.get(id(node))
    while parent is not None:
        if isinstance(parent, ast.If):
            test = parent.test
            if isinstance(test, ast.Name) and test.id == 'TYPE_CHECKING':
                return True
            if isinstance(test, ast.Attribute) and test.attr == 'TYPE_CHECKING':
                return True
        parent = parent_map.get(id(parent))
    return False


def _connection_imports_outside_type_checking(py_file: pathlib.Path) -> list[str]:
    tree = ast.parse(py_file.read_text())
    parent_map = _build_parent_map(tree)
    violations = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom):
            continue
        if not (node.module or '').startswith('database.connection'):
            continue
        if not _is_inside_type_checking(node, parent_map):
            names = [a.name for a in node.names]
            violations.append(
                f'{py_file.name}:{node.lineno}: '
                f'from database.connection import {names} (not in TYPE_CHECKING)'
            )
    return violations


def test_strategy_connection_imports_are_type_checking_only():
    """strategy/*.py must only import database.connection under TYPE_CHECKING.

    The strategy layer accepts ConnectionWrapper as a parameter and uses
    it via duck typing. Importing the wrapper at runtime would
    re-introduce the circular import that was eliminated in commit
    1b2cf70.
    """
    all_violations = []
    for py_file in sorted(_STRATEGY_SRC.glob('*.py')):
        if py_file.name == '__init__.py':
            continue
        all_violations.extend(_connection_imports_outside_type_checking(py_file))

    assert not all_violations, (
        'strategy/ files import database.connection outside TYPE_CHECKING:\n'
        + '\n'.join(f'  {v}' for v in all_violations)
    )


def test_unit_tests_do_not_import_connection_module():
    """tests/unit/*.py must not import from database.connection directly.

    Utility helpers (get_dialect_name, ensure_commit) live in
    database.utils. Importing database.connection pulls in SQLAlchemy,
    psycopg, pandas, etc. and couples unit tests to the heavy layer.
    """
    violations = []
    for py_file in sorted(_UNIT_TESTS.glob('test_*.py')):
        tree = ast.parse(py_file.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom):
                continue
            if (node.module or '').startswith('database.connection'):
                names = [a.name for a in node.names]
                violations.append(
                    f'{py_file.name}:{node.lineno}: '
                    f'from database.connection import {names}'
                )

    assert not violations, (
        'Unit tests import from database.connection '
        '(use database.utils instead):\n'
        + '\n'.join(f'  {v}' for v in violations)
    )


def test_all_registered_strategies_are_concrete():
    """Every dialect in _STRATEGY_REGISTRY must have no unimplemented
    abstract methods — otherwise the class can't be instantiated.
    """
    from database.strategy import _STRATEGY_REGISTRY

    unimplemented = {}
    for dialect, cls in _STRATEGY_REGISTRY.items():
        missing = cls.__abstractmethods__
        if missing:
            unimplemented[dialect] = sorted(missing)

    assert not unimplemented, (
        'Registered strategies have unimplemented abstract methods:\n'
        + '\n'.join(f'  {d}: {ms}' for d, ms in unimplemented.items())
    )


# Snapshot of the public API. Update this set whenever a name is
# intentionally added or removed from `database.__all__`; the test will
# fail to force a deliberate review of the API change.
_EXPECTED_ALL: frozenset[str] = frozenset({
    'Column',
    'ConnectionFailure',
    'ConnectionWrapper',
    'DatabaseError',
    'DatabaseOptions',
    'DbConnectionError',
    'IntegrityError',
    'IntegrityViolationError',
    'OperationalError',
    'ProgrammingError',
    'QueryError',
    'TypeConversionError',
    'UniqueViolation',
    'ValidationError',
    'cluster_table',
    'connect',
    'copy_from',
    'delete',
    'execute',
    'insert',
    'insert_row',
    'insert_rows',
    'reindex_table',
    'reset_table_sequence',
    'select',
    'select_column',
    'select_row',
    'select_row_or_none',
    'select_scalar',
    'select_scalar_or_none',
    'transaction',
    'update',
    'update_or_insert',
    'update_row',
    'upsert_rows',
    'vacuum_table',
})


def test_public_api_snapshot():
    """database.__all__ must match the expected snapshot.

    Added names: update _EXPECTED_ALL with the new names in the same
    commit and explain the addition in the commit message.

    Removed names: check all callers before removing — pre-1.0, this is
    still a breaking change for downstream consumers.
    """
    import database

    actual = frozenset(database.__all__)
    added = actual - _EXPECTED_ALL
    removed = _EXPECTED_ALL - actual

    messages = []
    if added:
        messages.append(
            'Names added to __all__ (update _EXPECTED_ALL if intentional): '
            f'{sorted(added)}'
        )
    if removed:
        messages.append(
            'Names removed from __all__ (breaking change — check callers): '
            f'{sorted(removed)}'
        )

    assert not messages, '\n'.join(messages)
