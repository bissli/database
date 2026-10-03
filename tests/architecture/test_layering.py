"""Structural rules: layering, public API, source text.
"""
import ast
import pathlib

import database
from database.strategy import _STRATEGY_REGISTRY

_REPO_ROOT = pathlib.Path(__file__).parent.parent.parent
_SRC_ROOT = _REPO_ROOT / 'src'
_TESTS_ROOT = _REPO_ROOT / 'tests'
_STRATEGY_SRC = _SRC_ROOT / 'database' / 'strategy'
_UNIT_TESTS = _TESTS_ROOT / 'unit'


def _is_inside_type_checking(node: ast.AST,
                             parent_map: dict[int, ast.AST]) -> bool:
    """True when node sits inside an `if TYPE_CHECKING:` block at any depth.
    """
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


def test_strategy_connection_imports_are_type_checking_only():
    """Verify strategy/*.py imports database.connection only for typing.

    Mutation: a runtime database.connection import in a strategy module.
    Oracle: an AST walk for an enclosing `if TYPE_CHECKING:` guard.
    """
    all_violations = []
    for py_file in sorted(_STRATEGY_SRC.glob('*.py')):
        if py_file.name == '__init__.py':
            continue
        tree = ast.parse(py_file.read_text())
        parent_map = {
            id(child): node
            for node in ast.walk(tree)
            for child in ast.iter_child_nodes(node)
            }
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom):
                continue
            if not (node.module or '').startswith('database.connection'):
                continue
            if not _is_inside_type_checking(node, parent_map):
                names = [a.name for a in node.names]
                all_violations.append(
                    f'{py_file.name}:{node.lineno}: '
                    f'from database.connection import {names} (not in TYPE_CHECKING)')

    assert not all_violations, (
        'strategy/ files import database.connection outside TYPE_CHECKING:\n'
        + '\n'.join(f'  {v}' for v in all_violations)
    )


def test_unit_tests_do_not_import_connection_module():
    """Verify no tests/unit/*.py file imports from database.connection.

    Mutation: a unit test importing from database.connection over utils.
    Oracle: an AST scan of every `from ... import` in tests/unit.
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
    """Verify every registered strategy class can be instantiated.

    Mutation: a new abstract method on DatabaseStrategy that a registered
        strategy does not implement.
    Oracle: each class's __abstractmethods__, empty for a concrete class.
    """
    unimplemented = {}
    for dialect, cls in _STRATEGY_REGISTRY.items():
        missing = cls.__abstractmethods__
        if missing:
            unimplemented[dialect] = sorted(missing)

    assert not unimplemented, (
        'Registered strategies have unimplemented abstract methods:\n'
        + '\n'.join(f'  {d}: {ms}' for d, ms in unimplemented.items())
    )


_EXPECTED_ALL: frozenset[str] = frozenset({
    'Column',
    'ColumnInfo',
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
    'ReadOnlyError',
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
    """Verify database.__all__ matches the _EXPECTED_ALL snapshot.

    Mutation: a name dropped from or added to database.__all__.
    Oracle: the hand-written _EXPECTED_ALL set.
    """
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
            'Names removed from __all__ (breaking change - check callers): '
            f'{sorted(removed)}'
        )

    assert not messages, '\n'.join(messages)


def test_python_sources_contain_no_non_ascii_characters():
    """Verify every .py file under src/ and tests/ is pure ASCII.

    Mutation: a Unicode dash or arrow written into a .py file.
    Oracle: a scan of every character's ord() against 128.
    """
    py_files = sorted(_SRC_ROOT.rglob('*.py'))
    py_files += sorted(_TESTS_ROOT.rglob('*.py'))
    scanned = {py_file.relative_to(_REPO_ROOT).as_posix() for py_file in py_files}

    roots_reached = {'src/database/sql.py', 'tests/unit/test_sql.py'}
    assert roots_reached <= scanned, (
        f'the walk missed a root - only {len(py_files)} files scanned')

    offenders = []
    for py_file in py_files:
        raw = py_file.read_bytes()
        if raw.isascii():
            continue
        # surrogateescape maps a non-UTF-8 byte above 127 for the scan.
        text = raw.decode('utf-8', errors='surrogateescape')
        for lineno, line in enumerate(text.splitlines(), start=1):
            for col, char in enumerate(line, start=1):
                if ord(char) < 128:
                    continue
                offenders.append(
                    f'{py_file.relative_to(_REPO_ROOT).as_posix()}:'
                    f'{lineno}:{col}: {ascii(char)} (U+{ord(char):04X})'
                )

    assert not offenders, (
        'Non-ASCII characters in Python sources - they mangle over SSM '
        'or S3 and break parsing on Windows:\n'
        + '\n'.join(f'  {o}' for o in offenders)
    )
