import importlib
import sys
from pathlib import Path


def test_circular_dependencies():
    """Test if modules can be imported without circular dependencies"""
    # Base directory for database package
    base_dir = Path('src')

    # Add src to path so imports work
    sys.path.insert(0, str(base_dir.parent))

    modules = [
        'database.exceptions',
        'database.cache',
        'database.sql',
        'database.types',
        'database.options',
        'database.strategy',
        'database.strategy.base',
        'database.strategy.postgres',
        'database.strategy.sqlite',
        'database.cursor',
        'database.connection',
        'database.transaction',
        'database',
    ]

    # Test each module in sequence
    results = {}
    for module in modules:
        print(f'Checking {module}... ', end='')
        try:
            # Try to import the module (don't delete from cache to avoid corrupting state)
            importlib.import_module(module)
            print('✓ Success')
            results[module] = True
        except Exception as e:
            print(f'✗ Failed: {e}')
            results[module] = False

    # Report summary
    success = sum(1 for v in results.values() if v)
    total = len(results)
    print(f'\nSummary: {success}/{total} modules imported successfully')

    # List failures
    failures = [m for m, v in results.items() if not v]
    if failures:
        print('\nFailed modules:')
        for module in failures:
            print(f'  - {module}')

    # Assert for pytest
    assert success == total, f'{len(failures)} modules failed circular dependency check'


if __name__ == '__main__':
    __import__('pytest').main([__file__])
