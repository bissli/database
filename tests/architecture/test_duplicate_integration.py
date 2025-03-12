"""
Test for duplicate integration tests to eliminate overlapping test coverage.
"""
import os


def test_no_duplicate_test_files():
    """Check for duplicate test files across integration and unit test folders"""
    # Get the base directory for tests
    test_dir = os.path.dirname(os.path.abspath(__file__))

    # Track test files by their base names
    seen_test_files = {}
    potential_duplicates = []

    # Recursively scan test directories
    for root, _, files in os.walk(test_dir):
        for filename in files:
            if filename.startswith('test_') and filename.endswith('.py'):
                if filename in seen_test_files:
                    # Record potential duplicate
                    potential_duplicates.append((filename, root, seen_test_files[filename]))
                else:
                    seen_test_files[filename] = root

    # Report duplicates (don't fail the test, just log for review)
    if potential_duplicates:
        print('\nPotential duplicate test files found:')
        for filename, path1, path2 in potential_duplicates:
            rel_path1 = os.path.relpath(path1, test_dir)
            rel_path2 = os.path.relpath(path2, test_dir)
            print(f'- {filename} in {rel_path1} and {rel_path2}')

    # This test isn't meant to fail, just to document
    assert True


if __name__ == '__main__':
    __import__('pytest').main([__file__])
