"""
Note: These tests have been moved to tests/integration/test_type_handling.py
and tests/unit/test_column.py which provide more comprehensive testing across
all database types.

This file is kept for backwards compatibility but may be removed in the future.
"""
import pytest
from database import Column


def test_column_import_availability(psql_docker, conn):
    """Test that Column class is directly importable from database module"""
    # Verify Column class is available and has expected methods
    assert hasattr(Column, 'get_names')
    assert hasattr(Column, 'get_column_by_name')
    assert hasattr(Column, 'get_column_types_dict')
    assert hasattr(Column, 'create_empty_columns')


if __name__ == '__main__':
    pytest.main([__file__])
