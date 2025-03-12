"""
Tests to verify SQLAlchemy-related documentation and API compatibility.
"""
import inspect
import pytest

from database import connect, Transaction
from database.core.connection import ConnectionWrapper
from database.utils.connection_utils import (
    get_engine_for_options, 
    create_url_from_options,
    dispose_all_engines
)


class TestSQLAlchemyDocumentation:
    """Verify that SQLAlchemy integration is properly documented."""
    
    def test_connect_docstring(self):
        """Verify that connect() function has SQLAlchemy documentation."""
        doc = connect.__doc__
        assert doc is not None
        assert "SQLAlchemy" in doc
        assert "connection management" in doc
        
    def test_connection_wrapper_docstring(self):
        """Verify that ConnectionWrapper has SQLAlchemy documentation."""
        doc = ConnectionWrapper.__doc__
        assert doc is not None
        assert "SQLAlchemy" in doc
        assert "connection object" in doc
        
    def test_connection_utils_docstrings(self):
        """Verify that SQLAlchemy utility functions are documented."""
        for func in [get_engine_for_options, create_url_from_options, dispose_all_engines]:
            doc = func.__doc__
            assert doc is not None
            assert "SQLAlchemy" in doc or "engine" in doc
            
    def test_transaction_docstring(self):
        """Verify that Transaction class documents SQLAlchemy usage."""
        doc = Transaction.__doc__
        assert doc is not None
        # Should mention using raw connections through SQLAlchemy
        assert "SQLAlchemy" in doc or "driver_connection" in doc
