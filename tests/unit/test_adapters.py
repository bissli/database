import unittest
from unittest.mock import MagicMock, patch
import datetime
import numpy as np
import pandas as pd
import pytest

from database.adapters import TypeConverter, register_adapters


class TestTypeConverter:
    """Test suite for TypeConverter class"""

    def test_convert_value_none(self):
        """Test conversion of None values"""
        assert TypeConverter.convert_value(None) is None

    def test_convert_numpy_float(self):
        """Test conversion of numpy float types"""
        # Test regular float
        assert TypeConverter.convert_value(np.float64(42.5)) == 42.5
        assert TypeConverter.convert_value(np.float32(42.5)) == 42.5
        assert TypeConverter.convert_value(np.float16(42.5)) == 42.5
        
        # Test NaN conversion to None
        assert TypeConverter.convert_value(np.float64('nan')) is None
        
        # Test infinity
        assert TypeConverter.convert_value(np.float64('inf')) == float('inf')

    def test_convert_numpy_int(self):
        """Test conversion of numpy integer types"""
        assert TypeConverter.convert_value(np.int64(42)) == 42
        assert TypeConverter.convert_value(np.int32(42)) == 42
        assert TypeConverter.convert_value(np.int16(42)) == 42
        assert TypeConverter.convert_value(np.int8(42)) == 42
        
        # Test unsigned integers
        assert TypeConverter.convert_value(np.uint64(42)) == 42
        assert TypeConverter.convert_value(np.uint32(42)) == 42
        assert TypeConverter.convert_value(np.uint16(42)) == 42
        assert TypeConverter.convert_value(np.uint8(42)) == 42

    def test_convert_pandas_na(self):
        """Test conversion of pandas NA values"""
        # Test pd.NA
        assert TypeConverter.convert_value(pd.NA) is None
        
        # Test pandas nullable integer
        val = pd.Series([1, 2, None], dtype="Int64")[2]
        assert TypeConverter.convert_value(val) is None
        
        val = pd.Series([1, 2, 3], dtype="Int64")[0]
        assert TypeConverter.convert_value(val) == 1

    def test_convert_params_list(self):
        """Test conversion of parameter list"""
        params = [1, np.float64(2.5), None, np.int32(3), np.float64('nan')]
        converted = TypeConverter.convert_params(params)
        assert converted == [1, 2.5, None, 3, None]

    def test_convert_params_dict(self):
        """Test conversion of parameter dict"""
        params = {
            'a': 1, 
            'b': np.float64(2.5), 
            'c': None, 
            'd': np.int32(3), 
            'e': np.float64('nan')
        }
        converted = TypeConverter.convert_params(params)
        assert converted == {'a': 1, 'b': 2.5, 'c': None, 'd': 3, 'e': None}


class TestAdapterRegistration:
    """Test suite for adapter registration"""
    
    @patch('psycopg.adapters.register_dumper')
    @patch('sqlite3.register_adapter')
    def test_register_adapters_global(self, mock_sqlite_register, mock_pg_register):
        """Test global adapter registration"""
        register_adapters()
        
        # Verify appropriate adapter registrations were made
        assert mock_pg_register.call_count > 0
        assert mock_sqlite_register.call_count > 0

    @patch('psycopg.adapters')
    @patch('psycopg.adapt')
    def test_register_adapters_isolated(self, mock_adapt, mock_adapters):
        """Test isolated adapter registration"""
        # Mock the necessary psycopg components
        mock_adapter_map = MagicMock()
        mock_adapt.AdaptersMap.return_value = mock_adapter_map
        
        # Set up the adapter map to handle the CustomNumericLoader registration
        adapter_maps = register_adapters(isolated=True)
        
        # Verify adapter maps were returned
        assert 'postgres' in adapter_maps
        assert 'sqlite' in adapter_maps
        assert 'sqlserver' in adapter_maps
        
        # Verify postgres adapter map is a proper object
        assert adapter_maps['postgres'] is mock_adapter_map
        
        # Verify sqlite adapter function exists
        assert callable(adapter_maps['sqlite'])


if __name__ == '__main__':
    __import__('pytest').main([__file__])
