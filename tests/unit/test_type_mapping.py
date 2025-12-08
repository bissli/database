"""
Tests for simplified type mapping system.
"""
import datetime

import psycopg
import pytest
from database.types import postgres_types, resolve_type, sqlite_types


def get_pg_oid(type_name):
    """Get PostgreSQL OID from type name"""
    return psycopg.postgres.types.get(type_name).oid


class TestPostgresTypeMapping:
    """Tests for PostgreSQL type resolution"""

    def test_postgres_types_populated(self):
        """Verify postgres_types dict is populated"""
        assert len(postgres_types) > 0

    @pytest.mark.parametrize(('pg_type', 'expected_python_type'), [
        ('int4', int),
        ('int8', int),
        ('int2', int),
        ('float8', float),
        ('float4', float),
        ('varchar', str),
        ('text', str),
        ('date', datetime.date),
        ('time', datetime.datetime),
        ('timestamp', datetime.datetime),
        ('timestamptz', datetime.datetime),
        ('bool', bool),
    ], ids=[
        'int4', 'int8', 'int2',
        'float8', 'float4',
        'varchar', 'text',
        'date', 'time', 'timestamp', 'timestamptz',
        'bool'
    ])
    def test_postgres_type_resolution(self, pg_type, expected_python_type):
        """Test PostgreSQL type code resolves to correct Python type"""
        result = resolve_type('postgresql', get_pg_oid(pg_type))
        assert result == expected_python_type


class TestSqliteTypeMapping:
    """Tests for SQLite type resolution"""

    def test_sqlite_types_populated(self):
        """Verify sqlite_types dict is populated"""
        assert len(sqlite_types) > 0

    @pytest.mark.parametrize(('sqlite_type', 'expected_python_type'), [
        ('INTEGER', int),
        ('REAL', float),
        ('TEXT', str),
        ('NUMERIC(10,2)', float),
        ('DATE', datetime.date),
        ('DATETIME', datetime.datetime),
    ], ids=[
        'integer', 'real', 'text', 'numeric_with_params',
        'date', 'datetime'
    ])
    def test_sqlite_type_resolution(self, sqlite_type, expected_python_type):
        """Test SQLite type name resolves to correct Python type"""
        result = resolve_type('sqlite', sqlite_type)
        assert result == expected_python_type


class TestColumnNamePatterns:
    """Tests for column name-based type resolution"""

    @pytest.mark.parametrize(('dialect', 'column_name', 'expected_type'), [
        # ID columns
        ('postgresql', 'user_id', int),
        ('sqlite', 'id', int),
        ('postgresql', 'record_id', int),
        # Datetime columns (_at suffix)
        ('postgresql', 'created_at', datetime.datetime),
        ('sqlite', 'updated_at', datetime.datetime),
        # Datetime columns (_datetime suffix)
        ('sqlite', 'updated_datetime', datetime.datetime),
        ('postgresql', 'created_datetime', datetime.datetime),
        # Date columns
        ('postgresql', 'birth_date', datetime.date),
        ('sqlite', 'start_date', datetime.date),
        # Boolean columns (is_ prefix)
        ('postgresql', 'is_active', bool),
        ('sqlite', 'is_enabled', bool),
        # Boolean columns (_flag suffix)
        ('sqlite', 'enabled_flag', bool),
        ('postgresql', 'active_flag', bool),
        # Money columns
        ('postgresql', 'total_amount', float),
        ('sqlite', 'price_amount', float),
    ], ids=[
        'pg_user_id', 'sl_id', 'pg_record_id',
        'pg_created_at', 'sl_updated_at',
        'sl_updated_datetime', 'pg_created_datetime',
        'pg_birth_date', 'sl_start_date',
        'pg_is_active', 'sl_is_enabled',
        'sl_enabled_flag', 'pg_active_flag',
        'pg_total_amount', 'sl_price_amount'
    ])
    def test_column_name_type_resolution(self, dialect, column_name, expected_type):
        """Test column name pattern resolves to correct Python type"""
        result = resolve_type(dialect, None, column_name=column_name)
        assert result == expected_type


class TestUnknownTypes:
    """Tests for unknown type handling"""

    @pytest.mark.parametrize(('dialect', 'type_code', 'description'), [
        ('postgresql', 99999, 'unknown PostgreSQL OID'),
        ('sqlite', 'UNKNOWN', 'unknown SQLite type'),
        ('unknown_db', 'type', 'unknown database dialect'),
    ], ids=['unknown_pg_oid', 'unknown_sqlite_type', 'unknown_dialect'])
    def test_unknown_types_default_to_str(self, dialect, type_code, description):
        """Test that unknown types default to str"""
        result = resolve_type(dialect, type_code)
        assert result == str, f'Expected str for {description}'


class TestPythonTypePassthrough:
    """Tests for Python type passthrough behavior"""

    @pytest.mark.parametrize(('dialect', 'python_type'), [
        ('postgresql', int),
        ('sqlite', str),
        ('postgresql', datetime.datetime),
    ], ids=['pg_int', 'sl_str', 'pg_datetime'])
    def test_python_type_returns_unchanged(self, dialect, python_type):
        """Test that passing a Python type returns it unchanged"""
        result = resolve_type(dialect, python_type)
        assert result == python_type


if __name__ == '__main__':
    __import__('pytest').main([__file__])
