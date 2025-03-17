"""
Tests for PostgreSQL index and constraint definition parsing.

These tests validate the correct extraction of column definitions and WHERE clauses
from PostgreSQL unique index definitions for use in ON CONFLICT clauses.
"""
import pytest
from database.strategy.postgres import extract_index_definition


class TestExtractIndexDefinition:
    """Test the extraction of column definitions from PostgreSQL index definitions."""

    def test_simple_index(self):
        """Test extraction from a simple single-column unique index."""
        definition = 'CREATE UNIQUE INDEX user_id_index ON public.users USING btree (id)'
        expected = '(id)'
        assert extract_index_definition(definition) == expected

    def test_multicolumn_index(self):
        """Test extraction from a multi-column unique index."""
        definition = 'CREATE UNIQUE INDEX products_name_category_vendor_index ON public.products USING btree (name, category, vendor_id)'
        expected = '(name, category, vendor_id)'
        assert extract_index_definition(definition) == expected

    def test_where_clause_index(self):
        """Test extraction from an index with a WHERE clause."""
        definition = 'CREATE UNIQUE INDEX uq_customer_email_active ON public.customers USING btree (email) WHERE is_active'
        expected = '(email) WHERE is_active'
        assert extract_index_definition(definition) == expected

    def test_complex_coalesce_index(self):
        """Test extraction from a complex index with COALESCE expressions."""
        definition = "CREATE UNIQUE INDEX financial_records_date_entity_values_idx ON public.financial_records USING btree (record_date, entity_id, COALESCE(value1, ('-1'::integer)::double precision), COALESCE(value2, ('-1'::integer)::double precision), COALESCE(region, '-'::character varying), COALESCE(manager, '-'::character varying), COALESCE(group_code, '-'::character varying), COALESCE(custom_field, '-'::character varying))"
        expected = "(record_date, entity_id, COALESCE(value1, ('-1'::integer)::double precision), COALESCE(value2, ('-1'::integer)::double precision), COALESCE(region, '-'::character varying), COALESCE(manager, '-'::character varying), COALESCE(group_code, '-'::character varying), COALESCE(custom_field, '-'::character varying))"
        assert extract_index_definition(definition) == expected

    def test_where_and_coalesce(self):
        """Test extraction from an index with both WHERE clause and complex expressions."""
        definition = 'CREATE UNIQUE INDEX account_settings_account_id_setting_type_idx ON public.account_settings USING btree (account_id, setting_type) WHERE (is_default = true)'
        expected = '(account_id, setting_type) WHERE (is_default = true)'
        assert extract_index_definition(definition) == expected

    def test_nulls_not_distinct(self):
        """Test extraction from an index with NULLS NOT DISTINCT option."""
        definition = 'CREATE UNIQUE INDEX account_preferences_user_id_category_option_date_idx ON public.account_preferences USING btree (user_id, category, option, valid_until) NULLS NOT DISTINCT WHERE (is_active = true)'
        expected = '(user_id, category, option, valid_until) WHERE (is_active = true)'
        assert extract_index_definition(definition) == expected

    def test_coalesce_multiple_columns(self):
        """Test extraction from an index with multiple COALESCE expressions."""
        definition = "CREATE UNIQUE INDEX complex_unique_constraint ON test_complex_constraint (id, COALESCE(name, ''), COALESCE(value, -1))"
        expected = "(id, COALESCE(name, ''), COALESCE(value, -1))"
        assert extract_index_definition(definition) == expected
        
    def test_btree_index_with_coalesce(self):
        """Test extraction from a btree index with COALESCE expressions."""
        definition = "CREATE UNIQUE INDEX complex_unique_index ON public.test_complex_index USING btree (id, COALESCE(name, ''), COALESCE(value, -1))"
        expected = "(id, COALESCE(name, ''), COALESCE(value, -1))"
        assert extract_index_definition(definition) == expected

    def test_malformed_definition(self):
        """Test handling of malformed index definitions."""
        definition = 'NOT A VALID INDEX DEFINITION'
        with pytest.raises(ValueError):
            extract_index_definition(definition)


if __name__ == '__main__':
    __import__('pytest').main([__file__])
