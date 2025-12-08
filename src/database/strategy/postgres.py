"""
PostgreSQL-specific strategy implementation.

This module implements the DatabaseStrategy interface with PostgreSQL-specific operations.
It handles PostgreSQL's unique features such as:
- VACUUM and ANALYZE for table optimization
- REINDEX for index rebuilding
- CLUSTER for physical reordering of table data
- Sequence management for auto-increment columns
- Metadata retrieval using PostgreSQL system catalogs
"""
import logging
import re

from database.core.query import execute
from database.core.transaction import Transaction
from database.operations.query import select, select_column
from database.strategy.base import DatabaseStrategy
from database.utils.auto_commit import enable_auto_commit

logger = logging.getLogger(__name__)


class PostgresStrategy(DatabaseStrategy):
    """PostgreSQL-specific operations"""

    def vacuum_table(self, cn, table):
        """Optimize a table with VACUUM"""
        # Save current autocommit state
        original_autocommit = cn.connection.autocommit
        try:
            # Set autocommit to True for VACUUM
            cn.connection.autocommit = True
            quoted_table = self.quote_identifier(table)
            execute(cn, f'vacuum (full, analyze) {quoted_table}')
        finally:
            # Restore original autocommit state
            cn.connection.autocommit = original_autocommit

    def reindex_table(self, cn, table):
        """Rebuild indexes for a table"""
        # Save current autocommit state
        original_autocommit = cn.connection.autocommit
        try:
            # Set autocommit to True for REINDEX
            cn.connection.autocommit = True
            quoted_table = self.quote_identifier(table)
            execute(cn, f'reindex table {quoted_table}')
        finally:
            # Restore original autocommit state
            cn.connection.autocommit = original_autocommit

    def cluster_table(self, cn, table, index=None):
        """Order table data according to an index"""
        original_autocommit = cn.connection.autocommit
        try:
            # Set autocommit to True for CLUSTER
            cn.connection.autocommit = True
            quoted_table = self.quote_identifier(table)

            if index is None:
                execute(cn, f'cluster {quoted_table}')
            else:
                quoted_index = self.quote_identifier(index)
                execute(cn, f'cluster {quoted_table} using {quoted_index}')
        finally:
            cn.connection.autocommit = original_autocommit

    def reset_sequence(self, cn, table, identity=None):
        """Reset the sequence for a table"""
        if identity is None:
            identity = self.find_sequence_column(cn, table)

        quoted_table = self.quote_identifier(table)
        quoted_identity = self.quote_identifier(identity)

        sql = f"""
select
    setval(pg_get_serial_sequence('{table}', '{identity}'), coalesce(max({quoted_identity}),0)+1, false)
from
{quoted_table}
"""
        if isinstance(cn, Transaction):
            cn.execute(sql)
        else:
            execute(cn, sql)

        logger.debug(f'Reset sequence for {table=} using {identity=}')

    def get_primary_keys(self, cn, table, bypass_cache=False):
        """Get primary key columns for a table

        Args:
            cn: Database connection object
            table: Table name to get primary keys for
            bypass_cache: If True, bypass cache and query database directly, by default False

        Returns
            list: List of primary key column names
        """
        sql = """
select a.attname as column
from pg_index i
join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey)
where i.indrelid = %s::regclass and i.indisprimary
"""
        return select_column(cn, sql, table)

    def get_columns(self, cn, table, bypass_cache=False):
        """Get all columns for a table

        Args:
            cn: Database connection object
            table: Table name to get columns for
            bypass_cache: If True, bypass cache and query database directly, by default False

        Returns
            list: List of column names for the specified table
        """
        # This PostgreSQL-specific query gives all columns
        sql = f"""
select skeys(hstore(null::{table})) as column
    """
        return select_column(cn, sql)

    def get_sequence_columns(self, cn, table, bypass_cache=False):
        """Get columns with sequences

        Args:
            cn: Database connection object
            table: Table name to get sequence columns for
            bypass_cache: If True, bypass cache and query database directly, by default False

        Returns
            list: List of sequence/identity column names for the specified table
        """
        sql = """
        SELECT column_name as column
        FROM information_schema.columns
        WHERE table_name = %s
        AND column_default LIKE 'nextval%%'
        """
        return select_column(cn, sql, table)

    def configure_connection(self, conn):
        """Configure connection settings for PostgreSQL"""

        # Set auto-commit for PostgreSQL connections
        enable_auto_commit(conn)

    def quote_identifier(self, identifier):
        """Quote an identifier for PostgreSQL"""
        return '"' + identifier.replace('"', '""') + '"'

    def get_constraint_definition(self, cn, table, constraint_name):
        """Get the definition of a constraint or unique index by name

        Args:
            cn: Database connection object
            table: The table containing the constraint
            constraint_name: Name of the constraint or unique index

        Returns
            str: SQL expression defining the constraint for use in ON CONFLICT
        """
        # Extract table and schema names
        parts = table.split('.')
        schema_name = parts[0].strip('"') if len(parts) > 1 else 'public'
        table_name = parts[-1].strip('"')

        # Use a UNION query to check both constraints and indexes in one go
        union_query = """
        -- Check for indexes
        SELECT
            indexdef AS definition,
            'index' AS source
        FROM
            pg_indexes
        WHERE
            indexname = %s
            AND tablename = %s
            AND indexdef ~ 'CREATE UNIQUE INDEX'

        UNION ALL

        -- Check for primary key
        SELECT
            pg_get_constraintdef(c.oid) AS definition,
            'constraint' AS source
        FROM
            pg_constraint c
            JOIN pg_class tbl ON c.conrelid = tbl.oid
            JOIN pg_namespace n ON tbl.relnamespace = n.oid
        WHERE
            c.conname = %s
            AND tbl.relname = %s
            AND c.contype IN ('c', 'p', 'u')

        """

        result = select(cn, union_query, constraint_name, table_name, constraint_name, table_name)

        if not result:
            raise ValueError(f"Constraint or unique index '{constraint_name}' not found on table '{table}'.")

        # Process result based on source
        definition = result[0]['definition']
        source = result[0]['source']

        definition = definition.strip()

        if source == 'constraint':
            # For constraints, extract column list from UNIQUE/PRIMARY KEY constraint
            match = re.search(r'(?:UNIQUE|PRIMARY KEY)\s*\(([^)]+)\)', definition, re.IGNORECASE)
            if match:
                return match.group(1)
            # For complex constraints with expressions, try to extract the column list
            # This handles cases with COALESCE or other expressions
            match = re.search(r'\(([^)]+)\)', definition)
            if match:
                return match.group(1)
        else:
            # For indexes, extract column list from CREATE INDEX statement
            return extract_index_definition(definition)

        raise ValueError(f'Failed to extract regex from definition: {definition}')

    def get_default_columns(self, cn, table, bypass_cache=False):
        """Get columns suitable for general data display

        Args:
            cn: Database connection object
            table: Table name to get default columns for
            bypass_cache: If True, bypass cache and query database directly, by default False

        Returns
            list: List of column names suitable for general data representation
        """
        sql = """
select
t.column_name
from information_schema.columns t
where
t.table_name = %s
and t.data_type in ('character', 'character varying', 'boolean',
    'text', 'double precision', 'real', 'integer', 'date',
    'time without time zone', 'timestamp without time zone')
order by
t.ordinal_position
"""
        return select_column(cn, sql, table)

    def get_ordered_columns(self, cn, table, bypass_cache=False):
        """Get all column names for a table ordered by their position

        Args:
            cn: Database connection object
            table: Table name to get columns for
            bypass_cache: If True, bypass cache and query database directly, by default False

        Returns
            list: List of column names ordered by position
        """
        sql = """
select
t.column_name
from information_schema.columns t
where
t.table_name = %s
order by
t.ordinal_position
"""
        return select_column(cn, sql, table)

    def find_sequence_column(self, cn, table, bypass_cache=False):
        """Find the best column to reset sequence for

        Args:
            cn: Database connection object
            table: Table name to analyze for sequence columns
            bypass_cache: If True, bypass cache and query database directly, by default False

        Returns
            str: Column name best suited for sequence resetting
        """
        # Use the common implementation from base class
        return self._find_sequence_column_impl(cn, table, bypass_cache=bypass_cache)


def extract_index_definition(definition):
    """Extract column list and WHERE clause from a PostgreSQL unique index definition.

    This function parses PostgreSQL CREATE UNIQUE INDEX statements to extract:
    1. The column specification, which may include functions like COALESCE
    2. Any WHERE clause that makes the uniqueness conditional

    Args:
        definition: The full CREATE UNIQUE INDEX statement

    Returns
        str: Column specification with optional WHERE clause for use in ON CONFLICT

    Raises
        ValueError: If the column definition cannot be extracted
    """
    # Pattern to extract column list and optional WHERE clause from index definitions
    # This handles complex cases including:
    # - Schema-qualified tables
    # - Different index types (btree, hash, etc.)
    # - Complex column expressions with functions like COALESCE
    # - WHERE clauses for partial indexes
    # - NULLS NOT DISTINCT option
    pattern = r'CREATE\s+UNIQUE\s+INDEX\s+\w+\s+ON\s+(?:[a-zA-Z0-9_]+\.)?[a-zA-Z0-9_]+(?:\s+USING\s+\w+)?\s+(\(.*?\))(?:\s+NULLS\s+NOT\s+DISTINCT)?(?:\s+WHERE\s+(.*?))?$'

    match = re.search(pattern, definition)
    if match:
        column_clause = match.group(1)
        where_clause = match.group(2)
        if where_clause:
            return f'{column_clause} WHERE {where_clause}'
        else:
            return column_clause

    # If regex didn't match, try a simpler fallback approach
    # Look for parentheses that likely contain the column definitions
    paren_match = re.search(r'\(([^()]*(?:\([^()]*\)[^()]*)*)\)', definition)
    if paren_match:
        column_def = paren_match.group(0)  # Get the entire parenthetical expression

        # Check for WHERE clause after the column definition
        where_match = re.search(r'\)\s+WHERE\s+(.*?)(?:\s*$|\s+NULLS)', definition)
        if where_match:
            return f'{column_def} WHERE {where_match.group(1)}'
        return column_def

    raise ValueError(f'Failed to extract column definition from index: {definition}')
