"""Percent signs in a multi-statement execute on PostgreSQL.
"""
import database as db


def test_named_multi_statement_keeps_single_percent_in_literal(psql_docker, pg_conn):
    """Verify a literal '%' in a statement without placeholders stores once.

    Mutation: prepare_query's doubled '%' left in the bare statement.
    Oracle: the literal as written, and the hand-chosen bound value.
    """
    sql = """
insert into test_table (name, value) values ('50%', 1);
update test_table set value = %(value)s where name = 'Bob'
"""
    db.execute(pg_conn, sql, {'value': 5})

    names = db.select_column(pg_conn, 'select name from test_table where value = 1')
    assert names == ['50%']
    assert db.select_scalar(pg_conn, "select value from test_table where name = 'Bob'") == 5


def test_positional_multi_statement_keeps_single_percent_in_literal(psql_docker, pg_conn):
    """Verify the positional split stores a literal '%' once.

    Mutation: Cursor._execute_multi_statement keeping the doubled '%'.
    Oracle: the literal as written, and the hand-chosen bound value.
    """
    sql = """
insert into test_table (name, value) values ('50%', 1);
update test_table set value = %s where name = 'Bob'
"""
    db.execute(pg_conn, sql, 5)

    names = db.select_column(pg_conn, 'select name from test_table where value = 1')
    assert names == ['50%']
    assert db.select_scalar(pg_conn, "select value from test_table where name = 'Bob'") == 5


def test_multi_statement_keeps_bare_modulo_without_placeholders(psql_docker, pg_conn):
    """Verify a bare '%' operator runs in a statement without placeholders.

    Mutation: empty parameters passed to the placeholder-free statement.
    Oracle: hand-computed 30 % 7 = 2 for Charlie.
    """
    sql = """
update test_table set value = value % 7 where name = 'Charlie';
update test_table set value = %(value)s where name = 'Bob'
"""
    db.execute(pg_conn, sql, {'value': 5})

    assert db.select_scalar(pg_conn, "select value from test_table where name = 'Charlie'") == 2
