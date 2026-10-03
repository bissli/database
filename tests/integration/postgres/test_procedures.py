"""Functions and multi-statement selects against a live PostgreSQL server.
"""
import database as db


def test_procedure_basic(psql_docker, pg_conn):
    """Verify a plpgsql function with semicolons in its body runs.

    Mutation: split_statements cutting at a ';' inside the $$ body.
    Oracle: the first three staged rows by name, listed by hand.
    """
    db.execute(pg_conn, """
create or replace function get_test_data() returns table(name varchar, value integer) as $$
begin
    return query select t.name, t.value from test_table t order by t.name limit 3;
end;
$$ language plpgsql;
""")

    result = db.select(pg_conn, 'select * from get_test_data()')

    assert result == [
        {'name': 'Alice', 'value': 10},
        {'name': 'Bob', 'value': 20},
        {'name': 'Charlie', 'value': 30},
        ]


def test_select_return_all_yields_each_statement_result_set(psql_docker, pg_conn):
    """Verify return_all gives one result set per statement, in order.

    Mutation: process_multiple_result_sets stopping after the first.
    Oracle: the staged rows above 50, then the one-row summary.
    """
    sql = """
select * from test_table where value > 50 order by value desc limit 3;
select 'Summary' as name, count(*)::integer as value from test_table;
"""
    all_results = db.select(pg_conn, sql, return_all=True)

    assert [row['name'] for row in all_results[0]] == ['George', 'Fiona']
    assert all_results[1] == [{'name': 'Summary', 'value': 6}]
    assert len(all_results) == 2

    assert db.select(pg_conn, sql) == all_results[0]
    assert db.select(pg_conn, sql, prefer_first=True) == all_results[0]


def test_select_multi_statement_with_empty_result_sets(psql_docker, pg_conn):
    """Verify empty result sets are kept, one per statement.

    Mutation: process_multiple_result_sets skipping an empty result set.
    Oracle: two statements whose predicates match no staged row.
    """
    sql = """
select * from test_table where 1=0;
select * from test_table where name = 'NonexistentName';
"""
    assert db.select(pg_conn, sql) == []
    assert db.select(pg_conn, sql, return_all=True) == [[], []]


if __name__ == '__main__':
    __import__('pytest').main([__file__])
