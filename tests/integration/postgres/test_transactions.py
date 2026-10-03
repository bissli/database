"""PostgreSQL transactions: returnid over a returning clause, and batches.
"""
import datetime

import database as db
import psycopg
import pytest

TODAY = datetime.date.today()
YESTERDAY = TODAY - datetime.timedelta(days=1)
NEXT_MONTH = TODAY + datetime.timedelta(days=30)

INSERT_SQL = 'insert into test_table (name, value) values (%s, %s)'
UPDATE_SQL = 'update test_table set value = %s where name = %s'


def test_transaction(psql_docker, pg_conn):
    """Verify an update and an insert in one block both take effect.

    Mutation: Transaction.execute dropping its values, or a clean rollback.
    Oracle: hand-written rows for the updated and inserted names.
    """
    with db.transaction(pg_conn) as tx:
        tx.execute(UPDATE_SQL, 91, 'George')
        tx.execute(INSERT_SQL, 'Hannah', 102)

    result = db.select(
        pg_conn,
        'select name, value from test_table where name in (%s, %s) order by name',
        'George', 'Hannah')

    assert [(r['name'], r['value']) for r in result] == [
        ('George', 91), ('Hannah', 102)]


def test_transaction_rollback(psql_docker, pg_conn):
    """Verify a failing statement rolls back the block's earlier update.

    Mutation: __exit__ committing on an exception, or auto-commit left on.
    Oracle: George's staged value 80, after a failing second statement.
    """
    with pytest.raises(psycopg.errors.UndefinedTable):
        with db.transaction(pg_conn) as tx:
            tx.execute(UPDATE_SQL, 999, 'George')
            tx.execute('insert into nonexistent_table values (1)')

    value = db.select_scalar(
        pg_conn, 'select value from test_table where name = %s', 'George')
    assert value == 80


def test_transaction_execute_returning_dict_params(psql_docker, pg_conn):
    """Verify returnid reads the id a returning insert with named values made.

    Mutation: returnid dropping a dict argument, or returning the whole row.
    Oracle: the row read back by the returned id.
    """
    db.execute(pg_conn, """
create temporary table returning_test (
    id serial primary key,
    name text not null,
    value integer not null
)
""")
    insert_sql = """
insert into returning_test (name, value)
values (%(name)s, %(value)s)
returning id
"""
    with db.transaction(pg_conn) as tx:
        result_id = tx.execute(
            insert_sql, {'name': 'TestDict', 'value': 100}, returnid='id')

        assert isinstance(result_id, int)
        row = tx.select_row('select * from returning_test where id = %s', result_id)
        assert row['name'] == 'TestDict'
        assert row['value'] == 100


def test_transaction_execute_returning_multiple_values(psql_docker, pg_conn):
    """Verify a list returnid yields the named columns in list order.

    Mutation: list returnid values in row order, or only the first name.
    Oracle: a returning clause listing inst_id before id, and 33476.
    """
    db.execute(pg_conn, """
create temporary table multi_return_test (
    id serial primary key,
    inst_id integer not null,
    name text not null,
    value integer not null
)
""")
    insert_sql = """
insert into multi_return_test (inst_id, name, value)
values (%(inst_id)s, %(name)s, %(value)s)
returning inst_id, id
"""
    params = {'inst_id': 33476, 'name': 'MultiReturn', 'value': 500}
    with db.transaction(pg_conn) as tx:
        inst_id, id_val = tx.execute(insert_sql, params, returnid=['inst_id', 'id'])

        assert inst_id == 33476
        row = tx.select_row('select * from multi_return_test where id = %s', id_val)
        assert (row['inst_id'], row['name'], row['value']) == (
            33476, 'MultiReturn', 500)


def test_transaction_execute_returning_multiple_rows(psql_docker, pg_conn):
    """Verify a list returnid over several rows yields one list per row.

    Mutation: Transaction.execute unwrapping only the first of several rows.
    Oracle: hand-computed rows, each staged value plus 100.
    """
    db.execute(pg_conn, """
create temporary table multi_row_test (
    id serial primary key,
    category text not null,
    value integer not null
)
""")
    for i, category in enumerate(['electronics', 'clothing', 'food']):
        db.execute(
            pg_conn,
            'insert into multi_row_test (category, value) values (%s, %s)',
            category, i * 10)

    update_sql = """
update multi_row_test
set value = value + 100
returning id, category, value
"""
    with db.transaction(pg_conn) as tx:
        results = tx.execute(update_sql, returnid=['id', 'category', 'value'])

    assert sorted(results) == [
        [1, 'electronics', 100], [2, 'clothing', 110], [3, 'food', 120]]


def test_postgres_hardcoded_literals_transaction(psql_docker, pg_conn):
    """Verify selects built from literals return their values in a block.

    Mutation: prepare_query rewriting a statement with no placeholder.
    Oracle: hand-computed sums of the inserted 10.5 and 20.5.
    """
    with db.transaction(pg_conn) as tx:
        tx.execute('drop table if exists literal_test')
        tx.execute(
            'create table literal_test (id int, name varchar(50), value decimal(10,2))')

    try:
        with db.transaction(pg_conn) as tx:
            tx.execute(
                "insert into literal_test (id, name, value) values (1, 'Test', 10.5)")

            assert tx.select('select * from literal_test where id = 1')[0]['id'] == 1
            by_param = tx.select('select * from literal_test where id = %s', 1)
            assert by_param[0]['id'] == 1
            sum_sql = 'select sum(value) as sum_value from literal_test'
            assert tx.select(sum_sql)[0]['sum_value'] == 10.5
            calc_sql = 'select 100.5 + sum(value) as calculated from literal_test'
            assert tx.select(calc_sql)[0]['calculated'] == 111.0
            assert tx.select('select 42 as answer')[0]['answer'] == 42
            assert tx.select("select 'hello' as greeting")[0]['greeting'] == 'hello'

            tx.execute(
                "insert into literal_test (id, name, value) "
                "values (2, 'Another', 20.5)")
            rows = tx.select('select id, name, value from literal_test order by id')
            assert [(r['id'], r['name'], r['value']) for r in rows] == [
                (1, 'Test', 10.5), (2, 'Another', 20.5)]
            count_sql = 'select count(*) as row_count from literal_test where value > 5'
            assert tx.select(count_sql)[0]['row_count'] == 2
            subquery_sql = (
                'select * from (select sum(value) as total from literal_test) t')
            assert tx.select(subquery_sql)[0]['total'] == 31.0
            assert tx.select('select sum(value) from literal_test')[0]['sum'] == 31.0
    finally:
        with db.transaction(pg_conn) as tx:
            tx.execute('drop table literal_test')


def test_transaction_with_multiple_statements(psql_docker, pg_conn):
    """Verify every statement of an unparameterized batch runs in a block.

    Mutation: the batch split so one statement never reaches the driver.
    Oracle: hand-computed salaries, each staged salary plus its raise.
    """
    db.execute(pg_conn, """
create temporary table employee_data (
    id serial primary key,
    dept text not null,
    salary integer not null,
    updated_at timestamp
)
""")
    db.execute(pg_conn, """
insert into employee_data (dept, salary, updated_at) values
('sales', 1000, now()),
('marketing', 2000, now()),
('engineering', 3000, now())
""")
    batch_sql = """
insert into employee_data (dept, salary, updated_at) values ('finance', 4000, now());
insert into employee_data (dept, salary, updated_at) values ('hr', 2500, now());
update employee_data set salary = salary + 100 where dept = 'sales';
update employee_data set salary = salary + 200 where dept = 'marketing';
update employee_data set salary = salary + 300, updated_at = now() where dept = 'engineering'
"""
    with db.transaction(pg_conn) as tx:
        tx.execute(batch_sql)

    result = db.select(pg_conn, 'select dept, salary from employee_data order by dept')

    assert [(r['dept'], r['salary']) for r in result] == [
        ('engineering', 3300),
        ('finance', 4000),
        ('hr', 2500),
        ('marketing', 2200),
        ('sales', 1100),
        ]


def test_transaction_with_complex_multiple_statements(psql_docker, pg_conn):
    """Verify a batch of inserts and correlated updates runs in a block.

    Mutation: splitting on a blank line or inside a subquery.
    Oracle: hand-worked parent ids; rows 3 and 4 tie on code 1002.
    """
    db.execute(pg_conn, """
create temporary table item (
    id serial primary key,
    category text not null,
    group_name text,
    duplicate_id integer,
    parent_id integer,
    item_code text
)
""")
    db.execute(pg_conn, """
insert into item (category, group_name, duplicate_id, parent_id, item_code) values
('Book', 'Primary', null, null, '1001'),
('Book', 'Secondary', null, null, '1001'),
('Book', 'Primary', null, null, '1002'),
('Magazine', 'Primary', null, null, '1002'),
('Video', 'Tertiary', null, null, '2001')
""")
    batch_sql = """
insert into item (category, group_name, duplicate_id, parent_id, item_code)
values ('eBook', 'Primary', null, null, '3001');

insert into item (category, group_name, duplicate_id, parent_id, item_code)
values ('Software', 'Secondary', null, null, '4001');

update item i
set parent_id = x.ref_id
from (
    select
        i.id as ref_id,
        i.item_code
    from item i
    where
        i.group_name = 'Primary'
        and i.duplicate_id is null
        and i.parent_id is null
        and i.item_code is not null
        and i.category in ('Book', 'Magazine')
) x
where i.item_code = x.item_code;

update item i
set parent_id = x.ref_id
from (
    select
        i.id as ref_id,
        i.category
    from item i
    where
        i.group_name = 'Primary'
        and i.duplicate_id is null
        and i.parent_id is null
        and i.category is not null
        and i.category in ('Book', 'Magazine')
) x
where i.category = x.category and i.parent_id is null;
"""
    with db.transaction(pg_conn) as tx:
        tx.execute(batch_sql)

    result = db.select(
        pg_conn,
        'select id, category, group_name, parent_id, item_code from item order by id')
    parent_by_id = {r['id']: r['parent_id'] for r in result}
    assert {k: parent_by_id[k] for k in (1, 2, 5, 6, 7)} == {
        1: 1, 2: 1, 5: None, 6: None, 7: None}
    assert parent_by_id[3] in {3, 4}
    assert parent_by_id[4] in {3, 4}
    assert [(r['category'], r['group_name'], r['item_code']) for r in result[5:]] == [
        ('eBook', 'Primary', '3001'), ('Software', 'Secondary', '4001')]


PARAM_TEST_POSITIONAL_SQL = """
insert into param_test (name, status, created_date)
values ('item1', 'active', %s);

insert into param_test (name, status, created_date, modified_date)
values ('item2', 'pending', %s, %s);

update param_test
set status = 'approved', modified_date = %s
where name = 'item1';
"""

PARAM_TEST_NAMED_SQL = """
insert into param_test (name, status, created_date)
values ('item1', 'active', %(yesterday)s);

insert into param_test (name, status, created_date, modified_date)
values ('item2', 'pending', %(yesterday)s, %(today)s);

update param_test
set status = 'approved', modified_date = %(today)s
where name = 'item1';
"""


@pytest.mark.parametrize(('batch_sql', 'params'), [
    (PARAM_TEST_POSITIONAL_SQL, (YESTERDAY, YESTERDAY, TODAY, TODAY)),
    (PARAM_TEST_NAMED_SQL, ({'yesterday': YESTERDAY, 'today': TODAY},)),
    ], ids=['positional', 'named'])
def test_multi_statement_values_bind_per_statement(
        psql_docker, pg_conn, batch_sql, params):
    """Verify values across a parameterized batch bind to their own statements.

    Mutation: every statement given all values, or a wrong positional offset.
    Oracle: hand-written dates; a one-off shift puts TODAY in created_date.
    """
    db.execute(pg_conn, """
create temporary table param_test (
    id serial primary key,
    name text not null,
    status text not null,
    created_date date,
    modified_date date
)
""")
    with db.transaction(pg_conn) as tx:
        tx.execute(batch_sql, *params)

    result = db.select(pg_conn, """
select name, status, created_date, modified_date
from param_test
order by name
""")
    assert [
        (r['name'], r['status'], r['created_date'], r['modified_date'])
        for r in result
        ] == [
        ('item1', 'approved', YESTERDAY, TODAY),
        ('item2', 'pending', YESTERDAY, TODAY),
        ]


MIX_PARAM_POSITIONAL_SQL = """
update mix_param_test
set status = 'updated'
where status = 'active';

update mix_param_test
set expire_date = %s
where status = 'pending';

update mix_param_test
set expire_date = %s
where status = 'inactive';
"""

MIX_PARAM_NAMED_SQL = """
update mix_param_test
set status = 'updated'
where status = 'active';

update mix_param_test
set expire_date = %(today)s
where status = 'pending';

update mix_param_test
set expire_date = %(future)s
where status = 'inactive';
"""


@pytest.mark.parametrize(('batch_sql', 'params'), [
    (MIX_PARAM_POSITIONAL_SQL, (TODAY, NEXT_MONTH)),
    (MIX_PARAM_NAMED_SQL, ({'today': TODAY, 'future': NEXT_MONTH},)),
    ], ids=['positional', 'named'])
def test_multi_statement_mixes_bound_and_bare_statements(
        psql_docker, pg_conn, batch_sql, params):
    """Verify a batch runs a statement with no placeholder among bound ones.

    Mutation: skipping, or binding values to, the bare leading statement.
    Oracle: hand-written status and dates per row.
    """
    db.execute(pg_conn, """
create temporary table mix_param_test (
    id serial primary key,
    name text not null,
    status text not null,
    last_updated timestamp,
    expire_date date
)
""")
    db.execute(pg_conn, """
insert into mix_param_test (name, status, last_updated, expire_date) values
('item1', 'active', now(), null),
('item2', 'pending', now(), null),
('item3', 'inactive', now(), null)
""")
    with db.transaction(pg_conn) as tx:
        tx.execute(batch_sql, *params)

    result = db.select(
        pg_conn, 'select name, status, expire_date from mix_param_test order by name')
    assert [(r['name'], r['status'], r['expire_date']) for r in result] == [
        ('item1', 'updated', None),
        ('item2', 'pending', TODAY),
        ('item3', 'inactive', NEXT_MONTH),
        ]


def test_multi_statement_matrix(psql_docker, pg_conn):
    """Verify batches with every mix of bound and bare statements.

    Mutation: values misaligned after a bare statement, or a dropped statement.
    Oracle: hand-computed rows after each batch.
    """
    db.execute(pg_conn, """
create temporary table tx_matrix_test (
    id serial primary key,
    name text not null,
    category text,
    value integer,
    created_at timestamp default now()
)
""")

    def values_where(condition, *args):
        sql = f'select name, value from tx_matrix_test where {condition} order by id'
        rows = db.select(pg_conn, sql, *args)
        return {r['name']: r['value'] for r in rows}

    with db.transaction(pg_conn) as tx:
        tx.execute("""
insert into tx_matrix_test (name, category, value) values ('tx-no-param-1', 'fixed', 101);
insert into tx_matrix_test (name, category, value) values ('tx-no-param-2', 'fixed', 102);
""")
    assert values_where("category = 'fixed'") == {
        'tx-no-param-1': 101, 'tx-no-param-2': 102}

    with db.transaction(pg_conn) as tx:
        tx.execute("""
insert into tx_matrix_test (name, category, value) values ('tx-all-param-1', %s, %s);
insert into tx_matrix_test (name, category, value) values ('tx-all-param-2', %s, %s);
""", 'param-category', 100, 'param-category', 200)
    assert values_where('category = %s', 'param-category') == {
        'tx-all-param-1': 100, 'tx-all-param-2': 200}

    with db.transaction(pg_conn) as tx:
        tx.execute("""
insert into tx_matrix_test (name, category, value) values ('tx-mixed-1', 'mixed-fixed', 201);
insert into tx_matrix_test (name, category, value) values ('tx-mixed-2', %s, %s);
insert into tx_matrix_test (name, category, value) values ('tx-mixed-3', 'mixed-fixed', 203);
""", 'mixed-params', 202)
    assert values_where("name like 'tx-mixed-%'") == {
        'tx-mixed-1': 201, 'tx-mixed-2': 202, 'tx-mixed-3': 203}

    with db.transaction(pg_conn) as tx:
        tx.execute("""
insert into tx_matrix_test (name, category, value) values ('tx-chain-1', 'chain', 301);
insert into tx_matrix_test (name, category, value) values ('tx-chain-2', 'chain', %s);
update tx_matrix_test set value = 311 where name = 'tx-chain-1';
update tx_matrix_test set value = %s where name = 'tx-chain-2';
delete from tx_matrix_test where name = 'tx-no-param-1';
delete from tx_matrix_test where value = %s;
""", 302, 320, 100)
    assert values_where("category = 'chain'") == {'tx-chain-1': 311, 'tx-chain-2': 320}
    assert values_where("name = 'tx-no-param-1' or value = 100") == {}

    with db.transaction(pg_conn) as tx:
        id1 = tx.execute("""
insert into tx_matrix_test (name, category, value)
values ('return-test-1', 'return-cat', %s)
returning id
""", 500, returnid='id')
        id2 = tx.execute("""
insert into tx_matrix_test (name, category, value)
values ('return-test-2', 'return-cat', 502)
returning id
""", returnid='id')
        tx.execute("""
update tx_matrix_test set value = value + 10 where category = 'return-cat';
delete from tx_matrix_test where name = 'tx-mixed-1';
update tx_matrix_test set value = %s where id in (
    select id from tx_matrix_test where category = 'return-cat' limit 1
);
""", 600)

    assert id2 == id1 + 1
    assert values_where("category = 'return-cat'") in (
        {'return-test-1': 600, 'return-test-2': 512},
        {'return-test-1': 510, 'return-test-2': 600},
        )
    assert values_where("name = 'tx-mixed-1'") == {}


if __name__ == '__main__':
    __import__('pytest').main([__file__])
