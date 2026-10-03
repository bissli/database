"""A to_char result named 'date' keeps its str type on PostgreSQL.
"""
import datetime

import database as db
import pytest
from database.options import iterdict_data_loader
from database.types import Column


def date_is_str_data_loader(data, columns, **kwargs):
    """iterdict_data_loader that first asserts 'date' type and value are str.
    """
    column_names = Column.get_names(columns)
    column_types = Column.get_types(columns)
    date_idx = next(
        (i for i, name in enumerate(column_names) if name.lower() == 'date'), None)

    if date_idx is not None:
        python_type = column_types[date_idx]
        assert python_type is str, (
            f"Expected 'date' column to be str, but got {python_type.__name__}")
        name = columns[date_idx].name
        if data and isinstance(data[0], dict) and name in data[0]:
            value_type = type(data[0][name])
            assert value_type is str, (
                f'Expected date value to be str, but got {value_type.__name__}')

    return iterdict_data_loader(data, columns, **kwargs)


@pytest.fixture
def event_dates(pg_conn):
    """Dates stored in date_test, read through date_is_str_data_loader.
    """
    today = datetime.date.today()
    one_day = datetime.timedelta(days=1)
    dates = (today - one_day, today, today + one_day)

    db.execute(pg_conn, 'drop table if exists date_test')
    db.execute(pg_conn,
               'create table date_test (id serial primary key, event_date date)')
    db.execute(pg_conn, 'insert into date_test (event_date) values (%s), (%s), (%s)',
               *dates)

    original_data_loader = pg_conn.options.data_loader
    pg_conn.options.data_loader = date_is_str_data_loader
    try:
        yield dates
    finally:
        pg_conn.options.data_loader = original_data_loader


def test_postgres_date_string_positional(psql_docker, pg_conn, event_dates):
    """Verify a to_char column named 'date' reads as str, positional binding.

    Mutation: resolving a column's Python type from its name.
    Oracle: each stored date, formatted by hand with isoformat().
    """
    yesterday, today, tomorrow = event_dates
    positional_query = """
select
    (case
        when event_date >= date_trunc('day', %s) and event_date < date_trunc('day', %s)
            then to_char(%s, 'YYYY-MM-DD')
        when event_date >= date_trunc('day', %s) and event_date < date_trunc('day', %s)
            then to_char(%s, 'YYYY-MM-DD')
        when event_date between date_trunc('day', %s) and %s
            then to_char(%s, 'YYYY-MM-DD')
        else 'unknown'
    end) as date,
    count(*) as count,
    max(event_date) as actual_date
from date_test
group by 1
order by date
"""
    with db.transaction(pg_conn) as tx:
        rows = tx.select(positional_query,
                         yesterday, today, yesterday,
                         today, tomorrow, today,
                         tomorrow, tomorrow, tomorrow)

    assert [(row['date'], row['count'], row['actual_date']) for row in rows] == [
        (day.isoformat(), 1, day) for day in event_dates]
    assert all(type(row['count']) is int for row in rows)


def test_postgres_date_string_named(psql_docker, pg_conn, event_dates):
    """Verify a to_char column named 'date' reads as str, named binding.

    Mutation: resolving a column's Python type from its name.
    Oracle: isoformat() dates, and a count of 2 only on ref_date's row.
    """
    yesterday, today, tomorrow = event_dates
    named_query = """
select
    (case
        when event_date >= date_trunc('day', %(yesterday)s) and event_date < date_trunc('day', %(today)s)
            then to_char(%(yesterday)s, 'YYYY-MM-DD')
        when event_date >= date_trunc('day', %(today)s) and event_date < date_trunc('day', %(tomorrow)s)
            then to_char(%(today)s, 'YYYY-MM-DD')
        when event_date between date_trunc('day', %(tomorrow)s) and %(tomorrow)s
            then to_char(%(tomorrow)s, 'YYYY-MM-DD')
        else 'unknown'
    end) as date,
    'TestType' as type,
    sum(case when event_date = %(ref_date)s then 2 else 1 end) as count,
    event_date as actual_date
from date_test
group by 1, 2, 4
order by date, type
"""
    with db.transaction(pg_conn) as tx:
        rows = tx.select(named_query, {
            'yesterday': yesterday,
            'today': today,
            'tomorrow': tomorrow,
            'ref_date': today,
            })

    assert [(row['date'], row['type'], row['count'], row['actual_date'])
            for row in rows] == [
        (yesterday.isoformat(), 'TestType', 1, yesterday),
        (today.isoformat(), 'TestType', 2, today),
        (tomorrow.isoformat(), 'TestType', 1, tomorrow),
        ]
    assert all(type(row['count']) is int for row in rows)


if __name__ == '__main__':
    __import__('pytest').main([__file__])
