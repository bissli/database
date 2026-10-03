"""Fixtures that run one integration test against PostgreSQL and SQLite.
"""
from typing import Any

import database as db
import pandas as pd
import pytest


def row(result: list[dict] | pd.DataFrame, index: int) -> Any:
    """Row at zero-based index of a select result, indexable by column name.
    """
    if hasattr(result, 'iloc'):
        return result.iloc[index]
    return result[index]


def col(result: list[dict] | pd.DataFrame, column_name: str) -> list:
    """Values of one column of a select result, in row order.
    """
    if hasattr(result, 'iloc'):
        return list(result[column_name])
    return [r[column_name] for r in result]


@pytest.fixture(params=['postgresql', 'sqlite'], ids=['pg', 'sl'])
def db_conn(request):
    """Connection whose test_table holds exactly Alice 10, Bob 20, Charlie 30.
    """
    if request.param == 'postgresql':
        conn = request.getfixturevalue('pg_conn')
        db.execute(conn, 'drop table if exists test_table cascade')
        create_sql = """
create table test_table (
    id serial not null,
    name varchar(255) not null,
    value integer not null,
    primary key (name)
)
"""
    else:
        conn = request.getfixturevalue('sl_conn')
        db.execute(conn, 'drop table if exists test_table')
        create_sql = """
create table test_table (
    id integer,
    name text not null,
    value integer not null,
    primary key (name)
)
"""
    db.execute(conn, create_sql)
    insert_sql = """
insert into test_table (name, value) values
('Alice', 10),
('Bob', 20),
('Charlie', 30)
"""
    db.execute(conn, insert_sql)
    return conn
