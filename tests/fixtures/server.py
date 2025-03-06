import logging
import os
import sys
import time

import database as db
import docker
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.append('..')
import config

logger = logging.getLogger(__name__)


@pytest.fixture(scope='session')
def psql_docker():
    client = docker.from_env()
    container = client.containers.run(
        image='postgres:12',
        auto_remove=True,
        environment={
            'POSTGRES_DB': 'test_db',
            'POSTGRES_USER': 'postgres',
            'POSTGRES_PASSWORD': 'postgres',
            'TZ': 'US/Eastern',
            'PGTZ': 'US/Eastern'},
        name='test_postgres',
        ports={'5432/tcp': ('127.0.0.1', 5432)},
        detach=True,
        remove=True,
    )
    time.sleep(5)
    yield
    container.stop()


def stage_test_data(cn):
    # Install hstore extension
    db.execute(cn, 'CREATE EXTENSION IF NOT EXISTS hstore')

    drop_table_if_exists = 'drop table if exists test_table'
    db.execute(cn, drop_table_if_exists)

    create_and_insert_data = """
create table test_table (
    id serial not null,
    name varchar(255) not null,
    value integer not null,
    primary key (name)
);

insert into test_table (name, value) values
('Alice', 10),
('Bob', 20),
('Charlie', 30),
('Ethan', 50),
('Fiona', 70),
('George', 80);
"""
    db.execute(cn, create_and_insert_data)


def terminate_postgres_connections(cn):
    try:
        cn.rollback()  # Reset any failed transaction state
        sql = """
select
    pg_terminate_backend(pg_stat_activity.pid)
from
    pg_stat_activity
where
    pg_stat_activity.datname = current_database()
    and pid <> pg_backend_pid()
"""
        db.execute(cn, sql)
    except Exception as e:
        logger.warning(f'Failed to terminate connections: {e}')


@pytest.fixture(scope='session')
def conn():
    cn = db.connect('postgres', config)
    assert db.isconnection(cn)
    terminate_postgres_connections(cn)
    stage_test_data(cn)
    try:
        yield cn
    finally:
        terminate_postgres_connections(cn)
        cn.close()
