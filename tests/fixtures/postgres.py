import logging

import config
import database as db
import pytest
from testcontainers.postgres import PostgresContainer

from libb import Setting

logger = logging.getLogger(__name__)


@pytest.fixture(scope='session')
def psql_docker(request):
    """postgres:12 container for the session; config.postgresql points at it.
    """
    container = PostgresContainer(
        image='postgres:12',
        username=config.postgresql.username,
        password=config.postgresql.password,
        dbname=config.postgresql.database)
    container.with_env('TZ', 'US/Eastern').with_env('PGTZ', 'US/Eastern')

    try:
        container.start()

        Setting.unlock()
        config.postgresql.hostname = container.get_container_host_ip()
        config.postgresql.port = int(container.get_exposed_port(5432))
        Setting.lock()

        logger.info(
            f'PostgreSQL container started at '
            f'{config.postgresql.hostname}:{config.postgresql.port}')

        cn = db.connect('postgresql', config=config)
        cn.close()

        def finalizer():
            try:
                container.stop()
                logger.info('PostgreSQL container stopped')
            except Exception as e:
                logger.warning(f'Error stopping container: {e}')

        request.addfinalizer(finalizer)
        return container

    except Exception as e:
        logger.error(f'Error setting up postgres container: {e}')
        try:
            container.stop()
        except Exception:
            pass
        raise


def stage_test_data(cn):
    """Recreate test_table with six rows keyed by name, and enable hstore.
    """
    db.execute(cn, 'create extension if not exists hstore')

    db.execute(cn, 'drop table if exists test_table')

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
    """End every other backend on the test database; log a failure.
    """
    try:
        cn.rollback()
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


@pytest.fixture
def pg_conn(psql_docker):
    """Fresh PostgreSQL connection per test, over a restaged test_table.
    """
    cn = db.connect('postgresql', config=config)

    try:
        stage_test_data(cn)
        yield cn
    finally:
        terminate_postgres_connections(cn)
        try:
            cn.close()
        except Exception as e:
            logger.warning(f'Error during connection cleanup: {e}')


@pytest.fixture
def pg_schema_conn(pg_conn):
    """pg_conn with myschema.t rows 'alpha' and 'beta'; drops myschema after.
    """
    create_table = """
create table myschema.t (
    id serial not null,
    name varchar(255) not null,
    value integer not null,
    primary key (name)
)
"""
    db.execute(pg_conn, 'create schema if not exists myschema')
    db.execute(pg_conn, 'drop table if exists myschema.t')
    db.execute(pg_conn, create_table)
    db.execute(
        pg_conn,
        "insert into myschema.t (name, value) values ('alpha', 1), ('beta', 2)")
    try:
        yield pg_conn
    finally:
        try:
            db.execute(pg_conn, 'drop schema myschema cascade')
        except Exception as e:
            logger.warning(f'Error dropping myschema: {e}')
