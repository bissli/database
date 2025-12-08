import logging
import pathlib
import sys

import database as db
import pytest
from testcontainers.postgres import PostgresContainer

from libb import Setting

HERE = pathlib.Path(pathlib.Path(__file__).resolve()).parent
sys.path.insert(0, HERE)
sys.path.append('..')
import config

logger = logging.getLogger(__name__)


@pytest.fixture(scope='session')
def psql_docker(request):
    """Session-scoped PostgreSQL container using testcontainers.

    Testcontainers automatically:
    - Assigns a random available port
    - Waits for the database to be ready
    - Handles cleanup when the session ends
    """
    container = PostgresContainer(
        image='postgres:12',
        username=config.postgresql.username,
        password=config.postgresql.password,
        dbname=config.postgresql.database,
    ).with_env('TZ', 'US/Eastern').with_env('PGTZ', 'US/Eastern')

    try:
        container.start()

        # Update config with dynamic host/port
        Setting.unlock()
        config.postgresql.hostname = container.get_container_host_ip()
        config.postgresql.port = int(container.get_exposed_port(5432))
        Setting.lock()

        logger.info(
            f'PostgreSQL container started at '
            f'{config.postgresql.hostname}:{config.postgresql.port}'
        )

        # Verify connection works
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


@pytest.fixture
def conn(psql_docker):
    """
    Connection fixture with function scope for clean tests.
    Each test gets a fresh connection with reset test data.
    """
    cn = db.connect('postgresql', config=config)
    assert db.isconnection(cn)

    try:
        stage_test_data(cn)
        yield cn
    finally:
        try:
            cn.rollback()
            cn.close()
        except Exception as e:
            logger.warning(f'Error during connection cleanup: {e}')
        finally:
            terminate_postgres_connections(cn)
