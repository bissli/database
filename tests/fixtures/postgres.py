import logging
import pathlib
import sys
import time

import database as db
import docker
import pytest

HERE = pathlib.Path(pathlib.Path(__file__).resolve()).parent
sys.path.insert(0, HERE)
sys.path.append('..')
import config

logger = logging.getLogger(__name__)


@pytest.fixture(scope='session')
def psql_docker(request):
    client = docker.from_env()

    # Check if container already exists and remove it
    try:
        old_container = client.containers.get('test_postgres')
        logger.info('Found existing test container, removing it')
        old_container.stop()
        old_container.remove()
    except docker.errors.NotFound:
        pass
    except Exception as e:
        logger.warning(f'Error when cleaning up container: {e}')

    try:
        container = client.containers.run(
            image='postgres:12',
            auto_remove=True,
            environment={
                'POSTGRES_DB': config.postgresql.database,
                'POSTGRES_USER': config.postgresql.username,
                'POSTGRES_PASSWORD': config.postgresql.password,
                'TZ': 'US/Eastern',
                'PGTZ': 'US/Eastern'},
            name='test_postgres',
            ports={'5432/tcp': ('127.0.0.1', 5432)},
            detach=True,
            remove=True,
        )

        # Register finalizer to ensure container is cleaned up after all tests
        def finalizer():
            try:
                container.stop()
            except Exception as e:
                logger.warning(f'Error stopping container during cleanup: {e}')

        request.addfinalizer(finalizer)

        for i in range(30):
            try:
                cn = db.connect('postgresql', config=config)
                cn.close()
                break
            except Exception as e:
                logger.debug(e)
                time.sleep(1)
        else:
            raise Exception('Postgres container failed to start in time', exc)

        return container

    except Exception as e:
        logger.error(f'Error setting up postgres container: {e}')
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
