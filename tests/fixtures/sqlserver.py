import logging
import os
import sys
import time

import database as db
import docker
import pyodbc
import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.append('..')
import config

logger = logging.getLogger(__name__)


@pytest.fixture(scope='session')
def sqlserver_docker(request):
    client = docker.from_env()

    # Check if container already exists and remove it
    try:
        old_container = client.containers.get('test_sqlserver')
        logger.info('Found existing test container, removing it')
        old_container.stop()
        old_container.remove()
    except docker.errors.NotFound:
        pass
    except Exception as e:
        logger.warning(f'Error when cleaning up container: {e}')

    try:
        container = client.containers.run(
            image='mcr.microsoft.com/mssql/server:2022-latest',
            auto_remove=True,
            environment={
                'ACCEPT_EULA': 'Y',
                'MSSQL_SA_PASSWORD': config.mssql.password,
                'MSSQL_PID': 'Developer',  # Explicitly set edition
                'TZ': 'US/Eastern'
            },
            name='test_sqlserver',
            ports={'1433/tcp': '1433'},
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

        # Wait for SQL Server to be ready
        logger.info('Waiting for SQL Server to initialize...')
        time.sleep(5)

        # Check if container is healthy
        for i in range(60):  # SQL Server takes longer to initialize than PostgreSQL
            container_info = client.containers.get('test_sqlserver').attrs
            container_status = container_info.get('State', {}).get('Status', '')
            if container_status != 'running':
                logger.info(f'Container not running, status: {container_status}')
                time.sleep(1)
                continue

            # Check if SQL Server is accepting connections
            try:
                # Make a direct connection to test readiness
                conn_str = (
                    f'DRIVER={{{config.mssql.driver}}};'
                    f'SERVER=localhost,{config.mssql.port};'
                    f'DATABASE=master;'
                    f'UID={config.mssql.username};'
                    f'PWD={config.mssql.password};'
                    f'Connection Timeout={config.mssql.timeout or 5};'
                    f'TrustServerCertificate={config.mssql.trust_server_certificate or "yes"};'
                )
                conn = pyodbc.connect(conn_str)
                conn.close()
                logger.info('SQL Server is ready')
                break
            except Exception as e:
                logger.info(f'Waiting for SQL Server to start: {e}')
                time.sleep(2)
        else:
            raise Exception('SQL Server container failed to start in time')

        return container

    except Exception as e:
        logger.error(f'Error setting up SQL Server container: {e}')
        raise


def stage_test_data(cn):
    drop_table_if_exists = "IF OBJECT_ID('test_table', 'U') IS NOT NULL DROP TABLE test_table"
    db.execute(cn, drop_table_if_exists)

    create_table_sql = """
CREATE TABLE test_table (
    id INT IDENTITY(1,1) NOT NULL,
    name VARCHAR(255) NOT NULL,
    value INT NOT NULL,
    PRIMARY KEY (name)
);
"""
    db.execute(cn, create_table_sql)

    insert_sql = """
INSERT INTO test_table (name, value) VALUES
(?, ?), (?, ?), (?, ?), (?, ?), (?, ?), (?, ?);
"""
    db.execute(cn, insert_sql,
               'Alice', 10, 'Bob', 20, 'Charlie', 30,
               'Ethan', 50, 'Fiona', 70, 'George', 80)


def terminate_sqlserver_connections(cn):
    try:
        cn.rollback()  # Reset any failed transaction state
    except Exception as e:
        logger.warning(f'Failed to rollback: {e}')


@pytest.fixture
def sconn(sqlserver_docker):
    """
    Connection fixture with function scope for clean tests.
    Each test gets a fresh connection with reset test data.
    """
    cn = db.connect('mssql', config=config)
    assert db.isconnection(cn)

    # Ensure clean state for each test
    cn.rollback()  # Ensure we're not in a failed transaction
    terminate_sqlserver_connections(cn)
    stage_test_data(cn)

    try:
        yield cn
    finally:
        # Clean up after the test
        try:
            cn.rollback()  # Make sure we don't have open transactions
            terminate_sqlserver_connections(cn)
            cn.close()
        except Exception as e:
            logger.warning(f'Error during connection cleanup: {e}')
