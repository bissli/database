"""Teardown of the pg_conn fixture.
"""
import pytest

from tests.fixtures.postgres import pg_conn


@pytest.mark.postgres
@pytest.mark.integration
def test_pg_conn_teardown_leaves_the_connection_closed(psql_docker):
    """Verify the fixture's teardown leaves no reopened connection behind.

    Mutation: terminate_postgres_connections run after cn.close(), whose
              execute reconnects through _ensure_connection.
    Oracle: sa_connection.closed read once the generator finishes.
    """
    fixture_steps = pg_conn.__wrapped__(psql_docker)
    cn = next(fixture_steps)
    with pytest.raises(StopIteration):
        next(fixture_steps)
    assert cn.sa_connection.closed
