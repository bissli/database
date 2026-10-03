"""Tests for the check_connection retry decorator.
"""
import pytest
from database.connection import check_connection
from database.exceptions import ConnectionFailure


@pytest.mark.parametrize('max_retries', [0, -1])
def test_check_connection_runs_once_when_max_retries_below_one(max_retries):
    """Verify a max_retries below 1 still runs the call, and never retries.

    Mutation: the attempt loop guarded by `tries < max_retries`.
    Oracle: a spy counting calls and sleeps, at 0 and at -1.
    """
    calls = []
    sleeps = []

    @check_connection(max_retries=max_retries, sleep_func=sleeps.append)
    def succeed():
        calls.append('succeed')
        return 42

    @check_connection(max_retries=max_retries, sleep_func=sleeps.append)
    def fail():
        calls.append('fail')
        raise ConnectionFailure('connection reset by peer')

    assert succeed() == 42
    with pytest.raises(ConnectionFailure):
        fail()

    assert calls == ['succeed', 'fail']
    assert sleeps == []


if __name__ == '__main__':
    pytest.main([__file__])
