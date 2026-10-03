"""Unit tests for error logging on the strategy's raw DBAPI cursor paths.
"""
import io
import logging

import pytest
from database.strategy.postgres import PostgresStrategy

BASE_LOGGER = 'database.strategy.base'
POSTGRES_LOGGER = 'database.strategy.postgres'


def _error_records(caplog, logger_name):
    """Every ERROR record emitted by logger_name, in order.
    """
    return [
        r for r in caplog.records
        if r.name == logger_name and r.levelno == logging.ERROR
        ]


@pytest.fixture
def raw_cn(mocker):
    """Factory for a connection stub whose DBAPI cursor is the given mock.
    """
    def factory(raw_cursor):
        cn = mocker.Mock()
        cn.readonly = False
        cn.dialect = 'postgresql'
        cn.dbapi_connection.cursor.return_value = raw_cursor
        return cn

    return factory


def test_raw_execute_error_logs_and_closes_cursor(mocker, raw_cn, caplog):
    """Verify a failed raw execute logs its SQL and exception, then closes.

    Mutation: the except branch of DatabaseStrategy._cursor dropped.
    Oracle: the exception instance handed to the stub cursor.
    """
    error = ValueError('nope')
    raw_cursor = mocker.Mock()
    raw_cursor.execute.side_effect = error
    cn = raw_cn(raw_cursor)

    with caplog.at_level(logging.ERROR, logger=BASE_LOGGER):
        with pytest.raises(ValueError, match='nope'):
            PostgresStrategy()._execute_raw(cn, 'select 1', (7,))

    records = _error_records(caplog, BASE_LOGGER)
    assert len(records) == 1
    assert records[0].args == ('select 1', (7,))
    assert records[0].exc_info[1] is error
    raw_cursor.close.assert_called_once_with()


def test_raw_fetch_error_logs_once(mocker, raw_cn, caplog):
    """Verify a fetch failure in the with-body logs exactly one record.

    Mutation: the try in DatabaseStrategy._cursor narrowed to execute.
    Oracle: the exception instance handed to the stub cursor's fetchall.
    """
    error = ValueError('nope')
    raw_cursor = mocker.Mock()
    raw_cursor.fetchall.side_effect = error
    cn = raw_cn(raw_cursor)

    with caplog.at_level(logging.ERROR, logger=BASE_LOGGER):
        with pytest.raises(ValueError, match='nope'):
            PostgresStrategy()._select_column_raw(cn, 'select 1')

    records = _error_records(caplog, BASE_LOGGER)
    assert len(records) == 1
    assert records[0].exc_info[1] is error


def test_copy_from_error_logs_and_closes_cursor(mocker, raw_cn, caplog):
    """Verify a failed copy logs its statement and closes the cursor.

    Mutation: cursor.close() outside a finally, or the except dropped.
    Oracle: the exception instance handed to the stub copy writer.
    """
    error = ValueError('nope')
    raw_cursor = mocker.MagicMock()
    copy = raw_cursor.copy.return_value.__enter__.return_value
    copy.write.side_effect = error
    cn = raw_cn(raw_cursor)

    with caplog.at_level(logging.ERROR, logger=POSTGRES_LOGGER):
        with pytest.raises(ValueError, match='nope'):
            PostgresStrategy().copy_from(cn, 't', io.StringIO('1\n'))

    records = _error_records(caplog, POSTGRES_LOGGER)
    assert len(records) == 1
    assert 'copy "t" from stdin' in records[0].getMessage()
    assert records[0].exc_info[1] is error
    raw_cursor.close.assert_called_once_with()
