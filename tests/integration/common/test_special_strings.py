"""
Database-agnostic tests for how bound null-like strings reach a column.

These tests run against both PostgreSQL and SQLite and hold both to one
expected result.
"""
import database as db
import numpy as np
import pyarrow as pa
import pytest
from tests.integration.common.conftest import col

SPELLED_NULLS = ('nan', 'NaN', 'None', 'none', 'NA', 'NaT', 'null', 'NULL')

CREATE_PROBE_TABLE = """
create table null_probe (
    label varchar(20) primary key,
    txt varchar(20)
)
"""


@pytest.fixture
def probe_conn(db_conn):
    """Connection holding an empty null_probe table."""
    db.execute(db_conn, 'drop table if exists null_probe')
    db.execute(db_conn, CREATE_PROBE_TABLE)
    yield db_conn
    db.execute(db_conn, 'drop table if exists null_probe')


def stored_by_label(cn) -> dict:
    """Map each stored label to its txt value."""
    result = db.select(cn, 'select label, txt from null_probe')
    return dict(zip(col(result, 'label'), col(result, 'txt')))


def test_execute_stores_spelled_nulls_as_text_and_empty_as_null(probe_conn):
    """Verify execute and executemany keep spelled nulls, null only ''.

    Mutation: restoring the SPECIAL_STRINGS lookup on either str path of
        TypeConverter.convert_value (every spelled null stores NULL), or
        dropping either '' arm (the empty string stores as text). The
        np.str_ values take the slow path, plain str the fast one.
    Oracle: hand-written expected mapping, identical for both dialects,
        plus a lookup by 'NA' that must find its row.
    """
    sql = 'insert into null_probe (label, txt) values (%s, %s)'
    for value in SPELLED_NULLS[:4]:
        db.execute(probe_conn, sql, f'e_{value}', value)
    probe_conn.cursor().executemany(
        sql,
        [(f'm_{value}', value) for value in SPELLED_NULLS[4:]])
    db.execute(probe_conn, sql, 'empty', '')
    db.execute(probe_conn, sql, 'np_nan', np.str_('nan'))
    db.execute(probe_conn, sql, 'np_empty', np.str_(''))

    expected = {f'e_{value}': value for value in SPELLED_NULLS[:4]}
    expected |= {f'm_{value}': value for value in SPELLED_NULLS[4:]}
    expected |= {'empty': None, 'np_nan': 'nan', 'np_empty': None}
    assert stored_by_label(probe_conn) == expected
    assert db.select_scalar(
        probe_conn,
        'select label from null_probe where txt = %s',
        'NA') == 'm_NA'


def test_row_apis_map_spelled_nulls_and_empty_to_null(probe_conn):
    """Verify insert_rows and upsert_rows store every null spelling as NULL.

    Mutation: dropping null_special_string from the params built in
        ConnectionWrapper.insert_rows or upsert_rows, which stores the
        spelled nulls as text.
    Oracle: hand-written expected mapping, identical for both dialects,
        with near-miss controls 'n/a' and 'nan ' that must survive. The
        PyArrow scalars are not str, so they need their own unwrap.
    """
    values = (*SPELLED_NULLS, '', pa.scalar('nan'), pa.scalar('None'), 'n/a', 'nan ')
    db.insert_rows(
        probe_conn,
        'null_probe',
        [{'label': f'i_{n}', 'txt': value} for n, value in enumerate(values)])
    db.insert_rows(
        probe_conn,
        'null_probe',
        [{'label': f'u_{n}', 'txt': 'seed'} for n in range(len(values))])
    db.upsert_rows(
        probe_conn,
        'null_probe',
        [{'label': f'u_{n}', 'txt': value} for n, value in enumerate(values)],
        update_cols_always=['txt'])

    expected = {}
    for prefix in ('i', 'u'):
        for n, value in enumerate(values):
            expected[f'{prefix}_{n}'] = value if value in {'n/a', 'nan '} else None
    assert stored_by_label(probe_conn) == expected


if __name__ == '__main__':
    __import__('pytest').main([__file__])
