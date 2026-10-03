"""Unit tests for SQL masking and statement splitting."""
import pytest
from database.sql import mask_protected_text, split_statements


@pytest.mark.parametrize(('sql', 'dialect'), [
    ('select 1 -- ; delete from t', 'postgresql'),
    ('select 1 -- ; delete from t', 'sqlite'),
    ('select 1 /* ; delete from t */', 'postgresql'),
    ])
def test_statement_behind_a_comment_does_not_split(sql, dialect):
    """Verify a semicolon inside a comment does not split a statement.

    Mutation: sql.split(';') in place of the masked scan in split_statements.
    Oracle: one statement out, holding the whole text.
    """
    assert split_statements(sql, dialect) == [sql]


@pytest.mark.parametrize(('sql', 'dialect', 'count'), [
    ("select * from t where n = 'a;b'", 'postgresql', 1),
    ('select 1; select 2', 'postgresql', 2),
    ('select 1;;\n  ;select 2;', 'postgresql', 2),
    ("select ';' as semi", 'sqlite', 1),
    ])
def test_split_statements_ignores_a_protected_semicolon(sql, dialect, count):
    """Verify only an unprotected semicolon splits.

    Mutation: sql.split(';') in place of the masked scan.
    Oracle: hand-counted statement totals.
    """
    assert len(split_statements(sql, dialect)) == count


@pytest.mark.parametrize('sql', [
    "select * from t where note = E'don''t care'",
    "select * from t where note = E'don\\'t delete this'",
    "select * from t where note = E'O\\'Brien goes into town'",
    ])
def test_postgres_escape_string_closes_where_postgres_closes_it(sql):
    """Verify an E'' literal ends at its real closing quote.

    Mutation: dropping the E'' escape branch from mask_protected_text.
    Oracle: same length, and no word from inside the quotes survives.
    """
    masked = mask_protected_text(sql, 'postgresql')

    assert len(masked) == len(sql)
    assert masked.startswith('select * from t where note = E')
    assert 'delete' not in masked.lower()
    assert 'into' not in masked.lower()


def test_nested_block_comment_is_scanned_as_nested():
    """Verify a PostgreSQL block comment nests, unlike a C one.

    Mutation: closing the comment at the first '*/'.
    Oracle: PostgreSQL nesting rules; only 'select 1' survives the mask.
    """
    nested = '/* outer /* inner */ still comment ; */ select 1'

    masked = mask_protected_text(nested, 'postgresql')

    assert len(masked) == len(nested)
    assert masked.strip() == 'select 1'
    assert split_statements(nested, 'postgresql') == [nested]


@pytest.mark.parametrize('sql', [
    "select * from t where a = 'unterminated",
    'select * from t /* unterminated',
    'select $q$ unterminated',
    ])
def test_unterminated_protected_text_returns_none(sql):
    """Verify text whose literals never close fails closed.

    Mutation: returning the partly masked text instead of None.
    Oracle: None, and the whole text as one statement.
    """
    assert mask_protected_text(sql, 'postgresql') is None
    assert split_statements(sql + '; select 1', 'postgresql') == [sql + '; select 1']
