"""Unit tests for the read-only guard's SQL classifier.

is_write_sql decides, in process, whether a statement may reach a
replica. Two failures matter and they pull in opposite directions: a
write let through reaches production, and a read blocked breaks the
read-only suite the feature exists to serve. The tests below pin both
edges.
"""
import re

import pytest
from database.exceptions import ReadOnlyError
from database.sql import _NESTED_WRITE_RE, _ROW_LOCK_RE, _WRITE_HINTS
from database.sql import is_write_sql, mask_protected_text
from database.sql import raise_on_readonly_write, split_statements

READ_STATEMENTS = [
    'select 1',
    'SELECT * FROM t WHERE a = %s',
    'with recent as (select 1) select * from recent',
    'values (1), (2)',
    'table t',
    'show timezone',
    'explain select * from t',
    'EXPLAIN ANALYZE SELECT * FROM t',
    'explain (analyze, buffers) select 1',
    'EXPLAIN QUERY PLAN SELECT * FROM t',
    '(select 1) union (select 2)',
    'select a from t where x in (select y from z)',
    'select name from pragma_table_info("t")',
    'select 1;\nselect 2;',
    ]

WRITE_STATEMENTS = [
    'insert into t values (1)',
    'update t set a = 1',
    'delete from t',
    'truncate t',
    'create table t (a int)',
    'drop table t',
    'alter table t add column b int',
    'vacuum (full, analyze) t',
    'reindex table t',
    'cluster t using ix',
    'analyze t',
    'grant select on t to u',
    'copy t from stdin',
    'merge into t using s on true when matched then update set a = 1',
    'call my_proc()',
    'begin',
    'commit',
    ]


@pytest.mark.parametrize('sql', READ_STATEMENTS)
@pytest.mark.parametrize('dialect', ['postgresql', 'sqlite'])
def test_reading_statement_is_not_a_write(sql, dialect):
    """Verify every ordinary read form classifies as a read.

    Mutation: dropping a word from _READ_LEAD_WORDS, or dropping the
        EXPLAIN prefix strip so 'explain select' falls to the
        unrecognized-keyword branch.
    Oracle: hand-classified statements a read-only suite really sends.
    """
    assert is_write_sql(sql, dialect) is False


@pytest.mark.parametrize('sql', WRITE_STATEMENTS)
@pytest.mark.parametrize('dialect', ['postgresql', 'sqlite'])
def test_writing_statement_is_a_write(sql, dialect):
    """Verify every write form classifies as a write.

    Mutation: inverting the leading-keyword test to allow an
        unrecognized keyword through, which would pass DDL, VACUUM,
        GRANT, and COPY to the replica.
    Oracle: hand-classified statements, each rejected by a standby.
    """
    assert is_write_sql(sql, dialect) is True


@pytest.mark.parametrize('sql', [
    'SET default_transaction_read_only = off',
    'reset all',
    'PRAGMA query_only = OFF',
    'pragma journal_mode = WAL',
    ])
def test_setting_that_clears_the_backstop_is_a_write(sql):
    """Verify SET, RESET, and PRAGMA cannot reach the server.

    Mutation: adding 'set' or 'pragma' to _READ_LEAD_WORDS, which
        would let a caller clear default_transaction_read_only or
        query_only and then write freely.
    Oracle: the two statements proven in-session to turn the
        server-side read-only setting back off.
    """
    assert is_write_sql(sql, 'postgresql') is True
    assert is_write_sql(sql, 'sqlite') is True


@pytest.mark.parametrize('sql', [
    'select * from t for update',
    'select * from t FOR SHARE',
    'select * from t for no key update',
    'select * from t for key share',
    ])
def test_row_locking_select_is_a_write(sql):
    """Verify a locking SELECT counts as a write.

    Mutation: deleting _ROW_LOCK_RE from the scan, which would send
        'for share' and 'for key share' to a standby that refuses the
        lock mid-transaction.
    Oracle: PostgreSQL's own rule - every row-lock clause raises
        'cannot execute SELECT FOR ...' in a read-only transaction.
    """
    assert is_write_sql(sql, 'postgresql') is True


@pytest.mark.parametrize('sql', [
    'with moved as (insert into t values (1) returning *) select * from moved',
    'with gone as (delete from t where a = 1 returning *) select * from gone',
    'with bumped as (update t set a = 2 returning *) select count(*) from bumped',
    ])
def test_data_modifying_cte_is_a_write(sql):
    """Verify a write nested in a CTE is not hidden by the leading WITH.

    Mutation: scanning only at paren depth zero, which misses the DML
        verb inside the CTE body and passes the whole statement as a
        read.
    Oracle: hand-classified CTEs, each of which writes rows.
    """
    assert is_write_sql(sql, 'postgresql') is True


@pytest.mark.parametrize('sql', [
    "select 'insert into t values (1)' as sample",
    'select a from t -- insert into audit\n',
    'select a from t /* delete from audit */ where b = 1',
    "select * from t where name = 'don''t update'",
    'select "insert" from t',
    ])
def test_write_word_inside_a_protected_context_is_not_a_write(sql):
    """Verify a keyword in a literal, comment, or quoted name is ignored.

    Mutation: scanning the raw SQL instead of the mask
        _protected_ranges builds, which would reject a query whose text
        merely mentions a write.
    Oracle: hand-classified reads whose only write word sits inside a
        string, a comment, or a double-quoted identifier.
    """
    assert is_write_sql(sql, 'postgresql') is False


@pytest.mark.parametrize('sql', [
    'select insert_date, update_ts, delete_flag from t',
    'select * from inserted_rows',
    'select market_share from positions',
    'select count(*) from updates',
    "select date_trunc('day', ts) from t",
    'select * from t order by update_time desc',
    ])
def test_write_word_inside_an_identifier_is_not_a_write(sql):
    r"""Verify a column or table whose name embeds a keyword still reads.

    Mutation: dropping the \\b anchors from _NESTED_WRITE_RE, which
        makes 'insert_date' and 'updates' match and blocks ordinary
        selects.
    Oracle: hand-classified reads over names that embed a write word.
    """
    assert is_write_sql(sql, 'postgresql') is False


def test_write_hints_cover_every_word_the_patterns_can_match():
    """Verify the substring gate cannot hide a pattern from the scan.

    is_write_sql skips both alternations when no hint word appears in
    the body. That is only sound while the hints are a superset of
    every word the alternations match, and nothing but this test ties
    the two together.

    Mutation: dropping 'for' or 'into' from _WRITE_HINTS, which makes
        'select * from t for share' and 'select a into b from t'
        classify as reads with no other test failing.
    Oracle: the words parsed out of the two patterns themselves, so a
        word added to either pattern must be added to the hints.
    """
    pattern_words = set()
    for pattern in (_NESTED_WRITE_RE, _ROW_LOCK_RE):
        bare = pattern.pattern.replace(r'\b', ' ').replace(r'\s', ' ')
        pattern_words.update(re.findall(r'[a-z]{3,}', bare))

    assert pattern_words, 'no words parsed out of the patterns'
    assert pattern_words <= set(_WRITE_HINTS), (
        f'words reachable by a pattern but absent from _WRITE_HINTS: '
        f'{sorted(pattern_words - set(_WRITE_HINTS))}')


@pytest.mark.parametrize('sql', [
    'select * from t for share',
    'select * from t for key share',
    'select a into b from t',
    ('with m as (merge into t using s on true when matched '
     'then update set a = 1) select 1'),
    ])
def test_substring_gate_does_not_swallow_a_write(sql):
    """Verify each hint word really reaches its pattern.

    Mutation: dropping any single word from _WRITE_HINTS, which makes
        the corresponding statement below classify as a read.
    Oracle: statements whose only write signal is the gated word.
    """
    assert is_write_sql(sql, 'postgresql') is True


@pytest.mark.parametrize(('sql', 'dialect'), [
    ('select 1 -- ; delete from t', 'postgresql'),
    ('select 1 -- ; delete from t', 'sqlite'),
    ('select 1 /* ; delete from t */', 'postgresql'),
    ])
def test_statement_hidden_behind_a_comment_does_not_split(sql, dialect):
    """Verify a semicolon inside a comment hides nothing from the guard.

    A statement behind a trailing '--' is invisible to a masked split
    and visible to a raw one, so the guard and the cursor must split
    the same way, or the cursor runs what the guard never classified.

    Mutation: sql.split(';') in place of the masked scan in
        split_statements, which yields a second statement leading with
        'delete' and executes it behind the guard's back.
    Oracle: one statement out, holding the whole text.
    """
    assert split_statements(sql, dialect) == [sql]
    assert is_write_sql(sql, dialect) is False


@pytest.mark.parametrize(('sql', 'dialect', 'count'), [
    ("select * from t where n = 'a;b'", 'postgresql', 1),
    ('select 1; select 2', 'postgresql', 2),
    ('select 1;;\n  ;select 2;', 'postgresql', 2),
    ("select ';' as semi", 'sqlite', 1),
    ])
def test_split_statements_ignores_a_protected_semicolon(sql, dialect, count):
    """Verify only an unprotected semicolon splits.

    Mutation: sql.split(';') in place of the masked scan, which breaks
        a query carrying a semicolon inside a string literal into two
        unparseable halves and sends both.
    Oracle: hand-counted statement totals.
    """
    assert len(split_statements(sql, dialect)) == count


@pytest.mark.parametrize('sql', [
    "select * from t where note = E'don''t care'",
    "select * from t where note = E'don\\'t delete this'",
    "select * from t where note = E'O\\'Brien goes into town'",
    ])
def test_postgres_escape_string_does_not_leak_into_the_scan(sql):
    """Verify an E'' literal closes where PostgreSQL closes it.

    A scanner blind to the backslash escape ends the literal at the
    escaped quote, reads the real closing quote as a new opener, and
    then scans the literal's own text as code.

    Mutation: dropping the _ESTRING_PREFIX_RE branch from
        mask_protected_text, which refuses both escaped cases on the
        'delete' and 'into' sitting inside the quoted value.
    Oracle: three reads PostgreSQL runs under
        default_transaction_read_only.
    """
    assert is_write_sql(sql, 'postgresql') is False


def test_postgres_escape_string_cannot_hide_a_write():
    """Verify the same desync cannot conceal a real write.

    Mutation: dropping the _ESTRING_PREFIX_RE branch, which masks from
        the literal's real closing quote to end of text and hides the
        INSERT behind it - one statement, no semicolon needed.
    Oracle: PostgreSQL's own answer, 'cannot execute INSERT in a
        read-only transaction'.
    """
    sql = "with c as (select E'it\\'s' as s) insert into t select 1 from c"

    assert is_write_sql(sql, 'postgresql') is True


def test_nested_block_comment_hides_nothing_and_blocks_nothing():
    """Verify PostgreSQL's nesting block comments are scanned as nested.

    Mutation: stopping the comment scan at the first '*/'. The write
        case then leaves '*/' as the statement head and files it as a
        read; the read case leaks the commented-out INSERT's keywords
        into the scan and refuses an ordinary select.
    Oracle: PostgreSQL's own behavior - it refuses the first under
        read-only and runs the second.
    """
    hidden_write = '/* /* inner */ */ insert into t values (1)'
    commented_out = ('/* superseded\n'
                     '   /* legacy */ insert into t values (8)\n'
                     '*/\n'
                     'select count(*) from t')

    assert is_write_sql(hidden_write, 'postgresql') is True
    assert is_write_sql(commented_out, 'postgresql') is False


@pytest.mark.parametrize('sql', [
    "select * from t where a = 'unterminated",
    'select * from t /* unterminated',
    'select $q$ unterminated',
    ])
def test_unscannable_text_is_a_write(sql):
    """Verify text whose literals never close fails closed.

    Mutation: returning the partly masked text instead of None from
        mask_protected_text. Masking to end of text then hides
        everything past the stray quote, so the appended INSERT below
        would classify as a read.
    Oracle: no offset after an unterminated literal can be trusted, so
        nothing after one can be cleared.
    """
    assert mask_protected_text(sql, 'postgresql') is None
    assert is_write_sql(sql, 'postgresql') is True
    assert is_write_sql(sql + '; insert into t values (1)', 'postgresql') is True


@pytest.mark.parametrize('sql', [
    'PRAGMA index_info("ix")',
    'pragma table_info(t)',
    'PRAGMA database_list',
    'pragma index_list("t")',
    ])
def test_reporting_pragma_reads(sql):
    """Verify a pragma that only reports is not refused.

    The library's own SQLite metadata path sends PRAGMA index_info
    through the guarded strategy cursor, so refusing every pragma
    breaks get_constraint_definition on a reader.

    Mutation: dropping the _READ_PRAGMAS branch, which refuses
        get_constraint_definition on every read-only SQLite
        connection.
    Oracle: SQLite runs all four under PRAGMA query_only = ON.
    """
    assert is_write_sql(sql, 'sqlite') is False


@pytest.mark.parametrize('sql', [
    'PRAGMA query_only = OFF',
    'pragma query_only(0)',
    'PRAGMA journal_mode = WAL',
    'pragma table_info = 1',
    ])
def test_assigning_or_unlisted_pragma_is_a_write(sql):
    """Verify the pragma whitelist cannot clear the backstop.

    Mutation: whitelisting a pragma by name alone, ignoring the '='
        and the parenthesized assignment form, which lets 'pragma
        query_only(0)' through and leaves the reader writable.
    Oracle: SQLite's own report - query_only reads 0 after either
        assignment form.
    """
    assert is_write_sql(sql, 'sqlite') is True


@pytest.mark.parametrize('sql', [
    'explain insert into t values (1)',
    'explain update t set a = 1',
    'explain (costs off) delete from t',
    'EXPLAIN QUERY PLAN INSERT INTO t VALUES (1)',
    ])
def test_explain_without_analyze_reads(sql):
    """Verify a plain EXPLAIN of a write is not refused.

    EXPLAIN plans the inner statement without running it, so a
    read-only suite asserting on query plans keeps working.

    Mutation: classifying the statement after the EXPLAIN prefix
        unconditionally, which refuses all four.
    Oracle: both servers run all four under their read-only setting.
    """
    assert is_write_sql(sql, 'postgresql') is False


@pytest.mark.parametrize('sql', [
    'explain analyze insert into t values (1)',
    'EXPLAIN (ANALYZE) delete from t',
    'explain analyze verbose update t set a = 1',
    ])
def test_explain_analyze_of_a_write_is_a_write(sql):
    """Verify ANALYZE brings the inner statement back into scope.

    Mutation: treating every EXPLAIN as a read, which sends EXPLAIN
        ANALYZE INSERT to the server, where ANALYZE really runs it.
    Oracle: PostgreSQL refuses all three under
        default_transaction_read_only.
    """
    assert is_write_sql(sql, 'postgresql') is True


@pytest.mark.parametrize('sql', [
    '*/ insert into t values (1)',
    ') insert into t values (1)',
    '1 insert into t values (1)',
    ])
def test_body_whose_first_token_is_not_a_word_is_a_write(sql):
    """Verify leftover syntax ahead of a statement fails closed.

    Mutation: 'continue' in place of 'return True' where the leading
        word does not match, which files any body starting with stray
        punctuation as a read.
    Oracle: bodies holding a real INSERT behind a token no reading
        statement can start with.
    """
    assert is_write_sql(sql, 'postgresql') is True


@pytest.mark.parametrize(('sql', 'expected'), [
    ('select 1; delete from t', True),
    ('create table t (a int); insert into t values (1)', True),
    ('select 1; select 2', False),
    ("select ';' as semi", False),
    ])
def test_multi_statement_sql_takes_the_writing_member(sql, expected):
    """Verify a write anywhere in a batch makes the whole batch a write.

    Mutation: classifying only the first statement, which would pass
        'select 1; delete from t' straight through.
    Oracle: hand-classified batches, including one whose semicolon is
        inside a literal and so splits nothing.
    """
    assert is_write_sql(sql, 'postgresql') is expected


@pytest.mark.parametrize('sql', ['', None, '   ', '-- just a comment\n'])
def test_nothing_to_execute_is_not_a_write(sql):
    """Verify blank or comment-only SQL classifies as a read.

    Mutation: returning True on an empty leading-word match, which
        would make a whitespace-only string raise instead of reaching
        the driver's own error.
    Oracle: inputs carrying no statement at all.
    """
    assert is_write_sql(sql, 'postgresql') is False


class _Connection:
    """Connection stand-in carrying only the two attributes read."""

    def __init__(self, readonly: bool, dialect: str = 'postgresql') -> None:
        self.readonly = readonly
        self.dialect = dialect


def test_guard_raises_only_for_a_write_on_a_readonly_connection():
    """Verify the guard fires on exactly one of the four combinations.

    Mutation: dropping the readonly check, which would block writes on
        every connection, or dropping the is_write_sql check, which
        would block reads on a reader.
    Oracle: the full two-by-two of readonly against write, asserted
        cell by cell.
    """
    raise_on_readonly_write(_Connection(False), 'insert into t values (1)')
    raise_on_readonly_write(_Connection(False), 'select 1')
    raise_on_readonly_write(_Connection(True), 'select 1')

    with pytest.raises(ReadOnlyError):
        raise_on_readonly_write(_Connection(True), 'insert into t values (1)')


def test_guard_quotes_the_rejected_statement():
    """Verify the error names the statement that was refused.

    Mutation: raising a bare ReadOnlyError with no message, leaving a
        caller with no way to find which of many statements tripped.
    Oracle: the table name, absent from the guard's own wording, found
        in the message.
    """
    with pytest.raises(ReadOnlyError, match='ledger_entries'):
        raise_on_readonly_write(
            _Connection(True), 'delete   from\n  ledger_entries')


def test_object_with_no_readonly_attribute_is_treated_as_a_writer():
    """Verify a raw DBAPI connection passes the guard untouched.

    Mutation: changing getattr(cn, 'readonly', False) to an attribute
        access, which raises AttributeError on every raw connection the
        strategy layer hands in.
    Oracle: an object with neither attribute, given a write statement.
    """
    class _Bare:
        pass

    raise_on_readonly_write(_Bare(), 'insert into t values (1)')


def test_guard_classifies_under_the_connection_dialect():
    """Verify the dialect comes from the connection, not the default.

    Mutation: hard-coding 'postgresql' in the guard, which would treat
        a dollar-quoted body as a protected literal on SQLite, where
        no such quoting exists.
    Oracle: one statement classified opposite ways by the two
        dialects.
    """
    sql = 'select $$insert into t$$ as sample'

    raise_on_readonly_write(_Connection(True, 'postgresql'), sql)
    with pytest.raises(ReadOnlyError):
        raise_on_readonly_write(_Connection(True, 'sqlite'), sql)
