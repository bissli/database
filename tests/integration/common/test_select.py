"""Select API tests run against both PostgreSQL and SQLite.
"""
import database as db
import pytest
from tests.integration.common.conftest import col, row


class TestSelectOperations:
    """Each select variant over the staged Alice, Bob, Charlie rows.
    """

    def test_select_all(self, db_conn):
        """Verify select returns every row of the table.

        Mutation: select returning only the first fetched row.
        Oracle: the three names the db_conn fixture stages.
        """
        result = db.select(db_conn, 'select * from test_table order by name')
        assert col(result, 'name') == ['Alice', 'Bob', 'Charlie']

    def test_select_with_where(self, db_conn):
        """Verify select returns the one row a literal predicate matches.

        Mutation: a loader that drops or misnames columns of the row.
        Oracle: Alice is staged with value 10.
        """
        result = db.select(db_conn, "select * from test_table where name = 'Alice'")
        assert len(result) == 1
        assert row(result, 0)['name'] == 'Alice'
        assert row(result, 0)['value'] == 10

    def test_select_with_param(self, db_conn):
        """Verify a %s placeholder binds its argument on both dialects.

        Mutation: skipping the %s to ? conversion on the SQLite path.
        Oracle: Bob is staged once.
        """
        result = db.select(db_conn, 'select * from test_table where name = %s', 'Bob')
        assert len(result) == 1
        assert row(result, 0)['name'] == 'Bob'

    def test_select_column(self, db_conn):
        """Verify select_column returns the first column as a list, in order.

        Mutation: returning whole rows, or the values in another order.
        Oracle: the staged names, sorted by hand.
        """
        names = db.select_column(db_conn, 'select name from test_table order by name')
        assert names == ['Alice', 'Bob', 'Charlie']

    def test_select_row(self, db_conn):
        """Verify select_row returns one row with attribute access.

        Mutation: returning a plain dict, which has no .name attribute.
        Oracle: Alice is staged with value 10.
        """
        result = db.select_row(db_conn, "select * from test_table where name = 'Alice'")
        assert result.name == 'Alice'
        assert result.value == 10

    def test_select_row_or_none_with_result(self, db_conn):
        """Verify select_row_or_none returns the row when exactly one matches.

        Mutation: returning None on any result.
        Oracle: Alice is staged once.
        """
        result = db.select_row_or_none(
            db_conn, "select * from test_table where name = 'Alice'")
        assert result.name == 'Alice'

    def test_select_row_or_none_without_result(self, db_conn):
        """Verify select_row_or_none returns None when no row matches.

        Mutation: raising ValidationError on zero rows, as select_row does.
        Oracle: no staged row is named Nonexistent.
        """
        result = db.select_row_or_none(
            db_conn, "select * from test_table where name = 'Nonexistent'")
        assert result is None

    def test_select_row_or_none_raises_on_two_rows(self, db_conn):
        """Verify select_row_or_none raises when the query returns two rows.

        Mutation: returning None for any row count other than one.
        Oracle: Alice and Bob are staged, so the in list matches two.
        """
        with pytest.raises(db.ValidationError, match='got 2'):
            db.select_row_or_none(
                db_conn, "select * from test_table where name in ('Alice', 'Bob')")

    def test_select_scalar_or_none_raises_on_two_rows(self, db_conn):
        """Verify select_scalar_or_none raises when the query returns two rows.

        Mutation: returning None on any ValidationError from select_scalar.
        Oracle: Alice and Bob are staged, so the in list matches two.
        """
        with pytest.raises(db.ValidationError, match='got 2'):
            db.select_scalar_or_none(
                db_conn, "select value from test_table where name in ('Alice', 'Bob')")

    def test_select_scalar(self, db_conn):
        """Verify select_scalar returns the single value, unwrapped.

        Mutation: returning the row or a one-element list.
        Oracle: the fixture stages three rows.
        """
        assert db.select_scalar(db_conn, 'select count(*) from test_table') == 3

    def test_select_scalar_or_none_with_result(self, db_conn):
        """Verify select_scalar_or_none returns the value when one row matches.

        Mutation: returning None on any result.
        Oracle: Alice is staged with value 10.
        """
        value = db.select_scalar_or_none(
            db_conn, "select value from test_table where name = 'Alice'")
        assert value == 10

    def test_select_scalar_or_none_without_result(self, db_conn):
        """Verify select_scalar_or_none returns None when no row matches.

        Mutation: raising ValidationError on zero rows, as select_scalar does.
        Oracle: no staged row is named Nonexistent.
        """
        value = db.select_scalar_or_none(
            db_conn, "select value from test_table where name = 'Nonexistent'")
        assert value is None


class TestSelectWithMultipleParams:
    """Select with more than one bound argument.
    """

    def test_select_with_multiple_params(self, db_conn):
        """Verify two %s placeholders bind their arguments in order.

        Mutation: binding only the first argument, or repeating it.
        Oracle: Alice and Bob are staged; Charlie is not named.
        """
        result = db.select(
            db_conn,
            'select * from test_table where name = %s or name = %s order by name',
            'Alice', 'Bob')
        assert col(result, 'name') == ['Alice', 'Bob']

    def test_select_with_in_clause(self, db_conn):
        """Verify a tuple bound to in %s expands to one placeholder per item.

        Mutation: binding the tuple as one value.
        Oracle: Alice and Charlie are staged; Bob is left out of the tuple.
        """
        result = db.select(
            db_conn,
            'select * from test_table where name in %s order by name',
            ('Alice', 'Charlie'))
        assert col(result, 'name') == ['Alice', 'Charlie']


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
