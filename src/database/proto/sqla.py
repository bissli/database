"""Go through https://github.com/sqlalchemy/sqlalchemy/wiki/UsageRecipes
Test recipes and add
"""
from __future__ import annotations

import typing
from typing import Any, Iterator

from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session

if typing.TYPE_CHECKING:
    from sqlalchemy import Result
    from sqlalchemy import Select
    from sqlalchemy import SQLColumnExpression

import typing

if typing.TYPE_CHECKING:
    from sqlalchemy import Result
    from sqlalchemy import Select
    from sqlalchemy import SQLColumnExpression

import logging
from math import ceil

from sqlalchemy.engine import reflection
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.schema import DropConstraint, DropTable, ForeignKeyConstraint
from sqlalchemy.schema import MetaData, Table
from sqlalchemy.sql.expression import ClauseElement, Executable
from sqlalchemy.sql.expression import _literal_as_text

logger = logging.getLogger(__name__)


def drop_everything(engine):
    """Drop Everything sqla recipe
    via https://bitbucket.org/zzzeek/sqlalchemy/wiki/UsageRecipes/DropEverything
    """
    with engine.connect() as conn, conn.begin() as trans:
        inspector = reflection.Inspector.from_engine(engine)
        metadata = MetaData()
        tbs = []
        all_fks = []
        for table_name in inspector.get_table_names():
            if table_name == 'spatial_ref_sys':
                logger.warning('Not dropping postgis gid table')
                continue
            logger.debug(table_name)
            fks = []
            for fk in inspector.get_foreign_keys(table_name):
                if not fk['name']:
                    continue
                fks.append(ForeignKeyConstraint((), (), name=fk['name']))
            t = Table(table_name, metadata, *fks)
            tbs.append(t)
            all_fks.extend(fks)
        for fkc in all_fks:
            conn.execute(DropConstraint(fkc))
        for table in tbs:
            conn.execute(DropTable(table))
        trans.commit()
    conn.close()
    logger.info('All tables successfully dropped')


#
#  == Paginating and Windowing support
#


def column_windows(
    session: Session,
    stmt: Select[Any],
    column: SQLColumnExpression[Any],
    windowsize: int,
) -> Iterator[SQLColumnExpression[bool]]:
    """Return a series of WHERE clauses against
    a given column that break it into windows.

    Result is an iterable of WHERE clauses that are packaged with
    the individual ranges to select from.

    Requires a database that supports window functions,
    i.e. Postgresql, SQL Server, Oracle.

    via zzzeek [link][https://github.com/sqlalchemy/sqlalchemy/wiki/RangeQuery-and-WindowedRangeQuery]
    """

    rownum = func.row_number().over(order_by=column).label('rownum')

    subq = stmt.add_columns(rownum).subquery()
    subq_column = list(subq.columns)[-1]

    target_column = subq.corresponding_column(column)
    new_stmt = select(target_column)

    if windowsize > 1:
        new_stmt = new_stmt.filter(subq_column % windowsize == 1)

    """
    # the SQL statement here is intended to give us a list of ranges,
    # and looks like:

    SELECT anon_1.data
    FROM (SELECT widget.id AS id, widget.data AS data, row_number() OVER (ORDER BY widget.data) AS rownum
    FROM widget) AS anon_1
    WHERE anon_1.rownum %% %(rownum_1)s = %(param_1)s
    """

    intervals = list(session.scalars(new_stmt))

    # yield out WHERE clauses for each range
    while intervals:
        start = intervals.pop(0)
        if intervals:
            yield and_(column >= start, column < intervals[0])
        else:
            yield column >= start


def windowed_query(
    session: Session,
    stmt: Select[Any],
    column: SQLColumnExpression[Any],
    windowsize: int,
) -> Iterator[Result[Any]]:
    """Given a Session and Select() object, organize and execute the statement
    such that it is invoked for ordered chunks of the total result.   yield
    out individual Result objects for each chunk.
    """
    for whereclause in column_windows(session, stmt, column, windowsize):
        yield session.execute(stmt.filter(whereclause).order_by(column))


class Pagination:
    """Internal helper class returned by :meth:`BaseQuery.paginate`.  You
    can also construct it from any other SQLAlchemy query object if you are
    working with other libraries.  Additionally it is possible to pass `None`
    as query object in which case the :meth:`prev` and :meth:`next` will
    no longer work.
    """

    def __init__(self, query, page, per_page, total, items):
        #: the unlimited query object that was used to create this
        #: pagination object.
        self.query = query
        #: the current page number (1 indexed)
        self.page = page
        #: the number of items to be displayed on a page.
        self.per_page = per_page
        #: the total number of items matching the query
        self.total = total
        #: the items for the current page
        self.items = items

    @property
    def pages(self):
        """The total number of pages"""
        if self.per_page == 0:
            pages = 0
        else:
            pages = int(ceil(self.total / float(self.per_page)))
        return pages

    def prev(self, error_out=False):
        """Returns a :class:`Pagination` object for the previous page."""
        assert self.query is not None, 'a query object is required for this method to work'
        return self.query.paginate(self.page - 1, self.per_page, error_out)

    @property
    def prev_num(self):
        """Number of the previous page."""
        return self.page - 1

    @property
    def has_prev(self):
        """True if a previous page exists"""
        return self.page > 1

    def next(self, error_out=False):
        """Returns a :class:`Pagination` object for the next page."""
        assert self.query is not None, 'a query object is required for this method to work'
        return self.query.paginate(self.page + 1, self.per_page, error_out)

    @property
    def has_next(self):
        """True if a next page exists."""
        return self.page < self.pages

    @property
    def next_num(self):
        """Number of the next page"""
        return self.page + 1

    def iter_pages(self, left_edge=2, left_current=2, right_current=5, right_edge=2):
        """Iterates over the page numbers in the pagination.  The four
        parameters control the thresholds how many numbers should be produced
        from the sides.  Skipped page numbers are represented as `None`.
        This is how you could render such a pagination in the templates:
        .. sourcecode:: html+jinja
            {% macro render_pagination(pagination, endpoint) %}
              <div class=pagination>
              {%- for page in pagination.iter_pages() %}
                {% if page %}
                  {% if page != pagination.page %}
                    <a href="{{ url_for(endpoint, page=page) }}">{{ page }}</a>
                  {% else %}
                    <strong>{{ page }}</strong>
                  {% endif %}
                {% else %}
                  <span class=ellipsis>...</span>
                {% endif %}
              {%- endfor %}
              </div>
            {% endmacro %}
        """
        last = 0
        for num in range(1, self.pages + 1):
            if (
                num <= left_edge
                or (num > self.page - left_current - 1 and num < self.page + right_current)
                or num > self.pages - right_edge
            ):
                if last + 1 != num:
                    yield None
                yield num
                last = num


def paginate(q, page, per_page=20):
    """Returns `per_page` items from page `page`.  By default it will
    Returns an :class:`Pagination` object.
    stolen from [flask-sqlalchemy][https://github.com/mitsuhiko/flask-sqlalchemy/]

    TODO: allow windowing instead of vanilla offset/limit

    >>> paginate(db_session.query(Foo).filter(Bar.baz==1), 5, 10).iter_pages  # doctest: +SKIP
    """
    items = q.limit(per_page).offset((page - 1) * per_page).all()

    # No need to count if there are fewer items than we expected.
    if page == 1 and len(items) < per_page:
        total = len(items)
    else:
        total = q.order_by(None).count()

    return Pagination(q, page, per_page, total, items)


#
#  == `EXPLAIN [ANALYZE]` SQL profiling support
#


class explain(Executable, ClauseElement):
    """== Wrap a Query object in an `EXPLAIN [ANALYZE]` statement
    via [@zzzeek][https://bitbucket.org/zzzeek/sqlalchemy/wiki/UsageRecipes/Explain]
    the `pg_explain` below is an example from the same recipe list that works well

    >>> from sqlalchemy.orm import sessionmaker  # doctest: +SKIP
    >>> engine = create_engine('postgresql://scott@localhost/test', echo=True)  # doctest: +SKIP
    >>> sess = sessionmaker(engine)()  # doctest: +SKIP
    >>> print(sess.execute(explain("SELECT * FROM foo")).fetchall())  # doctest: +SKIP
    """

    def __init__(self, stmt, analyze=False):
        self.statement = _literal_as_text(stmt)
        self.analyze = analyze
        # helps with INSERT statements
        self.inline = getattr(stmt, 'inline', None)


@compiles(explain, 'postgresql')
def pg_explain(element, compiler, **kw):
    text = 'EXPLAIN '
    if element.analyze:
        text += 'ANALYZE '
    text += compiler.process(element.statement, **kw)
    return text


@compiles(explain, 'mssql')
def ms_explain(element, compiler, **kw):
    """The inelegant equivalent options for `EXPLAIN [ANALYZE]` on mssql"""
    switch = 'SHOWPLAN_ALL'
    if element.analyze:
        switch = 'STATISTICS PROFILE'
    stmt = compiler.process(element.statement, **kw)
    text = f'SET {switch} ON; {stmt}; SET {switch} OFF;'
    return text
