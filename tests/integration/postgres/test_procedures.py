import database as db


def test_procedure_basic(psql_docker, pg_conn):
    """Test basic stored procedure functionality with a simple query"""
    # Create a function that returns a result set
    # First check the type of the name column
    column_type = db.select_scalar(pg_conn, """
    SELECT data_type FROM information_schema.columns
    WHERE table_name = 'test_table' AND column_name = 'name'
    """)

    # Create function with matching data types
    if column_type == 'character varying':
        db.execute(pg_conn, """
        CREATE OR REPLACE FUNCTION get_test_data() RETURNS TABLE(name VARCHAR, value INTEGER) AS $$
        BEGIN
            RETURN QUERY SELECT t.name, t.value FROM test_table t ORDER BY t.name LIMIT 3;
        END;
        $$ LANGUAGE plpgsql;
        """)
    else:
        db.execute(pg_conn, """
        CREATE OR REPLACE FUNCTION get_test_data() RETURNS TABLE(name TEXT, value INTEGER) AS $$
        BEGIN
            RETURN QUERY SELECT t.name, t.value FROM test_table t ORDER BY t.name LIMIT 3;
        END;
        $$ LANGUAGE plpgsql;
        """)

    # Call using select
    result = db.select(pg_conn, 'SELECT * FROM get_test_data()')

    # Verify results
    assert isinstance(result, list)
    assert len(result) == 3
    assert 'name' in result[0]
    assert 'value' in result[0]


def test_procedure_multiple_resultsets(psql_docker, pg_conn):
    """Test stored procedure with a function returning multiple result sets"""
    # Create a function that returns multiple result sets
    db.execute(pg_conn, """
    CREATE OR REPLACE FUNCTION multi_results() RETURNS SETOF RECORD AS $$
    DECLARE
        all_rows RECORD;
    BEGIN
        -- First result set: 3 rows
        FOR all_rows IN SELECT name, value FROM test_table ORDER BY value DESC LIMIT 3 LOOP
            RETURN NEXT all_rows;
        END LOOP;

        -- Second result set: 1 row (should not be returned by default as it's smaller)
        RETURN QUERY EXECUTE 'SELECT ''Summary'' AS name, COUNT(*)::integer AS value FROM test_table';

        RETURN;
    END;
    $$ LANGUAGE plpgsql ROWS 4;
    """)

    # Need to use multiple statements to force multiple result sets
    sql = """
    SELECT * FROM test_table WHERE value > 50 ORDER BY value DESC LIMIT 3;
    SELECT 'Summary' AS name, COUNT(*)::integer AS value FROM test_table;
    """

    # Call with return_all to get all result sets
    all_results = db.select(pg_conn, sql, return_all=True)

    # Verify we got multiple result sets
    assert isinstance(all_results, list)
    assert len(all_results) == 2

    # First result should be the larger one (3 rows)
    assert len(all_results[0]) > 0

    # Second result should be the summary (1 row)
    assert len(all_results[1]) == 1
    assert all_results[1][0]['name'] == 'Summary'

    # Default behavior should return largest result set
    default_result = db.select(pg_conn, sql)
    assert len(default_result) == len(all_results[0])

    # prefer_first should return first result set
    first_result = db.select(pg_conn, sql, prefer_first=True)
    assert len(first_result) == len(all_results[0])
    assert first_result == all_results[0]


def test_procedure_empty_resultset(psql_docker, pg_conn):
    """Test stored procedur with empty result sets"""
    # SQL that returns empty result sets
    sql = """
    SELECT * FROM test_table WHERE 1=0;
    SELECT * FROM test_table WHERE name = 'NonexistentName';
    """

    # Should return empty list
    result = db.select(pg_conn, sql)
    assert isinstance(result, list)
    assert len(result) == 0

    # With return_all, should return list of empty lists
    all_results = db.select(pg_conn, sql, return_all=True)
    assert isinstance(all_results, list)
    assert len(all_results) == 2
    assert len(all_results[0]) == 0
    assert len(all_results[1]) == 0


if __name__ == '__main__':
    __import__('pytest').main([__file__])
