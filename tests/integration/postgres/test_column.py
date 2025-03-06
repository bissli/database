import database as db
from database import Column


def test_column_type_extraction(psql_docker, conn):
    """Test that column type information is correctly extracted"""
    # Create a table with various types
    with db.transaction(conn) as tx:
        tx.execute("""
        CREATE TEMPORARY TABLE type_test (
            id INTEGER PRIMARY KEY,
            name TEXT,
            price NUMERIC(10,2),
            in_stock BOOLEAN,
            created_at TIMESTAMP
        )
        """)

        # Insert test data
        tx.execute("""
        INSERT INTO type_test (id, name, price, in_stock, created_at)
        VALUES (1, 'Test Item', 19.99, true, current_timestamp)
        """)

        # Query the data
        result = tx.select('SELECT * FROM type_test')

        # Verify DataFrame has type information
        assert 'column_types' in result.attrs
        column_types = result.attrs['column_types']

        # Check specific column types
        assert column_types['id']['python_type'] == 'int'
        assert column_types['name']['python_type'] == 'str'
        assert column_types['price']['python_type'] == 'float'
        assert column_types['in_stock']['python_type'] == 'bool'
        assert column_types['created_at']['python_type'] == 'datetime'


def test_column_import_availability(psql_docker, conn):
    """Test that Column class is directly importable from database module"""
    # Verify Column class is available and has expected methods
    assert hasattr(Column, 'get_names')
    assert hasattr(Column, 'get_column_by_name')
    assert hasattr(Column, 'get_column_types_dict')
    assert hasattr(Column, 'create_empty_columns')


def test_empty_result_types(psql_docker, conn):
    """Test that empty result sets preserve column type information"""
    # Create a table with various types but no data
    with db.transaction(conn) as tx:
        tx.execute("""
        CREATE TEMPORARY TABLE empty_type_test (
            id INTEGER PRIMARY KEY,
            name TEXT,
            price NUMERIC(10,2),
            in_stock BOOLEAN,
            created_at TIMESTAMP
        )
        """)

        # Query the empty table
        result = tx.select('SELECT * FROM empty_type_test')

        # Verify DataFrame has type information even though it's empty
        assert len(result) == 0
        assert 'column_types' in result.attrs
        column_types = result.attrs['column_types']

        # Check specific column types
        assert column_types['id']['python_type'] == 'int'
        assert column_types['name']['python_type'] == 'str'
        assert column_types['price']['python_type'] == 'float'
        assert column_types['in_stock']['python_type'] == 'bool'
        assert column_types['created_at']['python_type'] == 'datetime'


def test_custom_typed_loader(psql_docker, conn):
    """Test using a custom data loader that processes type information"""
    from database.options import DatabaseOptions

    # Define custom loader
    def typed_dict_data_loader(data, columns, **kwargs):
        column_names = Column.get_names(columns)

        if not data:
            return {
                'data': [],
                'columns': column_names,
                'column_types': Column.get_column_types_dict(columns)
            }

        return {
            'data': list(data),
            'columns': column_names,
            'column_types': Column.get_column_types_dict(columns)
        }

    # Create a connection with the custom loader
    options = DatabaseOptions(
        drivername='postgres',
        hostname=conn.options.hostname,
        username=conn.options.username,
        password=conn.options.password,
        database=conn.options.database,
        port=conn.options.port,
        timeout=conn.options.timeout,  # Include the timeout parameter
        data_loader=typed_dict_data_loader
    )

    typed_cn = db.connect(options)

    # Create a test table
    with db.transaction(typed_cn) as tx:
        tx.execute("""
        CREATE TEMPORARY TABLE custom_type_test (
            id INTEGER PRIMARY KEY,
            name TEXT,
            price NUMERIC(10,2)
        )
        """)

        tx.execute("""
        INSERT INTO custom_type_test (id, name, price)
        VALUES (1, 'Test Item', 19.99)
        """)

        # Query with the custom loader
        result = tx.select('SELECT * FROM custom_type_test')

        # Verify the structure of the result
        assert 'data' in result
        assert 'columns' in result
        assert 'column_types' in result

        # Verify the data
        assert len(result['data']) == 1
        assert result['data'][0]['id'] == 1
        assert result['data'][0]['name'] == 'Test Item'
        assert result['data'][0]['price'] == 19.99

        # Verify the columns
        assert result['columns'] == ['id', 'name', 'price']

        # Verify the column types
        assert result['column_types']['id']['python_type'] == 'int'
        assert result['column_types']['name']['python_type'] == 'str'
        assert result['column_types']['price']['python_type'] == 'float'


def test_column_create_methods(psql_docker, conn):
    """Test Column creation methods"""
    # Test creating empty columns
    column_names = ['id', 'name', 'active']
    empty_columns = Column.create_empty_columns(column_names)

    assert len(empty_columns) == 3
    assert empty_columns[0].name == 'id'
    assert empty_columns[1].name == 'name'
    assert empty_columns[2].name == 'active'

    # Test column name extraction
    extracted_names = Column.get_names(empty_columns)
    assert extracted_names == column_names

    # Test column type dictionary
    type_dict = Column.get_column_types_dict(empty_columns)
    assert 'id' in type_dict
    assert 'name' in type_dict
    assert 'active' in type_dict


if __name__ == '__main__':
    __import__('pytest').main([__file__])
