"""
Result set adapters to provide consistent interfaces across different database backends.
"""

from database.adapters.row_adapter import DatabaseRowAdapter


class ResultSetAdapter:
    """Adapter for sets of database rows providing consistent interface"""

    def __init__(self, connection, data):
        self.connection = connection
        self.data = data

    def to_dict_list(self):
        """Convert all rows to dictionaries"""
        return [DatabaseRowAdapter.create(self.connection, row).to_dict() for row in self.data]

    def get_first_value(self):
        """Get the first value from the first row"""
        if not self.data:
            return None
        return DatabaseRowAdapter.create(self.connection, self.data[0]).get_value()

    def get_first_row_dict(self):
        """Get the first row as a dictionary"""
        if not self.data:
            return None
        return DatabaseRowAdapter.create(self.connection, self.data[0]).to_dict()

    def get_column_values(self, column_index=0):
        """Get values from a specific column across all rows"""
        result = []
        for row in self.data:
            adapter = DatabaseRowAdapter.create(self.connection, row)
            if isinstance(column_index, int):
                # Get by position
                try:
                    if hasattr(row, '__getitem__'):
                        result.append(row[column_index])
                    else:
                        # Can't get by index, try first value
                        result.append(adapter.get_value())
                except (IndexError, KeyError):
                    result.append(None)
            else:
                # Get by column name
                try:
                    result.append(adapter.get_value(column_index))
                except (IndexError, KeyError):
                    result.append(None)
        return result
