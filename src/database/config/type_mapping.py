"""
Configuration for database column type mappings.
"""
import json
import logging
import pathlib
import re

logger = logging.getLogger(__name__)


class TypeMappingConfig:
    """Configuration for custom type mappings"""

    _instance = None

    @classmethod
    def get_instance(cls):
        """Get singleton instance"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self, config_file=None):
        # Default mappings
        self._pattern_mappings = {
            'postgresql': {
                'patterns': {},
                'columns': {}
            },
            'sqlite': {
                'patterns': {},
                'columns': {}
            }
        }

        # Load from config file if provided
        if config_file:
            self.load_config(config_file)
        else:
            # Try default locations
            default_locations = [
                pathlib.Path('~/.config/database/type_mapping.json').expanduser(),
                '/etc/database/type_mapping.json',
                'type_mapping.json'  # Current directory
            ]

            for location in default_locations:
                if pathlib.Path(location).exists():
                    self.load_config(location)
                    break

    def load_config(self, config_file):
        """Load configuration from file"""
        try:
            with pathlib.Path(config_file).open() as f:
                config = json.load(f)

            # Merge with defaults rather than replace entirely
            for db_type, mappings in config.items():
                if db_type not in self._pattern_mappings:
                    self._pattern_mappings[db_type] = {'patterns': {}, 'columns': {}}

                # Merge patterns
                if 'patterns' in mappings:
                    self._pattern_mappings[db_type]['patterns'].update(mappings['patterns'])

                # Merge columns
                if 'columns' in mappings:
                    self._pattern_mappings[db_type]['columns'].update(mappings['columns'])

            logger.info(f'Loaded type mapping configuration from {config_file}')
        except Exception as e:
            logger.warning(f'Failed to load type mapping config: {e}')

    def get_type_for_column(self, db_type, table_name, column_name):
        """Get configured type for a specific column"""
        if db_type not in self._pattern_mappings:
            return None

        # Check for exact column match first
        columns = self._pattern_mappings[db_type]['columns']

        # Try with table.column format
        if table_name:
            key = f'{table_name.lower()}.{column_name.lower()}'
            if key in columns:
                return columns[key]

        # Then try just column name
        if column_name.lower() in columns:
            return columns[column_name.lower()]

        # Fall back to patterns
        patterns = self._pattern_mappings[db_type]['patterns']

        for pattern, dtype in patterns.items():
            if re.search(pattern, column_name.lower()):
                return dtype

        return None

    def add_column_mapping(self, db_type, table_name, column_name, data_type):
        """Add a specific column mapping"""
        if db_type not in self._pattern_mappings:
            self._pattern_mappings[db_type] = {'patterns': {}, 'columns': {}}

        key = f'{table_name.lower()}.{column_name.lower()}' if table_name else column_name.lower()
        self._pattern_mappings[db_type]['columns'][key] = data_type
