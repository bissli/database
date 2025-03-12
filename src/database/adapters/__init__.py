"""
Database adapters package.

This package provides the following components:

- column_info: Column metadata classes
- structure: Database row and result structure mapping (no type conversion)
- type_conversion: Parameter conversion for database input using the AdapterRegistry
- type_mapping: Database type resolution and type handlers (no conversion)

Type conversion principles:
1. Database → Python: Handled SOLELY by database driver adapters
2. Python → Database: Handled by TypeConverter and registered adapters during parameter binding

The Column class and structure adapters do NOT perform conversions - they only
handle metadata and structure mapping.

The AdapterRegistry provides per-connection adapter registration for consistent
type handling across different database backends.
"""

# Export adapter classes and functions
from database.adapters.column_info import *
from database.adapters.structure import *
from database.adapters.type_mapping import *
from database.adapters.type_conversion import TypeConverter
from database.adapters.type_conversion import get_adapter_registry
