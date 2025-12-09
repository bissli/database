import pathlib
import site

import pytest
from database.cache import Cache

HERE = pathlib.Path(pathlib.Path(__file__).resolve()).parent
site.addsitedir(HERE)


@pytest.fixture(autouse=True)
def clear_caches():
    """Clear all caches before and after each test to ensure test isolation."""
    Cache.get_instance().clear_all()
    yield
    Cache.get_instance().clear_all()


pytest_plugins = [
    'tests.fixtures.mocks',
    'tests.fixtures.values',
    'tests.fixtures.sqlite',
    'tests.fixtures.postgres',
]
