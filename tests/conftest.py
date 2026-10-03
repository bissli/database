import pathlib
import site

import pytest
from database.cache import Cache

site.addsitedir(pathlib.Path(__file__).resolve().parent)


@pytest.fixture(autouse=True)
def clear_caches():
    """Empty the shared TTL cache before and after every test.
    """
    Cache.get_instance().clear_all()
    yield
    Cache.get_instance().clear_all()


pytest_plugins = [
    'tests.fixtures.mocks',
    'tests.fixtures.values',
    'tests.fixtures.sqlite',
    'tests.fixtures.postgres',
    ]
