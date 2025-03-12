import logging
import os
import site

import pytest

logger = logging.getLogger(__name__)

HERE = os.path.dirname(os.path.abspath(__file__))
site.addsitedir(HERE)


def pytest_addoption(parser):
    parser.addoption(
        '--log',
        action='store',
        default='INFO',
        help='set logging level',
    )


@pytest.fixture(scope='session')
def logger(request):

    loglevel = request.config.getoption('--log')

    numeric_level = getattr(logging, loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {loglevel}')

    logging.basicConfig()
    logger = logging.getLogger(__name__)
    logger.setLevel(numeric_level)
    return logger


pytest_plugins = [
    'tests.fixtures.mocks',
    'tests.fixtures.values',
    'tests.fixtures.postgres',
    'tests.fixtures.sqlserver',
    'tests.fixtures.sqlite'
]
