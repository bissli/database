import logging
import re
from dataclasses import dataclass
from typing import Any
from collections.abc import Sequence

import psycopg

logger = logging.getLogger(__name__)

# Error patterns
ERROR_TYPES = {
    'PERMISSION': r'(?i)permission denied|insufficient privilege',
    'DUPLICATE': r'(?i)duplicate key value|already exists',
    'NOT_FOUND': r'(?i)does not exist|no such|missing',
    'FOREIGN_KEY': r'(?i)violates foreign key',
    'NOT_NULL': r'(?i)null value.*violates not-null',
    'CHECK_CONSTRAINT': r'(?i)violates check constraint',
    'TYPE_ERROR': r'(?i)invalid input syntax for type|cannot cast type',
    'SYNTAX': r'(?i)syntax error',
    'CONNECTION': r'(?i)connection.*failed|timeout|terminated',
    'NUMERIC_RANGE': r'(?i)\w+ out of range',
}

COLUMN_PATTERNS = [
    r'(?i)column [""]?([^""\s]+)[""]? of relation',
    r'(?i)column [""]?([^""\s]+)[""]? does not exist',
    r'(?i)null value in column [""]?([^""\s]+)[""]? violates',
    r'(?i)invalid input syntax for type .* in column [""]?([^""\s]+)[""]?',
]

TABLE_PATTERNS = [
    r'relation [""]?([^""\s]+)[""]?',
    r'table [""]?([^""\s]+)[""]?',
    r'(?i)insert\s+into\s+([^\s(]+)',  # Case insensitive INSERT INTO
]

TYPE_PATTERNS = [
    (r'(?i)invalid input syntax for type (\w+): "([^"]+)"',
     lambda m: ('value', f"Cannot convert '{m.group(2)}' to type {m.group(1)}")),
    (r'(?i)cannot cast type (\w+) to (\w+)',
     lambda m: ('value', f'Cannot convert from type {m.group(1)} to {m.group(2)}')),
    (r'(?i)(\w+) out of range',
     lambda m: ('value', f'{m.group(1).capitalize()} value is outside allowed range')),
]


@dataclass
class QueryContext:
    """Context information for a database query"""
    sql: str
    args: Sequence[Any] | None = None
    kwargs: dict[str, Any] | None = None


@dataclass
class PostgresErrorInfo:
    """Structured error information with essential context"""
    message: str
    error_type: str
    detail: str | None = None
    column: str | None = None
    table: str | None = None
    context: QueryContext | None = None

    def __str__(self) -> str:
        """Format the error information as a string with all available details"""
        parts = [f'{self.error_type}: {self.message}']
        if self.detail:
            parts.append(f'Detail: {self.detail}')
        if self.table or self.column:
            location = ' '.join(filter(None, [
                f'table={self.table}' if self.table else None,
                f'column={self.column}' if self.column else None
            ]))
            parts.append(f'Location: {location}')
        if self.context:
            parts.append(f"SQL: {self.context.sql}")
            if self.context.args:
                parts.append(f"Args: {self.context.args}")
            if self.context.kwargs:
                parts.append(f"Kwargs: {self.context.kwargs}")
        return ' | '.join(parts)


def parse_error(error_message: str) -> tuple[str, str | None, str | None, str | None]:
    """Parse error message to extract type, column, table and detail"""
    # Get error type
    error_type = 'UNKNOWN'
    for type_name, pattern in ERROR_TYPES.items():
        if re.search(pattern, error_message, re.IGNORECASE):
            error_type = type_name
            break

    # Extract column name
    column = None
    for pattern in COLUMN_PATTERNS:
        if match := re.search(pattern, error_message, re.IGNORECASE):
            column = match.group(1).strip('"\'').split(',')[0].strip()
            break

    # Extract table name
    table = None
    for pattern in TABLE_PATTERNS:
        if match := re.search(pattern, error_message, re.IGNORECASE):
            table = match.group(1).strip('"\'').split('.')[-1]
            break

    # Handle type errors
    detail = None
    if error_type == 'TYPE_ERROR':
        for pattern, handler in TYPE_PATTERNS:
            if match := re.search(pattern, error_message):
                column, detail = handler(match)
                break

    return error_type, column, table, detail


def handle_pg_error(error: psycopg.Error, context: QueryContext | None = None) -> PostgresErrorInfo:
    """Handle PostgreSQL errors with pattern matching"""
    # Clean up error message
    error_message = str(error).split('\n')[0]
    error_message = re.sub(r'^(?:TYPE_ERROR|SYNTAX|ERROR):\s*', '', error_message)

    # Parse error message
    error_type, column, table, detail = parse_error(error_message)

    # Create error info
    error_info = PostgresErrorInfo(
        message=error_message,
        error_type=error_type,
        table=table,
        column=column,
        detail=detail,
        context=context
    )

    logger.error('Database error', extra={'error_info': str(error_info)})
    return error_info
