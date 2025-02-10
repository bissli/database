import logging
from dataclasses import dataclass

import psycopg

logger = logging.getLogger(__name__)


@dataclass
class PostgresErrorInfo:
    """Structured error information with essential context"""
    message: str
    detail: str | None = None
    hint: str | None = None
    column: str | None = None  # The critical field for error diagnosis
    context: dict | None = None  # For failing values/row data


def handle_pg_error(error: psycopg.Error, context: dict | None = None) -> PostgresErrorInfo:
    """Simplified error handler focused on column identification"""
    diag = getattr(error, 'diag', None)

    # Extract core information directly from diagnostics
    error_info = PostgresErrorInfo(
        message=getattr(diag, 'message_primary', str(error)),
        detail=getattr(diag, 'message_detail', None),
        hint=getattr(diag, 'message_hint', None),
        column=getattr(diag, 'column_name', None),  # Direct column name extraction
        context=context
    )

    # Enrich with context if provided
    if error_info.detail and context:
        error_info.detail += f'\nContext: {context}'

    logger.error(
        'Database error: %s [Column: %s]',
        error_info.message,
        error_info.column,
        extra={
            'error_type': type(error).__name__,
            'column': error_info.column,
            'values': context.get('values') if context else None
        }
    )

    return error_info
