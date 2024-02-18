class QaspenBaseError(Exception):
    """Base error for all errors."""


class DatabaseUrlParseError(QaspenBaseError):
    """If database from connection url cannot be parsed."""


class ColumnBaseError(QaspenBaseError):
    """Base error for all column-related errors."""


class ColumnValueValidationError(ColumnBaseError):
    """Error for column value validation error."""


class ColumnDeclarationError(ColumnBaseError):
    """Error for column declaration error."""


class ColumnComparisonError(ColumnBaseError):
    """Error for error in column comparison.

    Usually if types are incorrect.
    """


class FilterComparisonError(ColumnBaseError):
    """Error if there is an mistake in Filter."""


class QueryResultError(QaspenBaseError):
    """Base error for QueryResult exceptions."""


class QueryResultLookupError(QueryResultError):
    """Error for processing result from engine."""
