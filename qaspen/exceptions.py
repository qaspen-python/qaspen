class QaspenBaseError(Exception):
    """Base error for all errors."""


class FieldBaseError(QaspenBaseError):
    """Base error for all field-related errors."""


class FieldValueValidationError(FieldBaseError):
    """Error for field value validation error."""


class FieldDeclarationError(FieldBaseError):
    """Error for field declaration error."""


class FieldComparisonError(FieldBaseError):
    """Error for error in field comparison.

    Usually if types are incorrect.
    """


class FilterComparisonError(FieldBaseError):
    """Error if there is an mistake in Filter."""


class QueryResultError(QaspenBaseError):
    """Base error for QueryResult exceptions."""


class QueryResultLookupError(QueryResultError):
    """Error for processing result from engine."""
