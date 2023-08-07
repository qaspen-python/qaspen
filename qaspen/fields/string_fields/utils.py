from qaspen.exceptions import StringFieldDeclarationError
from qaspen.fields.string_fields.consts import MAX_STRING_FIELD_LENGTH


def validate_max_length(max_length: int) -> None:
    if max_length >= MAX_STRING_FIELD_LENGTH:
        raise StringFieldDeclarationError(
            f"max_length parameter must be less "
            f"or equal to {MAX_STRING_FIELD_LENGTH}",
        )
