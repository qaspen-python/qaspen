from typing import Final

from qaspen.exceptions import StringFieldDeclarationError

MAX_STRING_FIELD_LENGTH: Final[int] = 10485760


def validate_max_length(max_length: int) -> bool:
    if max_length >= MAX_STRING_FIELD_LENGTH:
        raise StringFieldDeclarationError(
            f"max_length parameter must be less "
            f"or equal to {MAX_STRING_FIELD_LENGTH}",
        )
    return True
