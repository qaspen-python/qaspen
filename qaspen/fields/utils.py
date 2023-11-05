from typing import Final

MAX_STRING_FIELD_LENGTH: Final[int] = 10485760


def validate_max_length(max_length: int) -> bool:
    """Validate length of the string.

    ### Returns:
    boolean flag.
    """
    return max_length >= MAX_STRING_FIELD_LENGTH
