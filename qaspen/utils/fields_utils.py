import typing


def transform_value_to_sql(value_to_convert: typing.Any) -> str:
    """Convert python value to SQL string.

    ### Parameters:
    - `value_to_convert`: value to convert into SQL string.

    ### Returns:
    SQL string.
    """
    if value_to_convert is None:
        return "NULL"
    if isinstance(value_to_convert, str):
        return f"'{value_to_convert}'"
    if isinstance(value_to_convert, int):
        return f"{value_to_convert}"

    return str(value_to_convert)
