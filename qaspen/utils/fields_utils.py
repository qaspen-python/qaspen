import typing


def transform_value_to_sql(value_to_transform: typing.Any) -> str:
    if isinstance(value_to_transform, str):
        return f"'{value_to_transform}'"

    return str(value_to_transform)
