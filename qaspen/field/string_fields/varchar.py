import typing
from qaspen.field.base_field import BaseField


class VarCharField(BaseField):
    """"""
    value: str | None = None
    max_length: int = 100

    def make_field_create_statement(
        self: typing.Self,
    ) -> str:
        return "Wow"
