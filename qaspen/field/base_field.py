import abc
import typing


class MetaClassField(abc.ABC):

    def __set_name__(
        self: typing.Self,
        owner: typing.Any,
        variable_name: str,
    ) -> None:
        self.field_name: str = variable_name

    @abc.abstractmethod
    def make_field_create_statement(
        self: typing.Self,
    ) -> str:
        ...


class BaseField(MetaClassField):
    def __init__(
        self: typing.Self,
        field_value: typing.Any = None,
        is_null: bool = False,
        default: typing.Any = None,
        db_field_name: str | None = None,
    ) -> None:
        self.field_value: typing.Any = field_value
        self.is_null: bool = is_null
        self.default: typing.Any = default

        if db_field_name:
            self.field_name = db_field_name
