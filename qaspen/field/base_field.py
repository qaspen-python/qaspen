import abc
import typing


class MetaClassField(abc.ABC):
    value: typing.Any = None

    @abc.abstractmethod
    def make_field_create_statement(
        self: typing.Self,
    ) -> str:
        ...


class BaseField(MetaClassField):

    def __set_name__(
        self: typing.Self,
        owner: typing.Any,
        variable_name: str,
    ) -> None:
        self.field_name: str = variable_name
