"""Operation when we want to create table."""
import inspect
import typing

from qaspen.fields.base_field import FieldType
from qaspen.fields.fields import Field
from qaspen.migrations.inheritance import ClassAsString
from qaspen.migrations.operations.base_operation import Operation
from qaspen.querystring.querystring import QueryString


class CreateTableOperation(Operation, ClassAsString):
    def __init__(
        self: typing.Self,
        table_name: str,
        fields: dict[str, Field[FieldType]],
        additional_options: dict[str, typing.Any] | None = None,
    ) -> None:
        self.table_name: typing.Final = table_name
        self.fields: typing.Final = fields
        self.additional_options: typing.Final = additional_options

    def statement(self: typing.Self) -> QueryString:
        return QueryString(
            sql_template="",
        )

    def turn_into_string(self: typing.Self) -> str:
        parameters_name: typing.Final = [
            parameter
            for parameter in inspect.signature(
                self.__class__.__init__,
            ).parameters.keys()
            if parameter not in ("self", "args", "kwargs", "fields")
        ]
        parameters_dict = {}
        for parameter_name in parameters_name:
            parameter_value = self.__dict__.get(parameter_name)
            parameters_dict[parameter_name] = parameter_value

        parameters_string = ", ".join(
            f"{parameter_name}={parameter_value.__repr__()}"
            for parameter_name, parameter_value in parameters_dict.items()
        )
        parameters_string += f", {self.process_fields_param()}"

        return f"{self.__class__.__name__}({parameters_string})"

    def process_fields_param(self: typing.Self) -> str:
        field_param_str: str = "fields={"
        for field_name, field_value in self.fields.items():
            field_param_str += (
                f"'{field_name}': {field_value.turn_into_string()},"
            )
        field_param_str += "}"

        return field_param_str
