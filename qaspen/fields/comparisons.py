import dataclasses
import typing

from qaspen.fields.base_field import Field
from qaspen.fields.operators import BaseOperator


@dataclasses.dataclass
class WhereComparison:
    field: Field[typing.Any]
    compare_with_value: typing.Any
    operator: type[BaseOperator]

    def to_sql_statement(self: typing.Self) -> str:
        where_clause: str = ""
        where_clause += (
            self
            .operator
            .operation_template
            .format(
                field_name=self.field.field_name,
                compare_value=self.compare_with_value,
            )
        )

        return where_clause
