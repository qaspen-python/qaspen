import typing
from qaspen.exceptions import OnJoinComparisonError
from qaspen.fields.base.base_field import BaseField
from qaspen.querystring.querystring import QueryString
from qaspen.statements.combinable_statements.combinations import (
    CombinableExpression,
    ExpressionsCombination
)
from qaspen.statements.combinable_statements.where_statement import (
    Where,
)

from qaspen.statements.statement import BaseStatement
from qaspen.table.meta_table import MetaTable


class JoinStatement(BaseStatement):

    operator: str = "JOIN"

    def __init__(
        self: typing.Self,
        fields: typing.Iterable[BaseField[typing.Any]],
        join_table: type[MetaTable],
        on: CombinableExpression,
        alias: str,
    ) -> None:
        self._fields = fields
        self._join_table = join_table
        self._on: CombinableExpression = on
        self._alias = alias

    def querystring(self: typing.Self) -> QueryString:
        sql_template: typing.Final[str] = (
            "{} {} as {} ON {}"
        )
        self._on = self._change_combinable_expression(self._on)
        return QueryString(
            self.operator,
            self._join_table._table_name(),
            self._alias,
            self._on.querystring(),
            sql_template=sql_template,
        )

    def _change_combinable_expression(
        self: typing.Self,
        expression: CombinableExpression,
    ) -> CombinableExpression:
        if not isinstance(expression, ExpressionsCombination):
            if isinstance(expression, Where):
                return self._change_where_combination(
                    expression=expression,
                )

        if isinstance(expression, ExpressionsCombination):
            self._change_combinable_expression(
                expression.left_expression,
            )
            self._change_combinable_expression(
                expression.right_expression,
            )
        return expression

    def _change_where_combination(
        self: typing.Self,
        expression: Where,
    ) -> Where:
        if not isinstance(expression.comparison_value, BaseField):
            raise OnJoinComparisonError(
                f"You can't use not `Field` class in ON clause in "
                f"`.join()` method. "
                f"You used - {expression.comparison_value}",
            )

        is_left_to_change: bool = (
            expression.field._field_data.from_table._table_name()
            == self._join_table._table_name()
        )

        if is_left_to_change:
            expression.field = (
                expression.field._with_prefix(self._alias)
            )
            return expression
        else:
            expression.comparison_value = (
                expression.comparison_value._with_prefix(  # type: ignore
                    self._alias,
                )
            )
            return expression
