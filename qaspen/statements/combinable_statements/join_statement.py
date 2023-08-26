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
    WhereBetween,
    WhereExclusive,
)

from qaspen.statements.statement import BaseStatement
from qaspen.table.meta_table import MetaTable


class JoinStatement(BaseStatement):

    operator: str = "JOIN"

    def __init__(
        self: typing.Self,
        fields: typing.Iterable[BaseField[typing.Any]],
        join_table: type[MetaTable],
        from_table: type[MetaTable],
        on: CombinableExpression,
        alias: str,
    ) -> None:
        self._fields: typing.Final[
            typing.Iterable[BaseField[typing.Any]]
        ] = fields
        self._join_table: typing.Final[type[MetaTable]] = join_table
        self._from_table: typing.Final[type[MetaTable]] = from_table
        self._on: CombinableExpression = on
        self._alias: typing.Final[str] = alias

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
            if isinstance(expression, WhereBetween):
                return self._change_where_between_combination(
                    expression=expression,
                )

        if isinstance(expression, ExpressionsCombination):
            self._change_combinable_expression(
                expression.left_expression,
            )
            self._change_combinable_expression(
                expression.right_expression,
            )

        if isinstance(expression, WhereExclusive):
            self._change_combinable_expression(
                expression.comparison,
            )

        return expression

    def _change_where_combination(
        self: typing.Self,
        expression: Where,
    ) -> Where:
        self._check_fields_in_join(
            (
                expression.field,
                expression.comparison_value,
            )
        )

        if_left_to_change: typing.Final[bool] = all(
            (
                isinstance(expression.field, BaseField),
                expression.field._field_data.from_table._table_name()
                == self._join_table._table_name()
            ),
        )

        is_right_to_change: typing.Final[bool] = (
            isinstance(expression.comparison_value, BaseField)
            and
            (expression.comparison_value.table_name)
            == self._join_table._table_name()
        )

        if if_left_to_change:
            expression.field = (
                expression.field._with_prefix(self._alias)
            )
        if is_right_to_change:
            expression.comparison_value = (
                expression.comparison_value._with_prefix(  # type: ignore
                    self._alias,
                )
            )

        return expression

    def _change_where_between_combination(
        self: typing.Self,
        expression: WhereBetween,
    ) -> WhereBetween:
        self._check_fields_in_join(
            (
                expression.field,
                expression.left_comparison_value,
                expression.right_comparison_value,
            ),
        )

        is_field_to_change: bool = (
            expression.field._field_data.from_table._table_name()
            == self._join_table._table_name()
        )

        if is_field_to_change:
            expression.field = (
                expression.field._with_prefix(self._alias)
            )
            return expression

        is_left_comparison_to_change: typing.Final[bool] = (
            isinstance(expression.left_comparison_value, BaseField)
            and
            expression.left_comparison_value.table_name
            == self._join_table._table_name()
        )

        if is_left_comparison_to_change:
            expression.left_comparison_value = (
                expression.left_comparison_value._with_prefix(
                    self._alias,
                )
            )

        is_right_comparison_to_change: typing.Final[bool] = (
            isinstance(expression.right_comparison_value, BaseField)
            and
            expression.right_comparison_value.table_name
            == self._join_table._table_name()
        )
        if is_right_comparison_to_change:
            expression.right_comparison_value = (
                expression.right_comparison_value._with_prefix(
                    self._alias,
                )
            )

        return expression

    def _check_fields_in_join(
        self: typing.Self,
        for_checks_join_fields: tuple[typing.Any, ...],
    ) -> None:
        for for_checks_join_field in for_checks_join_fields:
            if isinstance(for_checks_join_field, BaseField):
                self._is_field_in_join(for_checks_join_field)

    def _is_field_in_join(
        self: typing.Self,
        field: BaseField[typing.Any],
    ) -> None:
        is_field_from_from_table: bool = (
           field._field_data.from_table._table_name()
           == self._from_table._table_name()
        )
        if_field_from_join_table: bool = (
            field._field_data.from_table._table_name()
            == self._join_table._table_name()
        )

        available_condition: bool = any(
            (
                is_field_from_from_table,
                if_field_from_join_table,
            )
        )
        if not available_condition:
            raise OnJoinComparisonError(
                f"It's impossible to use field from table "
                f"`{field._field_data.from_table}` "
                f"in join with FROM table "
                f"`{self._from_table}` and JOIN table `{self._join_table}`"
            )
        return None
