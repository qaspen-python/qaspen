import dataclasses
import enum
import functools
import operator
import typing
from qaspen.exceptions import OnJoinComparisonError
from qaspen.fields.fields import Field
from qaspen.querystring.querystring import QueryString
from qaspen.statements.combinable_statements.combinations import (
    CombinableExpression,
    ExpressionsCombination
)
from qaspen.statements.combinable_statements.filter_statement import (
    Filter,
    FilterBetween,
    FilterExclusive,
)

from qaspen.statements.statement import BaseStatement
from qaspen.table.meta_table import MetaTable


class Join(CombinableExpression):

    join_type: str = "JOIN"

    def __init__(
        self: typing.Self,
        fields: typing.Iterable[Field[typing.Any]],
        from_table: type[MetaTable],
        join_table: type[MetaTable],
        on: CombinableExpression,
        join_alias: str,
    ) -> None:
        self._from_table: typing.Final[type[MetaTable]] = from_table
        self._join_table: typing.Final[type[MetaTable]] = join_table
        self._based_on: CombinableExpression = on
        self._alias: str = join_alias
        self._fields: list[Field[typing.Any]] = self._process_select_fields(
            fields=fields,
        )

    def __getattr__(
        self: typing.Self,
        attribute: str,
    ) -> Field[typing.Any]:
        return self._field_from_join(field_name=attribute)

    def querystring(self: typing.Self) -> QueryString:
        self._based_on = self._change_combinable_expression(self._based_on)
        return QueryString(
            self.join_type,
            self._join_table._table_name(),
            self._alias,
            self._based_on.querystring(),
            sql_template="{} {} AS {} ON {}",
        )

    def _change_combinable_expression(
        self: typing.Self,
        expression: CombinableExpression,
    ) -> CombinableExpression:
        if not isinstance(expression, ExpressionsCombination):
            if isinstance(expression, Filter):
                return self._change_where_combination(
                    expression=expression,
                )
            if isinstance(expression, FilterBetween):
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

        if isinstance(expression, FilterExclusive):
            self._change_combinable_expression(
                expression.comparison,
            )

        return expression

    def _change_where_combination(
        self: typing.Self,
        expression: Filter,
    ) -> Filter:
        self._check_fields_in_join(
            (
                expression.field,
                expression.comparison_value,
            )
        )

        if_left_to_change: typing.Final[bool] = all(
            (
                isinstance(expression.field, Field),
                expression.field._field_data.from_table._table_name()
                == self._join_table._table_name(),
                not expression.field._field_data.in_join
            ),
        )

        is_right_to_change: typing.Final[bool] = (
            isinstance(expression.comparison_value, Field)
            and
            (expression.comparison_value.table_name)
            == self._join_table._table_name()
            and not expression.comparison_value._field_data.in_join
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
        expression: FilterBetween,
    ) -> FilterBetween:
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
            isinstance(expression.left_comparison_value, Field)
            and
            expression.left_comparison_value.table_name
            == self._join_table._table_name()
            and not expression.left_comparison_value._field_data.in_join
        )

        if is_left_comparison_to_change:
            expression.left_comparison_value = (
                expression.left_comparison_value._with_prefix(
                    self._alias,
                )
            )

        is_right_comparison_to_change: typing.Final[bool] = (
            isinstance(expression.right_comparison_value, Field)
            and
            expression.right_comparison_value.table_name
            == self._join_table._table_name()
            and not expression.right_comparison_value._field_data.in_join
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
            if isinstance(for_checks_join_field, Field):
                if for_checks_join_field._field_data.in_join:
                    continue
                self._is_field_in_join(for_checks_join_field)

    def _is_field_in_join(
        self: typing.Self,
        field: Field[typing.Any],
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

    def _field_from_join(
        self: typing.Self,
        field_name: str,
    ) -> Field[typing.Any]:
        field_from_join: Field[typing.Any] = self._join_table.get_field(
            field_name=field_name,
        )
        field_from_join_with_alias: Field[
            typing.Any,
        ] = field_from_join._with_prefix(self._alias)
        field_from_join_with_alias._field_data.in_join = True
        return field_from_join_with_alias

    def _process_select_fields(
        self: typing.Self,
        fields: typing.Iterable[Field[typing.Any]]
    ) -> list[Field[typing.Any]]:
        fields_with_prefix: list[Field[typing.Any]] = []
        for field in fields:
            fields_with_prefix.append(
                field._with_prefix(self._alias)
            )
        return fields_with_prefix

    def _join_fields(self: typing.Self) -> list[Field[typing.Any]]:
        return self._fields


class InnerJoin(Join):
    join_type: str = "INNER JOIN"


class LeftOuterJoin(Join):
    join_type: str = "LEFT JOIN"


class RightOuterJoin(Join):
    join_type: str = "RIGHT JOIN"


class FullOuterJoin(Join):
    join_type: str = "FULL OUTER JOIN"


class JoinType(enum.Enum):
    JOIN: type[Join] = Join
    INNERJOIN: type[InnerJoin] = InnerJoin
    LEFTJOIN: type[LeftOuterJoin] = LeftOuterJoin
    RIGHTJOIN: type[RightOuterJoin] = RightOuterJoin
    FULLOUTERJOIN: type[FullOuterJoin] = FullOuterJoin


@dataclasses.dataclass
class JoinStatement(BaseStatement):
    operator: str = "JOIN"
    join_expressions: list[Join] = dataclasses.field(
        default_factory=list,
    )
    used_aliases: list[int] = dataclasses.field(
        default_factory=list,
    )

    def join(
        self: typing.Self,
        fields: typing.Iterable[Field[typing.Any]],
        join_table: type[MetaTable],
        from_table: type[MetaTable],
        on: CombinableExpression,
        join_type: JoinType,
    ) -> None:
        self.join_expressions.append(
            join_type.value(
                join_alias=self.__create_new_alias(),
                fields=fields,
                join_table=join_table,
                from_table=from_table,
                on=on,
            )
        )

    def add_join(
        self: typing.Self,
        *join_expressions: Join,
    ) -> None:
        for join_expression in join_expressions:
            if not join_expression.join_type:
                join_expression.join_type = self.__create_new_alias()
            self.join_expressions.append(join_expression)
        return None

    def querystring(self: typing.Self) -> QueryString:
        if not self.join_expressions:
            return QueryString.empty()

        final_join: QueryString = functools.reduce(
            operator.add,
            [
                join_expression.querystring()
                for join_expression
                in self.join_expressions
            ],
        )
        return final_join

    def _retrieve_all_join_fields(
        self: typing.Self,
    ) -> list[Field[typing.Any]]:
        all_joins_fields: list[Field[typing.Any]] = []
        for join_expression in self.join_expressions:
            all_joins_fields.extend(join_expression._join_fields())
        return all_joins_fields

    def __create_new_alias(
        self: typing.Self,
    ) -> str:
        if not self.used_aliases:
            self.used_aliases.append(1)
            return "J1"
        alias_number: typing.Final[int] = self.used_aliases[-1] + 1
        return f"J{alias_number}"
