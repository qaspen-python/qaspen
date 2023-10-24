import dataclasses
import enum
import functools
import operator
from typing import (
    TYPE_CHECKING,
    Any,
    Final,
    Iterable,
    List,
    Tuple,
    Type,
    Union,
    cast,
)

from typing_extensions import Self

from qaspen.exceptions import OnJoinComparisonError
from qaspen.fields.base_field import BaseField, FieldType
from qaspen.fields.fields import Field
from qaspen.querystring.querystring import QueryString
from qaspen.statements.combinable_statements.combinations import (
    CombinableExpression,
    ExpressionsCombination,
)
from qaspen.statements.combinable_statements.filter_statement import (
    Filter,
    FilterBetween,
    FilterExclusive,
)
from qaspen.statements.statement import BaseStatement

if TYPE_CHECKING:
    from qaspen.table.base_table import BaseTable


class Join(CombinableExpression):
    join_type: str = "JOIN"

    def __init__(
        self: Self,
        fields: Iterable[Field[Any]],
        from_table: Type["BaseTable"],
        join_table: Type["BaseTable"],
        on: CombinableExpression,
        join_alias: str,
    ) -> None:
        self._from_table: Final[Type["BaseTable"]] = from_table
        self._join_table: Final[Type["BaseTable"]] = join_table
        self._based_on: CombinableExpression = on
        self._alias: str = join_alias
        self._fields: List[Field[Any]] = self._process_select_fields(
            fields=fields,
        )

    def __getattr__(
        self: Self,
        attribute: str,
    ) -> Field[Any]:
        return self._field_from_join(field_name=attribute)

    def querystring(self: Self) -> QueryString:
        self._based_on = self._change_combinable_expression(self._based_on)
        return QueryString(
            self.join_type,
            self._join_table.original_table_name(),
            self._join_table._table_meta.alias or self._alias,
            self._based_on.querystring(),
            sql_template="{} {} AS {} ON {}",
        )

    def _prefixed_field(
        self: Self,
        field: Union[Field[FieldType], BaseField[FieldType]],
    ) -> Field[FieldType]:
        return cast(
            Field[FieldType],
            field._with_prefix(
                prefix=(
                    field._field_data.from_table._table_meta.alias
                    or self._alias
                ),
            ),
        )

    def _change_combinable_expression(
        self: Self,
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
        self: Self,
        expression: Filter,
    ) -> Filter:
        self._check_fields_in_join(
            (
                expression.field,
                expression.comparison_value,
            ),
        )

        if_left_to_change: Final[bool] = all(
            (
                isinstance(expression.field, Field),
                expression.field._field_data.from_table.original_table_name()
                == self._join_table.original_table_name(),
                not expression.field._field_data.in_join,
            ),
        )

        is_right_to_change: Final[bool] = (
            isinstance(expression.comparison_value, Field)
            and (expression.comparison_value.table_name)
            == self._join_table.original_table_name()
            and not expression.comparison_value._field_data.in_join
        )

        if if_left_to_change:
            expression.field = self._prefixed_field(
                field=expression.field,
            )
        if is_right_to_change:
            expression.comparison_value = self._prefixed_field(
                field=expression.comparison_value,  # type: ignore
            )

        return expression

    def _change_where_between_combination(
        self: Self,
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
            expression.field._field_data.from_table.original_table_name()
            == self._join_table.original_table_name()
        )

        if is_field_to_change:
            # expression.field = expression.field._with_prefix(self._alias)
            expression.field = self._prefixed_field(
                field=expression.field,
            )
            return expression

        is_left_comparison_to_change: Final[bool] = (
            isinstance(expression.left_comparison_value, Field)
            and expression.left_comparison_value.table_name
            == self._join_table.original_table_name()
            and not expression.left_comparison_value._field_data.in_join
        )

        if is_left_comparison_to_change:
            expression.left_comparison_value = self._prefixed_field(
                field=expression.left_comparison_value,
            )

        is_right_comparison_to_change: Final[bool] = (
            isinstance(expression.right_comparison_value, Field)
            and expression.right_comparison_value.table_name
            == self._join_table.original_table_name()
            and not expression.right_comparison_value._field_data.in_join
        )
        if is_right_comparison_to_change:
            expression.right_comparison_value = self._prefixed_field(
                field=expression.right_comparison_value,
            )

        return expression

    def _check_fields_in_join(
        self: Self,
        for_checks_join_fields: Tuple[Any, ...],
    ) -> None:
        for for_checks_join_field in for_checks_join_fields:
            if isinstance(for_checks_join_field, Field):
                if for_checks_join_field._field_data.in_join:
                    continue
                self._is_field_in_join(for_checks_join_field)

    def _is_field_in_join(
        self: Self,
        field: Field[Any],
    ) -> None:
        if field._field_data.from_table.is_aliased():
            return

        is_field_from_from_table: bool = (
            field._field_data.from_table.original_table_name()
            == self._from_table.original_table_name()
        )
        if_field_from_join_table: bool = (
            field._field_data.from_table.original_table_name()
            == self._join_table.original_table_name()
        )

        available_condition: bool = any(
            (
                is_field_from_from_table,
                if_field_from_join_table,
            ),
        )
        if not available_condition:
            raise OnJoinComparisonError(
                f"It's impossible to use field from table "
                f"`{field._field_data.from_table}` "
                f"in join with FROM table "
                f"`{self._from_table}` and JOIN table `{self._join_table}`",
            )

    def _field_from_join(
        self: Self,
        field_name: str,
    ) -> Field[Any]:
        field_from_join: Field[Any] = self._join_table._retrieve_field(
            field_name=field_name,
        )
        field_from_join_with_alias: Field[Any,] = self._prefixed_field(
            field=field_from_join,
        )
        field_from_join_with_alias._field_data.in_join = True
        return field_from_join_with_alias

    def _process_select_fields(
        self: Self,
        fields: Iterable[Field[Any]],
    ) -> List[Field[Any]]:
        fields_with_prefix: List[Field[Any]] = []
        for field in fields:
            fields_with_prefix.append(
                self._prefixed_field(
                    field=field,
                ),
            )
        return fields_with_prefix

    def _join_fields(self: Self) -> List[Field[Any]]:
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
    JOIN: Type[Join] = Join
    INNERJOIN: Type[InnerJoin] = InnerJoin
    LEFTJOIN: Type[LeftOuterJoin] = LeftOuterJoin
    RIGHTJOIN: Type[RightOuterJoin] = RightOuterJoin
    FULLOUTERJOIN: Type[FullOuterJoin] = FullOuterJoin


@dataclasses.dataclass
class JoinStatement(BaseStatement):
    join_expressions: List[Join] = dataclasses.field(
        default_factory=list,
    )
    used_aliases: List[int] = dataclasses.field(
        default_factory=list,
    )

    def join(
        self: Self,
        fields: Iterable[Field[Any]],
        join_table: Type["BaseTable"],
        from_table: Type["BaseTable"],
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
            ),
        )

    def add_join(
        self: Self,
        *join_expressions: Join,
    ) -> None:
        for join_expression in join_expressions:
            if not join_expression.join_type:
                join_expression.join_type = self.__create_new_alias()
            self.join_expressions.append(join_expression)
        return None

    def querystring(self: Self) -> QueryString:
        if not self.join_expressions:
            return QueryString.empty()

        final_join: QueryString = functools.reduce(
            operator.add,
            [
                join_expression.querystring()
                for join_expression in self.join_expressions
            ],
        )
        return final_join

    def _retrieve_all_join_fields(
        self: Self,
    ) -> List[Field[Any]]:
        all_joins_fields: List[Field[Any]] = []
        for join_expression in self.join_expressions:
            all_joins_fields.extend(join_expression._join_fields())
        return all_joins_fields

    def __create_new_alias(
        self: Self,
    ) -> str:
        if not self.used_aliases:
            self.used_aliases.append(1)
            return "J1"
        alias_number: Final[int] = self.used_aliases[-1] + 1
        return f"J{alias_number}"
