import dataclasses
import enum
import functools
import operator
import warnings
from typing import (
    TYPE_CHECKING,
    Any,
    Final,
    Iterable,
    List,
    Optional,
    Type,
    Union,
    cast,
)

from typing_extensions import Self

from qaspen.fields.base_field import BaseField
from qaspen.fields.fields import Field
from qaspen.qaspen_types import FieldType
from qaspen.querystring.querystring import QueryString
from qaspen.statements.combinable_statements.combinations import (
    CombinableExpression,
)
from qaspen.statements.statement import BaseStatement

if TYPE_CHECKING:
    from qaspen.table.base_table import BaseTable


class Join(CombinableExpression):
    join_type: str = "JOIN"

    def __init__(
        self: Self,
        fields: Optional[Iterable[Field[Any]]],
        from_table: Type["BaseTable"],
        join_table: Type["BaseTable"],
        on: CombinableExpression,
        join_alias: str,
    ) -> None:
        self._from_table: Final[Type["BaseTable"]] = from_table
        self._join_table: Final[Type["BaseTable"]] = join_table
        self._based_on: CombinableExpression = on
        self._alias: str = join_alias

        self._fields: Optional[List[Field[Any]]] = None
        if fields:
            self._fields = self._process_select_fields(
                fields=fields,
            )

    def querystring(self: Self) -> QueryString:
        if not self._fields:
            warnings.warn(
                f"You have JOIN with table {self._join_table.__name__} "
                f"but don't select any fields from this table. "
                f"It's possible mistake.",
            )
        return QueryString(
            self.join_type,
            self._join_table.original_table_name(),
            self._join_table._table_meta.alias or self._alias,
            self._based_on.querystring(),
            sql_template="{} {} AS {} ON {}",
        )

    def add_fields(
        self: Self,
        fields: List[Field[Any]],
    ) -> None:
        processed_fields = self._process_select_fields(
            fields=fields,
        )
        if self._fields:
            self._fields.extend(processed_fields)
            return

        self._fields = processed_fields

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

    def _process_select_fields(
        self: Self,
        fields: Iterable[Field[Any]],
    ) -> List[Field[Any]]:
        fields_with_prefix: List[Field[Any]] = [
            self._prefixed_field(field=field) for field in fields
        ]
        return fields_with_prefix

    def _join_fields(self: Self) -> Optional[List[Field[Any]]]:
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
        join_table: Type["BaseTable"],
        from_table: Type["BaseTable"],
        on: CombinableExpression,
        join_type: JoinType,
        fields: Optional[Iterable[Field[Any]]] = None,
    ) -> None:
        join_alias = (
            join_table._table_meta.alias or join_table.original_table_name()
        )
        self.join_expressions.append(
            join_type.value(
                join_alias=join_alias,
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
            if join_fields := join_expression._join_fields():
                all_joins_fields.extend(join_fields)
        return all_joins_fields
