from __future__ import annotations

import dataclasses
import functools
import operator
from typing import TYPE_CHECKING, Any, Final, Iterable

from qaspen.querystring.querystring import OrderByQueryString, QueryString
from qaspen.statements.statement import BaseStatement

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.fields.base import Field


class OrderBy:
    """Main class for PostgreSQL OrderBy."""

    def __init__(
        self: Self,
        field: Field[Any],
        ascending: bool = True,
        nulls_first: bool = True,
    ) -> None:
        self.field: Final[Field[Any]] = field  # type: ignore[arg-type]
        self.ascending: Final[bool] = ascending
        self.nulls_first: Final[bool] = nulls_first

    def querystring(self: Self) -> OrderByQueryString:
        """Build `QueryString`."""
        querystring_template: Final[str] = "{} {} {}"
        querystring_arguments: list[str] = [self.field.field_name]

        if self.ascending is True:
            querystring_arguments.append("ASC")
        elif self.ascending is False:
            querystring_arguments.append("DESC")

        if self.nulls_first is True:
            querystring_arguments.append("NULLS FIRST")
        elif self.nulls_first is False:
            querystring_arguments.append("NULLS LAST")

        return OrderByQueryString(
            *querystring_arguments,
            sql_template=querystring_template,
        )


@dataclasses.dataclass
class OrderByStatement(BaseStatement):
    """OrderBy statement for high-level statements.

    It is used in Select/Update/Insert/Delete Statements.

    `order_by_expressions` contains all created order_bys.
    """

    order_by_expressions: list[OrderBy] = dataclasses.field(
        default_factory=list,
    )

    def order_by(
        self: Self,
        field: Field[Any] | None = None,
        ascending: bool = True,
        nulls_first: bool = True,
        order_by_statements: Iterable[OrderBy] | None = None,
    ) -> None:
        """Create new `OrderBy`.

        ### Parameters:
        - `field`: field to order by.
        - `ascending`: `ASC` or `DESC` order.
        - `nulls_first`: `NULL` first or not.
        - `order_by_statements`: already initialized OrderBys.
        """
        if field:
            self.order_by_expressions.append(
                OrderBy(
                    field=field,
                    ascending=ascending,
                    nulls_first=nulls_first,
                ),
            )

        if order_by_statements:
            self.order_by_expressions.extend(
                order_by_statements,
            )

    def querystring(self: Self) -> QueryString:
        """Build `QueryString`."""
        if not self.order_by_expressions:
            return QueryString.empty()
        final_order_by: OrderByQueryString = functools.reduce(
            operator.add,
            [
                order_by_expression.querystring()
                for order_by_expression in self.order_by_expressions
            ],
        )

        return QueryString(
            *final_order_by.template_arguments,
            sql_template=f"ORDER BY {final_order_by.sql_template}",
        )
