from __future__ import annotations

import dataclasses
import functools
import operator
from typing import TYPE_CHECKING, Any, Iterable

from qaspen.clauses.order_by import OrderBy
from qaspen.querystring.querystring import (
    CommaSeparatedQueryString,
    QueryString,
)
from qaspen.statements.statement import BaseStatement

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.fields.base import Field


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
        order_by_expressions: Iterable[OrderBy] | None = None,
    ) -> None:
        """Create new `OrderBy`.

        ### Parameters:
        - `field`: field to order by.
        - `ascending`: `ASC` or `DESC` order.
        - `nulls_first`: `NULL` first or not.
        - `order_by_expressions`: already initialized OrderBys.
        """
        if field:
            self.order_by_expressions.append(
                OrderBy(
                    field=field,
                    ascending=ascending,
                    nulls_first=nulls_first,
                ),
            )

        if order_by_expressions:
            self.order_by_expressions.extend(
                order_by_expressions,
            )

    def querystring(self: Self) -> QueryString:
        """Build `QueryString`."""
        if not self.order_by_expressions:
            return QueryString.empty()
        final_order_by: CommaSeparatedQueryString = functools.reduce(
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
