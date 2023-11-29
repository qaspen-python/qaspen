from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    Any,
    Final,
    Generator,
    Generic,
    Literal,
    TypeVar,
    overload,
)

from qaspen.fields.base import Field
from qaspen.qaspen_types import FromTable
from qaspen.querystring.querystring import QueryString
from qaspen.statements.base import Executable
from qaspen.statements.statement import BaseStatement
from qaspen.utils.fields_utils import transform_value_to_sql

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.abc.db_engine import BaseEngine
    from qaspen.abc.db_transaction import BaseTransaction


ReturnResultType = TypeVar(
    "ReturnResultType",
)
ReturningField = TypeVar(
    "ReturningField",
    bound=Field[Any],
)


class InsertStatement(
    BaseStatement,
    Executable[ReturnResultType],
    Generic[FromTable, ReturnResultType],
):
    """Main entry point for all INSERT queries."""

    def __init__(
        self: Self,
        from_table: type[FromTable],
        values_to_insert: list[list[Any]],
        fields_to_insert: list[Field[Any]] | None = None,
    ) -> None:
        self._from_table: Final = from_table
        self._fields_to_insert: Final = fields_to_insert
        self._values_to_insert: Final = values_to_insert

        self._returning_field: Field[Any] | None = None
        self._returning_single: bool = False

    @overload
    def returning(  # type: ignore[overload-overlap]
        self: Self,
        return_field: ReturningField,
        single: Literal[False] = False,
    ) -> InsertStatement[FromTable, list[ReturningField]]:
        ...

    @overload
    def returning(
        self: Self,
        return_field: ReturningField,
        single: Literal[True] = True,
    ) -> InsertStatement[FromTable, ReturningField]:
        ...

    def returning(
        self: Self,
        return_field: ReturningField,
        single: bool = False,
    ) -> (
        InsertStatement[FromTable, list[ReturningField]]
        | InsertStatement[FromTable, ReturningField]
    ):
        """Add `RETURNING` to the query.

        ### Parameters:
        - `return_field`: field to return

        ### Returns:
        `self` with new return type.
        """
        self._returning_field = return_field
        self._returning_single = single
        return self  # type: ignore[return-value]

    def __await__(
        self: Self,
    ) -> Generator[None, None, ReturnResultType]:
        """InsertStatement can be awaited."""
        engine: Final = self._from_table._table_meta.database_engine
        if not engine:
            engine_err_msg: Final = "There is no database engine."
            raise AttributeError(engine_err_msg)

        return self.execute(engine=engine).__await__()

    async def execute(
        self: Self,
        engine: BaseEngine[Any, Any, Any],
    ) -> ReturnResultType:
        """Execute select statement.

        This is manual execution.
        You can pass specific engine.

        ### Parameters
        - `engine`: subclass of BaseEngine.
        - `fetch_results`: try to get results from the
        database response or not.

        ### Returns
        `SelectStatementResult`
        """
        raw_query_result: list[dict[str, Any]] | None = await engine.execute(
            querystring=self.querystring().build(),
            fetch_results=bool(self._returning_field),
        )

        return self._parse_raw_query_result(
            raw_query_result=raw_query_result,
        )

    async def transaction_execute(
        self: Self,
        transaction: BaseTransaction[Any, Any],
    ) -> ReturnResultType:
        """Execute statement inside a transaction context.

        This is manual execution.
        You can pass specific transaction.
        IMPORTANT: To commit the changes, with this way of execution,
        it's necessary to manually call `await transaction.commit()`.

        ### Parameters:
        - `transaction`: running transaction.
        database response or not.

        ### Returns
        `InsertStatement`
        """
        raw_query_result: list[
            dict[str, Any]
        ] | None = await transaction.execute(
            querystring=self.querystring().build(),
            fetch_results=bool(self._returning_field),
        )

        return self._parse_raw_query_result(
            raw_query_result=raw_query_result,
        )

    def querystring(self: Self) -> QueryString:
        """Build querystring for INSERT statement."""
        returning_qs = (
            QueryString(
                self._returning_field._original_field_name,
                sql_template="RETURNING {}",
            )
            if self._returning_field
            else QueryString.empty()
        )

        return QueryString(
            self._from_table.table_name(),
            self._make_fields_querystring(),
            self._make_values_querystring(),
            returning_qs,
            sql_template="INSERT INTO {} {} VALUES {} {}",
        )

    def _make_fields_querystring(
        self: Self,
    ) -> QueryString:
        """Create `QueryString` for fields that will be inserted.

        ### Returns:
        `Querystring` for fields in INSERT SQL.
        """
        if not self._fields_to_insert:
            return QueryString.empty()

        fields_sql_template = ", ".join(
            ["{}" for _ in self._fields_to_insert],
        )

        fields_sql_template_args = [
            field_to_insert._original_field_name
            for field_to_insert in self._fields_to_insert
        ]

        return QueryString(
            *fields_sql_template_args,
            sql_template="(" + fields_sql_template + ")",
        )

    def _make_values_querystring(
        self: Self,
    ) -> QueryString:
        """Create `QueryString` for VALUES that will be inserted.

        ### Returns:
        `Querystring` for VALUES in INSERT SQL.
        """
        values_sql_template = ", ".join(
            ["{}" for _ in self._values_to_insert],
        )

        values_sql_template_args = []
        single_values_template = ", ".join(
            ["{}" for _ in self._values_to_insert[0]],
        )
        values_sql_template_args = [
            QueryString(
                *[
                    transform_value_to_sql(value_in_record)
                    for value_in_record in values_record
                ],
                sql_template="(" + single_values_template + ")",
            )
            for values_record in self._values_to_insert
        ]

        return QueryString(
            *values_sql_template_args,
            sql_template=values_sql_template,
        )

    def _parse_raw_query_result(
        self: Self,
        raw_query_result: list[dict[str, Any]] | None,
    ) -> ReturnResultType:
        if not self._returning_field:
            return None  # type: ignore[return-value]
        if not raw_query_result:
            return None  # type: ignore[return-value]

        if self._returning_single:
            return raw_query_result[0][  # type: ignore[no-any-return]
                self._returning_field.field_name
            ]

        return [  # type: ignore[return-value]
            db_record[self._returning_field.field_name]
            for db_record in raw_query_result
        ]
