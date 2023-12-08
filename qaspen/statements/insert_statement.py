from __future__ import annotations

import functools
import operator
from typing import (
    TYPE_CHECKING,
    Any,
    Final,
    Generator,
    Generic,
    Sequence,
    TypeVar,
)

from qaspen.fields.base import Field
from qaspen.qaspen_types import EMPTY_FIELD_VALUE, FromTable
from qaspen.querystring.querystring import (
    FullStatementQueryString,
    QueryString,
)
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


class BaseInsertStatement(
    BaseStatement,
    Executable[ReturnResultType],
    Generic[FromTable, ReturnResultType],
):
    """Base class for all InsertStatements."""

    def __init__(self: Self, from_table: type[FromTable]) -> None:
        self._from_table: Final = from_table
        self._returning_field: Field[Any] | None = None

    def __await__(
        self: Self,
    ) -> Generator[None, None, ReturnResultType]:
        """InsertStatement can be awaited.

        ### Returns:
        result from `execute` method.
        """
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
            querystring_parameters=[],
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
            querystring_parameters=[],
            fetch_results=bool(self._returning_field),
        )

        return self._parse_raw_query_result(
            raw_query_result=raw_query_result,
        )

    def returning(
        self: Self,
        return_field: ReturningField,
    ) -> InsertStatement[FromTable, list[ReturningField]]:
        """Add `RETURNING` to the query.

        ### Parameters:
        - `return_field`: field to return

        ### Returns:
        `self` with new return type.
        """
        self._returning_field = return_field
        return self  # type: ignore[return-value]

    def _parse_raw_query_result(
        self: Self,
        raw_query_result: list[dict[str, Any]] | None,
    ) -> ReturnResultType:
        if not self._returning_field or not raw_query_result:
            return None  # type: ignore[return-value]

        return [  # type: ignore[return-value]
            db_record[self._returning_field._original_field_name]
            for db_record in raw_query_result
        ]


class InsertStatement(BaseInsertStatement[FromTable, ReturnResultType]):
    """Main entry point for all INSERT queries."""

    def __init__(
        self: Self,
        from_table: type[FromTable],
        fields_to_insert: list[Field[Any]],
        values_to_insert: tuple[list[Any], ...],
    ) -> None:
        super().__init__(from_table=from_table)

        self._not_passed_fields_with_default = (
            self._find_not_passed_field_with_default(
                fields_to_insert=fields_to_insert,
            )
        )
        self._fields_to_insert: Final = (
            fields_to_insert + self._not_passed_fields_with_default
        )

        self._values_to_insert: Final = values_to_insert

        self._returning_field: Field[Any] | None = None

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

    def _find_not_passed_field_with_default(
        self: Self,
        fields_to_insert: list[Field[Any]],
    ) -> list[Field[Any]]:
        """Find all not passed table fields with default values.

        ### Parameters:
        - `fields_to_insert`: user-passed fields.

        ### Returns:
        not passed into `InsertStatement` fields
        with default values.
        """
        all_fields_with_default = (
            self._from_table._fields_with_default().values()
        )
        set_fields_to_insert = {
            field._original_field_name for field in fields_to_insert
        }

        return [
            field
            for field in all_fields_with_default
            if field._original_field_name not in set_fields_to_insert
        ]

    def _prepare_insert_values(
        self: Self,
        values_to_insert: tuple[list[Any], ...],
    ) -> tuple[list[Any], ...]:
        """Prepare INSERT values.

        We need to process `values_to_insert` and
        add to them default value of the fields
        with `default` or `callable_default`.

        ### Parameters:
        - `values_to_insert`: user-passed values to insert.

        ### Returns:
        tuple of lists with values to insert.
        """
        for values_to_insert_list in values_to_insert:
            for element_idx, element in enumerate(values_to_insert_list):
                values_to_insert_list[element_idx] = transform_value_to_sql(
                    element,
                )

        if not self._not_passed_fields_with_default:
            return values_to_insert

        for list_value in values_to_insert:
            for field_with_default in self._not_passed_fields_with_default:
                if field_with_default._default:
                    list_value.append(
                        field_with_default._prepared_default,
                    )
                elif field_with_default._callable_default:
                    list_value.append(
                        transform_value_to_sql(
                            field_with_default._callable_default(),
                        ),
                    )

        return values_to_insert

    def _make_fields_querystring(self: Self) -> QueryString:
        """Create `QueryString` for fields that will be inserted.

        ### Returns:
        `Querystring` for fields in INSERT SQL.
        """
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

    def _make_values_querystring(self: Self) -> QueryString:
        """Create `QueryString` for VALUES that will be inserted.

        Firstly we prepare `values_to_insert`, we can't do it
        before this method because default values may contain
        callable objects based on date, time, etc.
        So we process them just before execution.

        ### Returns:
        `Querystring` for VALUES in INSERT SQL.
        """
        all_values_to_insert = self._prepare_insert_values(
            values_to_insert=self._values_to_insert,
        )

        values_sql_template = ", ".join(
            ["{}" for _ in all_values_to_insert],
        )

        values_sql_template_args = []
        single_values_template = ", ".join(
            ["{}" for _ in all_values_to_insert[0]],
        )
        values_sql_template_args = [
            QueryString(
                *values_record,
                sql_template="(" + single_values_template + ")",
            )
            for values_record in all_values_to_insert
        ]

        return QueryString(
            *values_sql_template_args,
            sql_template=values_sql_template,
        )


class InsertObjectsStatement(
    BaseInsertStatement[FromTable, ReturnResultType],
):
    """Main entry point for all INSERT queries based on python objects."""

    def __init__(
        self: Self,
        from_table: type[FromTable],
        insert_objects: Sequence[FromTable],
    ) -> None:
        super().__init__(from_table=from_table)
        self._insert_objects: Final = insert_objects

        self._returning_field: Field[Any] | None = None

    def querystring(self: Self) -> QueryString:
        """Build querystring for INSERT statement."""
        objects_insert_querystrings: list[FullStatementQueryString] = [
            self._build_object_querystring(table_object)
            for table_object in self._insert_objects
        ]

        final_querystring: QueryString = functools.reduce(
            operator.add,
            objects_insert_querystrings,
        )

        return final_querystring

    def _build_object_querystring(
        self: Self,
        table_object: FromTable,
    ) -> FullStatementQueryString:
        """Build querystring for single object.

        ### Parameters:
        - `table_object`: Table instance to insert.

        ### Returns:
        new generated `QueryString`.
        """
        fields_to_insert = self._retrieve_object_fields_to_insert(
            table_object,
        )

        insert_fields_qs = QueryString(
            *[field._original_field_name for field in fields_to_insert],
            sql_template="("
            + ", ".join(["{}" for _ in fields_to_insert])
            + ")",
        )

        values_to_insert_qs = QueryString(
            *[
                transform_value_to_sql(field_value)
                for field_value in self._prepare_values_to_insert(
                    fields_to_insert,
                )
            ],
            sql_template="("
            + ", ".join(["{}" for _ in fields_to_insert])
            + ")",
        )

        returning_qs = (
            QueryString(
                self._returning_field._original_field_name,
                sql_template=" RETURNING {}",
            )
            if self._returning_field
            else QueryString.empty()
        )

        return FullStatementQueryString(
            table_object.original_table_name(),
            insert_fields_qs,
            values_to_insert_qs,
            returning_qs,
            sql_template="INSERT INTO {}{} VALUES {}{}",
        )

    def _retrieve_object_fields_to_insert(
        self: Self,
        table_object: FromTable,
    ) -> list[Field[Any]]:
        """Find all fields that must be inserted.

        That means that if fields with default value on
        database level haven't passed we shouldn't specify them.
        """
        result_fields: list[Field[Any]] = []
        field: Field[Any]
        for field in table_object._table_meta.table_fields.values():
            available_condition: bool = any(
                (
                    field._field_data.field_value,
                    field._field_data.callable_default,
                    field._field_data.default,
                ),
            )
            if available_condition:
                result_fields.append(field)

        return result_fields

    def _prepare_values_to_insert(
        self: Self,
        table_fields: list[Field[Any]],
    ) -> list[Any]:
        """Prepare INSERT values.

        ### Parameters:
        - `values_to_insert`: user-passed values to insert.

        ### Returns:
        tuple of lists with values to insert.
        """
        final_values: list[Any] = []
        for field in table_fields:
            field_value_condition = field._field_data.field_value and (
                field._field_data.field_value != EMPTY_FIELD_VALUE
            )
            if field_value_condition:
                final_values.append(
                    field._field_data.field_value,
                )
        return final_values
