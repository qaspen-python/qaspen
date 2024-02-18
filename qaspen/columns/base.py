from __future__ import annotations

import abc
import copy
import dataclasses
import types
from typing import TYPE_CHECKING, Any, Callable, Final, Generic, Union, cast

from typing_extensions import Self

from qaspen.base.comparison_operators import (
    BetweenComparisonMixin,
    EqualComparisonMixin,
    GreaterComparisonMixin,
    GreaterEqualComparisonMixin,
    InComparisonMixin,
    LessComparisonMixin,
    LessEqualComparisonMixin,
    NotEqualComparisonMixin,
    NotInComparisonMixin,
)
from qaspen.exceptions import (
    ColumnDeclarationError,
    ColumnValueValidationError,
)
from qaspen.qaspen_types import (
    EMPTY_FIELD_VALUE,
    CallableDefaultType,
    ColumnDefaultType,
    ColumnType,
    EmptyColumnValue,
    OperatorTypes,
)
from qaspen.querystring.querystring import QueryString
from qaspen.utils.column_utils import transform_value_to_sql

if TYPE_CHECKING:
    from qaspen.sql_type.base import SQLType
    from qaspen.table.base_table import BaseTable


@dataclasses.dataclass
class ColumnData(Generic[ColumnType]):
    """All column data.

    ### Columns
    `column_name` - Name of the column

    `from_table` - From what table column is.

    `is_primary` - is fiels is a PRIMARY KEY

    `unique` - is fiels has a UNIQUE constraint

    `is_null` - is column can be NULL.

    `column_value` - value of the column in the database or
    if you create table instance with columns manually.

    `default` - default value for the column.
    If it is callable, do not use it in the database,
    use it in python and than save in the database.

    `callable_default` - callable default, can be function
    or class.

    `database_default` - this is default on

    `prefix` - prefix for the column in the query.

    `alias` - alias of the column.

    `in_join` - mark that column is used in join.
    """

    column_name: str
    from_table: type[BaseTable] = None  # type: ignore[assignment]
    is_primary: bool = False
    unique: bool = False
    is_null: bool = False
    column_value: ColumnType | EmptyColumnValue | None = EMPTY_FIELD_VALUE
    default: Any | None = None
    prepared_default: str | None = None
    callable_default: Callable[[], ColumnType] | None = None
    database_default: str | None = None
    prefix: str = ""
    alias: str = ""
    in_join: bool = False


class BaseColumn(Generic[ColumnType], abc.ABC):
    """Base column class for all Columns."""

    _column_data: ColumnData[ColumnType]
    _sql_type: type[SQLType]

    def __set_name__(
        self: Self,
        owner: Any,
        column_name: str,
    ) -> None:
        """Set name for the column.

        column_name is equal to the name of the Column variable.

        Example:
        -------
        ```
        class MyTable(BaseTable):
            # name of the column in the database is `meme_lord`
            meme_lord: VarCharColumn = VarCharColumn()
        ```
        """
        if not self._column_data.column_name:
            self._column_data.column_name = column_name
        self._column_data.from_table = owner

    @abc.abstractmethod
    def _validate_column_value(
        self: Self,
        column_value: ColumnType | None,
    ) -> None:
        """Validate column value.

        It must raise an error if something goes wrong
        or not return anything.
        """
        ...  # pragma: no cover

    @abc.abstractmethod
    def _validate_default_value(
        self: Self,
        default_value: ColumnType | None,
    ) -> None:
        """Validate column default value.

        It must raise an error in validation failed.
        """
        ...  # pragma: no cover

    @abc.abstractmethod
    def _prepare_default_value(
        self: Self,
        default_value: ColumnType,
    ) -> str | None:
        """Prepare default value to specify it in Column declaration.

        It uses only in the method `_column_default`.

        :returns: prepared default value.
        """

    @property
    def value(self: Self) -> ColumnType | EmptyColumnValue | None:
        """Return value of the column.

        It's valid only for columns after executing statement,
        otherwise it returns `None`.

        If you don't select column in the `.select()` and
        try to get its value after `as_objects()` method,
        you will retrieve `EmptyColumnValue` because it indicates
        that you don't select this Column.

        ### Returns:
        `Python type of the column` or `EmptyColumnValue` or `None`.
        """
        return self._column_data.column_value

    @property
    def _table_name(self: Self) -> str:
        """Return table name of this column.

        It's only the original table name, not aliased.

        ### Return
        `str` table name.
        """
        return self._column_data.from_table.original_table_name()

    @property
    def _schemed_table_name(self: Self) -> str:
        """Return aliased table name with schema of this column.

        ### Return
        `str` table name.
        """
        return self._column_data.from_table.schemed_table_name()

    @property
    def alias(self: Self) -> str | None:
        """Return alias to the column if it exists.

        ### Returns:
        `alias` or `None`.
        """
        return self._column_data.alias

    @property
    def column_name(self: Self) -> str:
        """Return column name with prefix and alias.

        ### Prefix logic:
        If `from_table` has alias than use this alias.

        If this column has a prefix than use prefix.

        Else use original name of the column's Table.

        ### Alias logic:
        If this Column has an alias, use it.

        ### Return
        `Column` as a `str`.
        """
        prefix: str = (
            self._column_data.from_table._table_meta.alias
            or self._column_data.prefix
            or self._column_data.from_table.table_name()
        )
        column_name: str = f"{prefix}.{self._column_data.column_name}"
        if alias := self._column_data.alias:
            column_name += f" AS {alias}"

        return column_name

    @property
    def _original_column_name(self: Self) -> str:
        """Return name of the column without prefix and alias.

        ### Return
        `str` Column name.
        """
        return self._column_data.column_name

    @property
    def _default(self: Self) -> Any | None:
        """Return default value of the column.

        ### Return
        default value.
        """
        return self._column_data.default

    @property
    def _prepared_default(self: Self) -> Any | None:
        """Return default value of the column.

        This default is already converted into SQL string.
        Or None.

        ### Return
        default value.
        """
        return self._column_data.prepared_default

    @property
    def _callable_default(
        self: Self,
    ) -> CallableDefaultType[ColumnType] | None:
        """Return callable object for default value for the column.

        This default value will be called in the statements
        that can be create new raws in the database.

        """
        return self._column_data.callable_default

    @property
    def _is_null(self: Self) -> bool:
        """Return flag that column can be `NULL`.

        ### Return
        `bool`
        """
        return self._column_data.is_null

    @property
    def _column_null(self: Self) -> str:
        """Return `NOT NULL` string if column cannot be NULL.

        ### Return
        `str`.
        """
        return "NOT NULL" if not self._is_null else ""

    @property
    def _column_default(self: Self) -> str:
        if self._default:
            return f"DEFAULT {self._default}" if self._default else ""
        return ""

    @property
    def _column_type(self: Self) -> str:
        """Property for final SQL column Type.

        It can be changed in subclasses.

        ### Return
        SQL `string`.
        """
        query, _ = self._sql_type.querystring().build()
        return query


class Column(
    BaseColumn[ColumnType],
    EqualComparisonMixin[
        Union["Column[Any]", OperatorTypes, ColumnType, None]
    ],
    NotEqualComparisonMixin[
        Union["Column[Any]", OperatorTypes, ColumnType, None],
    ],
    GreaterComparisonMixin[Union["Column[Any]", OperatorTypes, ColumnType]],
    GreaterEqualComparisonMixin[
        Union["Column[Any]", OperatorTypes, ColumnType]
    ],
    LessComparisonMixin[Union["Column[Any]", OperatorTypes, ColumnType]],
    LessEqualComparisonMixin[Union["Column[Any]", OperatorTypes, ColumnType]],
    BetweenComparisonMixin[Union[ColumnType, "Column[Any]"]],
    InComparisonMixin[ColumnType],
    NotInComparisonMixin[ColumnType],
):
    """Main Column class.

    All subclasses must be inherited from this class.

    `_available_comparison_types` defines what types
    can be used in comparison methods, like `__eq__`.

    `_set_available_types` defines what types can be
    set as a value to the column.
    """

    _available_comparison_types: tuple[type, ...]
    _set_available_types: tuple[type, ...]

    def __init__(
        self: Self,
        *args: Any,
        is_primary: bool = False,
        is_null: bool = True,
        unique: bool = False,
        default: ColumnDefaultType[ColumnType] = None,
        database_default: str | None = None,
        db_column_name: str | None = None,
    ) -> None:
        """Create new Column instance.

        It's not possible to use not keyword arguments for
        specifying parameters.

        ### Parameters:
        - `is_null`: Defines is Column can be `NULL` or not.
        - `default`: Default value for the Column.
            This default value can be callable object.
            Note! This value will be set at the python level.
        - `db_column_name`: name of the column in the database.
        """
        if default and database_default:
            default_err_msg = (
                "It's impossible to specify default and database_default. "
                "Please specify only one."
            )
            raise ColumnDeclarationError(default_err_msg)

        if args:
            args_err_msg: Final = "Use only keyword arguments."
            raise ColumnDeclarationError(args_err_msg)

        self.is_null: Final = is_null if not default else False
        self.default = default
        self.database_default = database_default
        self.is_primary = is_primary
        self.unique = unique

        self.prepared_default: Any | None = None
        self.callable_default_value: (
            CallableDefaultType[ColumnType] | None
        ) = None
        self.not_callable_default: ColumnType | None = None
        if callable(default):
            self.callable_default_value = default
        elif default is not None:
            self.not_callable_default = default
            self.prepared_default = self._prepare_default_value(
                default_value=default,
            )

        # python_is_null is a special flag for validations
        # and __set__ method.
        # In general, if there is is_null=False, we can't
        # set None to the column.
        # But for example in Serial columns, is_null is always False but
        # we don't need to always specify value to them.
        # Because value in this column will be calculated
        # on database side.
        if not hasattr(self, "python_is_null"):
            self.python_is_null = is_null

        if default:
            self._validate_default_value(
                default_value=default,
            )

        self._column_data: ColumnData[ColumnType] = ColumnData(
            column_name=db_column_name or "",
            is_primary=self.is_primary,
            unique=self.unique,
            is_null=is_null,
            default=self.not_callable_default,
            prepared_default=self.prepared_default,
            callable_default=self.callable_default_value,
            database_default=database_default,
        )

    def __hash__(
        self: Self,
    ) -> int:
        """Make Column hashable.

        ### Returns:
        hash number.
        """
        return hash(repr(self))

    def __get__(
        self: Self,
        instance: BaseTable | None,
        owner: type[BaseTable] | None,
    ) -> Self:
        try:
            return cast(
                Self,
                instance.__dict__[self._original_column_name],
            )
        except (AttributeError, KeyError):
            return cast(
                Self,
                owner._retrieve_column(  # type: ignore[union-attr]
                    self._original_column_name,
                ),
            )

    def __set__(
        self: Self,
        instance: object,
        value: ColumnType | EmptyColumnValue | None,
    ) -> None:
        column: Column[ColumnType]
        if value is None:
            if self.not_callable_default:
                column = instance.__dict__[self._original_column_name]
                column._column_data.column_value = self.not_callable_default
                return

            if self.callable_default_value:
                column = instance.__dict__[self._original_column_name]
                column._column_data.column_value = (
                    self.callable_default_value()
                )
                return

        if isinstance(value, EmptyColumnValue):
            column = instance.__dict__[self._original_column_name]
            column._column_data.column_value = value
            return

        if isinstance(value, self.__class__):
            instance.__dict__[self._original_column_name] = value
            return

        self._validate_column_value(
            column_value=value,
        )
        column = instance.__dict__[self._original_column_name]
        column._column_data.column_value = value

    def querystring(self: Self) -> QueryString:
        """Build QueryString class.

        ### Returns:
        Built `QueryString`.
        """
        return QueryString(
            self.column_name,
            sql_template=QueryString.arg_ph(),
        )

    def with_alias(
        self: Self,
        alias_name: str,
    ) -> Self:
        """Set alias to the column.

        ### Parameters:
        - `alias_name`: name of the alias to the column.

        ### Returns
        `Column` with new prefix.
        """
        return self._with_alias(
            alias=alias_name,
        )

    @property
    def _correct_method_value_types(self: Self) -> tuple[type, ...]:
        """Return types that can be used in most comparison methods.

        ### Returns:
        tuple of types.
        """
        return (
            *self._available_comparison_types,
            Column,
            OperatorTypes.__args__,  # type: ignore[attr-defined]
        )

    def _is_the_same_column(
        self: Self,
        other_column: BaseColumn[ColumnType],
    ) -> bool:
        """Compare two columns.

        Return `True` if they are the same, else `False`.
        They are equal if they `_column_data`s are the same.

        ### Parameters:
        - `other_column`: column to compare with.

        ### Returns:
        Are columns the same or not.
        """
        return self._column_data == other_column._column_data

    def _with_prefix(self: Self, prefix: str) -> Column[ColumnType]:
        """Give Column a prefix.

        Make a Column deepcopy and set new prefix.

        ### Parameters
        - `prefix`: prefix for the column.

        ### Returns
        `Column` with new prefix.
        """
        column: Column[ColumnType] = copy.deepcopy(self)
        column._column_data.prefix = prefix
        return column

    def _with_alias(self: Self, alias: str) -> Self:
        """Give Column an alias.

        Make a Column deepcopy and set new alias.

        ### Parameters
        - `alias`: alias for the column.

        ### Returns
        `Column` with new alias.
        """
        column: Self = copy.deepcopy(self)
        column._column_data.alias = alias
        return column

    def _validate_column_value(
        self: Self,
        column_value: ColumnType | None,
    ) -> None:
        """Validate column value.

        :param column_value: new value for the column.

        :raises ColumnValueValidationError: if the `max_length` is exceeded.
        """
        if self.python_is_null and column_value is None:
            return

        if not self.python_is_null and column_value is None:
            null_err_msg = (
                f"Value of the column {self.__class__.__name__} "
                "can't be `None` because parameter is_null is False"
            )
            raise ColumnValueValidationError(null_err_msg)

        if not isinstance(column_value, self._set_available_types):
            err_msg: Final = (
                f"Value of this column must be one of these - "
                f"{self._set_available_types}"
            )
            raise ColumnValueValidationError(err_msg)

    def _validate_default_value(
        self: Self,
        default_value: ColumnDefaultType[ColumnType],
    ) -> None:
        if default_value is None or types.FunctionType == type(default_value):
            return

        try:
            self._validate_column_value(
                column_value=default_value,  # type: ignore[arg-type]
            )
        except ColumnValueValidationError as exc:
            validation_err_msg: Final = (
                f"Default value of this column must be one of these - "
                f"{self._set_available_types}"
            )
            raise ColumnValueValidationError(
                validation_err_msg,
            ) from exc

    def _prepare_default_value(
        self: Self,
        default_value: ColumnType | None,
    ) -> str | None:
        """Prepare default value to specify it in Column declaration.

        It uses only in the method `_column_default`.

        :returns: prepared default value.
        """
        return (
            transform_value_to_sql(default_value)
            if default_value is not None
            else default_value
        )
