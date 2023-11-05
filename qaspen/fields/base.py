from __future__ import annotations

import abc
import copy
import dataclasses
import types
from typing import TYPE_CHECKING, Any, Callable, Final, Generic, Union, cast

from qaspen.base.operators import AllOperator, AnyOperator
from qaspen.exceptions import (
    FieldComparisonError,
    FieldDeclarationError,
    FieldValueValidationError,
    FilterComparisonError,
)
from qaspen.fields import operators
from qaspen.qaspen_types import (
    CallableDefaultType,
    FieldDefaultType,
    FieldType,
)
from qaspen.querystring.querystring import QueryString
from qaspen.statements.combinable_statements.filter_statement import (
    Filter,
    FilterBetween,
)

if TYPE_CHECKING:
    from typing_extensions import Self

    from qaspen.base.sql_base import SQLSelectable
    from qaspen.sql_type.base import SQLType
    from qaspen.table.base_table import BaseTable


class EmptyFieldValue:
    """Indicates that field wasn't queried from the database."""

    def __str__(self: Self) -> str:
        return self.__class__.__name__


EMPTY_FIELD_VALUE = EmptyFieldValue()


@dataclasses.dataclass
class FieldData(Generic[FieldType]):
    """All field data.

    ### Fields
    `field_name` - Name of the field

    `from_table` - From what table field is.

    `is_null` - is field can be NULL.

    `field_value` - value of the field in the database or
    if you create table instance with fields manually.

    `default` - default value for the field.
    If it is callable, do not use it in the database,
    use it in python and than save in the database.

    `prefix` - prefix for the field in the query.

    `alias` - alias of the field.

    `in_join` - mark that field is used in join.
    """

    field_name: str
    from_table: type[BaseTable] = None  # type: ignore[assignment]
    is_null: bool = False
    field_value: FieldType | EmptyFieldValue | None = EMPTY_FIELD_VALUE
    default: str | None = None
    callable_default: Callable[[], FieldType] | None = None
    prefix: str = ""
    alias: str = ""
    in_join: bool = False


class BaseField(Generic[FieldType], abc.ABC):
    """Base field class for all Fields."""

    _field_data: FieldData[FieldType]
    _sql_type: type[SQLType]

    def __set_name__(
        self: Self,
        owner: Any,
        field_name: str,
    ) -> None:
        """Set name for the field.

        field_name is equal to the name of the Field variable.

        Example:
        -------
        ```
        class MyTable(BaseTable):
            # name of the field in the database is `meme_lord`
            meme_lord: VarCharField = VarCharField()
        ```
        """
        self._field_data.from_table = owner
        self._field_data.field_name = field_name

    @abc.abstractmethod
    def _make_field_create_statement(
        self: Self,
    ) -> str:
        ...

    @abc.abstractmethod
    def _validate_field_value(
        self: Self,
        field_value: FieldType | None,
    ) -> None:
        """Validate field value.

        It must raise an error if something goes wrong
        or not return anything.
        """
        ...

    @abc.abstractmethod
    def _validate_default_value(
        self: Self,
        default_value: FieldType | None,
    ) -> None:
        """Validate field default value.

        It must raise an error in validation failed.
        """
        ...

    @abc.abstractmethod
    def _prepare_default_value(
        self: Self,
        default_value: FieldType,
    ) -> str | None:
        """Prepare default value to specify it in Field declaration.

        It uses only in the method `_field_default`.

        :returns: prepared default value.
        """

    @property
    def value(self: Self) -> FieldType | EmptyFieldValue | None:
        """Return value of the field.

        It's valid only for fields after executing statement,
        otherwise it returns `None`.

        If you don't select field in the `.select()` and
        try to get its value after `as_objects()` method,
        you will retrieve `EmptyFieldValue` because it indicates
        that you don't select this Field.

        ### Returns:
        `Python type of the field` or `EmptyFieldValue` or `None`.
        """
        return self._field_data.field_value

    @property
    def _table_name(self: Self) -> str:
        """Return table name of this field.

        It's only the original table name, not aliased.

        ### Return
        `str` table name.
        """
        return self._field_data.from_table.original_table_name()

    @property
    def _original_field_name(self: Self) -> str:
        """Return name of the field without prefix and alias.

        ### Return
        `str` Field name.
        """
        return self._field_data.field_name

    @property
    def field_name(self: Self) -> str:
        """Return field name with prefix and alias.

        ### Prefix logic:
        If `from_table` has alias than use this alias.

        If this field has a prefix than use prefix.

        Else use original name of the field's Table.

        ### Alias logic:
        If this Field has an alias, use it.

        ### Return
        `Field` as a `str`.
        """
        prefix: str = (
            self._field_data.from_table._table_meta.alias
            or self._field_data.prefix
            or self._field_data.from_table.table_name()
        )
        field_name: str = f"{prefix}.{self._field_data.field_name}"
        if alias := self._field_data.alias:
            field_name += f" AS {alias}"

        return field_name

    @property
    def _default(self: Self) -> str | None:
        """Return default value of the field.

        This default is already converted into SQL string.
        Or None.

        ### Return
        default value.
        """
        return self._field_data.default

    @property
    def _callable_default(
        self: Self,
    ) -> CallableDefaultType[FieldType] | None:
        """Return callable object for default value for the field.

        This default value will be called in the statements
        that can be create new raws in the database.

        """
        return self._field_data.callable_default

    @property
    def _is_null(self: Self) -> bool:
        """Return flag that field can be `NULL`.

        ### Return
        `bool`
        """
        return self._field_data.is_null

    @property
    def _field_null(self: Self) -> str:
        """Return `NOT NULL` string if field cannot be NULL.

        ### Return
        `str`.
        """
        return "NOT NULL" if not self._is_null else ""

    @property
    def _field_default(self: Self) -> str:
        is_default: Final = self._default and callable(self._default)
        if is_default:
            return f"DEFAULT {self._default}" if self._default else ""
        return ""

    @property
    def _field_type(self: Self) -> str:
        """Property for final SQL field Type.

        It can be changed in subclasses.

        ### Return
        SQL `string`.
        """
        return str(self._sql_type.querystring())


if TYPE_CHECKING:
    from qaspen.table.base_table import BaseTable

OperatorTypes = Union[AnyOperator, AllOperator]


class Field(BaseField[FieldType]):
    """Main Field class.

    All subclasses must be inherited from this class.

    `_available_comparison_types` defines what types
    can be used in comparison methods, like `__eq__`.

    `_set_available_types` defines what types can be
    set as a value to the field.
    """

    _available_comparison_types: tuple[type, ...]
    _set_available_types: tuple[type, ...]

    def __init__(
        self: Self,
        *args: Any,
        is_null: bool = False,
        default: FieldDefaultType[FieldType] = None,
        db_field_name: str | None = None,
    ) -> None:
        """Create new Field instance.

        It's not possible to use not keyword arguments for
        specifying parameters.

        ### Parameters:
        - `is_null`: Defines is Field can be `NULL` or not.
        - `default`: Default value for the Field.
            This default value can be callable object.
            Note! This value will be set at the python level.
        - `db_field_name`: name of the field in the database.
        """
        if args:
            args_err_msg: Final = "Use only keyword arguments."
            raise FieldDeclarationError(args_err_msg)

        if is_null and default:
            err_msg: Final = (
                "It's not possible to specify is_null and default. "
                "Specify either is_null or default"
            )
            raise FieldDeclarationError(err_msg)

        self._validate_default_value(
            default_value=default,
        )

        default_value: str | None = None
        callable_default_value: CallableDefaultType[FieldType] | None = None

        if callable(default):
            callable_default_value = default
        elif default_value is not None:
            default_value = self._prepare_default_value(
                default_value=default,
            )

        self._field_data: FieldData[FieldType] = FieldData(
            field_name=db_field_name or "",
            is_null=is_null,
            default=default_value,
            callable_default=callable_default_value,
        )

    def __get__(
        self: Self,
        instance: BaseTable | None,
        owner: type[BaseTable] | None,
    ) -> Field[FieldType]:
        try:
            return cast(
                Field[FieldType],
                instance.__dict__[self._original_field_name],
            )
        except (AttributeError, KeyError):
            return cast(
                Field[FieldType],
                owner.__dict__[self._original_field_name],
            )

    def __set__(
        self: Self,
        instance: object,
        value: FieldType | EmptyFieldValue | None,
    ) -> None:
        field: Field[FieldType]
        if isinstance(value, EmptyFieldValue) or value is None:
            field = instance.__dict__[self._original_field_name]
            field._field_data.field_value = value
            return

        if isinstance(value, self.__class__):
            instance.__dict__[self._original_field_name] = value
            return

        if not isinstance(value, self._set_available_types):
            err_msg: Final = (
                f"Can't assign not {self._sql_type.querystring()} "
                f"type to {self.__class__.__name__}",
            )
            raise TypeError(err_msg)

        self._validate_field_value(
            field_value=value,  # type: ignore[arg-type]
        )
        field = instance.__dict__[self._original_field_name]
        field._field_data.field_value = value  # type: ignore[assignment]

    def _prepare_default_value(
        self: Self,
        default_value: FieldType | None,
    ) -> str | None:
        """Prepare default value to specify it in Field declaration.

        It uses only in the method `_field_default`.

        :returns: prepared default value.
        """
        return (
            str(default_value) if default_value is not None else default_value
        )

    def in_(
        self: Self,
        *comparison_values: FieldType,
        subquery: SQLSelectable | None = None,
    ) -> Filter:
        """`IN` PostgreSQL clause.

        It allows you to use `IN` clause.
        You can specify either unlimited number of `comparison` values
        or `subquery`.

        ### Parameters:
        - `comparison_values`: values for `IN` clause,
            they must be correct type. For example, if you are
            working with string Field, you have to use str objects.
        - `subquery`: Any object that provides `queryset()` method.

        ### Returns:
        Initialized `Filter` class.

        Example:
        -------
        ```python
        class Buns(BaseTable, table_name="buns"):
            name: VarCharField = VarCharField()


        select_statement = (
            Buns
            .select()
            .where(
                Buns.name.in_(
                    "Awesome",
                    "Qaspen",
                    "Project",
                )
            )
        )

        select_statement = (
            Buns
            .select()
            .where(
                Buns.name.in_(
                    subquery=Buns.select(
                        Buns.name,
                    ),
                ),
            )
        )
        ```
        """
        if subquery and comparison_values:
            args_err_msg: Final = (
                "It's not possible to specify subquery "
                "with positional arguments in `in_` method. "
                "Please choose either subquery or positional arguments.",
            )
            raise FilterComparisonError(args_err_msg)

        for comparison_value in comparison_values:
            is_valid_type: bool = isinstance(
                comparison_value,
                self._available_comparison_types,
            )
            if not is_valid_type:
                err_msg = (
                    f"It's impossible to use `IN` operator "
                    f"to compare {self.__class__.__name__} "
                    f"and {type(comparison_value)}",
                )
                raise FieldComparisonError(err_msg)

        where_parameters: dict[str, Any] = {
            "field": self,
            "operator": operators.InOperator,
        }

        if subquery:
            where_parameters["comparison_value"] = subquery
        elif comparison_values:
            where_parameters["comparison_values"] = comparison_values

        return Filter(**where_parameters)

    def not_in(
        self: Self,
        *comparison_values: FieldType,
        subquery: SQLSelectable | None = None,
    ) -> Filter:
        """`NOT IN` PostgreSQL clause.

        It allows you to use `NOT IN` clause.
        You can specify either unlimited number of `comparison` values
        or `subquery`.

        ### Parameters:
        - `comparison_values`: values for `NOT IN` clause,
            they must be correct type. For example, if you are
            working with string Field, you have to use str objects.
        - `subquery`: Any object that provides `queryset()` method.

        ### Returns:
        Initialized `Filter` class.

        Example:
        -------
        ```python
        class Buns(BaseTable, table_name="buns"):
            name: VarCharField = VarCharField()


        select_statement = (
            Buns
            .select()
            .where(
                Buns.name.not_in(
                    "Awesome",
                    "Qaspen",
                    "Project",
                )
            )
        )

        select_statement = (
            Buns
            .select()
            .where(
                Buns.name.not_in(
                    subquery=Buns.select(
                        Buns.name,
                    ),
                ),
            )
        )
        ```
        """
        if subquery and comparison_values:
            args_err_msg: Final = (
                "It's not possible to specify subquery "
                "with positional arguments in `not_in` method. "
                "Please choose either subquery or positional arguments.",
            )
            raise FilterComparisonError(args_err_msg)

        for comparison_value in comparison_values:
            is_valid_type: bool = isinstance(
                comparison_value,
                self._available_comparison_types,
            )
            if not is_valid_type:
                err_msg = (
                    f"It's impossible to use `NOT IN` operator "
                    f"to compare {self.__class__.__name__} "
                    f"and {type(comparison_value)}",
                )
                raise FieldComparisonError(err_msg)

        where_parameters: dict[str, Any] = {
            "field": self,
            "operator": operators.NotInOperator,
        }

        if subquery:
            where_parameters["comparison_value"] = subquery
        elif comparison_values:
            where_parameters["comparison_values"] = comparison_values

        return Filter(**where_parameters)

    def between(
        self: Self,
        left_value: FieldType | Field[Any],
        right_value: FieldType | Field[Any],
    ) -> FilterBetween:
        """`BETWEEN` PostgreSQL clause.

        It allows you to use `BETWEEN` clause.

        ### Parameters:
        - `left_value`: left-side value in the between clause.
        - `right_value`: right-side value in the between clause.

        ### Returns:
        Initialized `FilterBetween`.
        """
        is_valid_type: Final[bool] = all(
            (
                isinstance(
                    left_value,
                    self._available_comparison_types,
                ),
                isinstance(
                    right_value,
                    self._available_comparison_types,
                ),
            ),
        )
        if is_valid_type:
            return FilterBetween(
                field=self,
                operator=operators.BetweenOperator,
                left_comparison_value=left_value,
                right_comparison_value=right_value,
            )

        type_err_msg = (
            f"Incorrect type of one of the values "
            f"in `BETWEEN operator`. "
            f"You can use one of these - {self._available_comparison_types}",
        )
        raise FieldComparisonError(type_err_msg)

    def _make_field_create_statement(
        self: Self,
    ) -> str:
        return (
            f"{self._original_field_name} {self._sql_type.sql_type()} "
            f"{self._field_null} {self._field_default}"
        )

    def querystring(self: Self) -> QueryString:
        """Build QueryString class.

        ### Returns:
        Built `QueryString`.
        """
        return QueryString(
            self.field_name,
            sql_template="{}",
        )

    def __eq__(  # type: ignore[override]
        self: Self,
        comparison_value: FieldType | Field[Any] | OperatorTypes,
    ) -> Filter:
        if comparison_value is None:
            return Filter(
                field=self,
                operator=operators.IsNullOperator,
            )
        if isinstance(comparison_value, self._available_comparison_types):
            return Filter(
                field=self,
                comparison_value=comparison_value,
                operator=operators.EqualOperator,
            )

        comparison_err_msg: Final = (
            f"It's impossible to use `!=` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}",
        )
        raise FieldComparisonError(comparison_err_msg)

    def eq(
        self: Self,
        comparison_value: FieldType | Field[Any] | OperatorTypes,
    ) -> Filter:
        """Analog for `==` (`__eq__` method) operation.

        Works exactly the same. It exists just because some
        people prefer to use methods instead of python comparison.

        ### Parameters:
        - `comparison_value`: value to compare with.

        ### Returns:
        Initialized `Filter`.
        """
        return self.__eq__(comparison_value)

    def __ne__(  # type: ignore[override]
        self: Self,
        comparison_value: FieldType | Field[Any] | OperatorTypes,
    ) -> Filter:
        if comparison_value is None:
            return Filter(
                field=self,
                operator=operators.IsNotNullOperator,
            )
        if isinstance(comparison_value, self._available_comparison_types):
            return Filter(
                field=self,
                comparison_value=comparison_value,
                operator=operators.NotEqualOperator,
            )

        comparison_err_msg: Final = (
            f"It's impossible to use `!=` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}",
        )
        raise FieldComparisonError(comparison_err_msg)

    def neq(
        self: Self,
        comparison_value: FieldType | OperatorTypes,
    ) -> Filter:
        """Analog for `!=` (`__ne__` method) operation.

        Works exactly the same. It exists just because some
        people prefer to use methods instead of python comparison.

        ### Parameters:
        - `comparison_value`: value to compare with.

        ### Returns:
        Initialized `Filter`.
        """
        return self.__ne__(comparison_value)

    def __gt__(
        self: Self,
        comparison_value: FieldType | OperatorTypes,
    ) -> Filter:
        if isinstance(comparison_value, self._available_comparison_types):
            return Filter(
                field=self,
                comparison_value=comparison_value,
                operator=operators.GreaterOperator,
            )

        comparison_err_msg: Final = (
            f"It's impossible to use `>` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}",
        )
        raise FieldComparisonError(comparison_err_msg)

    def gt(
        self: Self,
        comparison_value: FieldType | OperatorTypes,
    ) -> Filter:
        """Analog for `>` (`__gt__` method) operation.

        Works exactly the same. It exists just because some
        people prefer to use methods instead of python comparison.

        ### Parameters:
        - `comparison_value`: value to compare with.

        ### Returns:
        Initialized `Filter`.
        """
        return self.__gt__(comparison_value)

    def __ge__(
        self: Self,
        comparison_value: FieldType | OperatorTypes,
    ) -> Filter:
        if isinstance(comparison_value, self._available_comparison_types):
            return Filter(
                field=self,
                comparison_value=comparison_value,
                operator=operators.GreaterEqualOperator,
            )
        comparison_err_msg: Final = (
            f"It's impossible to use `>=` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}",
        )
        raise FieldComparisonError(comparison_err_msg)

    def gte(
        self: Self,
        comparison_value: FieldType | OperatorTypes,
    ) -> Filter:
        """Analog for `>=` (`__ge__` method) operation.

        Works exactly the same. It exists just because some
        people prefer to use methods instead of python comparison.

        ### Parameters:
        - `comparison_value`: value to compare with.

        ### Returns:
        Initialized `Filter`.
        """
        return self.__ge__(comparison_value)

    def __lt__(
        self: Self,
        comparison_value: FieldType | OperatorTypes,
    ) -> Filter:
        if isinstance(comparison_value, self._available_comparison_types):
            return Filter(
                field=self,
                comparison_value=comparison_value,
                operator=operators.LessOperator,
            )
        comparison_err_msg: Final = (
            f"It's impossible to use `<` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}",
        )
        raise FieldComparisonError(comparison_err_msg)

    def lt(
        self: Self,
        comparison_value: FieldType | OperatorTypes,
    ) -> Filter:
        """Analog for `<` (`__lt__` method) operation.

        Works exactly the same. It exists just because some
        people prefer to use methods instead of python comparison.

        ### Parameters:
        - `comparison_value`: value to compare with.

        ### Returns:
        Initialized `Filter`.
        """
        return self.__lt__(comparison_value)

    def __le__(
        self: Self,
        comparison_value: FieldType | OperatorTypes,
    ) -> Filter:
        if isinstance(comparison_value, self._available_comparison_types):
            return Filter(
                field=self,
                comparison_value=comparison_value,
                operator=operators.LessEqualOperator,
            )
        comparison_err_msg: Final = (
            f"It's impossible to use `<=` operator "
            f"to compare {self.__class__.__name__} "
            f"and {type(comparison_value)}",
        )
        raise FieldComparisonError(comparison_err_msg)

    def lte(
        self: Self,
        comparison_value: FieldType | OperatorTypes,
    ) -> Filter:
        """Analog for `<=` (`__le__` method) operation.

        Works exactly the same. It exists just because some
        people prefer to use methods instead of python comparison.

        ### Parameters:
        - `comparison_value`: value to compare with.

        ### Returns:
        Initialized `Filter`.
        """
        return self.__le__(comparison_value)

    def _is_the_same_field(
        self: Self,
        other_field: BaseField[FieldType],
    ) -> bool:
        """Compare two fields.

        Return `True` if they are the same, else `False`.
        They are equal if they `_field_data`s are the same.

        ### Parameters:
        - `other_field`: field to compare with.

        ### Returns:
        Are fields the same or not.
        """
        return self._field_data == other_field._field_data

    def _with_prefix(self: Self, prefix: str) -> Field[FieldType]:
        """Give Field a prefix.

        Make a Field deepcopy and set new prefix.

        ### Parameters
        - `prefix`: prefix for the field.

        ### Returns
        `Field` with new prefix.
        """
        field: Field[FieldType] = copy.deepcopy(self)
        field._field_data.prefix = prefix
        return field

    def _with_alias(self: Self, alias: str) -> Field[FieldType]:
        """Give Field an alias.

        Make a Field deepcopy and set new alias.

        ### Parameters
        - `alias`: alias for the field.

        ### Returns
        `Field` with new alias.
        """
        field: Field[FieldType] = copy.deepcopy(self)
        field._field_data.alias = alias
        return field

    def _validate_field_value(
        self: Self,
        field_value: FieldType | None,
    ) -> None:
        """Validate field value.

        If `field_value` is None but field declared as NOT NULL,
        throw an error.

        :param field_value: new value for the field.

        :raises FieldValueValidationError: if the `max_length` is exceeded.
        """
        if field_value is None and self._is_null:
            return

    def _validate_default_value(
        self: Self,
        default_value: FieldDefaultType[FieldType],
    ) -> None:
        if default_value is None or types.FunctionType == type(default_value):
            return

        try:
            self._validate_field_value(
                field_value=default_value,  # type: ignore[arg-type]
            )
        except FieldValueValidationError as exc:
            validation_err_msg: Final = (
                f"Wrong default value in the field "
                f"{self.__class__.__name__}",
            )
            raise FieldValueValidationError(
                validation_err_msg,
            ) from exc

        if not isinstance(default_value, self._set_available_types):
            type_err_msg: Final = (
                f"Wrong default value in the field "
                f"{self.__class__.__name__}",
            )
            raise FieldValueValidationError(type_err_msg)
