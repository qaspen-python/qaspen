from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final

import pytest

from qaspen.statements.combinable_statements.join_statement import (
    FullOuterJoin,
    InnerJoin,
    Join,
    JoinStatement,
    JoinType,
    LeftOuterJoin,
    RightOuterJoin,
)
from tests.test_statements.conftest import UserTest, VideoTest

if TYPE_CHECKING:
    from qaspen.columns.base import Column


@pytest.mark.parametrize(
    "join_class",
    [Join, InnerJoin, LeftOuterJoin, RightOuterJoin, FullOuterJoin],
)
def test_join_init_method(join_class: type[Join]) -> None:
    """Test `__init__` `Join` method."""
    columns = [UserTest.description]
    on_condition = VideoTest.user_id == UserTest.id
    alias = "video_join"
    inited_join: Final = join_class(
        columns=columns,
        from_table=UserTest,
        join_table=VideoTest,
        on=on_condition,
        join_alias="video_join",
    )

    expected_columns = [
        column._with_prefix(
            prefix=(
                column._column_data.from_table._table_meta.alias
                or inited_join._alias
            ),
        )
        for column in columns
    ]
    assert inited_join._columns == expected_columns
    assert inited_join._from_table == UserTest
    assert inited_join._join_table == VideoTest
    assert inited_join._based_on == on_condition
    assert inited_join._alias == alias


@pytest.mark.parametrize(
    "join_class",
    [Join, InnerJoin, LeftOuterJoin, RightOuterJoin, FullOuterJoin],
)
def test_join_querystring_method(join_class: type[Join]) -> None:
    """Test `querystring` `Join` method."""
    alias = "video_join"
    inited_join: Final = join_class(
        columns=[UserTest.description],
        from_table=UserTest,
        join_table=VideoTest,
        on=VideoTest.user_id == UserTest.id,
        join_alias=alias,
    )

    querystring, qs_parameters = inited_join.querystring().build()
    assert querystring == (
        f"{join_class.join_type} public.videotest AS {alias} ON videotest.user_id = usertest.id"  # noqa: E501
    )
    assert not qs_parameters


@pytest.mark.parametrize(
    "join_class",
    [Join, InnerJoin, LeftOuterJoin, RightOuterJoin, FullOuterJoin],
)
def test_join_add_columns_method(join_class: type[Join]) -> None:
    """Test `add_columns` `Join` method."""
    alias = "video_join"
    columns = [UserTest.description]
    inited_join: Final = join_class(
        columns=None,
        from_table=UserTest,
        join_table=VideoTest,
        on=VideoTest.user_id == UserTest.id,
        join_alias=alias,
    )

    assert not inited_join._columns

    inited_join.add_columns(columns)  # type: ignore[arg-type]

    expected_columns = [
        column._with_prefix(
            prefix=(
                column._column_data.from_table._table_meta.alias
                or inited_join._alias
            ),
        )
        for column in columns
    ]

    assert inited_join._join_columns() == expected_columns

    inited_join.add_columns(columns)  # type: ignore[arg-type]

    assert inited_join._join_columns() == expected_columns + expected_columns


@pytest.mark.parametrize(
    "join_type",
    [
        JoinType.JOIN,
        JoinType.INNERJOIN,
        JoinType.LEFTJOIN,
        JoinType.RIGHTJOIN,
        JoinType.FULLOUTERJOIN,
    ],
)
def test_join_statement_join_method(join_type: JoinType) -> None:
    """Test `join` in `JoinStatement`."""
    join_stmt: Final = JoinStatement()

    columns = [UserTest.description]
    join_stmt.join(
        columns=columns,
        from_table=UserTest,
        join_table=VideoTest,
        on=VideoTest.user_id == UserTest.id,
        join_type=join_type,
    )
    assert join_stmt.join_expressions


@pytest.mark.parametrize(
    "join_class",
    [Join, InnerJoin, LeftOuterJoin, RightOuterJoin, FullOuterJoin],
)
def test_join_statement_add_join_method(join_class: type[Join]) -> None:
    """Test `add_join` method in `JoinStatement`."""
    join_stmt: Final = JoinStatement()

    columns = [UserTest.description]
    alias = "video_join"
    join: Final = join_class(
        columns=columns,
        from_table=UserTest,
        join_table=VideoTest,
        on=VideoTest.user_id == UserTest.id,
        join_alias=alias,
    )

    join_stmt.add_join(join)

    assert join in join_stmt.join_expressions


@pytest.mark.parametrize(
    "join_class",
    [Join, InnerJoin, LeftOuterJoin, RightOuterJoin, FullOuterJoin],
)
def test_join_statement_retrieve_all_join_columns_method(
    join_class: type[Join],
) -> None:
    """Test `_retrieve_all_join_columns` method in `JoinStatement`."""
    join_stmt: Final = JoinStatement()

    columns: list[Column[Any]] = [UserTest.description, VideoTest.video_id]
    alias = "video_join"
    join: Final = join_class(
        columns=columns,
        from_table=UserTest,
        join_table=VideoTest,
        on=VideoTest.user_id == UserTest.id,
        join_alias=alias,
    )

    join_stmt.add_join(join)

    assert join_stmt._retrieve_all_join_columns() == columns


@pytest.mark.parametrize(
    "join_class",
    [Join, InnerJoin, LeftOuterJoin, RightOuterJoin, FullOuterJoin],
)
def test_join_statement_querystring_method(
    join_class: type[Join],
) -> None:
    """Test `querystring` method in `JoinStatement`."""
    join_stmt: Final = JoinStatement()

    columns: list[Column[Any]] = [UserTest.description, VideoTest.video_id]
    alias = "video_join"
    join1: Final = join_class(
        columns=columns,
        from_table=UserTest,
        join_table=VideoTest,
        on=VideoTest.user_id == UserTest.id,
        join_alias=alias,
    )

    join2: Final = join_class(
        columns=columns,
        from_table=UserTest,
        join_table=VideoTest,
        on=UserTest.id == VideoTest.user_id,
        join_alias=alias,
    )

    join_stmt.add_join(join1)
    join_stmt.add_join(join2)

    querystring, qs_parameters = join_stmt.querystring().build()
    assert querystring == (
        f"{join_class.join_type} public.videotest AS {alias} ON videotest.user_id = usertest.id "  # noqa: E501
        f"{join_class.join_type} public.videotest AS {alias} ON usertest.id = videotest.user_id"  # noqa: E501
    )
    assert not qs_parameters
