from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from qaspen.aggregate_functions.general_purpose import Count
from qaspen.clauses.order_by import OrderBy
from tests.test_statements.conftest import ProfileTable, UserTable, VideoTable

if TYPE_CHECKING:
    from qaspen_psycopg.engine import PsycopgEngine, PsycopgTransaction


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_select_await_method(
    test_engine: PsycopgEngine,
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `__await__` `SelectStatement` method."""
    stmt = UserTable.select()

    UserTable._table_meta.database_engine = None
    with pytest.raises(expected_exception=AttributeError):
        await stmt

    test_engine.running_transaction.set(test_db_transaction)
    UserTable._table_meta.database_engine = test_engine

    stmt_result = await stmt
    expected_number_of_results = 2
    expected_result = [
        {"user_id": 1, "fullname": "Qaspen"},
        {"user_id": 2, "fullname": "Python"},
    ]

    raw_result = stmt_result.result()
    assert len(raw_result) == expected_number_of_results
    assert raw_result == expected_result


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_select_execute_method(
    test_engine: PsycopgEngine,
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `execute` `SelectStatement` method."""
    test_engine.running_transaction.set(test_db_transaction)

    stmt_result = await UserTable.select().execute(
        engine=test_engine,
    )
    expected_number_of_results = 2
    expected_result = [
        {"user_id": 1, "fullname": "Qaspen"},
        {"user_id": 2, "fullname": "Python"},
    ]

    raw_result = stmt_result.result()
    assert len(raw_result) == expected_number_of_results
    assert raw_result == expected_result


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_select_transaction_execute_method(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `where` `SelectStatement` method."""
    stmt_result = await UserTable.select().transaction_execute(
        transaction=test_db_transaction,
    )
    expected_number_of_results = 2
    expected_result = [
        {"user_id": 1, "fullname": "Qaspen"},
        {"user_id": 2, "fullname": "Python"},
    ]

    raw_result = stmt_result.result()
    assert len(raw_result) == expected_number_of_results
    assert raw_result == expected_result


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_select_where_method(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `where` `SelectStatement` method."""
    stmt = UserTable.select().where(
        UserTable.user_id == 1,
    )

    querystring, qs_params = stmt.querystring().build()
    assert (
        querystring
        == "SELECT main_users.user_id, main_users.fullname FROM public.main_users WHERE main_users.user_id = %s"  # noqa: E501
    )
    assert qs_params == [1]

    stmt_result = await stmt.transaction_execute(
        transaction=test_db_transaction,
    )

    expected_result = [
        {"user_id": 1, "fullname": "Qaspen"},
    ]
    raw_result = stmt_result.result()
    assert len(raw_result) == 1
    assert raw_result == expected_result


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_select_limit_method(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `limit` `SelectStatement` method."""
    stmt = UserTable.select(
        UserTable.fullname,
    ).limit(limit=1)

    querystring, qs_params = stmt.querystring().build()
    assert (
        querystring
        == "SELECT main_users.fullname FROM public.main_users LIMIT 1"
    )
    assert not qs_params

    stmt_result = await stmt.transaction_execute(
        transaction=test_db_transaction,
    )

    expected_result = [
        {"fullname": "Qaspen"},
    ]
    raw_result = stmt_result.result()
    assert len(raw_result) == 1
    assert raw_result == expected_result


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_select_offset_method(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `offset` `SelectStatement` method."""
    stmt = UserTable.select(
        UserTable.fullname,
    ).offset(offset=1)

    querystring, qs_params = stmt.querystring().build()
    assert (
        querystring
        == "SELECT main_users.fullname FROM public.main_users OFFSET 1"
    )
    assert not qs_params

    stmt_result = await stmt.transaction_execute(
        transaction=test_db_transaction,
    )
    expected_result = [
        {"fullname": "Python"},
    ]

    raw_result = stmt_result.result()
    assert len(raw_result) == 1
    assert raw_result == expected_result


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_select_limit_offset_method(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `limit_offset` `SelectStatement` method."""
    stmt = UserTable.select(
        UserTable.fullname,
    ).limit_offset(
        limit=1,
        offset=1,
    )

    querystring, qs_params = stmt.querystring().build()
    assert (
        querystring
        == "SELECT main_users.fullname FROM public.main_users LIMIT 1 OFFSET 1"
    )
    assert not qs_params

    stmt_result = await stmt.transaction_execute(
        transaction=test_db_transaction,
    )
    expected_result = [
        {"fullname": "Python"},
    ]

    raw_result = stmt_result.result()
    assert len(raw_result) == 1
    assert raw_result == expected_result


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
@pytest.mark.parametrize(
    ("ascending", "nulls_first", "expected_query", "expected_result"),
    [
        (
            True,
            True,
            "SELECT main_users.fullname FROM public.main_users ORDER BY main_users.fullname ASC NULLS FIRST",  # noqa: E501
            [{"fullname": "Python"}, {"fullname": "Qaspen"}],
        ),
        (
            True,
            False,
            "SELECT main_users.fullname FROM public.main_users ORDER BY main_users.fullname ASC NULLS LAST",  # noqa: E501
            [{"fullname": "Python"}, {"fullname": "Qaspen"}],
        ),
        (
            False,
            True,
            "SELECT main_users.fullname FROM public.main_users ORDER BY main_users.fullname DESC NULLS FIRST",  # noqa: E501
            [{"fullname": "Qaspen"}, {"fullname": "Python"}],
        ),
        (
            False,
            False,
            "SELECT main_users.fullname FROM public.main_users ORDER BY main_users.fullname DESC NULLS LAST",  # noqa: E501
            [{"fullname": "Qaspen"}, {"fullname": "Python"}],
        ),
    ],
)
async def test_select_order_by_method_without_order_bys(
    test_db_transaction: PsycopgTransaction,
    ascending: bool,
    nulls_first: bool,
    expected_query: str,
    expected_result: list[dict[str, str]],
) -> None:
    """Test `order_by` `SelectStatement` method."""
    stmt = UserTable.select(
        UserTable.fullname,
    ).order_by(
        field=UserTable.fullname,
        ascending=ascending,
        nulls_first=nulls_first,
    )

    querystring, qs_params = stmt.querystring().build()
    assert querystring == expected_query
    assert not qs_params

    stmt_result = await stmt.transaction_execute(
        transaction=test_db_transaction,
    )

    raw_result = stmt_result.result()
    expected_number_of_results = 2
    assert len(raw_result) == expected_number_of_results
    assert raw_result == expected_result


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
@pytest.mark.parametrize(
    ("order_bys", "expected_query", "expected_result"),
    [
        (
            [
                OrderBy(
                    field=UserTable.fullname,
                    ascending=True,
                    nulls_first=True,
                ),
            ],
            "SELECT main_users.fullname FROM public.main_users ORDER BY main_users.fullname ASC NULLS FIRST",  # noqa: E501
            [{"fullname": "Python"}, {"fullname": "Qaspen"}],
        ),
        (
            [
                OrderBy(
                    field=UserTable.fullname,
                    ascending=True,
                    nulls_first=False,
                ),
            ],
            "SELECT main_users.fullname FROM public.main_users ORDER BY main_users.fullname ASC NULLS LAST",  # noqa: E501
            [{"fullname": "Python"}, {"fullname": "Qaspen"}],
        ),
        (
            [
                OrderBy(
                    field=UserTable.fullname,
                    ascending=False,
                    nulls_first=True,
                ),
            ],
            "SELECT main_users.fullname FROM public.main_users ORDER BY main_users.fullname DESC NULLS FIRST",  # noqa: E501
            [{"fullname": "Qaspen"}, {"fullname": "Python"}],
        ),
        (
            [
                OrderBy(
                    field=UserTable.fullname,
                    ascending=False,
                    nulls_first=False,
                ),
            ],
            "SELECT main_users.fullname FROM public.main_users ORDER BY main_users.fullname DESC NULLS LAST",  # noqa: E501
            [{"fullname": "Qaspen"}, {"fullname": "Python"}],
        ),
    ],
)
async def test_select_order_by_method_with_order_bys(
    test_db_transaction: PsycopgTransaction,
    order_bys: list[OrderBy],
    expected_query: str,
    expected_result: list[dict[str, str]],
) -> None:
    """Test `order_by` `SelectStatement` method."""
    stmt = UserTable.select(
        UserTable.fullname,
    ).order_by(order_bys=order_bys)

    querystring, qs_params = stmt.querystring().build()
    assert querystring == expected_query
    assert not qs_params

    stmt_result = await stmt.transaction_execute(
        transaction=test_db_transaction,
    )

    raw_result = stmt_result.result()
    expected_number_of_results = 2
    assert len(raw_result) == expected_number_of_results
    assert raw_result == expected_result


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_select_group_by_method(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `group_by` method."""
    stmt = UserTable.select(
        UserTable.fullname,
    ).group_by(
        UserTable.fullname,
    )

    querystring, qs_params = stmt.querystring().build()

    assert (
        querystring
        == "SELECT main_users.fullname FROM public.main_users GROUP BY main_users.fullname"  # noqa: E501
    )
    assert not qs_params

    stmt_raw_result = await stmt.transaction_execute(
        transaction=test_db_transaction,
    )
    stmt_result = stmt_raw_result.result()

    expected_number_of_results = 2
    assert len(stmt_result) == expected_number_of_results
    assert stmt_result == [
        {"fullname": "Python"},
        {"fullname": "Qaspen"},
    ]


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_select_union_method(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `union` `SelectStatement` method."""
    stmt = UserTable.select(
        UserTable.fullname,
    )
    stmt2 = UserTable.select(
        UserTable.fullname,
    )

    union = stmt.union(stmt2)
    union_qs, union_qs_params = union.querystring().build()

    assert (
        union_qs
        == "SELECT main_users.fullname FROM public.main_users UNION SELECT main_users.fullname FROM public.main_users"  # noqa: E501
    )
    assert not union_qs_params

    stmt_result = await union.transaction_execute(
        transaction=test_db_transaction,
    )

    expected_number_of_results = 2
    assert len(stmt_result) == expected_number_of_results
    assert stmt_result == [{"fullname": "Python"}, {"fullname": "Qaspen"}]

    union_all = stmt.union(stmt2, union_all=True)

    union_all_qs, union_all_qs_params = union_all.querystring().build()
    assert (
        union_all_qs
        == "SELECT main_users.fullname FROM public.main_users UNION ALL SELECT main_users.fullname FROM public.main_users"  # noqa: E501
    )
    assert not union_all_qs_params

    stmt_result = await union_all.transaction_execute(
        transaction=test_db_transaction,
    )
    expected_number_of_results = 4
    assert len(stmt_result) == expected_number_of_results
    assert stmt_result == [
        {"fullname": "Qaspen"},
        {"fullname": "Python"},
        {"fullname": "Qaspen"},
        {"fullname": "Python"},
    ]


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_select_intersect_method(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `intersect` `SelectStatement` method."""
    stmt = UserTable.select(
        UserTable.fullname,
    )
    stmt2 = UserTable.select(
        UserTable.fullname,
    )

    intersect = stmt.intersect(stmt2)

    querystring, qs_params = intersect.querystring().build()
    assert (
        querystring
        == "SELECT main_users.fullname FROM public.main_users INTERSECT SELECT main_users.fullname FROM public.main_users"  # noqa: E501
    )
    assert not qs_params

    stmt_result = await intersect.transaction_execute(
        transaction=test_db_transaction,
    )

    expected_number_of_results = 2
    assert len(stmt_result) == expected_number_of_results
    assert stmt_result == [{"fullname": "Python"}, {"fullname": "Qaspen"}]


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_select_exists_method(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `exists` `SelectStatement` method."""
    stmt = (
        UserTable.select(
            UserTable.fullname,
        )
        .where(UserTable.fullname == "Qaspen")
        .exists()
    )

    querystring, qs_params = stmt.querystring_for_select().build()
    assert (
        querystring
        == "SELECT EXISTS (SELECT 1 FROM public.main_users WHERE main_users.fullname = %s)"  # noqa: E501
    )
    assert qs_params == ["Qaspen"]

    stmt_result = await stmt.transaction_execute(
        transaction=test_db_transaction,
    )

    assert stmt_result


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_select_having_method(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `having` `SelectStatement` method."""
    stmt = (
        UserTable.select(
            UserTable.fullname,
        )
        .group_by(UserTable.fullname)
        .having(Count(UserTable.fullname) == "1")
    )

    querystring, qs_params = stmt.querystring().build()
    assert (
        querystring
        == "SELECT main_users.fullname FROM public.main_users GROUP BY main_users.fullname HAVING COUNT(main_users.fullname) = %s"  # noqa: E501
    )
    assert qs_params == ["1"]

    stmt_result = await stmt.transaction_execute(
        transaction=test_db_transaction,
    )
    assert stmt_result.result() == [
        {"fullname": "Python"},
        {"fullname": "Qaspen"},
    ]


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_select_join_method(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `join` `SelectStatement` method."""
    aliased_profile = ProfileTable.aliased(alias="palias")
    aliased_video = VideoTable.aliased(alias="valias")

    stmt = aliased_profile.select(
        aliased_profile.nickname,
        aliased_video.video_id,
    ).join(
        join_table=aliased_video,
        based_on=aliased_video.profile_id == aliased_profile.profile_id,
    )

    querystring, qs_params = stmt.querystring().build()
    assert (
        querystring
        == "SELECT palias.nickname, valias.video_id FROM public.profiles AS palias JOIN public.videos AS valias ON valias.profile_id = palias.profile_id"  # noqa: E501
    )
    assert not qs_params

    stmt_result = await stmt.transaction_execute(
        transaction=test_db_transaction,
    )
    expected_result = [
        {"nickname": "ORM", "video_id": 1},
        {"nickname": "PL", "video_id": 2},
    ]
    raw_result = stmt_result.result()
    assert raw_result == expected_result


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_select_inner_join_method(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `inner_join` `SelectStatement` method."""
    stmt = UserTable.select(
        UserTable.fullname,
        ProfileTable.nickname,
    ).inner_join(
        join_table=ProfileTable,
        based_on=UserTable.user_id == ProfileTable.user_id,
    )

    querystring, qs_params = stmt.querystring().build()
    assert (
        querystring
        == "SELECT main_users.fullname, profiles.nickname FROM public.main_users INNER JOIN public.profiles AS profiles ON main_users.user_id = profiles.user_id"  # noqa: E501
    )
    assert not qs_params

    stmt_result = await stmt.transaction_execute(
        transaction=test_db_transaction,
    )
    expected_result = [
        {"fullname": "Qaspen", "nickname": "ORM"},
        {"fullname": "Python", "nickname": "PL"},
    ]
    raw_result = stmt_result.result()
    assert raw_result == expected_result


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_select_left_join_method(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `left_join` `SelectStatement` method."""
    stmt = UserTable.select(
        UserTable.fullname,
        ProfileTable.nickname,
    ).left_join(
        join_table=ProfileTable,
        based_on=UserTable.user_id == ProfileTable.user_id,
    )

    querystring, qs_params = stmt.querystring().build()
    assert (
        querystring
        == "SELECT main_users.fullname, profiles.nickname FROM public.main_users LEFT JOIN public.profiles AS profiles ON main_users.user_id = profiles.user_id"  # noqa: E501
    )
    assert not qs_params

    stmt_result = await stmt.transaction_execute(
        transaction=test_db_transaction,
    )
    expected_result = [
        {"fullname": "Qaspen", "nickname": "ORM"},
        {"fullname": "Python", "nickname": "PL"},
    ]
    raw_result = stmt_result.result()
    assert raw_result == expected_result


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_select_right_join_method(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `right_join` `SelectStatement` method."""
    stmt = UserTable.select(
        UserTable.fullname,
        ProfileTable.nickname,
    ).right_join(
        join_table=ProfileTable,
        based_on=UserTable.user_id == ProfileTable.user_id,
    )

    querystring, qs_params = stmt.querystring().build()
    assert (
        querystring
        == "SELECT main_users.fullname, profiles.nickname FROM public.main_users RIGHT JOIN public.profiles AS profiles ON main_users.user_id = profiles.user_id"  # noqa: E501
    )
    assert not qs_params

    stmt_result = await stmt.transaction_execute(
        transaction=test_db_transaction,
    )
    expected_result = [
        {"fullname": "Qaspen", "nickname": "ORM"},
        {"fullname": "Python", "nickname": "PL"},
    ]
    raw_result = stmt_result.result()
    assert raw_result == expected_result


@pytest.mark.anyio()
@pytest.mark.usefixtures("_create_test_data")
async def test_select_full_outer_join_method(
    test_db_transaction: PsycopgTransaction,
) -> None:
    """Test `full_outer_join` `SelectStatement` method."""
    stmt = UserTable.select(
        UserTable.fullname,
        ProfileTable.nickname,
    ).full_outer_join(
        join_table=ProfileTable,
        based_on=UserTable.user_id == ProfileTable.user_id,
    )

    querystring, qs_params = stmt.querystring().build()
    assert (
        querystring
        == "SELECT main_users.fullname, profiles.nickname FROM public.main_users FULL OUTER JOIN public.profiles AS profiles ON main_users.user_id = profiles.user_id"  # noqa: E501
    )
    assert not qs_params

    stmt_result = await stmt.transaction_execute(
        transaction=test_db_transaction,
    )
    expected_result = [
        {"fullname": "Qaspen", "nickname": "ORM"},
        {"fullname": "Python", "nickname": "PL"},
    ]
    raw_result = stmt_result.result()
    assert raw_result == expected_result
