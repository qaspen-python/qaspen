from typing import Any, Final, List, Tuple

from psycopg import AsyncConnection, AsyncCursor
from psycopg_pool import AsyncConnectionPool
from typing_extensions import Self

from qaspen.engine.base import BaseEngine, BaseTransaction
from qaspen.engine.enums import DataBaseType
from qaspen.querystring.querystring import QueryString


class PsycopgPoolTransaction(
    BaseTransaction[AsyncConnection],  # type: ignore[type-arg]
):
    @property
    def cursor(self: Self) -> AsyncCursor[Any]:
        return self.transaction_connection.cursor()

    async def run_query(
        self: Self,
        querystring: QueryString,
    ) -> List[Tuple[Any, ...]]:
        result_cursor = await self.cursor.execute(
            query=str(querystring),
        )
        return await result_cursor.fetchall()

    async def run_query_without_result(
        self: Self,
        querystring: QueryString,
    ) -> None:
        await self.cursor.execute(
            query=str(querystring),
        )

    async def rollback(self: Self) -> None:
        await self.transaction_connection.rollback()

    async def commit(self: Self) -> None:
        await self.transaction_connection.commit()


class PsycopgPoolEngine(
    BaseEngine[AsyncConnectionPool, PsycopgPoolTransaction],
):
    database_type: DataBaseType = DataBaseType.POSTGRESQL

    async def startup(self: Self) -> None:
        connection_pool: AsyncConnectionPool = AsyncConnectionPool(
            conninfo=self.connection_string,
            **self.connection_parameters,
        )

        await connection_pool.open()

        self.connection_pool = connection_pool

    async def shutdown(self: Self) -> None:
        if self.connection_pool:
            await self.connection_pool.close()
            self.connection_pool = None

    async def transaction(self: Self) -> PsycopgPoolTransaction:
        if not self.connection_pool:
            await self.startup()
        if not self.connection_pool:
            raise ValueError()

        single_connection: Final = await self.connection_pool.getconn()
        async_cursor = single_connection.cursor()
        return PsycopgPoolTransaction(
            transaction_connection=await self.connection_pool.getconn(),
        )

    async def run_query(
        self: Self,
        querystring: QueryString,
        in_transaction: bool = True,
    ) -> List[Tuple[Any, ...]]:
        if not self.connection_pool:
            await self.startup()
        if not self.connection_pool:
            raise ValueError()

        async with self.connection_pool.connection() as async_conn:
            result_cursor = await async_conn.execute(str(querystring))
            result = await result_cursor.fetchall()
            return result

    async def run_query_without_result(
        self: Self,
        querystring: QueryString,
        in_transaction: bool = True,
    ) -> None:
        if not self.connection_pool:
            await self.startup()
        if not self.connection_pool:
            raise ValueError()

        async with self.connection_pool.connection() as async_conn:
            await async_conn.execute(str(querystring))
