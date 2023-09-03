import typing

from psycopg_pool import AsyncConnectionPool

from qaspen.engine.base_engine import BaseEngine
from qaspen.engine.enums import DataBaseType
from qaspen.querystring.querystring import QueryString


class PsycopgPoolEngine(BaseEngine[AsyncConnectionPool]):
    database_type: DataBaseType = DataBaseType.POSTGRESQL

    async def startup(self: typing.Self) -> None:
        connection_pool: AsyncConnectionPool = AsyncConnectionPool(
            conninfo=self.connection_string,
            **self.connection_parameters,
        )

        await connection_pool.open()

        self.connection = connection_pool

    async def shutdown(self: typing.Self) -> None:
        if self.connection:
            await self.connection.close()
            self.connection = None

    async def run_query(
        self: typing.Self,
        querystring: QueryString,
        in_transaction: bool = True,
    ) -> list[tuple[typing.Any, ...]]:
        if not self.connection:
            await self.startup()
        if not self.connection:
            raise ValueError()

        async with self.connection.connection() as async_conn:
            result_cursor = await async_conn.execute(str(querystring))
            return await result_cursor.fetchall()
