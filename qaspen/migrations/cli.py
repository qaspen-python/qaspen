"""CLI for Qaspen migrations."""
import asyncio

import typer

from qaspen.engine.psycopgpool_engine import PsycopgPoolEngine
from qaspen.migrations.manager import MigrationManager

cli_app: typer.Typer = typer.Typer()
engine = PsycopgPoolEngine(
    connection_string="postgres://postgres:12345@localhost:5432/qaspendb",
)


@cli_app.command(
    name="migrate",
)
def migrate() -> None:
    print("in migrate command")


@cli_app.command()
def apply() -> None:
    print("in apply command")


@cli_app.command()
def rollback() -> None:
    print("in rollback command")


@cli_app.command()
def init_db() -> None:
    async def init_db() -> None:
        await engine.startup()
        migration_manager = MigrationManager(
            db_engine=engine,
        )
        await migration_manager.init_db()
        await engine.shutdown()

    asyncio.run(init_db())
