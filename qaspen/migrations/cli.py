import typer

cli_app: typer.Typer = typer.Typer()


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
    print("In init_db command")
