import typer

from fintl.accounts_etl import process_accounts
from fintl.accounts_etl.schemas import Config
from fintl.fine_logging import setup_logging

app = typer.Typer(help="Run the accounts ETL pipeline.")


@app.command()
def run():
    """Load configuration and run the accounts ETL pipeline."""
    config = Config()
    setup_logging(config.logging)
    process_accounts.main(config)


if __name__ == "__main__":
    app()
