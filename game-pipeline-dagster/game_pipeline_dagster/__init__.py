from dagster import (AssetSelection, Definitions, FilesystemIOManager,
                     ScheduleDefinition, define_asset_job,
                     load_assets_from_modules)
from dagster_duckdb_pandas import DuckDBPandasIOManager

from . import assets

all_assets = load_assets_from_modules([assets])

# Insert this section anywhere above your `defs = Definitions(...)`
database_io_manager = DuckDBPandasIOManager(database="analytics.game_analytics.duckdb")

top_games_job = define_asset_job("top_games_job", selection=AssetSelection.all())

top_games_schedule = ScheduleDefinition(
    job=top_games_job,
    cron_schedule="0 0 * * *",  # every hour
)

io_manager = FilesystemIOManager(
    base_dir="data",  # Path is built relative to where `dagster dev` is run
)

defs = Definitions(
    assets=all_assets,
    schedules=[top_games_schedule],  # Addition: add the job to Definitions object (see below)
    resources={
        "io_manager": io_manager,
        "database_io_manager": database_io_manager,  # Define the I/O manager here
    },
)
