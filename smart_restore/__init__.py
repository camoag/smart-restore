#!/usr/bin/env python3
#

import click
import click_config_file
from sqlalchemy import text
from tqdm import tqdm

from .reader import DatabaseReader
from .sync import copy_row
from .writer import AsyncDatabaseWriter, SyncDatabaseWriter

# FUTURE: Ideas for improvement
# Separate source + target worker processes (2x faster)
#   Create "scenario" (name?) files with lists of objects to restore, in order
#   Consider checking (fast) Target database for dependency Foreign Key row existence BEFORE pulling form source (especially for larger tables like `geo_*`
#   Verify Alembic versions match on target + source database


@click.command()
@click.option("--source", required=True, help="Source database URL")
@click.option("--target", required=True, help="Target database URL")
@click.option(
    "--fanout",
    "fanout_tables",
    multiple=True,
    help="Fan out to tables that contain Foreign Keys to synced rows.",
)
@click.option(
    "--exclude-table", multiple=True, help="Don't restore any rows from these table(s)"
)
@click.option("--verbose", is_flag=True, help="Verbose logging output")
@click.option("--sync", is_flag=True)
@click.argument("table_expressions", nargs=-1)
@click_config_file.configuration_option(config_file_name="smart_restore.ini")
def restore(
    source: str,
    target: str,
    fanout_tables: list[str],
    exclude_table: list[str],
    table_expressions: list[str],
    verbose: bool,
    sync: bool,
):
    tqdm.write("\n\t".join(("Copying source data:",) + table_expressions))
    if fanout_tables:
        tqdm.write(
            "\n\t".join(
                ("Will fanout/populate Foreign-Keyed rows in these tables:",)
                + fanout_tables
            )
        )

    reader = DatabaseReader(
        database_uri=source,
        verbose=verbose,
        exclude_tables=exclude_table,
    )
    if sync:
        writer_class = SyncDatabaseWriter
    else:
        writer_class = AsyncDatabaseWriter
    writer = writer_class(database_uri=target, verbose=verbose)

    for table_expression in table_expressions:
        table, _, where = table_expression.partition(":")

        copy_row(
            source=reader,
            target=writer,
            table_name=table,
            where=text(where),
            fanout_tables=fanout_tables,
        )

    # Wait for all rows to be copied
    writer.join(verbose=verbose)
