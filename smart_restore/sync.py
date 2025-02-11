from sqlalchemy import and_, or_
from sqlalchemy.schema import Table
from sqlalchemy.sql.elements import ClauseElement
from sqlalchemy_utils import get_referencing_foreign_keys, group_foreign_keys

from .reader import DatabaseReader
from .utility import chunk_list
from .writer import DatabaseWriter


def get_foreign_key_rows(
    source: DatabaseReader, table: Table, rows
) -> list[tuple[str, list[dict]]]:
    # Copy all direct dependencies required by these rows
    foreign_key_rows = []

    for foreign_key in table.foreign_keys:
        if values := [
            row[foreign_key.parent.name]
            for row in rows
            if row[foreign_key.parent.name] is not None
        ]:
            foreign_table = foreign_key.column.table

            if foreign_table.name in source.exclude_tables:
                continue

            foreign_rows = list(
                source.select_rows(
                    table=foreign_table,
                    where=foreign_table.columns[foreign_key.column.name].in_(values),
                )
            )

            # Add recursive dependents first
            foreign_key_rows.extend(
                get_foreign_key_rows(
                    source=source, table=foreign_table, rows=foreign_rows
                )
            )

            foreign_key_rows.append((foreign_table.name, foreign_rows))

    return foreign_key_rows


def copy_row(
    source: DatabaseReader,
    target: DatabaseWriter,
    table_name: str,
    where: ClauseElement,
    fanout_tables: set[str],
):
    table = target.metadata.tables[table_name]

    # Process in chunks for better responsiveness

    for chunk_rows in chunk_list(
        source.select_rows(
            table=table,
            where=where,
        ),
        size=100,  # FUTURE: Tune this size value
    ):
        # Ensure all foreign key rows exist
        foreign_key_table_rows = get_foreign_key_rows(
            source=source, table=table, rows=chunk_rows
        )

        for foreign_table, foreign_rows in foreign_key_table_rows:
            target.upsert_rows(foreign_table, foreign_rows)

        # Copy these rows
        target.upsert_rows(table.name, chunk_rows)

        # Fanout out to tables that depend on THESE rows
        for foreign_table, foreign_rows in foreign_key_table_rows:
            fanout_table_rows(
                source=source,
                target=target,
                table=target.metadata.tables[foreign_table],
                rows=foreign_rows,
                fanout_tables=fanout_tables,
            )

        fanout_table_rows(
            source=source,
            target=target,
            table=table,
            rows=chunk_rows,
            fanout_tables=fanout_tables,
        )


def fanout_table_rows(
    source: DatabaseReader,
    target: DatabaseWriter,
    table: Table,
    rows: list[dict],
    fanout_tables: set[str],
):
    # Fanout sync to objects with foreign keys to THESE rows
    if rows:
        for (
            source_dependent_table,
            grouped_foreign_keys_iter,
        ) in group_foreign_keys(get_referencing_foreign_keys(table)):
            if source_dependent_table.name in fanout_tables:
                grouped_foreign_keys = list(grouped_foreign_keys_iter)
                copy_row(
                    source=source,
                    target=target,
                    table_name=source_dependent_table.name,
                    where=or_(
                        *[
                            and_(
                                *[
                                    source_dependent_table.columns[
                                        foreign_key.parent.name
                                    ]
                                    == row[foreign_key.column.name]
                                    for foreign_key in grouped_foreign_keys
                                ]
                            )
                            for row in rows
                        ]
                    ),
                    # Remove matched table from the list so we don't endlessly fanout/propagate
                    fanout_tables={
                        table
                        for table in fanout_tables
                        if table != source_dependent_table.name
                    },
                )
