from typing import Generator

from sqlalchemy.schema import Table
from sqlalchemy.sql.elements import ClauseElement

from .database import Database


def make_row_hash(table: Table, row) -> tuple[str, str]:
    primary_key_values = {
        column.name: row[column.name] for column in table.primary_key.columns
    }
    return (table.name, str(sorted(primary_key_values.items())))


class DatabaseReader(Database):
    def __init__(self, database_uri: str, exclude_tables: set[str], verbose: bool):
        super().__init__(database_uri=database_uri, verbose=verbose)
        self.select_cache = set()
        self.exclude_tables = exclude_tables

    def select_rows(
        self, table: Table, where: ClauseElement
    ) -> Generator[dict, None, None]:
        with self.engine.connect() as connection:
            # Simple pagination for large queries. Was unable to get more advance "yield_per" and "stream_results" options to work.
            offset = 0
            chunk_size = 1000  # TODO: Tune me
            while True:
                query = table.select().where(where)

                query = query.limit(chunk_size).offset(offset)

                result = connection.execute(query)
                batch_rows = result.fetchall()

                if not batch_rows:
                    break

                for row in batch_rows:
                    row_dict = row._asdict()

                    if (
                        row_hash := make_row_hash(table, row_dict)
                    ) not in self.select_cache:
                        self.select_cache.add(row_hash)
                        yield row_dict

                offset += chunk_size
