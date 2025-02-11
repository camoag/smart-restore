import re
import shelve
import warnings

from alembic.migration import MigrationContext
from sqlalchemy import MetaData, create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SAWarning
from sqlalchemy.orm import Session
from sqlalchemy.schema import Table
from tqdm import tqdm

from .utility import flatten_list


def make_engine(database_uri: str, verbose: bool):
    warnings.filterwarnings("ignore", category=SAWarning)
    if not database_uri.startswith("postgresql://"):
        database_uri = "postgresql://camo@localhost:5432/" + database_uri
    return create_engine(
        database_uri,
        isolation_level="AUTOCOMMIT",
        connect_args={"connect_timeout": 10},
    )


def get_database_revision_heads(engine) -> str:
    with engine.connect() as connection:
        context = MigrationContext.configure(connection)
        # We just need a unique tag for the database state, and this supports multiple heads too.
        return "|".join(context.get_current_heads())


class Database:
    def __init__(self, database_uri: str, verbose: bool):
        self.engine = make_engine(database_uri, verbose=verbose)


def load_metadata(engine: Engine, verbose: bool = False) -> MetaData:
    schema_version = get_database_revision_heads(engine)
    if verbose:
        tqdm.write(f"Schema Version: {schema_version}")

    with shelve.open("meta") as cache:
        if schema_version in cache:
            if verbose:
                tqdm.write("Using cached metadata")
            return cache[schema_version]
        else:
            tqdm.write("Reflecting metadata")
            metadata = MetaData()
            metadata.reflect(bind=engine)

            cache[schema_version] = metadata

            return metadata


def update_primary_key_sequences(
    engine: Engine, tables: list[Table], verbose: bool
) -> None:
    """After all rows copied, need to update any auto-incrementing sequences tied to primary keys."""

    tqdm.write("Fixing primary key sequences...")

    primary_keys = flatten_list(
        table.primary_key.columns.values() for table in tables if table.primary_key
    )

    with Session(engine) as session:
        for primary_key in primary_keys:
            # Best way I could find to identify PostgreSQL auto-incrementing sequences tied to primary key columns
            if server_default := primary_key.server_default:
                if match := re.match(
                    "nextval\('([^']+)'::regclass\)$", server_default.arg.text
                ):
                    sequence = match.group(1)
                    if verbose:
                        tqdm.write(f"\t{sequence}")
                    session.execute(
                        text(
                            f"""
                            SELECT setval(
                                '{sequence}',
                                (SELECT MAX({primary_key.name})
                                 FROM {primary_key.table.name}
                                 )
                            );
                        """
                        )
                    )
