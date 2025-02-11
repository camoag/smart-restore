from multiprocessing import JoinableQueue, Process

from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Session
from sqlalchemy.schema import Table

from .database import Database, load_metadata, make_engine, update_primary_key_sequences
from .progress import ProgressTracker


class DatabaseWriter(Database):
    def __init__(self, database_uri: str, verbose: bool):
        super().__init__(database_uri=database_uri, verbose=verbose)
        self.metadata = load_metadata(self.engine, verbose=verbose)
        self.progress = ProgressTracker()

    def upsert_rows(self, table_name: str, rows: list[dict]):
        self.progress.update(table_name=table_name, rows=rows)

    def join(self, verbose: bool):
        self.progress.finish()

        update_primary_key_sequences(
            engine=self.engine,
            tables=[
                self.metadata.tables[table] for table in self.progress.dirty_tables
            ],
            verbose=verbose,
        )

        self.progress.report()


class SyncDatabaseWriter(DatabaseWriter):
    def __init__(self, database_uri: str, verbose: bool):
        super().__init__(database_uri=database_uri, verbose=verbose)
        self.session = Session(self.engine)

    def upsert_rows(self, table_name: str, rows: list[dict]):
        super().upsert_rows(table_name, rows)
        if rows:
            # TODO: Only BEGIN once?
            with self.session.begin():
                upsert_rows(self.session, self.metadata.tables[table_name], rows)

    def join(self, verbose: bool):
        super().join(verbose)
        self.session.close()


class AsyncDatabaseWriter(DatabaseWriter):
    def __init__(self, database_uri: str, verbose: bool):
        super().__init__(database_uri=database_uri, verbose=verbose)

        # Start consumer process
        self.queue = JoinableQueue()
        consumer = Process(
            target=self._consumer,
            args=(database_uri, verbose, self.queue),
            daemon=True,
        )
        consumer.start()

    def upsert_rows(self, table_name: str, rows: list[dict]):
        super().upsert_rows(table_name, rows)
        if rows:
            self.queue.put((table_name, rows))

    def join(self, verbose: bool):
        super().join(verbose)
        self.queue.put(None)  # Send stop signal
        self.queue.join()

    @staticmethod
    def _consumer(target_database_uri: str, verbose: bool, queue: JoinableQueue):
        # Separate process for writing to target database
        engine = make_engine(target_database_uri, verbose=True)
        metadata = load_metadata(engine, verbose=verbose)

        with Session(engine) as session, session.begin():
            while True:
                item = queue.get()
                if item:
                    table_name, rows = item
                    upsert_rows(session, metadata.tables[table_name], rows)
                    queue.task_done()
                else:
                    # Stop signal
                    queue.task_done()
                    break


def upsert_rows(session, table: Table, values: list[dict]):
    insert_statement = postgresql.insert(table).values(values)

    if update_values := {
        c.name: c for c in insert_statement.excluded if not c.primary_key
    }:
        upsert_statement = insert_statement.on_conflict_do_update(
            constraint=table.primary_key,
            set_=update_values,
        )
    else:
        # No non-primary-key fields to update (all table fields are primary keys)
        upsert_statement = insert_statement.on_conflict_do_nothing()

    session.execute(upsert_statement)
