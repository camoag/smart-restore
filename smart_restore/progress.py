from collections import defaultdict
from operator import itemgetter

from tqdm import tqdm


class ProgressTracker:
    """Tracks table/row counts and displays progress bars."""

    def __init__(self, table_bars: int = 20) -> None:
        self.table_stats = defaultdict(int)
        self.main = tqdm(
            total=0,
            unit="rows",
            bar_format="Copied {total_fmt} {unit} in {elapsed} ({rate_fmt}){postfix}",
        )

        self.table_bars = [
            tqdm(
                unit="rows",
                leave=False,  # Hide on completion
                bar_format=" ",  # Must be non-Falsy to be blank at startup
                position=(index + 1),
            )
            for index in range(table_bars)
        ]

    @property
    def sorted_table_stats(self) -> list[tuple[str, int]]:
        return sorted(self.table_stats.items(), key=itemgetter(1), reverse=True)

    @property
    def dirty_tables(self) -> set[str]:
        return set(self.table_stats.keys())

    def update(self, table_name: str, rows: list[dict]):
        self.table_stats[table_name] += len(rows)

        total = sum(self.table_stats.values())
        self.main.total += len(rows)
        self.main.set_postfix(table=table_name, rows=len(rows))
        self.main.update(len(rows))

        for bar, (table_name, count) in zip(
            self.table_bars,
            self.sorted_table_stats,
        ):
            bar.bar_format = "{bar} {n_fmt} " + table_name
            bar.reset(total)
            bar.update(count)
            bar.set_description(table_name)
            bar.disable = False

        # Disable unused progress bars
        for bar in self.table_bars[len(self.table_stats) :]:
            bar.disable = True

    def finish(self) -> None:
        # Cleanup so only the "main" bar remains
        self.main.set_postfix()
        self.main.close()
        for bar in self.table_bars:
            bar.close()
        # Seems like this skips to next line after the progress bar, for clean output
        tqdm.write("")

    def report(self) -> None:
        tqdm.write("Rows copied:")
        for table, count in self.sorted_table_stats:
            tqdm.write(f"\t{count} {table}")
