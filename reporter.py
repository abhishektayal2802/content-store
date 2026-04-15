"""CLI progress reporting and error collection."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

from .types import Stage


class ProgressReporter:
    """Centralizes all CLI output: progress bars and error summary."""

    def __init__(self) -> None:
        self._console = Console()
        self._progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("{task.completed}/{task.total}"),
            TimeElapsedColumn(),
            console=self._console,
        )
        self._tasks: dict[Stage, TaskID] = {}
        self._errors: list[tuple[Stage, str, str]] = []

    @contextmanager
    def live(self) -> Iterator[ProgressReporter]:
        """Context manager that starts/stops progress display and prints summary."""
        try:
            with self._progress:
                self._tasks["scrape"] = self._progress.add_task("Scraping", total=None)
                self._tasks["extract"] = self._progress.add_task("Extracting", total=None)
                self._tasks["persist"] = self._progress.add_task("Persisting", total=None)
                yield self
        finally:
            self._print_summary()

    def grow(self, stage: Stage, delta: int) -> None:
        """Increment a stage's total by delta."""
        task_id = self._tasks[stage]
        current = self._progress.tasks[task_id].total or 0
        self._progress.update(task_id, total=current + delta)

    def advance(self, stage: Stage) -> None:
        """Mark one unit of work complete for a stage."""
        self._progress.advance(self._tasks[stage])

    def record_error(self, stage: Stage, context: str, exc: BaseException) -> None:
        """Record a non-fatal error for later summary."""
        msg = str(exc) or type(exc).__name__
        self._errors.append((stage, context, msg))

    def _print_summary(self) -> None:
        """Print error summary after progress bars close."""
        if not self._errors:
            return

        table = Table(title=f"{len(self._errors)} Errors", show_lines=True)
        table.add_column("Stage", style="bold")
        table.add_column("Context")
        table.add_column("Error", style="red")

        for stage, context, msg in self._errors[:20]:
            table.add_row(stage, context, msg[:80])

        self._console.print()
        self._console.print(table)

        if len(self._errors) > 20:
            self._console.print(f"... and {len(self._errors) - 20} more errors")
