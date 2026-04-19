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


# Per-stage progress bar labels. A stage's bar appears only once that stage
# actually starts doing work -- hides the misleading 0% row while upstream
# stages are still running.
_STAGE_LABELS: dict[Stage, str] = {
    "scrape": "Scraping PDFs",
    "extract": "Extracting pages",
    "stage": "Staging to GCS",
    "import": "Importing (LRO)",
}


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
                yield self
        finally:
            self._print_summary()

    def grow(self, stage: Stage, delta: int) -> None:
        """Increment a stage's total by delta, creating its bar on first call."""
        # Lazy bar creation: a stage only shows up once it has work to do.
        task_id = self._tasks.get(stage) or self._add_task(stage)
        current = self._progress.tasks[task_id].total or 0
        self._progress.update(task_id, total=current + delta)

    def advance(self, stage: Stage) -> None:
        """Mark one unit of work complete for a stage."""
        self._progress.advance(self._tasks[stage])

    def record_error(self, stage: Stage, context: str, exc: BaseException) -> None:
        """Record a non-fatal error for later summary."""
        msg = str(exc) or type(exc).__name__
        self._errors.append((stage, context, msg))

    def _add_task(self, stage: Stage) -> TaskID:
        """Register a stage's progress bar with the rich Progress view."""
        task_id = self._progress.add_task(_STAGE_LABELS[stage], total=None)
        self._tasks[stage] = task_id
        return task_id

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
