import csv
import json
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TextIO

from migration_pipeline.benchmarking.service import BATCH_FIELDS

BATCH_COLUMNS = ("execution", "phase", "repeat", "strategy", "run_id", *BATCH_FIELDS)


class BenchmarkReport:
    def __init__(self, root: Path, workspace: Path, lines: TextIO, csv_file: TextIO) -> None:
        self.root = root
        self.workspace = workspace
        self._lines = lines
        self._csv_file = csv_file
        self._batch_csv = csv.DictWriter(csv_file, fieldnames=BATCH_COLUMNS)
        self._batch_csv.writeheader()

    def append_batch(self, fields: dict[str, object]) -> None:
        self._batch_csv.writerow(fields)
        self._lines.write(json.dumps(fields, ensure_ascii=False, allow_nan=False) + "\n")

    def publish(self, report: dict[str, object]) -> Path:
        destination = self.root / str(report["benchmark_id"])
        if destination.exists():
            raise FileExistsError(f"Отчёт уже существует — {destination}.")
        self._csv_file.close()
        self._lines.seek(0)
        staging = self.workspace / "report"
        with (staging / "results.json").open("w", encoding="utf-8") as file:
            head = json.dumps(report, indent=2, ensure_ascii=False, allow_nan=False)
            file.write(head.removesuffix("\n}") + ',\n  "batches": [')
            separator = "\n"
            for line in self._lines:
                file.write(separator + "    " + line.rstrip("\n"))
                separator = ",\n"
            file.write("\n  ]\n}\n")
        _write_runs(staging / "runs.csv", report["runs"])
        staging.rename(destination)
        return destination


@contextmanager
def stage_benchmark_report(root: Path) -> Iterator[BenchmarkReport]:
    with TemporaryDirectory(prefix=".benchmark-", dir=root) as temp:
        workspace = Path(temp)
        staging = workspace / "report"
        staging.mkdir()
        with (
            (workspace / "batches.jsonl").open("w+", encoding="utf-8") as lines,
            (staging / "batches.csv").open("w", newline="", encoding="utf-8") as csv_file,
        ):
            yield BenchmarkReport(root, workspace, lines, csv_file)


def _write_runs(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        path.touch()
        return
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
