import json
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from typer.testing import CliRunner

from migration_pipeline import cli
from migration_pipeline.database.legacy import LegacySourceError
from migration_pipeline.reporting import service
from migration_pipeline.reporting.charts import BenchmarkChartError, make_benchmark_chart
from migration_pipeline.reporting.render import _cell


@pytest.mark.asyncio
async def test_source_count_prefers_saved_verification_and_matches_legacy_snapshot(monkeypatch):
    snapshot_id = uuid4()
    observed = []

    async def count_snapshot(_settings, expected):
        observed.append(expected)
        if len(observed) > 1:
            raise LegacySourceError("Другой снимок.")
        return 21

    monkeypatch.setattr(service, "count_snapshot", count_snapshot)
    run = SimpleNamespace(
        verification_report={"records": {"orders": {"source": 10}, "payments": {"source": 5}}},
        status="FAILED_VERIFICATION",
        records_processed=12,
        source_snapshot_id=snapshot_id,
    )
    assert await service._source_count(None, run) == (15, "VERIFICATION")
    run.verification_report = None
    run.status = "LOADED"
    assert await service._source_count(None, run) == (12, "COMPLETED_LOAD")
    run.status = "RUNNING"
    assert await service._source_count(None, run) == (21, "LEGACY_SNAPSHOT")
    assert await service._source_count(None, run) == (None, "UNAVAILABLE")
    assert observed == [snapshot_id, snapshot_id]


@pytest.mark.usefixtures("example_env")
def test_report_exports_json_and_refuses_to_replace_file(tmp_path, monkeypatch):
    run_id = uuid4()

    async def migration_report(_settings, expected):
        assert expected == run_id
        return {"run_id": str(run_id), "status": "SUCCESS", "peak_rss_bytes": 1024}

    monkeypatch.setattr(cli, "migration_report", migration_report)
    runner = CliRunner()
    command = ["report", "--run-id", str(run_id), "--format", "json"]
    result = runner.invoke(cli.app, command)
    assert json.loads(result.stdout)["peak_rss_bytes"] == 1024
    assert json.loads(result.stderr)["status"] == "SUCCESS"
    output = tmp_path / "report.json"
    output.write_text("existing")
    result = runner.invoke(cli.app, [*command, "--output", str(output)])
    assert result.exit_code == 1
    assert "уже существует" in result.stderr
    assert output.read_text() == "existing"


def test_markdown_cell_escapes_raw_html_and_table_delimiters():
    assert _cell("<b>Название</b>|текст\nстрока") == "&lt;b&gt;Название&lt;/b&gt;\\|текст строка"


def test_charts_mark_empty_experiment_as_not_measured(tmp_path):
    results = tmp_path / "results.json"
    results.write_text(
        json.dumps({"profile": "small", "source_total": 21, "runs": [], "batches": []})
    )
    path = tmp_path / "chart.png"
    with pytest.raises(BenchmarkChartError, match="NOT_MEASURED"):
        make_benchmark_chart(results, path)
    assert not path.exists()


def test_charts_render_from_recorded_small_experiment(tmp_path):
    pytest.importorskip("matplotlib")
    root = Path(__file__).resolve().parents[2]
    results = root / "benchmarks/6fde1748-c887-4ba2-8776-da5a6887043b/results.json"
    output = tmp_path / "chart.png"
    make_benchmark_chart(results, output)
    assert output.read_bytes().startswith(b"\x89PNG\r\n\x1a\n")
