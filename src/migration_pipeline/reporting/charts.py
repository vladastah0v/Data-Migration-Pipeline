from collections import defaultdict
from io import BytesIO
from pathlib import Path
from statistics import median

from pydantic import BaseModel, Field, ValidationError

from migration_pipeline.loaders.base import LoadStrategy
from migration_pipeline.reporting.service import NOT_MEASURED

_STRATEGIES = {
    LoadStrategy.INSERT: ("Одиночный INSERT", "#526482"),
    LoadStrategy.BATCH_INSERT: ("Пакетный INSERT", "#167B80"),
    LoadStrategy.UPSERT: ("UPSERT", "#7A5898"),
    LoadStrategy.COPY: ("COPY", "#1265B7"),
}


class BenchmarkChartError(RuntimeError):
    """Невозможно построить графики эксперимента."""


class _Trial(BaseModel):
    execution: int
    phase: str
    strategy: LoadStrategy
    verified_status: str
    records_per_second: float = Field(ge=0, allow_inf_nan=False)
    peak_rss_bytes: int = Field(gt=0)


class _Batch(BaseModel):
    execution: int
    records_extracted: int = Field(gt=0)
    batch_duration_seconds: float = Field(gt=0, allow_inf_nan=False)


class _Experiment(BaseModel):
    profile: str
    source_total: int
    runs: list[_Trial]
    batches: list[_Batch]


def make_benchmark_chart(results: Path, output: Path) -> None:
    try:
        experiment = _Experiment.model_validate_json(results.read_bytes())
    except (OSError, ValidationError) as exc:
        raise BenchmarkChartError("Не удалось прочитать замеры из results.json.") from exc

    successful: dict[LoadStrategy, list[_Trial]] = defaultdict(list)
    batches_by_trial: dict[int, list[_Batch]] = defaultdict(list)
    for trial in sorted(experiment.runs, key=lambda run: run.execution):
        if trial.phase == "measured" and trial.verified_status == "SUCCESS":
            successful[trial.strategy].append(trial)
    for batch in experiment.batches:
        batches_by_trial[batch.execution].append(batch)
    if not successful or any(
        trial.execution not in batches_by_trial
        for trials in successful.values()
        for trial in trials
    ):
        raise BenchmarkChartError(f"{NOT_MEASURED} — нет подтверждённых замеров партий и прогонов.")

    data = _draw(experiment, successful, batches_by_trial)
    try:
        with output.open("xb") as stream:
            try:
                stream.write(data)
            except BaseException:
                output.unlink(missing_ok=True)
                raise
    except FileExistsError as exc:
        raise BenchmarkChartError("Файл графиков уже существует. Выберите новое имя.") from exc
    except OSError as exc:
        raise BenchmarkChartError("Не удалось записать файл графиков.") from exc


def _draw(
    experiment: _Experiment,
    measured: dict[LoadStrategy, list[_Trial]],
    batches: dict[int, list[_Batch]],
) -> bytes:
    try:
        from matplotlib.backends.backend_agg import FigureCanvasAgg
        from matplotlib.figure import Figure
    except ModuleNotFoundError as exc:
        raise BenchmarkChartError(
            "Для построения графиков запустите uv run --extra charts."
        ) from exc

    strategies = [strategy for strategy in _STRATEGIES if strategy in measured]
    labels = [_STRATEGIES[strategy][0] for strategy in strategies]
    colors = [_STRATEGIES[strategy][1] for strategy in strategies]
    figure = Figure(figsize=(13, 8), layout="constrained", facecolor="#F3F6FA")
    FigureCanvasAgg(figure)
    number = f"{experiment.source_total:,}".replace(",", " ")
    figure.suptitle(
        f"Бенчмарк {experiment.profile.capitalize()} · {number} исходных записей",
        fontsize=17,
        fontweight="bold",
        color="#142A42",
    )
    throughput, durations, memory, temporal = figure.subplots(2, 2).flat
    for axis in (throughput, durations, memory, temporal):
        axis.set_axisbelow(True)
        axis.grid(axis="y", color="#DCE5EE", linewidth=0.8)
        axis.spines[["top", "right", "left"]].set_visible(False)
        axis.spines["bottom"].set_color("#B9C8D6")
        axis.tick_params(length=0, pad=7, labelsize=9)

    speeds = [median(trial.records_per_second for trial in measured[s]) for s in strategies]
    _bars(throughput, labels, speeds, colors, "Медиана пропускной способности", "записей/с", "%.0f")
    peaks = [max(trial.peak_rss_bytes for trial in measured[s]) / 2**20 for s in strategies]
    _bars(memory, labels, peaks, colors, "Пиковый RSS процесса CLI", "МиБ", "%.1f")

    durations.set(title="Распределение времени обработки партий", ylabel="секунды")
    durations.boxplot(
        [
            [
                batch.batch_duration_seconds
                for trial in measured[s]
                for batch in batches[trial.execution]
            ]
            for s in strategies
        ],
        tick_labels=labels,
        patch_artist=True,
        boxprops={"facecolor": "#B7D6EB", "edgecolor": "#526482"},
        medianprops={"color": "#18436D", "linewidth": 1.6},
        flierprops={"markersize": 3, "markeredgecolor": "#7B8DA4"},
    )
    durations.tick_params(axis="x", labelrotation=12)

    temporal.set(
        title="Скорость по времени партий · первый замер",
        xlabel="накопленное время партий, с",
        ylabel="записей/с",
    )
    for strategy, color in zip(strategies, colors, strict=True):
        elapsed = 0.0
        times, rates = [], []
        for batch in batches[measured[strategy][0].execution]:
            elapsed += batch.batch_duration_seconds
            times.append(elapsed)
            rates.append(batch.records_extracted / batch.batch_duration_seconds)
        temporal.plot(
            times,
            rates,
            color=color,
            label=_STRATEGIES[strategy][0],
            linewidth=1.5,
            marker=".",
            markersize=4,
        )
    temporal.legend(loc="best", fontsize=8, frameon=False)
    buffer = BytesIO()
    figure.savefig(buffer, dpi=180, format="png")
    return buffer.getvalue()


def _bars(axis, labels, values, colors, title, unit, number_format) -> None:
    bars = axis.barh(labels, values, color=colors, height=0.58)
    axis.invert_yaxis()
    axis.set(title=title, xlabel=unit, xlim=(0, max(values) * 1.23))
    axis.grid(axis="y", visible=False)
    axis.grid(axis="x", color="#DCE5EE", linewidth=0.8)
    axis.bar_label(bars, fmt=number_format, padding=5, fontsize=9, color="#142A42")
