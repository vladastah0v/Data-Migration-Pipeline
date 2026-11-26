import sys


def process_peak_rss() -> int | None:
    """Вернуть пиковый RSS процесса CLI в байтах на macOS и Linux."""
    if sys.platform not in {"darwin", "linux"}:
        return None

    from resource import RUSAGE_SELF, getrusage

    maximum = getrusage(RUSAGE_SELF).ru_maxrss
    return maximum * (1024 if sys.platform == "linux" else 1) if maximum > 0 else None
