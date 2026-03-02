"""Shared dependencies for API routes — DB instance and pipeline job state."""

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime
from collections import deque
from typing import Literal

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db_v2 import Database

log = logging.getLogger(__name__)

# Singleton DB — thread-safe with WAL mode
_db: Database | None = None


def get_db() -> Database:
    global _db
    if _db is None:
        _db = Database()
    return _db


# ── Pipeline Job State ──────────────────────────────────────────

STAGES = ("scrape", "parse", "download", "extract", "index", "run_all")

StageStatus = Literal["idle", "running", "done", "error"]

# Sub-stage detail statuses (used inside run_all's stages_detail)
StageDetailStatus = Literal["pending", "running", "done", "error"]


@dataclass
class StageDetail:
    """Tracks per-stage progress inside a run_all execution."""
    name: str
    status: StageDetailStatus = "pending"
    processed: int = 0
    total: int = 0
    started_at: str | None = None
    finished_at: str | None = None
    duration_s: float | None = None
    errors: list[str] = field(default_factory=list)


@dataclass
class JobState:
    status: StageStatus = "idle"
    progress: str = ""
    processed: int = 0
    total: int = 0
    error: str | None = None
    log_lines: deque = field(default_factory=lambda: deque(maxlen=200))
    started_at: str | None = None
    finished_at: str | None = None
    cancel_requested: bool = False
    thread: threading.Thread | None = field(default=None, repr=False)
    stages_detail: list[StageDetail] | None = None
    log_file: str | None = None

    def reset(self):
        self.status = "idle"
        self.progress = ""
        self.processed = 0
        self.total = 0
        self.error = None
        self.log_lines.clear()
        self.started_at = None
        self.finished_at = None
        self.cancel_requested = False
        self.thread = None
        self.stages_detail = None
        self.log_file = None


# One JobState per stage
jobs: dict[str, JobState] = {stage: JobState() for stage in STAGES}


class JobLogHandler(logging.Handler):
    """Captures log records into a JobState's log_lines deque."""

    def __init__(self, job: JobState):
        super().__init__()
        self.job = job

    def emit(self, record):
        try:
            msg = self.format(record)
            self.job.log_lines.append(msg)
        except Exception as e:
            import sys
            print(f"[JobLogHandler] emit failed: {e}", file=sys.stderr)


def start_job(stage: str, target, kwargs: dict | None = None) -> bool:
    """Start a pipeline job in a background thread. Returns False if already running."""
    job = jobs[stage]
    if job.status == "running":
        return False

    job.reset()
    job.status = "running"
    job.started_at = datetime.now().isoformat()

    # For run_all: initialize stages_detail
    if stage == "run_all":
        stage_names = ["scrape", "parse", "download", "extract", "index"]
        job.stages_detail = [StageDetail(name=n) for n in stage_names]

    # File logging — for all stages
    from config import TMP_DIR
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    log_path = TMP_DIR / f"magna-{stage}-{timestamp}.log"
    job.log_file = str(log_path)
    stream = open(str(log_path), "a", encoding="utf-8", buffering=1)  # line-buffered
    file_handler = logging.StreamHandler(stream)
    file_handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(message)s"))

    # Set up log capture — attach handler ONLY to the stage's pipeline logger,
    # not the root logger. This prevents log contamination between stages.
    handler = JobLogHandler(job)
    handler.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s %(message)s"))

    # Map stage names to their pipeline module logger names
    stage_logger_names = {
        "scrape": "pipeline.scraper",
        "parse": "pipeline.parser",
        "download": "pipeline.downloader",
        "extract": "pipeline.extractor",
        "index": "pipeline.indexer",
        "run_all": "pipeline.orchestrator",
    }

    def _run():
        # Attach handler to the specific pipeline module logger only
        loggers_to_attach = [logging.getLogger(stage_logger_names.get(stage, stage))]
        # run_all calls all sub-stage modules, so capture their logs too
        if stage == "run_all":
            for name in ("pipeline.scraper", "pipeline.parser", "pipeline.downloader",
                         "pipeline.extractor", "pipeline.indexer"):
                loggers_to_attach.append(logging.getLogger(name))
        for lgr in loggers_to_attach:
            lgr.addHandler(handler)
            if file_handler:
                lgr.addHandler(file_handler)
            lgr.setLevel(logging.INFO)

        import sys
        print(f"[DEBUG] Attached handler to loggers: {[l.name for l in loggers_to_attach]}", file=sys.stderr)
        print(f"[DEBUG] job.log_lines id={id(job.log_lines)}, len={len(job.log_lines)}", file=sys.stderr)
        job.log_lines.append(f"[DEBUG] Job {stage} starting...")

        try:
            target_kwargs = kwargs or {}
            target_kwargs["cancel_check"] = lambda: job.cancel_requested
            target_kwargs["progress_cb"] = lambda done, tot: _update_progress(job, done, tot)
            if stage == "run_all":
                target_kwargs["stages_detail"] = job.stages_detail
            target(**target_kwargs)
            if job.cancel_requested:
                job.status = "idle"
            else:
                job.status = "done"
        except Exception as e:
            job.error = str(e)
            job.status = "error"
            job.log_lines.append(f"[ERROR] Job {stage} failed: {e}")
            log.exception(f"Job {stage} failed")
        finally:
            job.finished_at = datetime.now().isoformat()
            for lgr in loggers_to_attach:
                lgr.removeHandler(handler)
                if file_handler:
                    lgr.removeHandler(file_handler)
            if file_handler:
                file_handler.close()
                if hasattr(file_handler, 'stream') and file_handler.stream:
                    file_handler.stream.close()

    t = threading.Thread(target=_run, daemon=True, name=f"pipeline-{stage}")
    job.thread = t
    t.start()
    return True


def _update_progress(job: JobState, done: int | float, total: int):
    job.processed = int(done)
    job.total = total
    if isinstance(done, float) and not done.is_integer():
        # Fractional progress: e.g. 2.7 = "stage 3, 70% through"
        job.progress = f"{done:.1f}/{total}"
    else:
        job.progress = f"{int(done)}/{total}"


def stop_job(stage: str) -> bool:
    """Request cancellation. Returns False if not running."""
    job = jobs[stage]
    if job.status != "running":
        return False
    job.cancel_requested = True
    return True


# ── Runtime Settings ──────────────────────────────────────────
# Thread-safe mutable settings dict. Resets to defaults on server restart.

_settings_lock = threading.Lock()
_settings: dict = {"extract_workers": 5}


def get_setting(key: str):
    with _settings_lock:
        return _settings[key]


def set_setting(key: str, value):
    with _settings_lock:
        _settings[key] = value


def get_all_settings() -> dict:
    with _settings_lock:
        return dict(_settings)
