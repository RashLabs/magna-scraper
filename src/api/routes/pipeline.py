"""Pipeline control endpoints."""

from fastapi import APIRouter, HTTPException

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from models import PipelineStartRequest, PipelineStatusResponse, PipelineLogResponse
from api.deps import jobs, STAGES, start_job, stop_job

router = APIRouter(prefix="/pipeline", tags=["pipeline"])


def _get_stage_status(stage: str) -> dict:
    job = jobs[stage]
    result = {
        "status": job.status,
        "progress": job.progress,
        "processed": job.processed,
        "total": job.total,
        "error": job.error,
        "started_at": job.started_at,
        "finished_at": job.finished_at,
    }
    # For run_all: include per-stage detail and log file path
    if stage == "run_all":
        if job.stages_detail is not None:
            result["stages_detail"] = [
                {
                    "name": d.name,
                    "status": d.status,
                    "processed": d.processed,
                    "total": d.total,
                    "started_at": d.started_at,
                    "finished_at": d.finished_at,
                    "duration_s": d.duration_s,
                    "errors": d.errors,
                }
                for d in job.stages_detail
            ]
        if job.log_file:
            result["log_file"] = job.log_file
    return result


@router.get("/status")
def all_status():
    """Status for all pipeline stages."""
    return {stage: _get_stage_status(stage) for stage in STAGES}


# ── Run-all (must be before /{stage} to avoid path param capture) ──

@router.post("/run-all/start")
def start_run_all(req: PipelineStartRequest | None = None):
    """Run all stages sequentially: scrape -> parse -> download -> extract -> index."""
    if req is None:
        req = PipelineStartRequest()

    # Block if any individual stage is already running
    for s in ("scrape", "parse", "download", "extract", "index"):
        if jobs[s].status == "running":
            raise HTTPException(409, f"Stage '{s}' is already running")

    from pipeline.orchestrator import run
    from config import DATA_DIR

    company_list = req.company_list
    if company_list and not Path(company_list).is_absolute():
        resolved = DATA_DIR / company_list
        if resolved.exists():
            company_list = str(resolved)
        else:
            from config import PROJECT_ROOT
            resolved2 = PROJECT_ROOT / company_list
            if resolved2.exists():
                company_list = str(resolved2)

    kwargs = {
        "since": req.since,
        "headless": req.headless,
        "company_list": company_list,
        "company_ids": req.company_ids,
        "rescrape": req.rescrape,
        "reprocess": req.reprocess,
        "skip_html": req.skip_html,
    }

    if not start_job("run_all", run, kwargs):
        raise HTTPException(409, "Run-all is already running")

    return {"status": "started"}


@router.post("/run-all/stop")
def stop_run_all():
    """Cancel the run-all orchestrator."""
    if not stop_job("run_all"):
        raise HTTPException(404, "Run-all is not running")
    return {"status": "stopping"}


@router.get("/run-all/log")
def run_all_log():
    """Get log output from the run-all orchestrator."""
    job = jobs["run_all"]
    return PipelineLogResponse(lines=list(job.log_lines))


# ── Individual stages ──────────────────────────────────────────

@router.post("/{stage}/start")
def start_stage(stage: str, req: PipelineStartRequest | None = None):
    if stage not in STAGES:
        raise HTTPException(404, f"Unknown stage: {stage}")

    if req is None:
        req = PipelineStartRequest()

    # Import the appropriate pipeline module and build kwargs
    if stage == "scrape":
        from pipeline.scraper import run
        from config import DATA_DIR
        # Resolve company_list path: if relative, resolve against data dir
        company_list = req.company_list
        if company_list and not Path(company_list).is_absolute():
            resolved = DATA_DIR / company_list
            if resolved.exists():
                company_list = str(resolved)
            else:
                # Also try project root
                from config import PROJECT_ROOT
                resolved2 = PROJECT_ROOT / company_list
                if resolved2.exists():
                    company_list = str(resolved2)
        kwargs = {
            "since": req.since,
            "headless": req.headless,
            "company_list": company_list,
            "company_ids": req.company_ids,
            "rescrape": req.rescrape,
            "fetch_html": not req.skip_html,
        }
    elif stage == "parse":
        from pipeline.parser import run
        kwargs = {"reprocess": req.reprocess, "since": req.since}
    elif stage == "download":
        from pipeline.downloader import run
        kwargs = {"headless": req.headless, "reprocess": req.reprocess, "since": req.since}
    elif stage == "extract":
        from pipeline.extractor import run
        kwargs = {"reprocess": req.reprocess, "since": req.since}
    elif stage == "index":
        from pipeline.indexer import run
        kwargs = {"reprocess": req.reprocess, "since": req.since}
    else:
        raise HTTPException(404)

    if not start_job(stage, run, kwargs):
        raise HTTPException(409, f"Stage '{stage}' is already running")

    return {"status": "started"}


@router.post("/{stage}/stop")
def stop_stage(stage: str):
    if stage not in STAGES:
        raise HTTPException(404, f"Unknown stage: {stage}")
    if not stop_job(stage):
        raise HTTPException(404, f"Stage '{stage}' is not running")
    return {"status": "stopping"}


@router.get("/{stage}/log")
def stage_log(stage: str):
    if stage not in STAGES:
        raise HTTPException(404, f"Unknown stage: {stage}")
    job = jobs[stage]
    return PipelineLogResponse(lines=list(job.log_lines))
