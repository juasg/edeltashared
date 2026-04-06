import asyncio
import uuid

from fastapi import APIRouter, BackgroundTasks, Depends, Query
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.connectors.hana.connection_pool import HANAConnectionPool
from app.core.auth import get_current_user
from app.core.exceptions import NotFoundError
from app.db.models import JobHistory
from app.db.session import get_session
from app.models.job import InitialLoadRequest, JobResponse
from app.services.initial_load_service import InitialLoadService

router = APIRouter(prefix="/jobs", tags=["jobs"], dependencies=[Depends(get_current_user)])


@router.post("/initial-load", response_model=JobResponse, status_code=201)
async def trigger_initial_load(
    data: InitialLoadRequest,
    background_tasks: BackgroundTasks,
    session: AsyncSession = Depends(get_session),
):
    """Trigger a windowed initial load for a replication config."""
    job = JobHistory(
        config_id=data.config_id,
        job_type="initial_load",
        status="queued",
        metrics={
            "chunk_size": data.chunk_size,
            "max_hana_cpu": data.max_hana_cpu,
        },
    )
    session.add(job)
    await session.commit()
    await session.refresh(job)

    # Dispatch background task
    pool = HANAConnectionPool()
    service = InitialLoadService(pool)
    background_tasks.add_task(
        _run_load, service, job.id, data.config_id, data.chunk_size, data.max_hana_cpu
    )

    return job


async def _run_load(
    service: InitialLoadService,
    job_id: uuid.UUID,
    config_id: uuid.UUID,
    chunk_size: int,
    max_hana_cpu: int,
):
    await service.run_initial_load(job_id, config_id, chunk_size, max_hana_cpu)


@router.get("/{job_id}", response_model=JobResponse)
async def get_job(
    job_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
):
    """Get job status and progress."""
    result = await session.execute(
        select(JobHistory).where(JobHistory.id == job_id)
    )
    job = result.scalar_one_or_none()
    if job is None:
        raise NotFoundError(f"Job {job_id} not found")
    return job


@router.post("/{job_id}/cancel")
async def cancel_job(
    job_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
):
    """Cancel a running or queued job."""
    result = await session.execute(
        select(JobHistory).where(JobHistory.id == job_id)
    )
    job = result.scalar_one_or_none()
    if job is None:
        raise NotFoundError(f"Job {job_id} not found")

    if job.status in ("completed", "failed", "cancelled"):
        return {"message": f"Job already {job.status}", "job_id": str(job_id)}

    await session.execute(
        update(JobHistory)
        .where(JobHistory.id == job_id)
        .values(status="cancelled")
    )
    await session.commit()

    return {"message": "Job cancelled", "job_id": str(job_id)}


@router.get("", response_model=list[JobResponse])
async def list_jobs(
    config_id: uuid.UUID | None = Query(None, description="Filter by replication config"),
    session: AsyncSession = Depends(get_session),
):
    """List job history, optionally filtered by config."""
    stmt = select(JobHistory).order_by(JobHistory.created_at.desc()).limit(100)
    if config_id:
        stmt = stmt.where(JobHistory.config_id == config_id)
    result = await session.execute(stmt)
    return list(result.scalars().all())
