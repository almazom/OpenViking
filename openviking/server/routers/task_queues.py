# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""External task queue endpoints for OpenViking HTTP Server."""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, ConfigDict, Field

from bot.vikingbot.compile.models import OKF_VERSION
from openviking.server.auth import get_request_context
from openviking.server.dependencies import get_service
from openviking.server.identity import RequestContext
from openviking.server.models import Response
from openviking.service.open_task_queue import OpenTaskQueueService
from openviking_cli.exceptions import FailedPreconditionError

router = APIRouter(prefix="/api/v1/task-queues/compile", tags=["task-queues"])


class CompileOpenTaskRequest(BaseModel):
    """Task payload accepted by the open compile queue."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    from_: list[str] = Field(alias="from", min_length=1)
    to: str = Field(min_length=1)
    skill: str = Field(min_length=1)
    reason: Optional[str] = None
    runtime_timeout_seconds: Optional[float] = Field(
        default=None,
        gt=0,
        allow_inf_nan=False,
    )


class TaskUpdateRequest(BaseModel):
    """Progress update from the worker that owns the active lease."""

    model_config = ConfigDict(extra="forbid")

    lease_id: str = Field(min_length=1)
    stage: Optional[str] = Field(default=None, min_length=1)
    progress: Optional[float] = Field(default=None, ge=0, le=1, allow_inf_nan=False)
    message: Optional[str] = None
    details: Optional[dict[str, Any]] = None


class CompileResultPayload(BaseModel):
    """Compile result payload reported by an external worker."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    from_: list[str] = Field(alias="from")
    to: str
    skill: str
    okf_version: str = OKF_VERSION
    created: list[str] = Field(default_factory=list)
    updated: list[str] = Field(default_factory=list)
    unchanged: list[str] = Field(default_factory=list)
    page_count: int = Field(default=0, ge=0)
    link_count: int = Field(default=0, ge=0)
    warnings: list[str] = Field(default_factory=list)


class CompleteTaskRequest(BaseModel):
    """Terminal success update from the worker that owns the active lease."""

    model_config = ConfigDict(extra="forbid")

    lease_id: str = Field(min_length=1)
    result: CompileResultPayload


class FailTaskError(BaseModel):
    """Terminal failure payload from an external worker."""

    model_config = ConfigDict(extra="forbid")

    code: str = Field(min_length=1)
    message: str = Field(min_length=1)


class FailTaskRequest(BaseModel):
    """Terminal failure update from the worker that owns the active lease."""

    model_config = ConfigDict(extra="forbid")

    lease_id: str = Field(min_length=1)
    error: FailTaskError


class AckTaskRequest(BaseModel):
    """Worker acknowledgement for a terminal task."""

    model_config = ConfigDict(extra="forbid")

    lease_id: str = Field(min_length=1)


def _open_task_queue_service() -> OpenTaskQueueService:
    service = get_service()
    viking_fs = getattr(service, "viking_fs", None)
    agfs = getattr(viking_fs, "agfs", None) if viking_fs is not None else None
    if agfs is None:
        raise FailedPreconditionError("Open task queue requires initialized storage")
    return OpenTaskQueueService(agfs)


@router.post("/tasks")
async def create_compile_task(
    request: CompileOpenTaskRequest,
    _ctx: RequestContext = Depends(get_request_context),
):
    """Create a compile task for the caller and enqueue it to the shared queue."""
    service = _open_task_queue_service()
    record = await service.create_compile_task(
        account_id=_ctx.account_id,
        user_id=_ctx.user.user_id,
        payload=request.model_dump(by_alias=True, exclude_none=True),
    )
    return Response(status="ok", result=record.to_dict())


@router.get("/tasks/{task_id}")
async def get_compile_task(
    task_id: str,
    _ctx: RequestContext = Depends(get_request_context),
):
    """Read one open compile task visible to the current account."""
    service = _open_task_queue_service()
    record = await service.get_task(
        account_id=_ctx.account_id,
        user_id=_ctx.user.user_id,
        task_id=task_id,
    )
    return Response(status="ok", result=record.to_dict())


@router.get("/tasks")
async def list_compile_tasks(
    status: Optional[str] = Query(None, description="pending/running/completed/failed"),
    limit: int = Query(50, ge=1, le=200, description="Max results"),
    _ctx: RequestContext = Depends(get_request_context),
):
    """List open compile tasks visible to the current account."""
    service = _open_task_queue_service()
    records = await service.list_tasks(
        account_id=_ctx.account_id,
        user_id=_ctx.user.user_id,
        status=status,
        limit=limit,
    )
    return Response(status="ok", result=[record.to_dict() for record in records])


@router.post("/claim")
async def claim_compile_task(
    _ctx: RequestContext = Depends(get_request_context),
):
    """Claim the oldest available open compile task from the shared QueueFS queue."""
    service = _open_task_queue_service()
    record = await service.claim_compile_task(
        account_id=_ctx.account_id,
        user_id=_ctx.user.user_id,
    )
    return Response(
        status="ok",
        result=None if record is None else record.to_dict(include_lease_id=True),
    )


@router.patch("/tasks/{task_id}")
async def update_compile_task(
    task_id: str,
    request: TaskUpdateRequest,
    _ctx: RequestContext = Depends(get_request_context),
):
    """Update progress for the task lease owner and renew the lease."""
    service = _open_task_queue_service()
    updates = request.model_dump(
        exclude={"lease_id"},
        exclude_unset=True,
        exclude_none=True,
    )
    record = await service.update_task(
        account_id=_ctx.account_id,
        user_id=_ctx.user.user_id,
        task_id=task_id,
        lease_id=request.lease_id,
        updates=updates,
    )
    return Response(status="ok", result=record.to_dict(include_lease_id=True))


@router.post("/tasks/{task_id}/complete")
async def complete_compile_task(
    task_id: str,
    request: CompleteTaskRequest,
    _ctx: RequestContext = Depends(get_request_context),
):
    """Mark a leased open compile task completed."""
    service = _open_task_queue_service()
    record = await service.complete_task(
        account_id=_ctx.account_id,
        user_id=_ctx.user.user_id,
        task_id=task_id,
        lease_id=request.lease_id,
        result=request.result.model_dump(by_alias=True),
    )
    return Response(status="ok", result=record.to_dict(include_lease_id=True))


@router.post("/tasks/{task_id}/fail")
async def fail_compile_task(
    task_id: str,
    request: FailTaskRequest,
    _ctx: RequestContext = Depends(get_request_context),
):
    """Mark a leased open compile task failed."""
    service = _open_task_queue_service()
    record = await service.fail_task(
        account_id=_ctx.account_id,
        user_id=_ctx.user.user_id,
        task_id=task_id,
        lease_id=request.lease_id,
        error=request.error.model_dump(),
    )
    return Response(status="ok", result=record.to_dict(include_lease_id=True))


@router.post("/tasks/{task_id}/ack")
async def ack_compile_task(
    task_id: str,
    request: AckTaskRequest,
    _ctx: RequestContext = Depends(get_request_context),
):
    """Acknowledge a terminal open compile task without deleting its record."""
    service = _open_task_queue_service()
    record = await service.ack_task(
        account_id=_ctx.account_id,
        user_id=_ctx.user.user_id,
        task_id=task_id,
        lease_id=request.lease_id,
    )
    return Response(status="ok", result=record.to_dict(include_lease_id=True))
