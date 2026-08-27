# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""QueueFS-backed external compile task queue.

This uses the same persistent task record location as TaskTracker. QueueFS owns
delivery; the task record owns user-visible state.
"""

from __future__ import annotations

import json
import math
import time
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Callable, Dict, List, Optional
from uuid import uuid4

from openviking.core.identifiers import validate_account_id, validate_user_id
from openviking.pyagfs import AsyncAGFSClient
from openviking.pyagfs.exceptions import AGFSNotFoundError
from openviking.service.task_store import PersistentTaskStore, task_record_path
from openviking.service.task_tracker import TaskRecord, TaskStatus
from openviking.storage.queuefs.named_queue import NamedQueue
from openviking_cli.exceptions import (
    ConflictError,
    FailedPreconditionError,
    InvalidArgumentError,
    NotFoundError,
)

DEFAULT_LEASE_SECONDS = 600.0
QUEUE_MOUNT_POINT = "/queue"
OPEN_COMPILE_QUEUE = "OpenCompileTask"
OPEN_TASK_META_KEY = "__open_task_queue"
OPEN_TASK_AUTH_KEY = "open_task_queue"
MAX_CLAIM_DEQUEUE_ATTEMPTS = 100

_TERMINAL_STATUSES = {TaskStatus.COMPLETED, TaskStatus.FAILED}


@dataclass(frozen=True)
class OpenTaskRecord:
    """HTTP-facing view over a standard persistent task record."""

    task: TaskRecord

    @property
    def task_id(self) -> str:
        return self.task.task_id

    @property
    def account_id(self) -> str:
        return self.task.account_id or ""

    @property
    def user_id(self) -> str:
        return self.task.user_id or ""

    @property
    def payload(self) -> Dict[str, Any]:
        payload = self.task.meta.get("payload")
        return deepcopy(payload) if isinstance(payload, dict) else {}

    @property
    def attempt(self) -> int:
        return int(self.task.meta.get("attempt") or 0)

    @property
    def claimed_by_user_id(self) -> Optional[str]:
        value = self.task.meta.get("claimed_by_user_id")
        return str(value) if value else None

    @property
    def lease_id(self) -> Optional[str]:
        value = _task_auth(self.task).get("lease_id")
        return str(value) if value else None

    def to_dict(
        self,
        *,
        include_lease_id: bool = False,
        include_queue_delivery: bool = False,
    ) -> Dict[str, Any]:
        meta = self.task.meta
        auth = _task_auth(self.task)
        data: Dict[str, Any] = {
            "task_id": self.task.task_id,
            "task_type": self.task.task_type,
            "status": self.task.status.value,
            "stage": self.task.stage,
            "progress": meta.get("progress"),
            "message": meta.get("message"),
            "details": deepcopy(meta.get("details") or {}),
            "attempt": self.attempt,
            "lease_expires_at": meta.get("lease_expires_at"),
            "claimed_by_user_id": meta.get("claimed_by_user_id"),
            "account_id": self.account_id,
            "user_id": self.user_id,
            "created_by_user_id": self.user_id,
            "payload": self.payload,
            "result": deepcopy(self.task.result),
            "error": deepcopy(meta.get("error")),
            "acknowledged_at": meta.get("acknowledged_at"),
            "ack_by": meta.get("ack_by"),
            "created_at": self.task.created_at,
            "updated_at": self.task.updated_at,
            "created_at_iso": _iso_timestamp(self.task.created_at),
            "updated_at_iso": _iso_timestamp(self.task.updated_at),
        }
        if data["lease_expires_at"] is not None:
            data["lease_expires_at_iso"] = _iso_timestamp(float(data["lease_expires_at"]))
        if data["acknowledged_at"] is not None:
            data["acknowledged_at_iso"] = _iso_timestamp(float(data["acknowledged_at"]))
        if include_lease_id:
            data["lease_id"] = auth.get("lease_id")
        if include_queue_delivery:
            data["queue_name"] = auth.get("queue_name")
            data["queue_message_id"] = auth.get("queue_message_id")
        return data


@dataclass(frozen=True)
class _ClaimMutationResult:
    record: Optional[OpenTaskRecord]
    ack_queue_message: bool = False


class OpenTaskQueueService:
    """Open compile queue API backed by QueueFS and standard task records."""

    def __init__(
        self,
        agfs: Any,
        *,
        lease_seconds: float = DEFAULT_LEASE_SECONDS,
        queue_factory: Optional[Callable[[str], Any]] = None,
    ) -> None:
        if isinstance(agfs, AsyncAGFSClient):
            self._agfs = agfs
            self._sync_agfs = agfs._client
        else:
            self._sync_agfs = agfs
            self._agfs = AsyncAGFSClient(agfs)
        self._store = PersistentTaskStore(self._agfs)
        self._lease_seconds = float(lease_seconds)
        self._queue_factory = queue_factory
        self._queues: Dict[str, Any] = {}

    async def create_compile_task(
        self,
        *,
        account_id: str,
        user_id: str,
        payload: Dict[str, Any],
    ) -> OpenTaskRecord:
        """Create a standard task record and enqueue compile work."""
        _validate_owner(account_id, user_id)
        task = TaskRecord(
            task_id=str(uuid4()),
            task_type="compile",
            status=TaskStatus.PENDING,
            account_id=account_id,
            user_id=user_id,
            meta=_initial_meta(payload),
            stage="queued",
            auth=_initial_auth(),
        )
        await self._store.create(task)
        record = OpenTaskRecord(task)
        try:
            await self._compile_queue().enqueue(_queue_delivery_payload(record))
        except Exception as exc:
            await self._mark_enqueue_failed(record, exc)
            raise
        return record

    async def get_task(
        self,
        *,
        account_id: str,
        user_id: str,
        task_id: str,
    ) -> OpenTaskRecord:
        _validate_owner(account_id, user_id)
        _validate_task_id(task_id)
        task = await self._read_task(account_id, user_id, task_id)
        if task is None or not _is_open_compile_task(task):
            raise NotFoundError(task_id, "task")
        return OpenTaskRecord(task)

    async def list_tasks(
        self,
        *,
        account_id: str,
        user_id: str,
        status: Optional[str] = None,
        limit: int = 50,
    ) -> List[OpenTaskRecord]:
        _validate_owner(account_id, user_id)
        status_filter = _parse_status_filter(status)
        records = [
            OpenTaskRecord(task)
            for task in await self._read_all_tasks(account_id, user_id)
            if _is_open_compile_task(task)
            and (status_filter is None or task.status == status_filter)
        ]
        records.sort(key=lambda record: record.task.created_at, reverse=True)
        return records[:limit]

    async def claim_compile_task(
        self,
        *,
        account_id: str,
        user_id: str,
    ) -> Optional[OpenTaskRecord]:
        """Dequeue shared QueueFS work and attach a lease to the owner task."""
        _validate_owner(account_id, user_id)
        queue = self._compile_queue()
        for _ in range(MAX_CLAIM_DEQUEUE_ATTEMPTS):
            message = await queue.dequeue_raw()
            if message is None:
                return None
            parsed = _parse_queue_message(message)
            if parsed is None:
                msg_id = _queue_message_id(message)
                if msg_id:
                    await queue.ack(msg_id, message)
                continue
            msg_id, payload = parsed
            if payload.get("task_type") != "compile":
                await queue.ack(msg_id, message)
                continue

            owner = _queue_owner(payload)
            if owner is None:
                await queue.ack(msg_id, message)
                continue
            owner_account_id, owner_user_id = owner
            result = await self._claim_dequeued_task(
                owner_account_id=owner_account_id,
                owner_user_id=owner_user_id,
                claimed_by_user_id=user_id,
                task_id=str(payload.get("task_id", "")),
                queue_message_id=msg_id,
            )
            if result.record is not None:
                return result.record
            if result.ack_queue_message:
                await queue.ack(msg_id, message)
        return None

    async def update_task(
        self,
        *,
        account_id: str,
        user_id: str,
        task_id: str,
        lease_id: str,
        updates: Dict[str, Any],
    ) -> OpenTaskRecord:
        """Update running task progress and renew its lease."""
        if not updates:
            raise InvalidArgumentError("At least one task update field is required")
        return await self._mutate_with_lease(
            account_id=account_id,
            user_id=user_id,
            task_id=task_id,
            lease_id=lease_id,
            mutator=lambda task, now: self._apply_update(task, updates, now),
        )

    async def complete_task(
        self,
        *,
        account_id: str,
        user_id: str,
        task_id: str,
        lease_id: str,
        result: Dict[str, Any],
    ) -> OpenTaskRecord:
        """Mark a running task completed while keeping QueueFS delivery unacked."""
        return await self._mutate_with_lease(
            account_id=account_id,
            user_id=user_id,
            task_id=task_id,
            lease_id=lease_id,
            mutator=lambda task, now: self._finish_task(
                task,
                now,
                status=TaskStatus.COMPLETED,
                result=result,
                error=None,
            ),
        )

    async def fail_task(
        self,
        *,
        account_id: str,
        user_id: str,
        task_id: str,
        lease_id: str,
        error: Dict[str, Any],
    ) -> OpenTaskRecord:
        """Mark a running task failed while keeping QueueFS delivery unacked."""
        return await self._mutate_with_lease(
            account_id=account_id,
            user_id=user_id,
            task_id=task_id,
            lease_id=lease_id,
            mutator=lambda task, now: self._finish_task(
                task,
                now,
                status=TaskStatus.FAILED,
                result=None,
                error=error,
            ),
        )

    async def ack_task(
        self,
        *,
        account_id: str,
        user_id: str,
        task_id: str,
        lease_id: str,
    ) -> OpenTaskRecord:
        """Acknowledge a terminal QueueFS delivery without deleting the task record."""
        delivery: Dict[str, str] = {}

        def ack(task: TaskRecord, now: float) -> TaskRecord:
            if task.status not in _TERMINAL_STATUSES:
                raise FailedPreconditionError("Only completed or failed tasks can be acked")
            auth = _task_auth(task)
            queue_message_id = auth.get("queue_message_id")
            if not queue_message_id:
                raise FailedPreconditionError("Task has no QueueFS delivery to ack")
            delivery["queue_message_id"] = str(queue_message_id)
            updated = _copy_task(task)
            if updated.meta.get("acknowledged_at") is None:
                updated.meta["acknowledged_at"] = now
                updated.meta["ack_by"] = user_id
                updated.updated_at = _next_updated_at(task, now=now)
            return updated

        record = await self._mutate_with_lease(
            account_id=account_id,
            user_id=user_id,
            task_id=task_id,
            lease_id=lease_id,
            mutator=ack,
            require_running=False,
        )
        await self._compile_queue().ack(
            delivery["queue_message_id"],
            _queue_delivery_payload(record),
        )
        return record

    async def _claim_dequeued_task(
        self,
        *,
        owner_account_id: str,
        owner_user_id: str,
        claimed_by_user_id: str,
        task_id: str,
        queue_message_id: str,
    ) -> _ClaimMutationResult:
        try:
            _validate_owner(owner_account_id, owner_user_id)
            _validate_task_id(task_id)
        except InvalidArgumentError:
            return _ClaimMutationResult(record=None, ack_queue_message=True)

        now = time.time()
        async with self._locked_task(owner_account_id, owner_user_id, task_id):
            task = await self._read_task(owner_account_id, owner_user_id, task_id)
            if task is None or not _is_open_compile_task(task):
                return _ClaimMutationResult(record=None, ack_queue_message=True)
            if task.meta.get("acknowledged_at") is not None:
                return _ClaimMutationResult(record=None, ack_queue_message=True)
            if task.status == TaskStatus.RUNNING and not _lease_expired(task, now):
                return _ClaimMutationResult(record=None)

            updated = _copy_task(task)
            updated.auth[OPEN_TASK_AUTH_KEY] = {
                "lease_id": f"lease_{uuid4().hex}",
                "queue_name": OPEN_COMPILE_QUEUE,
                "queue_message_id": queue_message_id,
            }
            updated.meta["lease_expires_at"] = now + self._lease_seconds
            updated.meta["claimed_by_user_id"] = claimed_by_user_id
            updated.updated_at = _next_updated_at(task, now=now)
            if updated.status not in _TERMINAL_STATUSES:
                updated.status = TaskStatus.RUNNING
                updated.stage = "running"
                updated.meta["attempt"] = int(updated.meta.get("attempt") or 0) + 1
            await self._store.update(updated)
            return _ClaimMutationResult(record=OpenTaskRecord(updated))

    async def _mutate_with_lease(
        self,
        *,
        account_id: str,
        user_id: str,
        task_id: str,
        lease_id: str,
        mutator: Any,
        require_running: bool = True,
    ) -> OpenTaskRecord:
        _validate_owner(account_id, user_id)
        _validate_task_id(task_id)
        if not lease_id:
            raise InvalidArgumentError("lease_id is required")
        now = time.time()
        async with self._locked_task(account_id, user_id, task_id):
            task = await self._read_task(account_id, user_id, task_id)
            if task is None or not _is_open_compile_task(task):
                raise NotFoundError(task_id, "task")
            self._check_lease(task, lease_id, now)
            if require_running and task.status != TaskStatus.RUNNING:
                raise FailedPreconditionError("Task is not running")
            updated = mutator(task, now)
            await self._store.update(updated)
            return OpenTaskRecord(updated)

    def _apply_update(
        self,
        task: TaskRecord,
        updates: Dict[str, Any],
        now: float,
    ) -> TaskRecord:
        if task.status != TaskStatus.RUNNING:
            raise FailedPreconditionError("Task is not running")
        updated = _copy_task(task)
        for field_name in ("message", "progress", "details"):
            if field_name in updates:
                updated.meta[field_name] = updates[field_name]
        if "stage" in updates:
            updated.stage = updates["stage"]
        updated.meta["lease_expires_at"] = now + self._lease_seconds
        updated.updated_at = _next_updated_at(task, now=now)
        return updated

    def _finish_task(
        self,
        task: TaskRecord,
        now: float,
        *,
        status: TaskStatus,
        result: Optional[Dict[str, Any]],
        error: Optional[Dict[str, Any]],
    ) -> TaskRecord:
        if task.status != TaskStatus.RUNNING:
            raise FailedPreconditionError("Task is not running")
        updated = _copy_task(task)
        updated.status = status
        updated.stage = status.value
        updated.meta["progress"] = 1.0 if status == TaskStatus.COMPLETED else task.meta.get(
            "progress"
        )
        updated.meta["lease_expires_at"] = now + self._lease_seconds
        updated.result = deepcopy(result)
        updated.meta["error"] = deepcopy(error)
        updated.error = error.get("message") if error else None
        updated.updated_at = _next_updated_at(task, now=now)
        return updated

    @staticmethod
    def _check_lease(task: TaskRecord, lease_id: str, now: float) -> None:
        auth = _task_auth(task)
        if auth.get("lease_id") != lease_id:
            raise ConflictError("Task lease does not match", resource=task.task_id)
        if _lease_expired(task, now):
            raise ConflictError("Task lease has expired", resource=task.task_id)

    async def _mark_enqueue_failed(self, record: OpenTaskRecord, error: BaseException) -> None:
        try:
            async with self._locked_task(record.account_id, record.user_id, record.task_id):
                task = await self._read_task(record.account_id, record.user_id, record.task_id)
                if task is None:
                    return
                updated = _copy_task(task)
                updated.status = TaskStatus.FAILED
                updated.stage = TaskStatus.FAILED.value
                updated.error = str(error)
                updated.meta["error"] = {
                    "code": "QUEUE_ENQUEUE_FAILED",
                    "message": str(error),
                }
                updated.updated_at = _next_updated_at(task)
                await self._store.update(updated)
        except Exception:
            return

    async def _read_task(
        self,
        account_id: str,
        user_id: str,
        task_id: str,
    ) -> Optional[TaskRecord]:
        payload = await self._store.get(task_id, account_id=account_id, user_id=user_id)
        if payload is None:
            return None
        return _task_from_payload(payload)

    async def _read_all_tasks(self, account_id: str, user_id: str) -> List[TaskRecord]:
        return [
            _task_from_payload(payload)
            for payload in await self._store.list(account_id, user_id=user_id)
        ]

    @asynccontextmanager
    async def _locked_task(
        self,
        account_id: str,
        user_id: str,
        task_id: str,
    ) -> AsyncIterator[None]:
        path = task_record_path(account_id, user_id, task_id)
        try:
            lease = await self._agfs.pathlock_acquire_exact(path, timeout_secs=10.0)
        except AGFSNotFoundError:
            yield
            return
        try:
            yield
        finally:
            await self._agfs.pathlock_release(lease)

    def _compile_queue(self) -> Any:
        if OPEN_COMPILE_QUEUE not in self._queues:
            if self._queue_factory is not None:
                self._queues[OPEN_COMPILE_QUEUE] = self._queue_factory(OPEN_COMPILE_QUEUE)
            else:
                self._queues[OPEN_COMPILE_QUEUE] = NamedQueue(
                    self._sync_agfs,
                    QUEUE_MOUNT_POINT,
                    OPEN_COMPILE_QUEUE,
                )
        return self._queues[OPEN_COMPILE_QUEUE]


def _initial_meta(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        OPEN_TASK_META_KEY: {"queue": OPEN_COMPILE_QUEUE, "task_type": "compile"},
        "payload": deepcopy(payload),
        "progress": None,
        "message": None,
        "details": {},
        "attempt": 0,
        "lease_expires_at": None,
        "claimed_by_user_id": None,
        "acknowledged_at": None,
        "ack_by": None,
        "error": None,
    }


def _initial_auth() -> Dict[str, Any]:
    return {
        OPEN_TASK_AUTH_KEY: {
            "lease_id": None,
            "queue_name": OPEN_COMPILE_QUEUE,
            "queue_message_id": None,
        }
    }


def _task_auth(task: TaskRecord) -> Dict[str, Any]:
    auth = task.auth.get(OPEN_TASK_AUTH_KEY)
    return auth if isinstance(auth, dict) else {}


def _is_open_compile_task(task: TaskRecord) -> bool:
    marker = task.meta.get(OPEN_TASK_META_KEY)
    return (
        task.task_type == "compile"
        and isinstance(marker, dict)
        and marker.get("queue") == OPEN_COMPILE_QUEUE
    )


def _queue_delivery_payload(record: OpenTaskRecord) -> Dict[str, Any]:
    return {
        "task_id": record.task_id,
        "task_type": record.task.task_type,
        "account_id": record.account_id,
        "user_id": record.user_id,
        "created_at": record.task.created_at,
    }


def _parse_queue_message(message: Any) -> Optional[tuple[str, Dict[str, Any]]]:
    msg_id = _queue_message_id(message)
    if not msg_id:
        return None
    payload = _queue_payload(message)
    return (msg_id, payload) if isinstance(payload, dict) else None


def _queue_message_id(message: Any) -> str:
    if not isinstance(message, dict):
        return ""
    msg_id = message.get("id")
    return str(msg_id) if msg_id else ""


def _queue_payload(message: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(message, dict):
        return None
    payload: Any = message.get("data", message)
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8")
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except (TypeError, ValueError):
            return None
    return payload if isinstance(payload, dict) else None


def _queue_owner(payload: Dict[str, Any]) -> Optional[tuple[str, str]]:
    account_id = payload.get("account_id")
    user_id = payload.get("user_id")
    if not isinstance(account_id, str) or not isinstance(user_id, str):
        return None
    try:
        _validate_owner(account_id, user_id)
    except InvalidArgumentError:
        return None
    return account_id, user_id


def _task_from_payload(payload: Dict[str, Any]) -> TaskRecord:
    data = dict(payload)
    data["status"] = TaskStatus(data["status"])
    return TaskRecord(**data)


def _copy_task(task: TaskRecord) -> TaskRecord:
    return deepcopy(task)


def _parse_status_filter(status: Optional[str]) -> Optional[TaskStatus]:
    if status is None:
        return None
    try:
        return TaskStatus(status)
    except ValueError as exc:
        raise InvalidArgumentError(
            "Invalid task status",
            details={
                "status": status,
                "allowed": [item.value for item in TaskStatus],
            },
        ) from exc


def _lease_expired(task: TaskRecord, now: float) -> bool:
    expires_at = task.meta.get("lease_expires_at")
    return expires_at is not None and float(expires_at) <= now


def _next_updated_at(task: TaskRecord, *, now: Optional[float] = None) -> float:
    current = time.time() if now is None else now
    return max(current, math.nextafter(task.updated_at, math.inf))


def _iso_timestamp(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def _validate_account(account_id: str) -> None:
    error = validate_account_id(account_id)
    if error:
        raise InvalidArgumentError(error)


def _validate_owner(account_id: str, user_id: str) -> None:
    _validate_account(account_id)
    error = validate_user_id(user_id)
    if error:
        raise InvalidArgumentError(error)


def _validate_task_id(task_id: str) -> None:
    if not isinstance(task_id, str) or not task_id:
        raise InvalidArgumentError("Invalid task id")
    if "/" in task_id or "\\" in task_id:
        raise InvalidArgumentError("Invalid task id")
