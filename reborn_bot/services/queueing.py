from __future__ import annotations
import asyncio
from asyncio import QueueEmpty
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional

from ..logging_setup import logger


@dataclass(order=True)
class QueueItem:
    priority: int
    sequence: int
    session: Any = field(compare=False)
    runner: Callable[[Any], Awaitable[None]] = field(compare=False)


class PriorityGenerationQueue:
    def __init__(self, workers: int = 1):
        self.workers = max(1, workers)
        self._queue: asyncio.PriorityQueue[QueueItem] = asyncio.PriorityQueue()
        self._worker_tasks: list[asyncio.Task] = []
        self._counter = 0
        self._update_callback: Optional[Callable[[], Awaitable[None]]] = None

    def set_update_callback(self, callback: Callable[[], Awaitable[None]]) -> None:
        self._update_callback = callback

    async def start(self) -> None:
        if self._worker_tasks:
            return
        self._worker_tasks = [asyncio.create_task(self._worker_loop(index)) for index in range(self.workers)]

    async def stop(self) -> None:
        for task in self._worker_tasks:
            task.cancel()
        for task in self._worker_tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._worker_tasks.clear()

    async def enqueue(self, *, priority: int, session: Any, runner: Callable[[Any], Awaitable[None]]) -> None:
        self._counter += 1
        await self._queue.put(QueueItem(priority=priority, sequence=self._counter, session=session, runner=runner))
        logger.info("Enqueued request | priority=%s pending=%s", priority, self._queue.qsize())
        await self._notify()

    async def cancel_pending(self, session: Any) -> bool:
        removed = False
        pending: list[QueueItem] = []

        while True:
            try:
                item = self._queue.get_nowait()
            except QueueEmpty:
                break

            if item.session is session:
                removed = True
                self._queue.task_done()
                continue

            pending.append(item)
            self._queue.task_done()

        for item in pending:
            self._queue.put_nowait(item)

        if removed:
            logger.info("Removed pending request from queue | pending=%s", self._queue.qsize())
            await self._notify()
        return removed

    async def _worker_loop(self, worker_index: int) -> None:
        while True:
            item = await self._queue.get()
            logger.info("Dequeued request for worker %s | pending=%s", worker_index, self._queue.qsize())
            await self._notify()
            try:
                await item.runner(item.session)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("Queue worker %s failed: %s", worker_index, exc)
            finally:
                self._queue.task_done()
                await self._notify()

    def pending_sessions(self) -> list[Any]:
        return [item.session for item in sorted(self._queue._queue)]  # type: ignore[attr-defined]

    def size(self) -> int:
        return self._queue.qsize()

    async def _notify(self) -> None:
        if self._update_callback:
            try:
                await self._update_callback()
            except Exception as exc:
                logger.debug("Queue update callback failed: %s", exc)
