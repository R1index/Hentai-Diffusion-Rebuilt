from __future__ import annotations

import asyncio
import json
import os
import socket
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import discord

from ..config import AppSettings
from ..logging_setup import logger

_PREFIX = "REBORN_SYNC "
_VERSION = 1


@dataclass(frozen=True)
class SyncQueueEntry:
    session_id: str
    user_id: str
    priority: int
    is_img2vid: bool
    instance_id: str
    message_id: int
    created_at: float


@dataclass
class SyncState:
    global_workers: int
    open_queue: list[SyncQueueEntry]
    generation_committed_today: dict[str, int]
    generation_reserved_today: dict[str, int]
    img2vid_committed_today: dict[str, int]
    img2vid_reserved_today: dict[str, int]
    active_by_user: dict[str, int]

    def queue_position(self, session_id: str) -> Optional[int]:
        for index, entry in enumerate(self.open_queue, start=1):
            if entry.session_id == session_id:
                return index
        return None

    def total_queue(self) -> int:
        return len(self.open_queue)


class BotSyncService:
    def __init__(self, settings: AppSettings):
        self.settings = settings
        self.instance_id = self._build_instance_id(settings.sync.instance_id)
        self._channel: Optional[discord.TextChannel] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._cache_state: Optional[SyncState] = None
        self._cache_expires_at = 0.0
        self._lock = asyncio.Lock()

    @property
    def enabled(self) -> bool:
        return bool(self.settings.sync.enabled)

    async def start(self, client: discord.Client) -> None:
        if not self.enabled:
            return
        channel = await self._resolve_channel(client)
        if channel is None:
            logger.warning(
                "Bot sync enabled but channel was not found | channel_id=%s channel_name=%s",
                self.settings.sync.channel_id,
                self.settings.sync.channel_name,
            )
            return
        logger.info(
            "Bot sync enabled | instance=%s channel=%s workers=%s",
            self.instance_id,
            channel.id,
            self.settings.comfyui.queue_workers,
        )
        await self.publish(client, "heartbeat", workers=self.settings.comfyui.queue_workers)
        if self._heartbeat_task is None or self._heartbeat_task.done():
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(client), name="bot-sync-heartbeat")

    async def stop(self, client: Optional[discord.Client] = None) -> None:
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        self._heartbeat_task = None
        if self.enabled and client is not None:
            try:
                await self.publish(client, "heartbeat", workers=0, shutting_down=True)
            except Exception as exc:
                logger.debug("Failed to publish shutdown heartbeat: %s", exc)

    async def publish(self, client: discord.Client, kind: str, **payload: Any) -> bool:
        if not self.enabled:
            return False
        channel = await self._resolve_channel(client)
        if channel is None:
            return False
        body = {
            "v": _VERSION,
            "kind": kind,
            "instance": self.instance_id,
            "ts": time.time(),
            **payload,
        }
        try:
            await channel.send(_PREFIX + json.dumps(body, ensure_ascii=False, separators=(",", ":")))
            self.invalidate_cache()
            return True
        except discord.HTTPException as exc:
            logger.warning("Failed to publish bot sync event %s: %s", kind, exc)
            return False

    async def register_queue_entry(self, client: discord.Client, *, session_id: str, user_id: str, priority: int, is_img2vid: bool) -> SyncState:
        await self.publish(
            client,
            "queue_enter",
            session_id=session_id,
            user_id=user_id,
            priority=int(priority),
            is_img2vid=bool(is_img2vid),
        )
        return await self.get_state(client, force=True)

    async def leave_queue(self, client: discord.Client, *, session_id: str, reason: str) -> None:
        await self.publish(client, "queue_leave", session_id=session_id, reason=reason)

    async def commit_generation(self, client: discord.Client, *, session_id: str, user_id: str) -> None:
        await self.publish(client, "generation_commit", session_id=session_id, user_id=user_id)

    async def commit_img2vid(self, client: discord.Client, *, session_id: str, user_id: str) -> None:
        await self.publish(client, "img2vid_commit", session_id=session_id, user_id=user_id)

    async def wait_for_turn(self, client: discord.Client, *, session_id: str, cancel_event: asyncio.Event) -> None:
        if not self.enabled:
            return
        while not cancel_event.is_set():
            state = await self.get_state(client, force=False)
            position = state.queue_position(session_id)
            if position is None:
                return
            if position <= max(1, state.global_workers):
                return
            await asyncio.sleep(1.0)

    async def get_state(self, client: discord.Client, *, force: bool = False) -> SyncState:
        if not self.enabled:
            return SyncState(
                global_workers=max(1, self.settings.comfyui.queue_workers),
                open_queue=[],
                generation_committed_today={},
                generation_reserved_today={},
                img2vid_committed_today={},
                img2vid_reserved_today={},
                active_by_user={},
            )

        now = time.monotonic()
        if not force and self._cache_state is not None and now < self._cache_expires_at:
            return self._cache_state

        async with self._lock:
            now = time.monotonic()
            if not force and self._cache_state is not None and now < self._cache_expires_at:
                return self._cache_state

            channel = await self._resolve_channel(client)
            if channel is None:
                state = SyncState(
                    global_workers=max(1, self.settings.comfyui.queue_workers),
                    open_queue=[],
                    generation_committed_today={},
                    generation_reserved_today={},
                    img2vid_committed_today={},
                    img2vid_reserved_today={},
                    active_by_user={},
                )
                self._cache_state = state
                self._cache_expires_at = time.monotonic() + max(0.5, float(self.settings.sync.cache_ttl_seconds))
                return state

            state = await self._build_state(channel)
            self._cache_state = state
            self._cache_expires_at = time.monotonic() + max(0.5, float(self.settings.sync.cache_ttl_seconds))
            return state

    def invalidate_cache(self) -> None:
        self._cache_expires_at = 0.0

    async def _build_state(self, channel: discord.TextChannel) -> SyncState:
        now_dt = datetime.now(tz=timezone.utc)
        cutoff_dt = now_dt - timedelta(hours=max(6, int(self.settings.sync.history_window_hours)))
        heartbeat_cutoff_ts = (now_dt - timedelta(seconds=max(60, int(self.settings.sync.heartbeat_interval_seconds) * 3))).timestamp()
        offline_grace_ts = (now_dt - timedelta(seconds=max(180, int(self.settings.sync.offline_grace_seconds)))).timestamp()
        today = now_dt.date()

        latest_heartbeats: dict[str, dict[str, Any]] = {}
        queue_entries: dict[str, SyncQueueEntry] = {}
        closed_sessions: set[str] = set()
        generation_commits: set[str] = set()
        img2vid_commits: set[str] = set()
        generation_committed_today: dict[str, int] = defaultdict(int)
        img2vid_committed_today: dict[str, int] = defaultdict(int)

        async for message in channel.history(limit=None, after=cutoff_dt, oldest_first=True):
            content = (message.content or "").strip()
            if not content.startswith(_PREFIX):
                continue
            try:
                event = json.loads(content[len(_PREFIX):])
            except json.JSONDecodeError:
                continue
            if int(event.get("v", 0)) != _VERSION:
                continue

            kind = str(event.get("kind", "")).strip()
            ts = float(event.get("ts", message.created_at.replace(tzinfo=timezone.utc).timestamp()))
            instance_id = str(event.get("instance", "unknown"))

            if kind == "heartbeat":
                latest_heartbeats[instance_id] = {
                    "workers": max(0, int(event.get("workers", 0))),
                    "ts": ts,
                    "shutting_down": bool(event.get("shutting_down", False)),
                }
                continue

            if kind == "queue_enter":
                session_id = str(event.get("session_id", "")).strip()
                user_id = str(event.get("user_id", "")).strip()
                if not session_id or not user_id or session_id in closed_sessions:
                    continue
                queue_entries[session_id] = SyncQueueEntry(
                    session_id=session_id,
                    user_id=user_id,
                    priority=int(event.get("priority", 0)),
                    is_img2vid=bool(event.get("is_img2vid", False)),
                    instance_id=instance_id,
                    message_id=message.id,
                    created_at=ts,
                )
                continue

            if kind == "queue_leave":
                session_id = str(event.get("session_id", "")).strip()
                if session_id:
                    closed_sessions.add(session_id)
                    queue_entries.pop(session_id, None)
                continue

            if kind == "generation_commit":
                session_id = str(event.get("session_id", "")).strip()
                user_id = str(event.get("user_id", "")).strip()
                if session_id and session_id not in generation_commits:
                    generation_commits.add(session_id)
                    if datetime.fromtimestamp(ts, tz=timezone.utc).date() == today and user_id:
                        generation_committed_today[user_id] += 1
                continue

            if kind == "img2vid_commit":
                session_id = str(event.get("session_id", "")).strip()
                user_id = str(event.get("user_id", "")).strip()
                if session_id and session_id not in img2vid_commits:
                    img2vid_commits.add(session_id)
                    if datetime.fromtimestamp(ts, tz=timezone.utc).date() == today and user_id:
                        img2vid_committed_today[user_id] += 1
                continue

        active_instances = {
            instance_id: data
            for instance_id, data in latest_heartbeats.items()
            if data["ts"] >= heartbeat_cutoff_ts and not data.get("shutting_down")
        }

        queue_entries = {
            session_id: entry
            for session_id, entry in queue_entries.items()
            if entry.instance_id in active_instances or entry.created_at >= offline_grace_ts
        }

        open_queue = sorted(queue_entries.values(), key=lambda item: (int(item.priority), int(item.message_id)))
        active_by_user: dict[str, int] = defaultdict(int)
        generation_reserved_today: dict[str, int] = defaultdict(int)
        img2vid_reserved_today: dict[str, int] = defaultdict(int)

        for entry in open_queue:
            active_by_user[entry.user_id] += 1
            if datetime.fromtimestamp(entry.created_at, tz=timezone.utc).date() == today:
                if entry.session_id not in generation_commits:
                    generation_reserved_today[entry.user_id] += 1
                if entry.is_img2vid and entry.session_id not in img2vid_commits:
                    img2vid_reserved_today[entry.user_id] += 1

        global_workers = sum(max(0, int(item.get("workers", 0))) for item in active_instances.values())
        if global_workers <= 0:
            global_workers = max(1, self.settings.comfyui.queue_workers)

        return SyncState(
            global_workers=global_workers,
            open_queue=open_queue,
            generation_committed_today=dict(generation_committed_today),
            generation_reserved_today=dict(generation_reserved_today),
            img2vid_committed_today=dict(img2vid_committed_today),
            img2vid_reserved_today=dict(img2vid_reserved_today),
            active_by_user=dict(active_by_user),
        )

    async def _resolve_channel(self, client: discord.Client) -> Optional[discord.TextChannel]:
        if self._channel is not None:
            return self._channel

        channel: Optional[discord.TextChannel] = None
        if self.settings.sync.channel_id is not None:
            raw_channel = client.get_channel(self.settings.sync.channel_id)
            if raw_channel is None:
                try:
                    raw_channel = await client.fetch_channel(self.settings.sync.channel_id)
                except discord.HTTPException:
                    raw_channel = None
            if isinstance(raw_channel, discord.TextChannel):
                channel = raw_channel

        guild: Optional[discord.Guild] = None
        if channel is None and self.settings.security.access_guild_id is not None:
            guild = client.get_guild(self.settings.security.access_guild_id)
            if guild is None:
                try:
                    guild = await client.fetch_guild(self.settings.security.access_guild_id)
                except discord.HTTPException:
                    guild = None

        if channel is None and guild is not None:
            desired_names = {self.settings.sync.channel_name.strip(), self.settings.sync.channel_name.strip().lstrip("#")}
            normalized_desired = {self._normalize_channel_name(name) for name in desired_names if name}
            for item in guild.text_channels:
                if item.name in desired_names or self._normalize_channel_name(item.name) in normalized_desired:
                    channel = item
                    break

        if channel is None:
            for raw_guild in client.guilds:
                desired_names = {self.settings.sync.channel_name.strip(), self.settings.sync.channel_name.strip().lstrip("#")}
                normalized_desired = {self._normalize_channel_name(name) for name in desired_names if name}
                for item in raw_guild.text_channels:
                    if item.name in desired_names or self._normalize_channel_name(item.name) in normalized_desired:
                        channel = item
                        break
                if channel is not None:
                    break

        self._channel = channel
        return channel

    async def _heartbeat_loop(self, client: discord.Client) -> None:
        try:
            while True:
                await asyncio.sleep(max(10, int(self.settings.sync.heartbeat_interval_seconds)))
                await self.publish(client, "heartbeat", workers=self.settings.comfyui.queue_workers)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("Bot sync heartbeat loop failed: %s", exc)

    @staticmethod
    def _build_instance_id(config_value: Optional[str]) -> str:
        if config_value:
            return str(config_value)
        host = socket.gethostname().replace(" ", "-")[:32] or "host"
        return f"{host}-{os.getpid()}-{uuid.uuid4().hex[:8]}"

    @staticmethod
    def _normalize_channel_name(value: str) -> str:
        return value.strip().lstrip("#").lstrip("・").strip().lower()
