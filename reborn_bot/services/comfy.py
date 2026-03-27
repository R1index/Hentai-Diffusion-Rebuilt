from __future__ import annotations

import asyncio
import base64
import io
import json
import random
import ssl
import urllib.parse
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Optional

import aiohttp
import discord
import websockets

from ..config import ComfyInstanceSettings
from ..logging_setup import logger


class LoadBalanceStrategy(Enum):
    ROUND_ROBIN = "round_robin"
    RANDOM = "random"
    LEAST_BUSY = "least_busy"


@dataclass
class ComfyUIAuth:
    username: Optional[str] = None
    password: Optional[str] = None
    api_key: Optional[str] = None
    ssl_verify: bool = True
    ssl_cert: Optional[str] = None


@dataclass
class ComfyUIInstance:
    config: ComfyInstanceSettings
    base_url: str = field(init=False)
    ws_url: str = field(init=False)
    weight: int = field(init=False)
    timeout: int = field(init=False)
    auth: Optional[ComfyUIAuth] = field(init=False)
    client_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    session: Optional[aiohttp.ClientSession] = None
    ws: Any = None
    reader_task: Optional[asyncio.Task] = None
    active_prompts: set[str] = field(default_factory=set)
    prompt_channels: dict[str, asyncio.Queue] = field(default_factory=dict)
    active_generations: int = 0
    total_generations: int = 0
    last_used: datetime = field(default_factory=datetime.now)
    connected: bool = False
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def __post_init__(self) -> None:
        self.base_url = self.config.url.rstrip("/")
        self.ws_url = self.base_url.replace("http", "ws", 1)
        self.weight = self.config.weight
        self.timeout = self.config.timeout
        self.auth = ComfyUIAuth(**self.config.auth.__dict__) if self.config.auth else None

    async def get_session(self) -> aiohttp.ClientSession:
        if self.session and not self.session.closed:
            return self.session

        headers = {}
        if self.auth:
            if self.auth.api_key:
                headers["Authorization"] = f"Bearer {self.auth.api_key}"
            elif self.auth.username and self.auth.password:
                token = base64.b64encode(f"{self.auth.username}:{self.auth.password}".encode()).decode()
                headers["Authorization"] = f"Basic {token}"

        ssl_context = None
        if self.auth and self.auth.ssl_cert:
            ssl_context = ssl.create_default_context()
            ssl_context.load_verify_locations(self.auth.ssl_cert)

        connector_ssl = ssl_context if ssl_context is not None else self.auth.ssl_verify if self.auth else True
        self.session = aiohttp.ClientSession(headers=headers, connector=aiohttp.TCPConnector(ssl=connector_ssl))
        return self.session

    async def initialize(self) -> None:
        session = await self.get_session()
        async with session.get(f"{self.base_url}/history") as response:
            if response.status != 200:
                raise RuntimeError(f"Failed to connect to ComfyUI {self.base_url}: HTTP {response.status}")

        ws_kwargs: dict[str, Any] = {"origin": self.base_url}
        if self.ws_url.startswith("wss://"):
            ws_ssl: ssl.SSLContext | bool = self.auth.ssl_verify if self.auth else True
            if self.auth and self.auth.ssl_cert:
                ws_ssl = ssl.create_default_context()
                ws_ssl.load_verify_locations(self.auth.ssl_cert)
            ws_kwargs["ssl"] = ws_ssl

        self.ws = await websockets.connect(
            f"{self.ws_url}/ws?clientId={self.client_id}",
            ping_interval=15,
            ping_timeout=60,
            extra_headers={"Authorization": f"Bearer {self.auth.api_key}"} if self.auth and self.auth.api_key else None,
            **ws_kwargs,
        )
        self.connected = True
        self.last_used = datetime.now()
        if self.reader_task and not self.reader_task.done():
            self.reader_task.cancel()
            try:
                await self.reader_task
            except asyncio.CancelledError:
                pass
        self.reader_task = asyncio.create_task(self._reader_loop(), name=f"comfy-reader:{self.base_url}")
        logger.info("Connected to ComfyUI instance: %s", self.base_url)

    async def cleanup(self) -> None:
        async with self._lock:
            self.connected = False
            if self.reader_task:
                self.reader_task.cancel()
                try:
                    await self.reader_task
                except asyncio.CancelledError:
                    pass
                self.reader_task = None
            if self.ws:
                await self.ws.close()
                self.ws = None
            if self.session and not self.session.closed:
                await self.session.close()
                self.session = None
            await self._notify_prompt_channels(RuntimeError(f"Connection to {self.base_url} closed"))

    async def _notify_prompt_channels(self, event: Exception) -> None:
        for queue in list(self.prompt_channels.values()):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                pass

    async def _reader_loop(self) -> None:
        try:
            while self.ws is not None:
                raw_message = await self.ws.recv()
                try:
                    payload = json.loads(raw_message)
                except Exception:
                    continue

                prompt_id = str((payload.get("data", {}) or {}).get("prompt_id") or "").strip()
                if not prompt_id:
                    continue
                queue = self.prompt_channels.get(prompt_id)
                if queue:
                    await queue.put(payload)
        except asyncio.CancelledError:
            raise
        except websockets.ConnectionClosed as exc:
            self.connected = False
            await self._notify_prompt_channels(RuntimeError(f"WebSocket connection lost: {exc}"))
        except Exception as exc:
            self.connected = False
            logger.exception("ComfyUI reader loop failed for %s: %s", self.base_url, exc)
            await self._notify_prompt_channels(RuntimeError(f"ComfyUI reader failed: {exc}"))

    def is_timed_out(self) -> bool:
        if self.timeout <= 0:
            return False
        return datetime.now() - self.last_used > timedelta(seconds=self.timeout)

    async def mark_used(self) -> None:
        self.last_used = datetime.now()

    def register_prompt(self, prompt_id: str) -> asyncio.Queue:
        queue = self.prompt_channels.get(prompt_id)
        if queue is None:
            queue = asyncio.Queue()
            self.prompt_channels[prompt_id] = queue
        return queue

    def release_prompt(self, prompt_id: str) -> None:
        self.active_prompts.discard(prompt_id)
        self.prompt_channels.pop(prompt_id, None)


class ComfyUIClient:
    def __init__(self, instances_config: list[ComfyInstanceSettings]):
        self.instances = [ComfyUIInstance(config) for config in instances_config]
        self.strategy = LoadBalanceStrategy.LEAST_BUSY
        self.current_index = 0
        self.prompt_to_instance: dict[str, ComfyUIInstance] = {}
        self.prompt_workflows: dict[str, dict[str, Any]] = {}
        self.timeout_task: Optional[asyncio.Task] = None

    async def connect(self) -> None:
        if not self.instances:
            raise RuntimeError("No ComfyUI instances configured")
        results = await asyncio.gather(*(instance.initialize() for instance in self.instances), return_exceptions=True)
        connected = sum(1 for instance in self.instances if instance.connected)
        if connected == 0:
            raise RuntimeError(f"Failed to connect to any ComfyUI instance: {results}")
        self.timeout_task = asyncio.create_task(self._timeout_loop(), name="comfy-timeout-loop")
        logger.info("ComfyUI pool online | connected=%s/%s", connected, len(self.instances))

    async def close(self) -> None:
        if self.timeout_task:
            self.timeout_task.cancel()
            try:
                await self.timeout_task
            except asyncio.CancelledError:
                pass
        await asyncio.gather(*(instance.cleanup() for instance in self.instances), return_exceptions=True)

    async def _timeout_loop(self) -> None:
        while True:
            for instance in self.instances:
                if instance.connected and instance.is_timed_out() and not instance.active_prompts:
                    logger.info("Closing idle ComfyUI connection: %s", instance.base_url)
                    await instance.cleanup()
            await asyncio.sleep(5)

    async def _select_instance(self) -> ComfyUIInstance:
        available = [instance for instance in self.instances if instance.connected and not instance.is_timed_out()]
        if not available:
            for instance in self.instances:
                if not instance.connected and not instance.active_prompts:
                    try:
                        await instance.initialize()
                    except Exception as exc:
                        logger.warning("Reconnection failed for %s: %s", instance.base_url, exc)
            available = [instance for instance in self.instances if instance.connected]
        if not available:
            raise RuntimeError("No available ComfyUI instances")

        if self.strategy == LoadBalanceStrategy.ROUND_ROBIN:
            instance = available[self.current_index % len(available)]
            self.current_index = (self.current_index + 1) % len(available)
        elif self.strategy == LoadBalanceStrategy.RANDOM:
            instance = random.choice(available)
        else:
            instance = min(available, key=lambda item: len(item.active_prompts) / max(item.weight, 1))
        await instance.mark_used()
        return instance

    async def generate(self, workflow_json: dict) -> dict:
        instance = await self._select_instance()
        async with instance._lock:
            instance.active_generations += 1
            try:
                session = await instance.get_session()
                async with session.post(f"{instance.base_url}/prompt", json={"prompt": workflow_json, "client_id": instance.client_id}) as response:
                    if response.status != 200:
                        raise RuntimeError(f"ComfyUI prompt failed: HTTP {response.status} - {await response.text()}")
                    data = await response.json()
                    prompt_id = str(data.get("prompt_id") or "").strip()
                    if prompt_id:
                        instance.active_prompts.add(prompt_id)
                        instance.register_prompt(prompt_id)
                        self.prompt_to_instance[prompt_id] = instance
                        self.prompt_workflows[prompt_id] = workflow_json
                    instance.total_generations += 1
                    logger.info("Submitted prompt %s to %s", prompt_id or "<unknown>", instance.base_url)
                    return data
            finally:
                instance.active_generations -= 1

    @staticmethod
    def _create_progress_bar(value: int, max_value: int, length: int = 12) -> str:
        if max_value <= 0:
            max_value = 1
        filled = int(length * (value / max_value))
        return "[" + ("█" * filled) + ("░" * (length - filled)) + f"] {int((value / max_value) * 100)}%"

    @staticmethod
    def _is_video_filename(filename: str) -> bool:
        return filename.lower().endswith((".mp4", ".webm", ".mov", ".mkv", ".avi", ".gif"))

    def _build_file_url(self, instance: ComfyUIInstance, payload: dict) -> str:
        params = []
        for key in ("filename", "subfolder", "type"):
            value = payload.get(key)
            if value:
                params.append(f"{key}={urllib.parse.quote(str(value))}")
        return f"{instance.base_url}/view?" + "&".join(params)

    def _extract_outputs(self, node_output: dict) -> tuple[list[dict], list[dict]]:
        videos: list[dict] = []
        images: list[dict] = []

        for key in ("videos", "gifs"):
            for item in node_output.get(key, []):
                if isinstance(item, dict) and item.get("filename"):
                    videos.append(item)

        for item in node_output.get("images", []):
            if not isinstance(item, dict) or not item.get("filename"):
                continue
            if self._is_video_filename(item["filename"]):
                videos.append(item)
            else:
                images.append(item)

        for item in node_output.get("files", []):
            if not isinstance(item, dict) or not item.get("filename"):
                continue
            if self._is_video_filename(item["filename"]):
                videos.append(item)
            else:
                images.append(item)
        return videos, images

    @staticmethod
    def _get_node_class_type(node_id: str, workflow_json: Optional[dict[str, Any]]) -> str:
        if not workflow_json:
            return ""
        node = workflow_json.get(str(node_id)) or {}
        return str(node.get("class_type", "") or "").lower()

    @classmethod
    def _is_preview_node(cls, node_id: str, workflow_json: Optional[dict[str, Any]]) -> bool:
        return "preview" in cls._get_node_class_type(node_id, workflow_json)

    @classmethod
    def _is_save_image_node(cls, node_id: str, workflow_json: Optional[dict[str, Any]]) -> bool:
        return "saveimage" in cls._get_node_class_type(node_id, workflow_json)

    @classmethod
    def _node_output_priority(cls, node_id: str, workflow_json: Optional[dict[str, Any]]) -> int:
        class_type = cls._get_node_class_type(node_id, workflow_json)
        if not class_type:
            return 50
        if "preview" in class_type:
            return 100
        if "saveimage" in class_type:
            return 0
        if any(token in class_type for token in ("video", "gif", "combine", "output", "save")):
            return 5
        return 25

    async def _fetch_history_payload(self, instance: ComfyUIInstance, prompt_id: str, *, attempts: int = 8, delay: float = 0.4) -> Optional[dict[str, Any]]:
        session = await instance.get_session()
        for _ in range(max(attempts, 1)):
            try:
                async with session.get(f"{instance.base_url}/history/{prompt_id}") as response:
                    if response.status == 200:
                        payload = await response.json()
                        if isinstance(payload, dict) and payload:
                            return payload.get(prompt_id) if prompt_id in payload else payload
            except Exception as exc:
                logger.debug("Failed to fetch ComfyUI history for %s: %s", prompt_id, exc)
            await asyncio.sleep(delay)
        return None

    async def _download_output_file(
        self,
        session: aiohttp.ClientSession,
        instance: ComfyUIInstance,
        item: dict,
        *,
        attempts: int = 1,
        delay: float = 0.35,
    ) -> Optional[discord.File]:
        url = self._build_file_url(instance, item)
        for _ in range(max(attempts, 1)):
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        payload_bytes = await response.read()
                        return discord.File(io.BytesIO(payload_bytes), filename=item.get("filename", "output.bin"))
            except Exception as exc:
                logger.debug("Failed to download ComfyUI output %s: %s", item.get("filename"), exc)
            await asyncio.sleep(delay)
        return None

    async def _resolve_final_outputs(
        self,
        instance: ComfyUIInstance,
        prompt_id: str,
        *,
        video_only: bool = False,
        fallback_outputs: Optional[list[tuple[int, str, dict, bool, bool]]] = None,
    ) -> list[tuple[str, discord.File]]:
        session = await instance.get_session()
        candidates: list[tuple[int, str, dict, bool, bool]] = []

        history_payload = await self._fetch_history_payload(
            instance,
            prompt_id,
            attempts=80 if video_only else 8,
            delay=1.0 if video_only else 0.4,
        )
        workflow_json: Optional[dict[str, Any]] = None
        if isinstance(history_payload, dict):
            prompt_meta = history_payload.get("prompt")
            if isinstance(prompt_meta, list) and len(prompt_meta) >= 3 and isinstance(prompt_meta[2], dict):
                workflow_json = prompt_meta[2]
            outputs = history_payload.get("outputs", {})
            if isinstance(outputs, dict):
                for node_id, node_output in outputs.items():
                    if not isinstance(node_output, dict):
                        continue
                    node_id = str(node_id)
                    priority = self._node_output_priority(node_id, workflow_json)
                    is_preview = self._is_preview_node(node_id, workflow_json)
                    is_save_image = self._is_save_image_node(node_id, workflow_json)
                    videos, images = self._extract_outputs(node_output)
                    for item in videos:
                        candidates.append((priority, "video", item, is_save_image, is_preview))
                    for item in images:
                        candidates.append((priority, "image", item, is_save_image, is_preview))

        if not candidates and fallback_outputs:
            candidates.extend(fallback_outputs)

        if not candidates:
            return []

        if video_only:
            preferred = [item for item in candidates if item[1] == "video"]
            if not preferred:
                return []
        else:
            preferred = [item for item in candidates if item[1] == "image" and item[3]]
            if not preferred:
                preferred = [item for item in candidates if item[1] == "image" and not item[4]]
            if not preferred:
                preferred = [item for item in candidates if item[1] == "image"]
            if not preferred:
                preferred = [item for item in candidates if item[1] == "video"]

        ordered = sorted(preferred or candidates, key=lambda item: (item[0], item[2].get("filename", "")))

        resolved: list[tuple[str, discord.File]] = []
        for _, kind, item, _, _ in ordered:
            file = await self._download_output_file(
                session,
                instance,
                item,
                attempts=20 if (video_only or kind == "video") else 3,
                delay=0.75 if (video_only or kind == "video") else 0.35,
            )
            if file is None:
                continue
            status = "🎬 Video ready" if kind == "video" else "🖼 Image ready"
            resolved.append((status, file))
        return resolved

    async def listen_for_updates(
        self,
        prompt_id: str,
        message_callback: Callable[[str, Optional[discord.File], str], asyncio.Future | asyncio.Task | None],
        *,
        cancel_event: Optional[asyncio.Event] = None,
        video_only: bool = False,
    ) -> None:
        instance = self.prompt_to_instance.get(prompt_id)
        if not instance or not instance.connected:
            raise RuntimeError(f"No connected instance found for prompt {prompt_id}")

        event_queue = instance.register_prompt(prompt_id)
        progress_state: dict[str, int] = {}
        start_ts = datetime.now().timestamp()
        fallback_outputs: list[tuple[int, str, dict, bool, bool]] = []
        preview_state: dict[str, str] = {}
        workflow_json = self.prompt_workflows.get(prompt_id)
        http_session = await instance.get_session()

        async def emit(status: str, file: Optional[discord.File] = None, output_role: str = "status") -> None:
            result = message_callback(status, file, output_role)
            if asyncio.iscoroutine(result):
                await result

        try:
            while True:
                if cancel_event and cancel_event.is_set():
                    await emit("🛑 Generation cancelled by user.")
                    await self.cancel_prompt(prompt_id)
                    break

                try:
                    event = await asyncio.wait_for(event_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                if isinstance(event, Exception):
                    await emit(f"❌ {event}")
                    raise event

                payload = event
                msg_type = payload.get("type")
                data = payload.get("data", {}) or {}

                if msg_type == "progress":
                    node = str(data.get("node") or "")
                    if not node:
                        continue
                    value = int(data.get("value", 0))
                    maximum = int(data.get("max", 100) or 100)
                    progress_pct = int((value / max(maximum, 1)) * 100)
                    last = progress_state.get(node, 0)
                    milestone = max([mark for mark in (25, 50, 75, 100) if progress_pct >= mark] or [0])
                    if milestone > last:
                        progress_state[node] = milestone
                        await emit(
                            f"🔄 Processing node {node}\n{self._create_progress_bar(value, maximum)}\n⏱ {(datetime.now().timestamp() - start_ts):.1f}s"
                        )

                elif msg_type == "executing":
                    node = data.get("node")
                    if node:
                        await emit(f"🔄 Executing node {node}\n⏱ {(datetime.now().timestamp() - start_ts):.1f}s")
                    else:
                        await emit(f"✅ Generation completed\n📦 Collecting final output...\n⏱ {(datetime.now().timestamp() - start_ts):.1f}s")
                        final_outputs = await self._resolve_final_outputs(
                            instance,
                            prompt_id,
                            video_only=video_only,
                            fallback_outputs=fallback_outputs,
                        )
                        if final_outputs:
                            for status, file in final_outputs:
                                await emit(f"{status}\n⏱ {(datetime.now().timestamp() - start_ts):.1f}s", file, "final")
                        else:
                            if video_only:
                                await emit(
                                    f"✅ Generation completed\n⚠️ Video output was not found in ComfyUI history.\n⏱ {(datetime.now().timestamp() - start_ts):.1f}s",
                                    None,
                                    "final_missing",
                                )
                            else:
                                await emit(f"✅ Generation completed\n⚠️ Final output was not found in ComfyUI history.\n⏱ {(datetime.now().timestamp() - start_ts):.1f}s")
                        break

                elif msg_type == "executed":
                    output = data.get("output")
                    if not isinstance(output, dict):
                        continue
                    node_id = str(data.get("node") or "")
                    priority = self._node_output_priority(node_id, workflow_json)
                    is_preview = self._is_preview_node(node_id, workflow_json)
                    is_save_image = self._is_save_image_node(node_id, workflow_json)
                    videos, images = self._extract_outputs(output)
                    for item in videos:
                        fallback_outputs.append((priority, "video", item, is_save_image, is_preview))
                    for item in images:
                        fallback_outputs.append((priority, "image", item, is_save_image, is_preview))

                    if is_preview and images:
                        preview_item = images[0]
                        preview_name = str(preview_item.get("filename") or "")
                        if preview_name and preview_state.get(node_id) != preview_name:
                            preview_state[node_id] = preview_name
                            preview_file = await self._download_output_file(http_session, instance, preview_item)
                            if preview_file is not None:
                                await emit(
                                    f"🖼 Preview updated\n⏱ {(datetime.now().timestamp() - start_ts):.1f}s",
                                    preview_file,
                                    "preview",
                                )

                elif msg_type == "error":
                    raise RuntimeError(str(data.get("error") or "Unknown ComfyUI error"))
        finally:
            instance.release_prompt(prompt_id)
            self.prompt_to_instance.pop(prompt_id, None)
            self.prompt_workflows.pop(prompt_id, None)

    async def cancel_prompt(self, prompt_id: str) -> None:
        instance = self.prompt_to_instance.get(prompt_id)
        if not instance:
            return

        try:
            if instance.ws and not getattr(instance.ws, "closed", False):
                for payload in ({"type": "interrupt"}, {"type": "cancel", "data": {"prompt_id": prompt_id}}):
                    try:
                        await instance.ws.send(json.dumps(payload))
                    except Exception:
                        pass

            session = await instance.get_session()
            for url, data in (
                (f"{instance.base_url}/interrupt", {"client_id": instance.client_id}),
                (f"{instance.base_url}/queue", {"prompt_id": prompt_id, "client_id": instance.client_id}),
            ):
                try:
                    async with session.post(url, json=data):
                        pass
                except Exception:
                    pass
        finally:
            instance.active_prompts.discard(prompt_id)
