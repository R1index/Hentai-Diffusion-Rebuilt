from __future__ import annotations
import asyncio
from dataclasses import dataclass, field
from typing import Any, Optional

import discord


@dataclass(frozen=True)
class RoleTier:
    level: int
    name: str
    role_id: Optional[int]
    daily_limit: Optional[int]
    queue_priority: int
    max_parallel_generations: int


@dataclass(frozen=True)
class PromptPreset:
    name: str
    tags: str
    value: str

    def apply(self, prompt: Optional[str]) -> str:
        base = (prompt or "").strip()
        tags = self.tags.strip()
        if not tags:
            return base
        return f"{tags} {base}".strip()


@dataclass(frozen=True)
class ModelPreset:
    name: str
    model: str
    value: str


@dataclass(frozen=True)
class LoRAPreset:
    name: str
    lora: str
    value: str


@dataclass
class GenerationRequest:
    workflow_type: str
    prompt: str
    workflow_name: Optional[str] = None
    settings: Optional[str] = None
    resolution: Optional[str] = None
    prompt_preset: Optional[str] = None
    model_preset: Optional[str] = None
    lora_preset: Optional[str] = None
    seed: Optional[int] = None
    controlnet_strength: Optional[float] = None
    input_attachment: Optional[discord.Attachment] = None


@dataclass
class StoredRequest:
    workflow_type: str
    prompt: str
    workflow_name: Optional[str]
    settings: Optional[str]
    resolution: Optional[str]
    prompt_preset: Optional[str]
    model_preset: Optional[str]
    lora_preset: Optional[str]
    seed: Optional[int]
    controlnet_strength: Optional[float]


@dataclass
class GenerationSession:
    user_id: str
    user: discord.abc.User
    interaction: discord.Interaction
    request: GenerationRequest
    tier: RoleTier
    message: Optional[discord.Message] = None
    prompt_id: Optional[str] = None
    prompt: Optional[str] = None
    settings: Optional[str] = None
    resolution: Optional[str] = None
    seed: Optional[int] = None
    workflow_name: Optional[str] = None
    prompt_preset_name: Optional[str] = None
    model_preset_name: Optional[str] = None
    lora_preset_name: Optional[str] = None
    force_spoiler: bool = False
    counted_usage: bool = False
    counted_img2vid: bool = False
    submitted_to_comfy: bool = False
    completed: bool = False
    queued: bool = False
    processing: bool = False
    finalized: bool = False
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def status_key(self) -> str:
        return self.prompt_id or f"local-{id(self)}"
