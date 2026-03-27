from __future__ import annotations
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml

from .logging_setup import logger
from .models import RoleTier


@dataclass
class DiscordSettings:
    token: str = ""
    footer_text: str = "Support us ❤️ boosty.to/rindex"


@dataclass
class ComfyAuthSettings:
    username: Optional[str] = None
    password: Optional[str] = None
    api_key: Optional[str] = None
    ssl_verify: bool = True
    ssl_cert: Optional[str] = None


@dataclass
class ComfyInstanceSettings:
    url: str
    weight: int = 1
    timeout: int = 900
    auth: Optional[ComfyAuthSettings] = None


@dataclass
class ComfySettings:
    input_dir: str = "COMFYUI_INPUT_DIR"
    queue_workers: int = 1
    instances: list[ComfyInstanceSettings] = field(default_factory=list)


@dataclass
class StorageSettings:
    usage_file: str = "data/generation_counts.yml"
    manual_subscriptions_file: str = "data/manual_subscriptions.yml"


@dataclass
class LimitSettings:
    retention_days: int = 90


@dataclass
class InputSettings:
    max_image_size_mb: int = 20
    allowed_mime_types: list[str] = field(default_factory=lambda: ["image/png", "image/jpeg", "image/webp"])


@dataclass
class SyncSettings:
    enabled: bool = False
    channel_id: Optional[int] = None
    channel_name: str = "・bot-sync"
    instance_id: Optional[str] = None
    heartbeat_interval_seconds: int = 30
    history_window_hours: int = 48
    cache_ttl_seconds: float = 1.5
    offline_grace_seconds: int = 180


@dataclass
class SecuritySettings:
    access_guild_id: Optional[int] = None
    blocked_users: list[str] = field(default_factory=list)
    donor_users: list[str] = field(default_factory=list)
    img2img_allowed_role_ids: list[int] = field(default_factory=list)
    img2vid_daily_limits: dict[str, int] = field(default_factory=dict)
    role_tiers: list[RoleTier] = field(default_factory=list)
    manual_subscription_manager_role_id: Optional[int] = 1305936232500564009


@dataclass
class PresetSettings:
    prompt_file: str = "data/prompt_presets.json"
    model_file: str = "data/model_presets.json"
    lora_file: str = "data/lora_presets.json"


@dataclass
class AppSettings:
    path: Path
    raw: dict[str, Any]
    discord: DiscordSettings
    comfyui: ComfySettings
    storage: StorageSettings
    limits: LimitSettings
    inputs: InputSettings
    sync: SyncSettings
    security: SecuritySettings
    presets: PresetSettings
    spoilers: list[str]
    resolutions: list[Any]
    workflows: dict[str, dict[str, Any]]

    def save_spoilers(self, tags: list[str]) -> None:
        self.spoilers = list(tags)
        self.raw.setdefault("spoilers", {})["tags"] = list(tags)
        with self.path.open("w", encoding="utf-8") as fh:
            yaml.safe_dump(self.raw, fh, allow_unicode=True, sort_keys=False)


def _default_tiers() -> list[RoleTier]:
    return [
        RoleTier(4, "Level 4", 1451769149045997588, None, 40, 30),
        RoleTier(3, "Level 3", 1451768900453925030, None, 30, 3),
        RoleTier(2, "Level 2", 1361296590777745560, None, 0, 1),
        RoleTier(1, "Level 1", 1450781064418299914, 100, 0, 1),
        RoleTier(0, "Public", None, 25, 0, 1),
    ]


def load_settings(path: str | Path) -> AppSettings:
    config_path = Path(path).resolve()
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}

    # Compatibility with the old single-level preset keys.
    presets_raw = raw.get("presets", {})
    if not presets_raw:
        presets_raw = {
            "prompt_file": raw.get("prompt_presets_file", "data/prompt_presets.json"),
            "model_file": raw.get("model_presets_file", "data/model_presets.json"),
            "lora_file": raw.get("lora_presets_file", "data/lora_presets.json"),
        }
        raw["presets"] = presets_raw

    comfy_raw = raw.get("comfyui", {})
    instances: list[ComfyInstanceSettings] = []
    for item in comfy_raw.get("instances", []):
        auth_raw = item.get("auth")
        auth = ComfyAuthSettings(**auth_raw) if auth_raw else None
        instances.append(
            ComfyInstanceSettings(
                url=item["url"],
                weight=int(item.get("weight", 1)),
                timeout=int(item.get("timeout", 900)),
                auth=auth,
            )
        )

    security_raw = raw.get("security", {})
    role_tiers_raw = security_raw.get("role_tiers")
    if role_tiers_raw:
        role_tiers = [
            RoleTier(
                level=int(item.get("level", 0)),
                name=str(item.get("name", f"Tier {idx}")),
                role_id=int(item["role_id"]) if item.get("role_id") is not None else None,
                daily_limit=int(item["daily_limit"]) if item.get("daily_limit") is not None else None,
                queue_priority=int(item.get("queue_priority", 0)),
                max_parallel_generations=int(item.get("max_parallel_generations", 1)),
            )
            for idx, item in enumerate(role_tiers_raw, start=1)
        ]
    else:
        role_tiers = _default_tiers()

    settings = AppSettings(
        path=config_path,
        raw=raw,
        discord=DiscordSettings(**raw.get("discord", {})),
        comfyui=ComfySettings(
            input_dir=str(comfy_raw.get("input_dir", "COMFYUI_INPUT_DIR")),
            queue_workers=max(1, int(comfy_raw.get("queue_workers", max(1, len(instances) or 1)))),
            instances=instances,
        ),
        storage=StorageSettings(**raw.get("storage", {})),
        limits=LimitSettings(retention_days=int(raw.get("limits", {}).get("retention_days", 90))),
        inputs=InputSettings(**raw.get("inputs", {})),
        sync=SyncSettings(
            enabled=bool(raw.get("sync", {}).get("enabled", False)),
            channel_id=(int(raw.get("sync", {}).get("channel_id")) if raw.get("sync", {}).get("channel_id") is not None else None),
            channel_name=str(raw.get("sync", {}).get("channel_name", "・bot-sync")),
            instance_id=(str(raw.get("sync", {}).get("instance_id")) if raw.get("sync", {}).get("instance_id") else None),
            heartbeat_interval_seconds=max(10, int(raw.get("sync", {}).get("heartbeat_interval_seconds", 30))),
            history_window_hours=max(6, int(raw.get("sync", {}).get("history_window_hours", 48))),
            cache_ttl_seconds=max(0.5, float(raw.get("sync", {}).get("cache_ttl_seconds", 1.5))),
            offline_grace_seconds=max(60, int(raw.get("sync", {}).get("offline_grace_seconds", 180))),
        ),
        security=SecuritySettings(
            access_guild_id=int(security_raw["access_guild_id"]) if security_raw.get("access_guild_id") else None,
            blocked_users=[str(v) for v in security_raw.get("blocked_users", [])],
            donor_users=[str(v) for v in security_raw.get("donor_users", [])],
            img2img_allowed_role_ids=[int(v) for v in security_raw.get("img2img_allowed_role_ids", [1451768900453925030, 1451769149045997588])],
            img2vid_daily_limits={str(k): int(v) for k, v in security_raw.get("img2vid_daily_limits", {"1451768900453925030": 6, "1451769149045997588": 30}).items()},
            role_tiers=sorted(role_tiers, key=lambda item: item.level, reverse=True),
            manual_subscription_manager_role_id=(
                int(security_raw["manual_subscription_manager_role_id"])
                if security_raw.get("manual_subscription_manager_role_id") is not None
                else 1305936232500564009
            ),
        ),
        presets=PresetSettings(**presets_raw),
        spoilers=list(raw.get("spoilers", {}).get("tags", [])),
        resolutions=list(raw.get("resolutions", [])),
        workflows=dict(raw.get("workflows", {})),
    )

    logger.info(
        "Loaded config: %s workflows, %s preset files, %s ComfyUI instances",
        len(settings.workflows),
        3,
        len(settings.comfyui.instances),
    )
    return settings
