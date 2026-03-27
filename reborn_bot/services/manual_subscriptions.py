from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import yaml

from ..logging_setup import logger
from ..models import RoleTier


class ManualSubscriptionStore:
    def __init__(self, file_path: Path):
        self.file_path = file_path
        self.subscriptions: dict[str, dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.file_path.exists():
            self._save()
            return
        try:
            data = yaml.safe_load(self.file_path.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError as exc:
            logger.warning("Manual subscription storage is corrupted, recreating %s: %s", self.file_path, exc)
            data = {}
        subscriptions = data.get("subscriptions", {}) or {}
        if not isinstance(subscriptions, dict):
            subscriptions = {}
        self.subscriptions = {str(user_id): record for user_id, record in subscriptions.items() if isinstance(record, dict)}

    def _save(self) -> None:
        payload = {
            "subscriptions": self.subscriptions,
            "updated_at": self._now().isoformat(),
        }
        temp_path = self.file_path.with_suffix(self.file_path.suffix + ".tmp")
        temp_path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")
        temp_path.replace(self.file_path)

    @staticmethod
    def _now() -> datetime:
        return datetime.now(tz=timezone.utc)

    @staticmethod
    def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            dt = datetime.fromisoformat(value)
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    def get_subscription(self, user_id: str) -> Optional[dict[str, Any]]:
        record = self.subscriptions.get(str(user_id))
        return dict(record) if record else None

    def list_active(self) -> list[dict[str, Any]]:
        now = self._now()
        active = []
        for record in self.subscriptions.values():
            expires_at = self._parse_datetime(record.get("expires_at"))
            if expires_at and expires_at > now:
                active.append(dict(record))
        active.sort(key=lambda item: item.get("expires_at", ""))
        return active

    def get_expired(self) -> list[dict[str, Any]]:
        now = self._now()
        expired = []
        for record in self.subscriptions.values():
            expires_at = self._parse_datetime(record.get("expires_at"))
            if expires_at and expires_at <= now:
                expired.append(dict(record))
        return expired

    def grant_subscription(
        self,
        *,
        member: Any,
        tier: RoleTier,
        days: int,
        granted_by: Any,
        guild_id: int,
    ) -> dict[str, Any]:
        if days <= 0:
            raise ValueError("days must be greater than zero")
        if tier.role_id is None:
            raise ValueError("tier role_id is required for manual subscriptions")

        now = self._now()
        user_id = str(member.id)
        existing = self.subscriptions.get(user_id, {}) or {}
        current_expiry = self._parse_datetime(existing.get("expires_at"))
        base_time = current_expiry if current_expiry and current_expiry > now else now
        new_expiry = base_time + timedelta(days=days)
        issued_at = existing.get("issued_at") if current_expiry and current_expiry > now else now.isoformat()

        record = {
            "user_id": user_id,
            "guild_id": str(guild_id),
            "username": getattr(member, "name", "") or str(member.id),
            "display_name": getattr(member, "display_name", None) or getattr(member, "global_name", None) or getattr(member, "name", str(member.id)),
            "issued_at": issued_at,
            "expires_at": new_expiry.isoformat(),
            "level": int(tier.level),
            "tier_name": str(tier.name),
            "role_id": str(tier.role_id),
            "granted_by_id": str(getattr(granted_by, "id", "")),
            "granted_by_name": getattr(granted_by, "name", None) or str(getattr(granted_by, "id", "")),
            "last_granted_at": now.isoformat(),
            "last_granted_days": int(days),
            "total_days_granted": int(existing.get("total_days_granted", 0)) + int(days),
        }
        self.subscriptions[user_id] = record
        self._save()
        return dict(record)

    def remove_subscription(self, user_id: str) -> Optional[dict[str, Any]]:
        record = self.subscriptions.pop(str(user_id), None)
        if record is not None:
            self._save()
            return dict(record)
        return None
