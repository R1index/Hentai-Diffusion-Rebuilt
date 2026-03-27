from __future__ import annotations
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

from ..logging_setup import logger


class UsageStore:
    def __init__(self, file_path: Path, retention_days: int = 90):
        self.file_path = file_path
        self.retention_days = retention_days
        self.user_generation_counts: dict[str, int] = defaultdict(int)
        self.user_img2vid_counts: dict[str, int] = defaultdict(int)
        self.user_generation_stats: dict[str, dict] = {}
        self.last_reset_time: float = datetime.now(tz=timezone.utc).timestamp()
        self._load()

    def _load(self) -> None:
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.file_path.exists():
            self._save()
            return
        try:
            data = yaml.safe_load(self.file_path.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError as exc:
            logger.warning("Usage storage is corrupted, recreating %s: %s", self.file_path, exc)
            data = {}
        self.user_generation_counts = defaultdict(int, data.get("counts", {}))
        self.user_img2vid_counts = defaultdict(int, data.get("img2vid_counts", {}))
        self.user_generation_stats = data.get("stats", {}) or {}
        self.last_reset_time = float(data.get("last_reset", datetime.now(tz=timezone.utc).timestamp()))

    def _save(self) -> None:
        payload = {
            "counts": dict(self.user_generation_counts),
            "img2vid_counts": dict(self.user_img2vid_counts),
            "stats": self.user_generation_stats,
            "last_reset": self.last_reset_time,
        }
        temp_path = self.file_path.with_suffix(self.file_path.suffix + ".tmp")
        temp_path.write_text(yaml.safe_dump(payload, allow_unicode=True), encoding="utf-8")
        temp_path.replace(self.file_path)

    @staticmethod
    def _today() -> datetime.date:
        return datetime.now(tz=timezone.utc).date()

    def reset_if_needed(self) -> None:
        now = datetime.now(tz=timezone.utc)
        last = datetime.fromtimestamp(self.last_reset_time, tz=timezone.utc)
        if now.date() == last.date():
            return
        self.user_generation_counts.clear()
        self.user_img2vid_counts.clear()
        self.last_reset_time = now.timestamp()
        self._save()
        logger.info("Daily limits reset")

    def increment_daily(self, user_id: str) -> int:
        self.reset_if_needed()
        self.user_generation_counts[user_id] = int(self.user_generation_counts.get(user_id, 0)) + 1
        self._save()
        return self.user_generation_counts[user_id]

    def rollback_daily(self, user_id: str) -> int:
        self.user_generation_counts[user_id] = max(0, int(self.user_generation_counts.get(user_id, 0)) - 1)
        self._save()
        return self.user_generation_counts[user_id]

    def increment_img2vid(self, user_id: str) -> int:
        self.reset_if_needed()
        self.user_img2vid_counts[user_id] = int(self.user_img2vid_counts.get(user_id, 0)) + 1
        self._save()
        return self.user_img2vid_counts[user_id]

    def rollback_img2vid(self, user_id: str) -> int:
        self.user_img2vid_counts[user_id] = max(0, int(self.user_img2vid_counts.get(user_id, 0)) - 1)
        self._save()
        return self.user_img2vid_counts[user_id]

    def record_success(self, user_id: str) -> None:
        stats = self.user_generation_stats.setdefault(user_id, {"total": 0, "daily": {}})
        stats["total"] = int(stats.get("total", 0)) + 1
        daily = stats.setdefault("daily", {})
        today = self._today().isoformat()
        daily[today] = int(daily.get(today, 0)) + 1
        self._prune_history(daily)
        self._save()

    def _prune_history(self, daily: dict[str, int]) -> None:
        cutoff = self._today() - timedelta(days=max(self.retention_days - 1, 0))
        for key in list(daily.keys()):
            try:
                day = datetime.strptime(key, "%Y-%m-%d").date()
            except ValueError:
                daily.pop(key, None)
                continue
            if day < cutoff:
                daily.pop(key, None)

    def get_generation_count(self, user_id: str) -> int:
        self.reset_if_needed()
        return int(self.user_generation_counts.get(user_id, 0))

    def get_img2vid_count(self, user_id: str) -> int:
        self.reset_if_needed()
        return int(self.user_img2vid_counts.get(user_id, 0))

    def summary(self, user_id: str) -> dict[str, int]:
        stats = self.user_generation_stats.get(user_id)
        if not stats:
            return {"day": 0, "week": 0, "month": 0, "total": 0}
        daily = stats.get("daily", {}) or {}
        self._prune_history(daily)
        today = self._today()
        week_cutoff = today - timedelta(days=6)
        month_cutoff = today - timedelta(days=29)
        day_total = week_total = month_total = 0
        for key, value in daily.items():
            try:
                day = datetime.strptime(key, "%Y-%m-%d").date()
            except ValueError:
                continue
            count = int(value)
            if day == today:
                day_total += count
            if week_cutoff <= day <= today:
                week_total += count
            if month_cutoff <= day <= today:
                month_total += count
        return {
            "day": day_total,
            "week": week_total,
            "month": month_total,
            "total": int(stats.get("total", 0)),
        }

    def time_until_reset(self) -> str:
        now = datetime.now(tz=timezone.utc)
        next_reset = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        diff = next_reset - now
        total_seconds = max(0, int(diff.total_seconds()))
        hours, rem = divmod(total_seconds, 3600)
        minutes, _ = divmod(rem, 60)
        return f"{hours}h {minutes}m"
