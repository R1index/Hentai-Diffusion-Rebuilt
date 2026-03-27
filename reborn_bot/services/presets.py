from __future__ import annotations
import json
import re
import uuid
from pathlib import Path
from typing import Optional

from ..logging_setup import logger
from ..models import LoRAPreset, ModelPreset, PromptPreset


class PresetStore:
    def __init__(self, base_dir: Path, prompt_file: str, model_file: str, lora_file: str):
        self.base_dir = base_dir
        self.prompt_presets = self._load_prompt_presets(prompt_file)
        self.model_presets = self._load_model_presets(model_file)
        self.lora_presets = self._load_lora_presets(lora_file)

        self._prompt_by_value = {item.value: item for item in self.prompt_presets}
        self._prompt_by_name = {item.name.lower(): item for item in self.prompt_presets}
        self._model_by_value = {item.value: item for item in self.model_presets}
        self._model_by_name = {item.name.lower(): item for item in self.model_presets}
        self._lora_by_value = {item.value: item for item in self.lora_presets}
        self._lora_by_name = {item.name.lower(): item for item in self.lora_presets}

    def _resolve(self, file_path: str) -> Path:
        path = Path(file_path)
        return path if path.is_absolute() else self.base_dir / path

    def _read_json(self, file_path: str):
        path = self._resolve(file_path)
        if not path.exists():
            logger.warning("Preset file not found: %s", path)
            return []
        with path.open("r", encoding="utf-8") as fh:
            return json.load(fh)

    @staticmethod
    def _slugify(value: str) -> str:
        slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
        return slug or uuid.uuid4().hex

    def _load_prompt_presets(self, file_path: str) -> list[PromptPreset]:
        raw = self._read_json(file_path)
        items: list[PromptPreset] = []
        if isinstance(raw, list):
            for idx, item in enumerate(raw, start=1):
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name") or item.get("title") or f"Preset {idx}")
                tags = str(item.get("tags") or "").strip()
                if tags:
                    items.append(PromptPreset(name=name, tags=tags, value=str(item.get("value") or self._slugify(name))))
        return items

    def _load_model_presets(self, file_path: str) -> list[ModelPreset]:
        raw = self._read_json(file_path)
        items: list[ModelPreset] = []
        if isinstance(raw, list):
            for idx, item in enumerate(raw, start=1):
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name") or item.get("title") or f"Model {idx}")
                model = str(item.get("model") or "").strip()
                if model:
                    items.append(ModelPreset(name=name, model=model, value=str(item.get("value") or self._slugify(name))))
        return items

    def _load_lora_presets(self, file_path: str) -> list[LoRAPreset]:
        raw = self._read_json(file_path)
        items: list[LoRAPreset] = []
        if isinstance(raw, list):
            for idx, item in enumerate(raw, start=1):
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name") or item.get("title") or f"LoRA {idx}")
                lora = str(item.get("lora") or "").strip()
                if lora:
                    items.append(LoRAPreset(name=name, lora=lora, value=str(item.get("value") or self._slugify(name))))
        return items

    @staticmethod
    def _search(items, query: str, attrs: tuple[str, ...], limit: int):
        normalized = (query or "").strip().lower()
        if not normalized:
            return items[:limit]

        def score(item) -> tuple[int, str]:
            haystacks = [str(getattr(item, attr, "")).lower() for attr in attrs]
            priority = 0
            for hay in haystacks:
                if hay.startswith(normalized):
                    priority = max(priority, 3)
                elif normalized in hay:
                    priority = max(priority, 2)
            return (-priority, haystacks[0] if haystacks else "")

        filtered = [item for item in items if any(normalized in str(getattr(item, attr, "")).lower() for attr in attrs)]
        if not filtered:
            return items[:limit]
        return sorted(filtered, key=score)[:limit]

    def search_prompt_presets(self, query: str, limit: int = 25) -> list[PromptPreset]:
        return self._search(self.prompt_presets, query, ("name", "tags"), limit)

    def search_model_presets(self, query: str, limit: int = 25) -> list[ModelPreset]:
        return self._search(self.model_presets, query, ("name", "model"), limit)

    def search_lora_presets(self, query: str, limit: int = 25) -> list[LoRAPreset]:
        return self._search(self.lora_presets, query, ("name", "lora"), limit)

    def apply_prompt_preset(self, preset_value: Optional[str], prompt: Optional[str]) -> tuple[str, Optional[str], Optional[str]]:
        if not preset_value:
            return prompt or "", None, None
        key = preset_value.strip().lower()
        preset = self._prompt_by_value.get(preset_value) or self._prompt_by_name.get(key)
        if not preset:
            logger.warning("Prompt preset '%s' not found", preset_value)
            return prompt or "", None, None
        return preset.apply(prompt), preset.name, preset.tags

    def apply_model_preset(self, preset_value: Optional[str]) -> tuple[Optional[str], Optional[str]]:
        if not preset_value:
            return None, None
        key = preset_value.strip().lower()
        preset = self._model_by_value.get(preset_value) or self._model_by_name.get(key)
        if not preset:
            logger.warning("Model preset '%s' not found", preset_value)
            return None, None
        return preset.model, preset.name

    def apply_lora_preset(self, preset_value: Optional[str]) -> tuple[Optional[str], Optional[str]]:
        if not preset_value:
            return None, None
        key = preset_value.strip().lower()
        preset = self._lora_by_value.get(preset_value) or self._lora_by_name.get(key)
        if not preset:
            logger.warning("LoRA preset '%s' not found", preset_value)
            return None, None
        return preset.lora, preset.name
