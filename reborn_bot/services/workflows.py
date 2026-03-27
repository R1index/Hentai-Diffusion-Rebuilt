from __future__ import annotations
import copy
import json
import random
import re
import uuid
from pathlib import Path
from typing import Any, Optional

from ..config import AppSettings
from ..logging_setup import logger


class WorkflowService:
    def __init__(self, settings: AppSettings):
        self.settings = settings
        self.base_dir = settings.path.parent
        self.input_dir = Path(settings.comfyui.input_dir)
        if not self.input_dir.is_absolute():
            self.input_dir = self.base_dir / self.input_dir
        self.input_dir.mkdir(parents=True, exist_ok=True)

    def get_workflow(self, name: str) -> Optional[dict[str, Any]]:
        cfg = self.settings.workflows.get(name)
        if cfg:
            cfg = dict(cfg)
            cfg["name"] = name
        return cfg

    def get_workflows_by_type(self, workflow_type: Optional[str] = None) -> dict[str, dict]:
        result: dict[str, dict] = {}
        for name, cfg in self.settings.workflows.items():
            if workflow_type and cfg.get("type", "txt2img") != workflow_type:
                continue
            result[name] = cfg
        return result

    def search_workflows(self, workflow_type: str, current: str, limit: int = 25) -> list[tuple[str, str]]:
        query = (current or "").strip().lower()
        matches: list[tuple[str, str]] = []
        for name, cfg in self.get_workflows_by_type(workflow_type).items():
            description = str(cfg.get("description", "") or "")
            haystack = f"{name} {description}".lower()
            if query and query not in haystack:
                continue
            label = name if not description else f"{name} — {description}"
            if len(label) > 100:
                label = label[:97].rstrip() + "…"
            matches.append((label, name))
        matches.sort(key=lambda item: item[1].lower())
        return matches[:limit]

    def get_default_workflow(self, workflow_type: str) -> Optional[str]:
        fallback_name: Optional[str] = None
        explicit_defaults: list[str] = []

        for name, cfg in self.settings.workflows.items():
            if cfg.get("type", "txt2img") != workflow_type:
                continue
            if fallback_name is None:
                fallback_name = name
            if cfg.get("default"):
                explicit_defaults.append(name)

        if len(explicit_defaults) > 1:
            logger.warning(
                "Multiple default workflows configured for type=%s: %s. Using the first one.",
                workflow_type,
                ", ".join(explicit_defaults),
            )
        return explicit_defaults[0] if explicit_defaults else fallback_name

    def list_resolution_choices(self) -> list[tuple[str, str]]:
        presets = []
        for item in self.settings.resolutions:
            if isinstance(item, dict):
                label = str(item.get("label") or item.get("name") or item.get("value"))
                value = str(item.get("value") or item.get("label") or item.get("name"))
            else:
                label = value = str(item)
            presets.append((label, value))
        return presets

    def workflow_outputs_video(self, workflow_name: str, workflow_json: dict[str, Any]) -> bool:
        workflow_cfg = self.get_workflow(workflow_name) or {}
        explicit = workflow_cfg.get("outputs_video")
        if explicit is not None:
            return bool(explicit)

        for node in workflow_json.values():
            if not isinstance(node, dict):
                continue
            class_type = str(node.get("class_type", "")).lower()
            if not class_type:
                continue

            # Не считаем input-loader'ы видеовыходом.
            if any(token in class_type for token in ("loadimage", "loadvideo", "loadgif")):
                continue

            if any(token in class_type for token in ("videocombine", "savevideo", "savegif", "saveanimated", "videooutput")):
                return True
            if "output" in class_type and any(token in class_type for token in ("video", "gif")):
                return True
            if any(token in class_type for token in ("wanimagetovideo", "imagetovideo")):
                return True

        return workflow_name.upper() == "IMG2VID"

    def prepare_workflow(
        self,
        workflow_name: str,
        prompt: Optional[str],
        settings: Optional[str],
        resolution: Optional[str],
        image_data: Optional[bytes],
        seed: Optional[int],
        controlnet_strength: Optional[float],
        temp_files: Optional[list[Path]] = None,
    ) -> tuple[dict[str, Any], int]:
        workflow_cfg = self.get_workflow(workflow_name)
        if not workflow_cfg:
            raise ValueError(f"Workflow '{workflow_name}' not found")

        workflow_file = workflow_cfg["workflow"]
        workflow_path = Path(workflow_file)
        if not workflow_path.is_absolute():
            workflow_path = self.base_dir / workflow_path
        workflow_json = json.loads(workflow_path.read_text(encoding="utf-8"))
        workflow_json = copy.deepcopy(workflow_json)

        self._apply_prompt(workflow_json, workflow_cfg, prompt)
        self._apply_image(workflow_json, workflow_cfg, image_data, temp_files=temp_files)
        self._apply_resolution(workflow_json, workflow_cfg, workflow_name, resolution)
        self._apply_settings(workflow_json, workflow_cfg, settings)
        self._apply_controlnet_strength(workflow_json, workflow_cfg, workflow_name, controlnet_strength)

        actual_seed = self._apply_seed(workflow_json, workflow_cfg, workflow_name, seed)
        return workflow_json, actual_seed

    def _apply_prompt(self, workflow_json: dict[str, Any], workflow_cfg: dict[str, Any], prompt: Optional[str]) -> None:
        node_id = workflow_cfg.get("text_prompt_node_id")
        if node_id is None or not prompt:
            return
        node = workflow_json.get(str(node_id))
        if not node:
            return
        inputs = node.setdefault("inputs", {})
        if "text" in inputs:
            inputs["text"] = prompt
        elif "prompt" in inputs:
            inputs["prompt"] = prompt

    def _apply_image(
        self,
        workflow_json: dict[str, Any],
        workflow_cfg: dict[str, Any],
        image_data: Optional[bytes],
        *,
        temp_files: Optional[list[Path]] = None,
    ) -> None:
        node_id = workflow_cfg.get("image_input_node_id")
        if node_id is None or not image_data:
            return
        node = workflow_json.get(str(node_id))
        if not node:
            raise ValueError(f"Image node '{node_id}' not found in workflow")

        filename = f"input_{uuid.uuid4().hex}.png"
        file_path = self.input_dir / filename
        file_path.write_bytes(image_data)
        if temp_files is not None:
            temp_files.append(file_path)

        inputs = node.setdefault("inputs", {})
        class_type = str(node.get("class_type", ""))
        if "image" in inputs:
            inputs["image"] = str(file_path) if class_type == "VHS_LoadImagePath" else filename
        elif "path" in inputs:
            inputs["path"] = str(file_path)

    def _apply_resolution(self, workflow_json: dict[str, Any], workflow_cfg: dict[str, Any], workflow_name: str, resolution: Optional[str]) -> None:
        if not resolution:
            resolution = workflow_cfg.get("default_resolution")
        if not resolution:
            return

        node_id = workflow_cfg.get("resolution_node_id")
        if node_id is None:
            return

        node = workflow_json.get(str(node_id))
        if not node:
            logger.warning("Resolution node %s not found for workflow %s", node_id, workflow_name)
            return

        inputs = node.setdefault("inputs", {})
        if "resolution" in inputs:
            inputs["resolution"] = resolution

        match = re.search(r"(\d+)\s*x\s*(\d+)", resolution.lower())
        if not match:
            return

        width, height = int(match.group(1)), int(match.group(2))
        orientation = "square" if width == height else "landscape" if width > height else "portrait"

        if "dimensions" in inputs:
            inputs["dimensions"] = f"{width:>4} x {height:<4}  ({orientation})"
        if "width" in inputs:
            inputs["width"] = width
        if "height" in inputs:
            inputs["height"] = height

    def _apply_seed(self, workflow_json: dict[str, Any], workflow_cfg: dict[str, Any], workflow_name: str, seed: Optional[int]) -> int:
        node_id = workflow_cfg.get("seed_node_id")
        if node_id is None:
            node_id = self._find_seed_node(workflow_json)

        actual_seed = int(seed) if seed is not None else random.randint(0, 2**32 - 1)
        if node_id is None:
            logger.warning("Seed node not found for workflow %s", workflow_name)
            return actual_seed

        node = workflow_json.get(str(node_id))
        if node and isinstance(node.get("inputs"), dict):
            node["inputs"]["seed"] = actual_seed
        return actual_seed

    @staticmethod
    def _find_seed_node(workflow_json: dict[str, Any]) -> Optional[str]:
        for key, node in workflow_json.items():
            if isinstance(node, dict) and isinstance(node.get("inputs"), dict) and "seed" in node["inputs"]:
                return key
        return None

    def _apply_controlnet_strength(self, workflow_json: dict[str, Any], workflow_cfg: dict[str, Any], workflow_name: str, controlnet_strength: Optional[float]) -> None:
        if controlnet_strength is None:
            return
        node_id = workflow_cfg.get("controlnet_strength_node_id")
        if node_id is None:
            return
        node = workflow_json.get(str(node_id))
        if not node or not isinstance(node.get("inputs"), dict):
            logger.warning("ControlNet node %s not found for workflow %s", node_id, workflow_name)
            return
        node["inputs"]["strength"] = float(controlnet_strength)

    def _find_setting_def(self, workflow_cfg: dict[str, Any], setting_name: str) -> Optional[dict[str, Any]]:
        for item in workflow_cfg.get("settings", []) or []:
            if item.get("name") == setting_name:
                return item
        return None

    def _apply_settings(self, workflow_json: dict[str, Any], workflow_cfg: dict[str, Any], settings_str: Optional[str]) -> None:
        before = self._find_setting_def(workflow_cfg, "__before")
        if before:
            self._execute_setting(workflow_json, "__before", before, [])

        if settings_str:
            for chunk in [part.strip() for part in settings_str.split(";") if part.strip()]:
                if "(" in chunk and chunk.endswith(")"):
                    func_name = chunk[:chunk.index("(")].strip()
                    params = [item.strip() for item in chunk[chunk.index("(") + 1 : -1].split(",") if item.strip()]
                else:
                    func_name = chunk.strip()
                    params = []
                setting_def = self._find_setting_def(workflow_cfg, func_name)
                if not setting_def:
                    logger.warning("Setting '%s' was not found in workflow config", func_name)
                    continue
                self._execute_setting(workflow_json, func_name, setting_def, params)

        after = self._find_setting_def(workflow_cfg, "__after")
        if after:
            self._execute_setting(workflow_json, "__after", after, [])

    def _execute_setting(self, workflow_json: dict[str, Any], setting_name: str, setting_def: dict[str, Any], params: list[str]) -> None:
        code = setting_def.get("code")
        if not code:
            return
        namespace: dict[str, Any] = {}
        exec(code, namespace, namespace)
        fn = namespace.get(setting_name)
        if not callable(fn):
            raise ValueError(f"Setting '{setting_name}' does not define a callable with the same name")
        fn(workflow_json, *params)
