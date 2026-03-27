from __future__ import annotations

import logging
import os
import sys
from datetime import datetime

logger = logging.getLogger("reborn_bot")


class ConsoleFormatter(logging.Formatter):
    RESET = "\x1b[0m"
    DIM = "\x1b[2m"
    COLORS = {
        logging.DEBUG: "\x1b[38;5;245m",
        logging.INFO: "\x1b[38;5;39m",
        logging.WARNING: "\x1b[38;5;220m",
        logging.ERROR: "\x1b[38;5;203m",
        logging.CRITICAL: "\x1b[1;38;5;196m",
    }
    ICONS = {
        logging.DEBUG: "…",
        logging.INFO: "•",
        logging.WARNING: "!",
        logging.ERROR: "x",
        logging.CRITICAL: "X",
    }

    def __init__(self, use_color: bool = True):
        super().__init__()
        self.use_color = use_color

    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created).strftime("%H:%M:%S")
        level = record.levelname.ljust(8)
        icon = self.ICONS.get(record.levelno, "•")
        name = record.name
        message = record.getMessage()
        base = f"{ts} | {level} | {name} | {icon} {message}"

        if record.exc_info:
            base = f"{base}\n{self.formatException(record.exc_info)}"

        if not self.use_color:
            return base

        color = self.COLORS.get(record.levelno, "")
        return f"{self.DIM}{ts}{self.RESET} | {color}{level}{self.RESET} | {self.DIM}{name}{self.RESET} | {color}{icon}{self.RESET} {message}" + (
            f"\n{color}{self.formatException(record.exc_info)}{self.RESET}" if record.exc_info else ""
        )


def format_user(user: object | None) -> str:
    if user is None:
        return "<unknown-user>"

    username = str(getattr(user, "name", "") or "").strip()
    global_name = str(getattr(user, "global_name", "") or "").strip()
    display_name = str(getattr(user, "display_name", "") or "").strip()

    if global_name and username and global_name != username:
        return f"{global_name} (@{username})"
    if display_name and username and display_name != username:
        return f"{display_name} (@{username})"
    if username:
        return f"@{username}"
    if global_name:
        return global_name
    if display_name:
        return display_name
    return "<unknown-user>"


def _resolve_level(default: int) -> int:
    raw = str(os.getenv("LOG_LEVEL", "")).strip().upper()
    if not raw:
        return default
    return getattr(logging, raw, default)


def configure_logging(level: int = logging.INFO) -> None:
    resolved_level = _resolve_level(level)
    use_color = sys.stdout.isatty() and os.getenv("NO_COLOR") is None

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(ConsoleFormatter(use_color=use_color))

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(resolved_level)
    root.addHandler(handler)

    for noisy_name, noisy_level in {
        "discord": logging.WARNING,
        "websockets": logging.WARNING,
        "aiohttp.access": logging.WARNING,
    }.items():
        noisy_logger = logging.getLogger(noisy_name)
        noisy_logger.setLevel(max(resolved_level, noisy_level))

    logger.debug("Logging configured at level %s", logging.getLevelName(resolved_level))
