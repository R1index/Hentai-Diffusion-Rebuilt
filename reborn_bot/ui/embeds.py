from __future__ import annotations

from typing import Iterable, Optional

import discord

ACCENT_COLOR = 0x5865F2
PROGRESS_COLOR = 0x3498DB
SUCCESS_COLOR = 0x57F287
WARNING_COLOR = 0xFEE75C
ERROR_COLOR = 0xED4245


def _code(value: Optional[str], limit: int) -> Optional[str]:
    if not value:
        return None
    compact = value.strip()
    if len(compact) > limit:
        compact = compact[: max(0, limit - 1)].rstrip() + "…"
    return f"```{compact}```"


def _normalize_field_value(value: str, limit: int = 1024) -> str:
    value = (value or "").strip() or "—"
    if len(value) > limit:
        return value[: max(0, limit - 1)].rstrip() + "…"
    return value


def build_generation_embed(
    *,
    title: str,
    user: discord.abc.User,
    workflow_name: str,
    status: str,
    footer_text: str,
    color: int = ACCENT_COLOR,
    prompt: Optional[str] = None,
    settings: Optional[str] = None,
    usage: Optional[str] = None,
    extra_fields: Optional[Iterable[tuple[str, str, bool]]] = None,
) -> discord.Embed:
    embed = discord.Embed(
        title=title,
        description=f"**Workflow** · `{workflow_name}`\n**Requested by** · {user.mention}",
        color=color,
        timestamp=discord.utils.utcnow(),
    )
    embed.set_author(name=getattr(user, "display_name", getattr(user, "name", "User")), icon_url=user.display_avatar.url)
    embed.add_field(name="📊 Status", value=_normalize_field_value(status), inline=False)

    if extra_fields:
        for name, value, inline in extra_fields:
            embed.add_field(name=name, value=_normalize_field_value(value), inline=inline)

    if prompt:
        embed.add_field(name="🧠 Prompt", value=_code(prompt, 980), inline=False)
    if settings:
        embed.add_field(name="⚙️ Settings", value=_code(settings, 460), inline=False)
    if usage:
        embed.add_field(name="📈 Usage", value=_normalize_field_value(usage), inline=False)

    embed.set_footer(text=footer_text)
    return embed


def build_notice_embed(*, title: str, description: str, footer_text: str, color: int = ACCENT_COLOR) -> discord.Embed:
    embed = discord.Embed(title=title, description=description, color=color, timestamp=discord.utils.utcnow())
    embed.set_footer(text=footer_text)
    return embed


def format_usage_bar(used: int, total: int, reset_hint: str) -> str:
    total = max(total, 1)
    used = max(0, min(used, total))
    filled = round((used / total) * 12)
    bar = "█" * filled + "░" * (12 - filled)
    return f"`{used}/{total}` • resets in {reset_hint}\n`{bar}`"
