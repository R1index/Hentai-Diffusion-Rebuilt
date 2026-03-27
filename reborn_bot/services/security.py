from __future__ import annotations
import re
from typing import Optional

import discord

from ..config import AppSettings
from ..logging_setup import format_user, logger
from ..models import RoleTier


class SecurityService:
    def __init__(self, settings: AppSettings):
        self.settings = settings
        self.blocked_users = set(settings.security.blocked_users)
        self.donor_users = set(settings.security.donor_users)
        self.role_tiers = list(settings.security.role_tiers)

    async def get_access_guild(self, client: discord.Client) -> Optional[discord.Guild]:
        guild_id = self.settings.security.access_guild_id
        if not guild_id:
            return None

        guild = client.get_guild(guild_id)
        if guild is not None:
            return guild

        try:
            return await client.fetch_guild(guild_id)
        except discord.HTTPException:
            return None

    async def get_access_member(self, interaction: discord.Interaction) -> Optional[discord.Member]:
        guild_id = self.settings.security.access_guild_id
        if not guild_id:
            return interaction.user if isinstance(interaction.user, discord.Member) else None

        guild = await self.get_access_guild(interaction.client)
        if guild is None:
            return None

        member = guild.get_member(interaction.user.id)
        if member is not None:
            return member

        try:
            return await guild.fetch_member(interaction.user.id)
        except discord.HTTPException:
            return None

    async def resolve_access_member(self, client: discord.Client, raw_value: str) -> Optional[discord.Member]:
        guild = await self.get_access_guild(client)
        if guild is None:
            return None

        value = str(raw_value or "").strip()
        if not value:
            return None

        match = re.fullmatch(r"<@!?(\d+)>", value)
        lookup_id = match.group(1) if match else value if value.isdigit() else None
        if lookup_id:
            member = guild.get_member(int(lookup_id))
            if member is not None:
                return member
            try:
                return await guild.fetch_member(int(lookup_id))
            except discord.HTTPException:
                return None

        normalized = value.casefold()
        exact_matches = []
        partial_matches = []

        for member in guild.members:
            identifiers = {
                str(member.id),
                getattr(member, "name", "") or "",
                getattr(member, "global_name", "") or "",
                getattr(member, "display_name", "") or "",
                str(member),
            }
            normalized_identifiers = {item.strip().casefold() for item in identifiers if item}
            if normalized in normalized_identifiers:
                exact_matches.append(member)
                continue
            if any(normalized in item for item in normalized_identifiers if item):
                partial_matches.append(member)

        if exact_matches:
            return exact_matches[0]
        if partial_matches:
            return partial_matches[0]

        try:
            queried = await guild.query_members(query=value, limit=10)
        except (discord.Forbidden, discord.HTTPException, TypeError):
            queried = []

        for member in queried:
            identifiers = {
                getattr(member, "name", "") or "",
                getattr(member, "global_name", "") or "",
                getattr(member, "display_name", "") or "",
                str(member),
            }
            normalized_identifiers = {item.strip().casefold() for item in identifiers if item}
            if normalized in normalized_identifiers:
                return member

        return queried[0] if queried else None

    async def is_blocked(self, user_id: str) -> bool:
        return user_id in self.blocked_users

    async def is_member_of_access_guild(self, interaction: discord.Interaction) -> bool:
        if not self.settings.security.access_guild_id:
            return True
        return await self.get_access_member(interaction) is not None

    async def determine_tier(self, interaction: discord.Interaction) -> RoleTier:
        member = await self.get_access_member(interaction)
        role_ids = {role.id for role in getattr(member, "roles", [])}
        highest = next((tier for tier in reversed(sorted(self.role_tiers, key=lambda item: item.level)) if tier.role_id is None), None)
        highest = highest or RoleTier(0, "Public", None, 25, 0, 1)

        for tier in self.role_tiers:
            if tier.role_id and tier.role_id in role_ids and tier.level > highest.level:
                highest = tier

        if str(interaction.user.id) in self.donor_users and highest.daily_limit is not None:
            return RoleTier(
                level=max(highest.level, 2),
                name="Donor",
                role_id=None,
                daily_limit=None,
                queue_priority=max(highest.queue_priority, 0),
                max_parallel_generations=max(highest.max_parallel_generations, 1),
            )
        return highest

    async def has_img2img_access(self, interaction: discord.Interaction) -> bool:
        allowed = set(self.settings.security.img2img_allowed_role_ids)
        if not allowed:
            return True
        member = await self.get_access_member(interaction)
        if not member:
            return False
        role_ids = {role.id for role in getattr(member, "roles", [])}
        return bool(role_ids.intersection(allowed))

    async def get_img2vid_daily_limit(self, interaction: discord.Interaction) -> Optional[int]:
        member = await self.get_access_member(interaction)
        if not member:
            return None
        role_ids = {str(role.id) for role in getattr(member, "roles", [])}
        matched_limits = [
            int(limit)
            for role_id, limit in self.settings.security.img2vid_daily_limits.items()
            if role_id in role_ids
        ]
        if not matched_limits:
            return None
        return max(matched_limits)

    @staticmethod
    def _normalize_member_identifiers(member: discord.abc.User) -> tuple[set[str], set[str]]:
        identifiers = {
            str(member.id),
            getattr(member, "name", "") or "",
            getattr(member, "global_name", "") or "",
            getattr(member, "display_name", "") or "",
        }
        identifiers = {item.strip().lower() for item in identifiers if item}
        role_names = {
            str(getattr(role, "name", "")).strip().lower()
            for role in getattr(member, "roles", [])
            if getattr(role, "name", None)
        }
        return identifiers, role_names

    def validate_workflow_access(self, member: discord.abc.User, workflow_config: dict, settings_str: Optional[str]) -> tuple[bool, str]:
        security_cfg = workflow_config.get("security", {}) or {}
        if not self._check_permissions(member, security_cfg):
            return False, f"You don't have permission to use the '{workflow_config.get('name', 'workflow')}' workflow"

        if not settings_str:
            return True, ""

        for raw_setting in [item.strip() for item in settings_str.split(";") if item.strip()]:
            name = raw_setting.split("(")[0].strip()
            if name in {"__before", "__after"}:
                continue
            setting_cfg = next((item for item in workflow_config.get("settings", []) if item.get("name") == name), None)
            if not setting_cfg:
                continue
            if not self._check_permissions(member, setting_cfg.get("security", {}) or {}):
                return False, f"You don't have permission to use the '{name}' setting"
        return True, ""

    def _check_permissions(self, member: discord.abc.User, security_cfg: dict) -> bool:
        if not security_cfg or not security_cfg.get("enabled", False):
            return True

        identifiers, role_names = self._normalize_member_identifiers(member)

        allowed_users = {str(item).strip().lower() for item in security_cfg.get("allowed_users", []) if item}
        if identifiers & allowed_users:
            return True

        allowed_roles = {str(item).strip().lower() for item in security_cfg.get("allowed_roles", []) if item}
        if role_names & allowed_roles:
            return True

        logger.info("Permission denied for member=%s security=%s", format_user(member), security_cfg)
        return False
