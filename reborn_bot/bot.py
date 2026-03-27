from __future__ import annotations
import asyncio
import io
import re
import time
from datetime import datetime, timezone
from collections import defaultdict
from pathlib import Path
from typing import Any, Optional

import discord
from discord import app_commands
from discord.ext import commands
from PIL import Image, UnidentifiedImageError

from .commands import build_generation_commands
from .config import AppSettings
from .logging_setup import format_user, logger
from .models import GenerationRequest, GenerationSession, RoleTier, StoredRequest
from .services.comfy import ComfyUIClient
from .services.bot_sync import BotSyncService, SyncState
from .services.manual_subscriptions import ManualSubscriptionStore
from .services.presets import PresetStore
from .services.queueing import PriorityGenerationQueue
from .services.security import SecurityService
from .services.usage import UsageStore
from .services.workflows import WorkflowService
from .ui import embeds as ui_embeds
from .ui.views import GenerationView


class RebornComfyBot(commands.Bot):
    def __init__(self, settings: AppSettings):
        intents = discord.Intents.default()
        intents.guilds = True
        intents.members = True
        intents.message_content = True

        super().__init__(command_prefix="/", intents=intents)

        self.settings = settings
        self.workflow_service = WorkflowService(settings)
        self.presets = PresetStore(settings.path.parent, settings.presets.prompt_file, settings.presets.model_file, settings.presets.lora_file)
        self.usage = UsageStore((settings.path.parent / settings.storage.usage_file).resolve(), settings.limits.retention_days)
        self.manual_subscriptions = ManualSubscriptionStore((settings.path.parent / settings.storage.manual_subscriptions_file).resolve())
        self.security = SecurityService(settings)
        self.comfy = ComfyUIClient(settings.comfyui.instances)
        self.queue = PriorityGenerationQueue(settings.comfyui.queue_workers)
        self.sync = BotSyncService(settings)

        self.active_sessions: dict[str, list[GenerationSession]] = defaultdict(list)
        self.last_requests: dict[str, StoredRequest] = {}
        self._user_locks: dict[str, asyncio.Lock] = {}
        self._spoiler_tags = {self._normalize_tag(tag): str(tag).strip() for tag in settings.spoilers if str(tag).strip()}

        self._queue_refresh_task: Optional[asyncio.Task] = None
        self._manual_subscription_cleanup_task: Optional[asyncio.Task] = None
        self._queue_refresh_pending = False
        self._manual_subscription_lock = asyncio.Lock()

        self.queue.set_update_callback(self._schedule_queue_refresh)
        logger.info("Bot services initialized | workflows=%s prompt_presets=%s model_presets=%s lora_presets=%s workers=%s", len(self.settings.workflows), len(self.presets.prompt_presets), len(self.presets.model_presets), len(self.presets.lora_presets), self.settings.comfyui.queue_workers)

    async def setup_hook(self) -> None:
        await self.comfy.connect()
        await self.sync.start(self)
        await self.queue.start()
        logger.info("Setup hook completed core startup")

        for cmd in build_generation_commands(self):
            self.tree.add_command(cmd)

        self.tree.add_command(self._build_workflows_command())
        self.tree.add_command(self._build_limits_command())
        self.tree.add_command(self._build_profile_command())
        self.tree.add_command(self._build_cancel_command())
        self.tree.add_command(self._build_spoiler_group())
        self.tree.add_command(self._build_subscription_group())

        if self._manual_subscription_cleanup_task is None or self._manual_subscription_cleanup_task.done():
            self._manual_subscription_cleanup_task = asyncio.create_task(self._manual_subscription_cleanup_loop())

        synced = await self.tree.sync()
        logger.info("Synced %s commands", len(synced))

    async def close(self) -> None:
        sessions = [session for group in self.active_sessions.values() for session in group]
        for session in sessions:
            try:
                await self._cancel_session(session, source="shutdown")
            except Exception as exc:
                logger.debug("Failed to cancel session during shutdown for user=%s: %s", format_user(session.user), exc)
        if self._queue_refresh_task and not self._queue_refresh_task.done():
            self._queue_refresh_task.cancel()
            try:
                await self._queue_refresh_task
            except asyncio.CancelledError:
                pass
        if self._manual_subscription_cleanup_task and not self._manual_subscription_cleanup_task.done():
            self._manual_subscription_cleanup_task.cancel()
            try:
                await self._manual_subscription_cleanup_task
            except asyncio.CancelledError:
                pass
        await self.sync.stop(self)
        await self.queue.stop()
        await self.comfy.close()
        await super().close()

    async def on_ready(self) -> None:
        logger.info("Bot ready: %s", self.user)

    def _get_user_lock(self, user_id: str) -> asyncio.Lock:
        lock = self._user_locks.get(user_id)
        if lock is None:
            lock = asyncio.Lock()
            self._user_locks[user_id] = lock
        return lock

    def _get_sync_session_id(self, session: GenerationSession) -> str:
        sync_id = str(session.metadata.get("sync_session_id", "")).strip()
        if not sync_id:
            sync_id = f"{self.sync.instance_id}:{session.interaction.id}"
            session.metadata["sync_session_id"] = sync_id
        return sync_id

    async def _get_synced_state(self, *, force: bool = False) -> SyncState:
        return await self.sync.get_state(self, force=force)

    async def _get_synced_generation_usage(self, user_id: str, *, force: bool = False) -> tuple[int, int]:
        state = await self._get_synced_state(force=force)
        committed = int(state.generation_committed_today.get(user_id, 0))
        reserved = int(state.generation_reserved_today.get(user_id, 0))
        return committed, reserved

    async def _get_synced_img2vid_usage(self, user_id: str, *, force: bool = False) -> tuple[int, int]:
        state = await self._get_synced_state(force=force)
        committed = int(state.img2vid_committed_today.get(user_id, 0))
        reserved = int(state.img2vid_reserved_today.get(user_id, 0))
        return committed, reserved

    async def _register_synced_session(self, session: GenerationSession, *, is_img2vid: bool, img2vid_limit: Optional[int]) -> tuple[bool, Optional[str], SyncState]:
        sync_id = self._get_sync_session_id(session)
        state = await self.sync.register_queue_entry(
            self,
            session_id=sync_id,
            user_id=session.user_id,
            priority=-session.tier.queue_priority,
            is_img2vid=is_img2vid,
        )

        queue_position = state.queue_position(sync_id)
        if queue_position is not None:
            session.metadata["queue_position"] = queue_position

        synced_parallel = int(state.active_by_user.get(session.user_id, 0))
        if synced_parallel > session.tier.max_parallel_generations:
            previous = max(0, synced_parallel - 1)
            return False, (
                f"You already have **{previous}** active/queued requests across synced bots. "
                f"Your tier allows **{session.tier.max_parallel_generations}**."
            ), state

        if session.tier.daily_limit is not None:
            synced_total = int(state.generation_committed_today.get(session.user_id, 0)) + int(state.generation_reserved_today.get(session.user_id, 0))
            if synced_total > session.tier.daily_limit:
                used_before = max(0, synced_total - 1)
                return False, (
                    f"Daily limit reached across synced bots: **{min(used_before, session.tier.daily_limit)}/{session.tier.daily_limit}**. "
                    f"Reset in {self.usage.time_until_reset()}."
                ), state

        if is_img2vid and img2vid_limit is not None:
            synced_img2vid_total = int(state.img2vid_committed_today.get(session.user_id, 0)) + int(state.img2vid_reserved_today.get(session.user_id, 0))
            if synced_img2vid_total > img2vid_limit:
                used_before = max(0, synced_img2vid_total - 1)
                return False, f"IMG2VID daily limit reached across synced bots: **{min(used_before, img2vid_limit)}/{img2vid_limit}**.", state

        return True, None, state

    async def _leave_synced_queue(self, session: GenerationSession, *, reason: str) -> None:
        if not self.sync.enabled or session.metadata.get("sync_left"):
            return
        sync_id = str(session.metadata.get("sync_session_id", "")).strip()
        if not sync_id:
            return
        await self.sync.leave_queue(self, session_id=sync_id, reason=reason)
        session.metadata["sync_left"] = True

    # --------------------------------------------------------------
    # Commands
    # --------------------------------------------------------------
    def _build_workflows_command(self):
        @app_commands.command(name="workflows", description="List available workflows")
        async def workflows(interaction: discord.Interaction, type: Optional[str] = None) -> None:
            items = self.workflow_service.get_workflows_by_type(type)
            embed = discord.Embed(title="📋 Available Workflows", color=ui_embeds.ACCENT_COLOR)
            if type:
                embed.description = f"Showing only `{type}` workflows"
            if not items:
                embed.description = (embed.description + "\n\n" if embed.description else "") + "No workflows found."
            for name, cfg in items.items():
                embed.add_field(
                    name=name,
                    value=cfg.get("description", "No description"),
                    inline=False,
                )
            await interaction.response.send_message(embed=embed, ephemeral=True)
        return workflows

    def _build_limits_command(self):
        @app_commands.command(name="limits", description="Show your current daily limits")
        async def limits(interaction: discord.Interaction) -> None:
            self.usage.reset_if_needed()
            tier = await self.security.determine_tier(interaction)
            user_id = str(interaction.user.id)
            used, reserved = await self._get_synced_generation_usage(user_id, force=True)
            footer = self.settings.discord.footer_text

            if tier.daily_limit is None:
                embed = ui_embeds.build_notice_embed(
                    title="💎 Unlimited access",
                    description=(
                        f"Tier: **{tier.name}**\n"
                        f"Queue priority: **{tier.queue_priority}**\n"
                        f"Parallel slots: **{tier.max_parallel_generations}**"
                    ),
                    footer_text=footer,
                    color=ui_embeds.SUCCESS_COLOR,
                )
            else:
                description = ui_embeds.format_usage_bar(used, tier.daily_limit, self.usage.time_until_reset())
                if reserved:
                    description += f"\nPending across synced bots: **{reserved}**"
                embed = ui_embeds.build_notice_embed(
                    title="📊 Daily usage",
                    description=description,
                    footer_text=footer,
                    color=ui_embeds.WARNING_COLOR,
                )
            await interaction.response.send_message(embed=embed, ephemeral=True)
        return limits

    def _build_profile_command(self):
        @app_commands.command(name="profile", description="Show your usage and tier")
        async def profile(interaction: discord.Interaction) -> None:
            user_id = str(interaction.user.id)
            stats = self.usage.summary(user_id)
            tier = await self.security.determine_tier(interaction)
            img2vid_limit = await self.security.get_img2vid_daily_limit(interaction)
            synced_used, synced_reserved = await self._get_synced_generation_usage(user_id, force=True)
            img2vid_used, img2vid_reserved = await self._get_synced_img2vid_usage(user_id, force=True)
            embed = discord.Embed(title="👤 User Profile", description=f"Stats for {interaction.user.mention}", color=ui_embeds.ACCENT_COLOR)

            if interaction.user.display_avatar:
                embed.set_thumbnail(url=interaction.user.display_avatar.url)

            embed.add_field(
                name="📈 Generations",
                value=(
                    f"Today: **{synced_used}**\n"
                    f"7 days: **{stats['week']}**\n"
                    f"30 days: **{stats['month']}**\n"
                    f"All time: **{stats['total']}**"
                ),
                inline=False,
            )
            embed.add_field(
                name="🎚 Access",
                value=(
                    f"Tier: **{tier.name}**\n"
                    f"Queue priority: **{tier.queue_priority}**\n"
                    f"Parallel slots: **{tier.max_parallel_generations}**\n"
                    f"Daily limit: **{'Unlimited' if tier.daily_limit is None else tier.daily_limit}**"
                ),
                inline=False,
            )
            if img2vid_limit is not None:
                img2vid_value = f"Used today: **{img2vid_used}/{img2vid_limit}**"
                if img2vid_reserved:
                    img2vid_value += f"\nPending across synced bots: **{img2vid_reserved}**"
                embed.add_field(
                    name="🎬 IMG2VID",
                    value=img2vid_value,
                    inline=False,
                )
            if synced_reserved:
                embed.add_field(
                    name="🕒 Synced queue",
                    value=f"Reserved generations today: **{synced_reserved}**",
                    inline=False,
                )
            embed.set_footer(text=self.settings.discord.footer_text)
            await interaction.response.send_message(embed=embed, ephemeral=True)
        return profile

    def _build_cancel_command(self):
        @app_commands.command(name="cancel", description="Cancel your active generation")
        async def cancel(interaction: discord.Interaction, cancel_all: bool = False) -> None:
            sessions = list(self.active_sessions.get(str(interaction.user.id), []))
            if not sessions:
                await interaction.response.send_message("You have no active generations.", ephemeral=True)
                return

            targets = sessions if cancel_all else [sessions[-1]]
            cancelled = 0
            for session in targets:
                if await self._cancel_session(session, source="command"):
                    cancelled += 1

            await interaction.response.send_message(
                f"Cancelled {cancelled} generation{'s' if cancelled != 1 else ''}." if cancelled else "Nothing was cancelled.",
                ephemeral=True,
            )
        return cancel

    def _build_spoiler_group(self):
        group = app_commands.Group(name="spoiler", description="Manage spoiler tags")

        @group.command(name="list", description="List spoiler trigger tags")
        async def spoiler_list(interaction: discord.Interaction) -> None:
            if self._spoiler_tags:
                description = "\n".join(f"• `{value}`" for value in sorted(self._spoiler_tags.values()))
            else:
                description = "No spoiler tags configured."
            embed = ui_embeds.build_notice_embed(
                title="📑 Spoiler tags",
                description=description,
                footer_text=self.settings.discord.footer_text,
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)

        @group.command(name="add", description="Add a spoiler trigger tag")
        async def spoiler_add(interaction: discord.Interaction, tag: str) -> None:
            if not await self._ensure_manage_guild(interaction):
                return
            normalized = self._normalize_tag(tag)
            if not normalized:
                await interaction.response.send_message("Provide a non-empty tag.", ephemeral=True)
                return
            self._spoiler_tags[normalized] = tag.strip()
            self.settings.save_spoilers(list(self._spoiler_tags.values()))
            await interaction.response.send_message(f"Added spoiler tag `{tag.strip()}`.", ephemeral=True)

        @group.command(name="remove", description="Remove a spoiler trigger tag")
        async def spoiler_remove(interaction: discord.Interaction, tag: str) -> None:
            if not await self._ensure_manage_guild(interaction):
                return
            normalized = self._normalize_tag(tag)
            if normalized not in self._spoiler_tags:
                await interaction.response.send_message("Tag not found.", ephemeral=True)
                return
            removed = self._spoiler_tags.pop(normalized)
            self.settings.save_spoilers(list(self._spoiler_tags.values()))
            await interaction.response.send_message(f"Removed spoiler tag `{removed}`.", ephemeral=True)

        return group

    async def _ensure_manage_guild(self, interaction: discord.Interaction) -> bool:
        perms = getattr(interaction.user, "guild_permissions", None)
        if perms and perms.manage_guild:
            return True
        await interaction.response.send_message("You need the **Manage Server** permission to do this.", ephemeral=True)
        return False

    def _build_subscription_group(self):
        group = app_commands.Group(name="subscription", description="Manage manual subscriptions")

        @group.command(name="grant", description="Grant or extend a manual subscription")
        async def subscription_grant(
            interaction: discord.Interaction,
            target: str,
            level: app_commands.Range[int, 1, 100],
            days: app_commands.Range[int, 1, 3650],
        ) -> None:
            if not await self._ensure_manual_subscription_manager(interaction):
                return

            guild = await self.security.get_access_guild(self)
            if guild is None:
                await interaction.response.send_message("Access guild is not available right now.", ephemeral=True)
                return

            tier = self._get_manual_subscription_tier(level)
            if tier is None or tier.role_id is None:
                available = ", ".join(str(item.level) for item in self.settings.security.role_tiers if item.role_id is not None)
                await interaction.response.send_message(
                    f"Level `{level}` is not configured. Available levels: {available or 'none'}.",
                    ephemeral=True,
                )
                return

            target_member = await self.security.resolve_access_member(self, target)
            if target_member is None:
                await interaction.response.send_message(
                    "Member not found in the access guild. Use username, display name, mention, or user ID.",
                    ephemeral=True,
                )
                return

            role = await self._get_guild_role(guild, tier.role_id)
            if role is None:
                await interaction.response.send_message(
                    f"Role for level `{level}` was not found in the guild.",
                    ephemeral=True,
                )
                return

            async with self._manual_subscription_lock:
                existing = self.manual_subscriptions.get_subscription(str(target_member.id))
                old_role_id = int(existing["role_id"]) if existing and existing.get("role_id") else None

                if old_role_id and old_role_id != role.id:
                    old_role = await self._get_guild_role(guild, old_role_id)
                    if old_role is not None and old_role in target_member.roles:
                        await target_member.remove_roles(old_role, reason=f"Manual subscription level changed by {interaction.user}")

                if role not in target_member.roles:
                    await target_member.add_roles(role, reason=f"Manual subscription granted by {interaction.user}")

                record = self.manual_subscriptions.grant_subscription(
                    member=target_member,
                    tier=tier,
                    days=days,
                    granted_by=interaction.user,
                    guild_id=guild.id,
                )

            embed = ui_embeds.build_notice_embed(
                title="✅ Manual subscription saved",
                description=(
                    f"User: {target_member.mention}\n"
                    f"Level: **{record['level']}** ({record['tier_name']})\n"
                    f"Granted days: **{days}**\n"
                    f"Issued at: **{self._format_subscription_dt(record.get('issued_at'))}**\n"
                    f"Expires at: **{self._format_subscription_dt(record.get('expires_at'))}**"
                ),
                footer_text=self.settings.discord.footer_text,
                color=ui_embeds.SUCCESS_COLOR,
            )
            await interaction.response.send_message(embed=embed, ephemeral=True)

        @group.command(name="list", description="List active manual subscriptions")
        async def subscription_list(interaction: discord.Interaction) -> None:
            if not await self._ensure_manual_subscription_manager(interaction):
                return

            items = self.manual_subscriptions.list_active()
            if not items:
                await interaction.response.send_message("There are no active manual subscriptions.", ephemeral=True)
                return

            embed = discord.Embed(title="📋 Active manual subscriptions", color=ui_embeds.ACCENT_COLOR)
            for item in items[:20]:
                embed.add_field(
                    name=f"{item.get('display_name') or item.get('username') or item.get('user_id')} · level {item.get('level')}",
                    value=(
                        f"Username: `{item.get('username')}`\n"
                        f"Issued: **{self._format_subscription_dt(item.get('issued_at'))}**\n"
                        f"Expires: **{self._format_subscription_dt(item.get('expires_at'))}**"
                    ),
                    inline=False,
                )
            if len(items) > 20:
                embed.set_footer(text=f"Showing 20 of {len(items)} active manual subscriptions")
            else:
                embed.set_footer(text=self.settings.discord.footer_text)
            await interaction.response.send_message(embed=embed, ephemeral=True)

        return group

    async def _ensure_manual_subscription_manager(self, interaction: discord.Interaction) -> bool:
        required_role_id = self.settings.security.manual_subscription_manager_role_id
        member = await self.security.get_access_member(interaction)
        if member is None:
            await interaction.response.send_message("You must be in the access guild to use this command.", ephemeral=True)
            return False

        perms = getattr(member, "guild_permissions", None)
        if perms and perms.administrator:
            return True

        if required_role_id and any(role.id == required_role_id for role in getattr(member, "roles", [])):
            return True

        await interaction.response.send_message(
            f"You need the role `{required_role_id}` (or administrator) to manage manual subscriptions.",
            ephemeral=True,
        )
        return False

    def _get_manual_subscription_tier(self, level: int) -> Optional[RoleTier]:
        return next((item for item in self.settings.security.role_tiers if item.level == level and item.role_id is not None), None)

    async def _get_guild_role(self, guild: discord.Guild, role_id: int) -> Optional[discord.Role]:
        role = guild.get_role(int(role_id))
        if role is not None:
            return role
        try:
            roles = await guild.fetch_roles()
        except discord.HTTPException:
            return None
        return next((item for item in roles if item.id == int(role_id)), None)

    @staticmethod
    def _format_subscription_dt(value: Optional[str]) -> str:
        if not value:
            return "Unknown"
        try:
            dt = datetime.fromisoformat(value)
        except ValueError:
            return value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M UTC")

    async def _manual_subscription_cleanup_loop(self) -> None:
        await self.wait_until_ready()
        while not self.is_closed():
            try:
                await self._cleanup_expired_manual_subscriptions()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Manual subscription cleanup failed")
            await asyncio.sleep(60)

    async def _cleanup_expired_manual_subscriptions(self) -> None:
        guild = await self.security.get_access_guild(self)
        if guild is None:
            return

        expired = self.manual_subscriptions.get_expired()
        if not expired:
            return

        async with self._manual_subscription_lock:
            for record in expired:
                user_id = str(record.get("user_id") or "")
                role_id_raw = record.get("role_id")
                role_id = int(role_id_raw) if str(role_id_raw).isdigit() else None
                removable = True
                member = None

                if user_id.isdigit():
                    member = guild.get_member(int(user_id))
                    if member is None:
                        try:
                            member = await guild.fetch_member(int(user_id))
                        except discord.HTTPException:
                            member = None

                if member is not None and role_id is not None:
                    role = await self._get_guild_role(guild, role_id)
                    if role is not None and role in member.roles:
                        try:
                            await member.remove_roles(role, reason="Manual subscription expired")
                        except discord.HTTPException:
                            removable = False
                            logger.warning(
                                "Failed to remove expired manual subscription role | user=%s role_id=%s",
                                user_id,
                                role_id,
                            )

                if removable:
                    self.manual_subscriptions.remove_subscription(user_id)
                    logger.info(
                        "Manual subscription expired | user_id=%s username=%s role_id=%s",
                        user_id,
                        record.get("username"),
                        role_id,
                    )

    # --------------------------------------------------------------
    # Main generation flow
    # --------------------------------------------------------------
    async def handle_generation_request(
        self,
        *,
        interaction: discord.Interaction,
        workflow_type: str,
        prompt: str,
        workflow_name: Optional[str] = None,
        settings: Optional[str] = None,
        resolution: Optional[str] = None,
        prompt_preset: Optional[str] = None,
        model_preset: Optional[str] = None,
        lora_preset: Optional[str] = None,
        seed: Optional[int] = None,
        cfg: Optional[float] = None,
        controlnet_strength: Optional[float] = None,
        input_attachment: Optional[discord.Attachment] = None,
    ) -> None:
        self.usage.reset_if_needed()
        user_id = str(interaction.user.id)

        if await self.security.is_blocked(user_id):
            await interaction.response.send_message("You are blocked from using this bot.", ephemeral=True)
            return

        if not await self.security.is_member_of_access_guild(interaction):
            await interaction.response.send_message("You must be a member of the access guild to use this bot.", ephemeral=True)
            return

        is_img2vid = workflow_type == "img2img" and (workflow_name or "").upper() == "IMG2VID"
        requires_source_image = workflow_type in {"img2img", "upscale"} or is_img2vid

        if workflow_type == "img2img" and not await self.security.has_img2img_access(interaction):
            await interaction.response.send_message("You do not have the required role for IMG2IMG/IMG2VID.", ephemeral=True)
            return

        if controlnet_strength is not None and controlnet_strength < 0:
            await interaction.response.send_message("ControlNet strength must be >= 0.", ephemeral=True)
            return

        if cfg is not None and cfg <= 0:
            await interaction.response.send_message("CFG must be > 0.", ephemeral=True)
            return

        if requires_source_image and input_attachment is None:
            await interaction.response.send_message("This workflow requires a source image.", ephemeral=True)
            return

        tier = await self.security.determine_tier(interaction)

        if is_img2vid:
            img2vid_limit = await self.security.get_img2vid_daily_limit(interaction)
            if img2vid_limit is None:
                await interaction.response.send_message("You do not have IMG2VID access.", ephemeral=True)
                return
        else:
            img2vid_limit = None

        final_prompt, prompt_preset_name, _prompt_preset_tags = self.presets.apply_prompt_preset(prompt_preset, prompt)
        model_name, model_preset_name = self.presets.apply_model_preset(model_preset)
        lora_name, lora_preset_name = self.presets.apply_lora_preset(lora_preset)

        final_settings = settings
        config_parts = []
        if model_name:
            config_parts.append(f"model={model_name}")
        if lora_name:
            config_parts.append(f"lora={lora_name}")
        if seed is not None:
            config_parts.append(f"seed={seed}")
        if cfg is not None:
            config_parts.append(f"cfg={cfg}")
        if config_parts:
            config_block = f"config({', '.join(config_parts)})"
            final_settings = f"{settings};{config_block}" if settings else config_block

        resolved_workflow = workflow_name or self.workflow_service.get_default_workflow(workflow_type)
        if not resolved_workflow:
            await interaction.response.send_message(f"No workflow configured for type `{workflow_type}`.", ephemeral=True)
            return

        workflow_cfg = self.workflow_service.get_workflow(resolved_workflow)
        if not workflow_cfg:
            await interaction.response.send_message(f"Workflow `{resolved_workflow}` not found.", ephemeral=True)
            return

        valid, error = self.security.validate_workflow_access(interaction.user, workflow_cfg, final_settings)
        if not valid:
            await interaction.response.send_message(error, ephemeral=True)
            return

        user_lock = self._get_user_lock(user_id)
        async with user_lock:
            current_sessions = self.active_sessions.get(user_id, [])
            if len(current_sessions) >= tier.max_parallel_generations:
                await interaction.response.send_message(
                    f"You already have **{len(current_sessions)}** active/queued requests. Your tier allows **{tier.max_parallel_generations}**.",
                    ephemeral=True,
                )
                return

            if tier.daily_limit is not None:
                current_daily = self.usage.get_generation_count(user_id)
                if current_daily >= tier.daily_limit:
                    await interaction.response.send_message(
                        f"Daily limit reached: **{current_daily}/{tier.daily_limit}**. Reset in {self.usage.time_until_reset()}.",
                        ephemeral=True,
                    )
                    return

            if is_img2vid and img2vid_limit is not None:
                img2vid_used = self.usage.get_img2vid_count(user_id)
                if img2vid_used >= img2vid_limit:
                    await interaction.response.send_message(
                        f"IMG2VID daily limit reached: **{img2vid_used}/{img2vid_limit}**.",
                        ephemeral=True,
                    )
                    return

            session = GenerationSession(
                user_id=user_id,
                user=interaction.user,
                interaction=interaction,
                request=GenerationRequest(
                    workflow_type=workflow_type,
                    prompt=prompt,
                    workflow_name=resolved_workflow,
                    settings=settings,
                    resolution=resolution,
                    prompt_preset=prompt_preset,
                    model_preset=model_preset,
                    lora_preset=lora_preset,
                    seed=seed,
                    controlnet_strength=controlnet_strength,
                    input_attachment=input_attachment,
                ),
                tier=tier,
                prompt=final_prompt,
                settings=final_settings,
                resolution=resolution or workflow_cfg.get("default_resolution"),
                seed=seed,
                workflow_name=resolved_workflow,
                prompt_preset_name=prompt_preset_name,
                model_preset_name=model_preset_name,
                lora_preset_name=lora_preset_name,
                force_spoiler=self._prompt_contains_spoiler_tag(final_prompt),
            )

            self._get_sync_session_id(session)
            self.last_requests[user_id] = StoredRequest(
                workflow_type=workflow_type,
                prompt=prompt,
                workflow_name=resolved_workflow,
                settings=settings,
                resolution=resolution,
                prompt_preset=prompt_preset,
                model_preset=model_preset,
                lora_preset=lora_preset,
                seed=seed,
                controlnet_strength=controlnet_strength,
            )

            self.active_sessions[user_id].append(session)
            session.queued = True

        sync_valid, sync_error, _sync_state = await self._register_synced_session(session, is_img2vid=is_img2vid, img2vid_limit=img2vid_limit)
        if not sync_valid:
            async with self._get_user_lock(user_id):
                if session in self.active_sessions.get(user_id, []):
                    self.active_sessions[user_id].remove(session)
                if not self.active_sessions.get(user_id):
                    self.active_sessions.pop(user_id, None)
            await self._leave_synced_queue(session, reason="validation_failed")
            await interaction.response.send_message(sync_error or "Request rejected by synced queue.", ephemeral=True)
            return

        logger.info("Accepted request | user=%s workflow=%s type=%s tier=%s", format_user(interaction.user), resolved_workflow, workflow_type, tier.name)

        view = self._create_generation_view(session)
        embed = self._build_generation_embed(session, status="🕒 Queued", title="Generation queued", color=ui_embeds.PROGRESS_COLOR)

        try:
            await interaction.response.send_message(embed=embed, view=view)
            session.message = await interaction.original_response()
            session.metadata["view"] = view

            await self.queue.enqueue(
                priority=-session.tier.queue_priority,
                session=session,
                runner=self._run_generation_session,
            )
        except Exception as exc:
            logger.exception("Failed to initialize queued request | user=%s workflow=%s", format_user(interaction.user), resolved_workflow)
            async with self._get_user_lock(user_id):
                await self._rollback_session_accounting(session)
                if session in self.active_sessions.get(user_id, []):
                    self.active_sessions[user_id].remove(session)
                if not self.active_sessions.get(user_id):
                    self.active_sessions.pop(user_id, None)
            await self._leave_synced_queue(session, reason="queue_init_failed")

            if session.message:
                await self._update_generation_message(session, f"❌ Failed to queue request: {exc}", "Generation failed", ui_embeds.ERROR_COLOR)
                await self._finalize_session(session)
                return
            raise

    async def _run_generation_session(self, session: GenerationSession) -> None:
        image_data: Optional[bytes] = None
        try:
            await self.sync.wait_for_turn(self, session_id=self._get_sync_session_id(session), cancel_event=session.cancel_event)
            if session.cancel_event.is_set():
                await self._update_generation_message(session, "🛑 Generation cancelled", "Generation cancelled", ui_embeds.WARNING_COLOR)
                return

            session.queued = False
            session.processing = True
            session.metadata.pop("queue_position", None)
            logger.info("Starting generation | user=%s workflow=%s", format_user(session.user), session.workflow_name)
            await self._update_generation_message(session, "🔄 Preparing workflow", "Preparing generation", ui_embeds.PROGRESS_COLOR)

            temp_files: list[Path] = []
            if session.request.input_attachment is not None:
                image_data = await self._read_and_normalize_attachment(session.request.input_attachment)

            if session.cancel_event.is_set():
                await self._update_generation_message(session, "🛑 Generation cancelled", "Generation cancelled", ui_embeds.WARNING_COLOR)
                return

            workflow_json, actual_seed = self.workflow_service.prepare_workflow(
                workflow_name=session.workflow_name or "",
                prompt=session.prompt,
                settings=session.settings,
                resolution=session.resolution,
                image_data=image_data,
                seed=session.seed,
                controlnet_strength=session.request.controlnet_strength,
                temp_files=temp_files,
            )
            if temp_files:
                session.metadata.setdefault("temp_files", []).extend(temp_files)
            session.seed = actual_seed
            explicit_img2vid = (session.request.workflow_type == "img2img" and (session.workflow_name or "").upper() == "IMG2VID")
            video_only = explicit_img2vid or self.workflow_service.workflow_outputs_video(session.workflow_name or "", workflow_json)

            if session.cancel_event.is_set():
                await self._update_generation_message(session, "🛑 Generation cancelled", "Generation cancelled", ui_embeds.WARNING_COLOR)
                return

            async with self._get_user_lock(session.user_id):
                if session.cancel_event.is_set():
                    await self._update_generation_message(session, "🛑 Generation cancelled", "Generation cancelled", ui_embeds.WARNING_COLOR)
                    return

                if session.tier.daily_limit is not None:
                    current_daily = self.usage.get_generation_count(session.user_id)
                    if current_daily >= session.tier.daily_limit:
                        await self._update_generation_message(
                            session,
                            f"🚫 Daily limit reached before submission: **{current_daily}/{session.tier.daily_limit}**. Reset in {self.usage.time_until_reset()}.",
                            "Generation cancelled",
                            ui_embeds.WARNING_COLOR,
                        )
                        return

                explicit_img2vid = (session.request.workflow_type == "img2img" and (session.workflow_name or "").upper() == "IMG2VID")
                if explicit_img2vid:
                    img2vid_limit = await self.security.get_img2vid_daily_limit(session.interaction)
                    if img2vid_limit is not None:
                        img2vid_used = self.usage.get_img2vid_count(session.user_id)
                        if img2vid_used >= img2vid_limit:
                            await self._update_generation_message(
                                session,
                                f"🚫 IMG2VID daily limit reached before submission: **{img2vid_used}/{img2vid_limit}**.",
                                "Generation cancelled",
                                ui_embeds.WARNING_COLOR,
                            )
                            return

                result = await self.comfy.generate(workflow_json)
                session.submitted_to_comfy = True

                if session.tier.daily_limit is not None and not session.counted_usage:
                    self.usage.increment_daily(session.user_id)
                    session.counted_usage = True
                    await self.sync.commit_generation(self, session_id=self._get_sync_session_id(session), user_id=session.user_id)

                if explicit_img2vid and not session.counted_img2vid:
                    self.usage.increment_img2vid(session.user_id)
                    session.counted_img2vid = True
                    await self.sync.commit_img2vid(self, session_id=self._get_sync_session_id(session), user_id=session.user_id)

                prompt_id = result.get("prompt_id")
                if not prompt_id:
                    raise RuntimeError("ComfyUI accepted the prompt but did not return prompt_id")

                session.prompt_id = str(prompt_id)

            async def callback(status: str, file: Optional[discord.File] = None, output_role: str = "status") -> None:
                title = "Generation in progress"
                color = ui_embeds.PROGRESS_COLOR
                clear_attachments = False
                if file is not None:
                    file = self._prepare_output_file(file, session.force_spoiler)
                    if output_role == "final":
                        title = "Generation completed"
                        color = ui_embeds.SUCCESS_COLOR
                        delivered_count = int(session.metadata.get("delivered_output_count", 0))
                        if delivered_count > 0:
                            logger.debug(
                                "Skipping additional final output | user=%s | workflow=%s | seed=%s | output_index=%s",
                                format_user(session.user),
                                session.workflow_name or session.request.workflow_type,
                                session.seed,
                                delivered_count + 1,
                            )
                            return
                    elif output_role == "preview":
                        title = "Preview updated"
                        color = ui_embeds.PROGRESS_COLOR
                elif output_role == "final_missing":
                    title = "Generation completed"
                    color = ui_embeds.WARNING_COLOR
                    clear_attachments = video_only

                updated = await self._update_generation_message(session, status, title, color, file, clear_attachments=clear_attachments)
                if file is not None and output_role == "final" and updated:
                    session.metadata["delivered_output_count"] = int(session.metadata.get("delivered_output_count", 0)) + 1

            await self._update_generation_message(session, "🚀 Submitted to ComfyUI", "Generation submitted", ui_embeds.PROGRESS_COLOR)

            await self.comfy.listen_for_updates(
                session.prompt_id,
                callback,
                cancel_event=session.cancel_event,
                video_only=video_only,
            )

            if session.cancel_event.is_set():
                await self._update_generation_message(session, "🛑 Generation cancelled", "Generation cancelled", ui_embeds.WARNING_COLOR)
            else:
                session.completed = True
                self.usage.record_success(session.user_id)
                logger.info("Generation completed | user=%s workflow=%s seed=%s", format_user(session.user), session.workflow_name, session.seed)

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Generation failed for user %s: %s", format_user(session.user), exc)
            await self._update_generation_message(session, f"❌ {exc}", "Generation failed", ui_embeds.ERROR_COLOR)
        finally:
            await self._finalize_session(session)

    async def _finalize_session(self, session: GenerationSession) -> None:
        if session.finalized:
            return
        session.finalized = True

        await self._leave_synced_queue(session, reason="finalized")

        async with self._get_user_lock(session.user_id):
            active = self.active_sessions.get(session.user_id, [])
            if session in active:
                active.remove(session)
            if not active and session.user_id in self.active_sessions:
                self.active_sessions.pop(session.user_id, None)

            if not session.submitted_to_comfy:
                await self._rollback_session_accounting(session)

        for temp_path in session.metadata.pop("temp_files", []):
            try:
                Path(temp_path).unlink(missing_ok=True)
            except Exception as exc:
                logger.debug("Failed to remove temp input file %s: %s", temp_path, exc)

        view: GenerationView | None = session.metadata.get("view")
        if view and session.message:
            view.disable()
            try:
                await session.message.edit(view=view)
            except discord.HTTPException:
                pass

    async def _cancel_session(self, session: GenerationSession, *, source: str) -> bool:
        if session.finalized or session.cancel_event.is_set():
            return False

        session.cancel_event.set()
        logger.info("Cancelling session | user=%s workflow=%s source=%s", format_user(session.user), session.workflow_name, source)
        removed = await self.queue.cancel_pending(session)
        if session.prompt_id:
            try:
                await self.comfy.cancel_prompt(session.prompt_id)
            except Exception:
                pass

        if removed and session.message:
            await self._update_generation_message(session, "🛑 Cancelled while waiting in queue", "Generation cancelled", ui_embeds.WARNING_COLOR)
            await self._finalize_session(session)
        return True

    async def _handle_reuse(self, interaction: discord.Interaction, session: GenerationSession) -> None:
        stored = self.last_requests.get(session.user_id)
        if not stored:
            await interaction.response.send_message("No previous request to reuse.", ephemeral=True)
            return

        attachment = session.request.input_attachment
        if attachment is None and interaction.message and interaction.message.attachments:
            attachment = interaction.message.attachments[0]

        await self.handle_generation_request(
            interaction=interaction,
            workflow_type=stored.workflow_type,
            prompt=stored.prompt,
            workflow_name=stored.workflow_name,
            settings=stored.settings,
            resolution=stored.resolution,
            prompt_preset=stored.prompt_preset,
            model_preset=stored.model_preset,
            lora_preset=stored.lora_preset,
            seed=stored.seed,
            controlnet_strength=stored.controlnet_strength,
            input_attachment=attachment,
        )

    def _create_generation_view(self, session: GenerationSession) -> GenerationView:
        async def cancel_callback(interaction: discord.Interaction) -> None:
            cancelled = await self._cancel_session(session, source="button")
            if interaction.response.is_done():
                await interaction.followup.send("Generation cancelled." if cancelled else "Generation already finished.", ephemeral=True)
            else:
                await interaction.response.send_message("Generation cancelled." if cancelled else "Generation already finished.", ephemeral=True)

        async def reuse_callback(interaction: discord.Interaction) -> None:
            await self._handle_reuse(interaction, session)

        return GenerationView(owner_id=int(session.user_id), cancel_callback=cancel_callback, reuse_callback=reuse_callback)

    async def _schedule_queue_refresh(self) -> None:
        if self._queue_refresh_task and not self._queue_refresh_task.done():
            self._queue_refresh_pending = True
            return
        self._queue_refresh_pending = False
        self._queue_refresh_task = asyncio.create_task(self._queue_refresh_loop(), name="queue-view-refresh")

    async def _queue_refresh_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(0.35)
                await self._refresh_queue_views()
                if not self._queue_refresh_pending:
                    break
                self._queue_refresh_pending = False
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("Queue refresh loop failed: %s", exc)

    async def _refresh_queue_views(self) -> None:
        state = await self._get_synced_state(force=True)
        total = state.total_queue()
        positions = {entry.session_id: index for index, entry in enumerate(state.open_queue, start=1)}
        sessions = [session for group in self.active_sessions.values() for session in group]
        for session in sessions:
            if not session.message or session.cancel_event.is_set() or session.processing or not session.queued:
                continue
            sync_id = self._get_sync_session_id(session)
            position = positions.get(sync_id)
            if position is None:
                continue
            previous = session.metadata.get("queue_position")
            if previous == position:
                continue
            session.metadata["queue_position"] = position
            await self._update_generation_message(
                session,
                status=f"🕒 In queue • position **{position}** of **{total}**",
                title="Generation queued",
                color=ui_embeds.PROGRESS_COLOR,
            )

    def _build_generation_embed(self, session: GenerationSession, *, status: str, title: str, color: int) -> discord.Embed:
        usage = None
        if session.tier.daily_limit is not None:
            used = self.usage.get_generation_count(session.user_id)
            usage = ui_embeds.format_usage_bar(used, session.tier.daily_limit, self.usage.time_until_reset())

        extra_fields = [("🎚 Tier", session.tier.name, True)]
        if session.metadata.get("queue_position") and session.queued:
            extra_fields.append(("📍 Queue", f"#{session.metadata['queue_position']}", True))
        if session.resolution:
            extra_fields.append(("📐 Resolution", session.resolution, True))
        if session.seed is not None:
            extra_fields.append(("🎲 Seed", str(session.seed), True))
        if session.prompt_preset_name:
            extra_fields.append(("🧠 Prompt preset", session.prompt_preset_name, True))
        if session.model_preset_name:
            extra_fields.append(("🧩 Model", session.model_preset_name, True))
        if session.lora_preset_name:
            extra_fields.append(("🪄 LoRA", session.lora_preset_name, True))

        return ui_embeds.build_generation_embed(
            title=title,
            user=session.user,
            workflow_name=session.workflow_name or session.request.workflow_type,
            status=status,
            footer_text=self.settings.discord.footer_text,
            color=color,
            prompt=session.prompt,
            settings=session.settings,
            usage=usage,
            extra_fields=extra_fields,
        )

    async def _update_generation_message(
        self,
        session: GenerationSession,
        status: str,
        title: str,
        color: int,
        file: Optional[discord.File] = None,
        *,
        clear_attachments: bool = False,
    ) -> bool:
        if not session.message:
            return False

        render_key = (title, status, color, bool(file), session.metadata.get("queue_position"))
        last_render = session.metadata.get("last_render_key")
        last_edit_ts = float(session.metadata.get("last_edit_ts", 0.0))
        is_terminal = color in {ui_embeds.SUCCESS_COLOR, ui_embeds.WARNING_COLOR, ui_embeds.ERROR_COLOR}

        if file is None and render_key == last_render and not is_terminal:
            return False
        if file is None and not is_terminal and (time.monotonic() - last_edit_ts) < 0.75:
            return False

        embed = self._build_generation_embed(session, status=status, title=title, color=color)
        kwargs: dict[str, Any] = {"embed": embed, "view": session.metadata.get("view")}
        if file is not None:
            kwargs["attachments"] = [file]
        elif clear_attachments:
            kwargs["attachments"] = []

        try:
            await session.message.edit(**kwargs)
            session.metadata["last_render_key"] = render_key
            session.metadata["last_edit_ts"] = time.monotonic()
            return True
        except discord.HTTPException as exc:
            logger.warning("Failed to update generation message | workflow=%s | status=%s | file=%s | error=%s", session.workflow_name or session.request.workflow_type, title, getattr(file, 'filename', None), exc)
            if file is not None and title == "Generation completed":
                fallback_embed = self._build_generation_embed(session, status=status, title=title, color=color)
                try:
                    channel = getattr(session.message, 'channel', None)
                    if channel is not None:
                        sent_message = await channel.send(embed=fallback_embed, file=file, view=session.metadata.get("view"))
                        session.message = sent_message
                        session.metadata["last_render_key"] = render_key
                        session.metadata["last_edit_ts"] = time.monotonic()
                        return True
                except discord.HTTPException as fallback_exc:
                    logger.warning("Failed to send final output as a separate message | workflow=%s | file=%s | error=%s", session.workflow_name or session.request.workflow_type, getattr(file, 'filename', None), fallback_exc)
            return False

    async def _read_and_normalize_attachment(self, attachment: discord.Attachment) -> bytes:
        allowed = set(self.settings.inputs.allowed_mime_types)
        max_size = self.settings.inputs.max_image_size_mb * 1024 * 1024

        if attachment.size > max_size:
            raise ValueError(f"Input image is too large. Limit: {self.settings.inputs.max_image_size_mb} MB")
        if attachment.content_type and attachment.content_type not in allowed:
            raise ValueError(f"Unsupported image type: {attachment.content_type}")

        raw = await attachment.read()
        try:
            with Image.open(io.BytesIO(raw)) as image:
                converted = image.convert("RGBA")
                output = io.BytesIO()
                converted.save(output, format="PNG")
                return output.getvalue()
        except UnidentifiedImageError as exc:
            raise ValueError("Attachment is not a valid image") from exc

    async def _rollback_session_accounting(self, session: GenerationSession) -> None:
        if session.counted_usage:
            self.usage.rollback_daily(session.user_id)
            session.counted_usage = False
        if session.counted_img2vid:
            self.usage.rollback_img2vid(session.user_id)
            session.counted_img2vid = False

    def _prepare_output_file(self, file: discord.File, force_spoiler: bool) -> discord.File:
        if not force_spoiler:
            return file
        filename = file.filename or "output.bin"
        if not filename.startswith("SPOILER_"):
            filename = f"SPOILER_{filename}"
        file.filename = filename
        return file

    @staticmethod
    def _normalize_tag(tag: str) -> str:
        return re.sub(r"\s+", " ", tag.strip()).lower()

    def _prompt_contains_spoiler_tag(self, prompt: str) -> bool:
        prompt_normalized = self._normalize_tag(prompt)
        return any(tag in prompt_normalized for tag in self._spoiler_tags)

