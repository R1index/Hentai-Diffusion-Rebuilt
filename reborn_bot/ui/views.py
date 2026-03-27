from __future__ import annotations

from typing import Awaitable, Callable, Optional

import discord


class GenerationView(discord.ui.View):
    def __init__(
        self,
        owner_id: int,
        cancel_callback: Callable[[discord.Interaction], Awaitable[None]],
        reuse_callback: Optional[Callable[[discord.Interaction], Awaitable[None]]] = None,
    ) -> None:
        super().__init__(timeout=None)
        self.owner_id = owner_id
        self.cancel_callback = cancel_callback
        self.reuse_callback = reuse_callback
        if self.reuse_callback is None:
            self.reuse.disabled = True

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id == self.owner_id:
            return True
        await interaction.response.send_message("You can only use controls on your own generation.", ephemeral=True)
        return False

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger, emoji="🛑", row=0)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await self.cancel_callback(interaction)

    @discord.ui.button(label="Reuse", style=discord.ButtonStyle.secondary, emoji="🔁", row=0)
    async def reuse(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        if not self.reuse_callback:
            await interaction.response.send_message("This request cannot be reused.", ephemeral=True)
            return
        await self.reuse_callback(interaction)

    def disable(self, *, keep_reuse: bool = True) -> None:
        for child in self.children:
            if not isinstance(child, discord.ui.Button):
                continue
            if keep_reuse and child is self.reuse:
                child.disabled = False
                child.style = discord.ButtonStyle.secondary
                continue
            child.disabled = True
            if child is self.cancel:
                child.style = discord.ButtonStyle.secondary
                child.label = "Closed"
