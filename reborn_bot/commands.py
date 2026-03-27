from __future__ import annotations
from typing import Callable, Optional

import discord
from discord import app_commands


def _make_autocomplete(search_func: Callable[[str, int], list]) -> Callable[[discord.Interaction, str], list[app_commands.Choice[str]]]:
    async def _inner(interaction: discord.Interaction, current: str) -> list[app_commands.Choice[str]]:
        return [app_commands.Choice(name=item.name, value=item.value) for item in search_func(current, 25)]
    return _inner


def build_generation_commands(bot):
    resolution_choices = [
        app_commands.Choice(name=label, value=value)
        for label, value in bot.workflow_service.list_resolution_choices()[:25]
    ]

    prompt_autocomplete = _make_autocomplete(bot.presets.search_prompt_presets)
    model_autocomplete = _make_autocomplete(bot.presets.search_model_presets)
    lora_autocomplete = _make_autocomplete(bot.presets.search_lora_presets)

    def workflow_autocomplete(workflow_type: str) -> Callable[[discord.Interaction, str], list[app_commands.Choice[str]]]:
        async def _inner(interaction: discord.Interaction, current: str) -> list[app_commands.Choice[str]]:
            return [
                app_commands.Choice(name=label, value=value)
                for label, value in bot.workflow_service.search_workflows(workflow_type, current, 25)
            ]
        return _inner

    async def _run_generation(
        interaction: discord.Interaction,
        *,
        workflow_type: str,
        prompt: str,
        image: Optional[discord.Attachment] = None,
        prompt_preset: Optional[str] = None,
        model_preset: Optional[str] = None,
        lora_preset: Optional[str] = None,
        seed: Optional[int] = None,
        cfg: Optional[float] = None,
        resolution: Optional[app_commands.Choice[str]] = None,
        controlnet_strength: Optional[float] = None,
        workflow: Optional[str] = None,
        settings: Optional[str] = None,
    ) -> None:
        await bot.handle_generation_request(
            interaction=interaction,
            workflow_type=workflow_type,
            prompt=prompt,
            workflow_name=workflow,
            settings=settings,
            resolution=resolution.value if resolution else None,
            prompt_preset=prompt_preset,
            model_preset=model_preset,
            lora_preset=lora_preset,
            seed=seed,
            cfg=cfg,
            controlnet_strength=controlnet_strength,
            input_attachment=image,
        )

    async def _run_img2vid(
        interaction: discord.Interaction,
        *,
        image: discord.Attachment,
        prompt: str,
        prompt_preset: Optional[str] = None,
        model_preset: Optional[str] = None,
        lora_preset: Optional[str] = None,
        seed: Optional[int] = None,
        settings: Optional[str] = None,
    ) -> None:
        await bot.handle_generation_request(
            interaction=interaction,
            workflow_type="img2img",
            prompt=prompt,
            workflow_name="IMG2VID",
            settings=settings,
            prompt_preset=prompt_preset,
            model_preset=model_preset,
            lora_preset=lora_preset,
            seed=seed,
            input_attachment=image,
        )

    def decorate_with_presets(command):
        command = app_commands.autocomplete(prompt_preset=prompt_autocomplete)(command)
        command = app_commands.autocomplete(model_preset=model_autocomplete)(command)
        command = app_commands.autocomplete(lora_preset=lora_autocomplete)(command)
        return command

    @app_commands.command(name="txt2img", description="Generate an image from text")
    async def txt2img(
        interaction: discord.Interaction,
        prompt: str,
        prompt_preset: Optional[str] = None,
        model_preset: Optional[str] = None,
        lora_preset: Optional[str] = None,
        seed: Optional[int] = None,
        cfg: Optional[float] = None,
        resolution: Optional[app_commands.Choice[str]] = None,
        workflow: Optional[str] = None,
        settings: Optional[str] = None,
    ):
        await _run_generation(
            interaction,
            workflow_type="txt2img",
            prompt=prompt,
            prompt_preset=prompt_preset,
            model_preset=model_preset,
            lora_preset=lora_preset,
            seed=seed,
            cfg=cfg,
            resolution=resolution,
            workflow=workflow,
            settings=settings,
        )

    @app_commands.command(name="rgen", description="Legacy alias for txt2img")
    async def rgen(
        interaction: discord.Interaction,
        prompt: str,
        prompt_preset: Optional[str] = None,
        model_preset: Optional[str] = None,
        lora_preset: Optional[str] = None,
        seed: Optional[int] = None,
        cfg: Optional[float] = None,
        resolution: Optional[app_commands.Choice[str]] = None,
        workflow: Optional[str] = None,
        settings: Optional[str] = None,
    ):
        await _run_generation(
            interaction,
            workflow_type="txt2img",
            prompt=prompt,
            prompt_preset=prompt_preset,
            model_preset=model_preset,
            lora_preset=lora_preset,
            seed=seed,
            cfg=cfg,
            resolution=resolution,
            workflow=workflow,
            settings=settings,
        )

    @app_commands.command(name="img2img", description="Generate an image from a source image")
    async def img2img(
        interaction: discord.Interaction,
        image: discord.Attachment,
        prompt: str,
        prompt_preset: Optional[str] = None,
        model_preset: Optional[str] = None,
        lora_preset: Optional[str] = None,
        seed: Optional[int] = None,
        cfg: Optional[float] = None,
        resolution: Optional[app_commands.Choice[str]] = None,
        controlnet_strength: Optional[float] = None,
        workflow: Optional[str] = None,
        settings: Optional[str] = None,
    ):
        await _run_generation(
            interaction,
            workflow_type="img2img",
            image=image,
            prompt=prompt,
            prompt_preset=prompt_preset,
            model_preset=model_preset,
            lora_preset=lora_preset,
            seed=seed,
            cfg=cfg,
            resolution=resolution,
            controlnet_strength=controlnet_strength,
            workflow=workflow,
            settings=settings,
        )

    @app_commands.command(name="reforge", description="Legacy alias for img2img")
    async def reforge(
        interaction: discord.Interaction,
        image: discord.Attachment,
        prompt: str,
        prompt_preset: Optional[str] = None,
        model_preset: Optional[str] = None,
        lora_preset: Optional[str] = None,
        seed: Optional[int] = None,
        cfg: Optional[float] = None,
        resolution: Optional[app_commands.Choice[str]] = None,
        controlnet_strength: Optional[float] = None,
        workflow: Optional[str] = None,
        settings: Optional[str] = None,
    ):
        await _run_generation(
            interaction,
            workflow_type="img2img",
            image=image,
            prompt=prompt,
            prompt_preset=prompt_preset,
            model_preset=model_preset,
            lora_preset=lora_preset,
            seed=seed,
            cfg=cfg,
            resolution=resolution,
            controlnet_strength=controlnet_strength,
            workflow=workflow,
            settings=settings,
        )

    @app_commands.command(name="img2vid", description="Generate a video from a source image")
    async def img2vid(
        interaction: discord.Interaction,
        image: discord.Attachment,
        prompt: str,
        prompt_preset: Optional[str] = None,
        model_preset: Optional[str] = None,
        lora_preset: Optional[str] = None,
        seed: Optional[int] = None,
        settings: Optional[str] = None,
    ):
        await _run_img2vid(
            interaction,
            image=image,
            prompt=prompt,
            prompt_preset=prompt_preset,
            model_preset=model_preset,
            lora_preset=lora_preset,
            seed=seed,
            settings=settings,
        )

    @app_commands.command(name="upscale", description="Upscale an image")
    async def upscale(
        interaction: discord.Interaction,
        image: discord.Attachment,
        prompt: str,
        prompt_preset: Optional[str] = None,
        model_preset: Optional[str] = None,
        lora_preset: Optional[str] = None,
        seed: Optional[int] = None,
        workflow: Optional[str] = None,
        settings: Optional[str] = None,
    ):
        await _run_generation(
            interaction,
            workflow_type="upscale",
            image=image,
            prompt=prompt,
            prompt_preset=prompt_preset,
            model_preset=model_preset,
            lora_preset=lora_preset,
            seed=seed,
            workflow=workflow,
            settings=settings,
        )

    txt2img = app_commands.autocomplete(workflow=workflow_autocomplete("txt2img"))(txt2img)
    rgen = app_commands.autocomplete(workflow=workflow_autocomplete("txt2img"))(rgen)
    img2img = app_commands.autocomplete(workflow=workflow_autocomplete("img2img"))(img2img)
    reforge = app_commands.autocomplete(workflow=workflow_autocomplete("img2img"))(reforge)
    upscale = app_commands.autocomplete(workflow=workflow_autocomplete("upscale"))(upscale)

    if resolution_choices:
        txt2img = app_commands.choices(resolution=resolution_choices)(txt2img)
        rgen = app_commands.choices(resolution=resolution_choices)(rgen)
        img2img = app_commands.choices(resolution=resolution_choices)(img2img)
        reforge = app_commands.choices(resolution=resolution_choices)(reforge)

    return [
        decorate_with_presets(txt2img),
        decorate_with_presets(rgen),
        decorate_with_presets(img2img),
        decorate_with_presets(reforge),
        decorate_with_presets(img2vid),
        decorate_with_presets(upscale),
    ]
