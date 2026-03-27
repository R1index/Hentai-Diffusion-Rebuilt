import asyncio
import os
import sys

from reborn_bot.bot import RebornComfyBot
from reborn_bot.config import load_settings
from reborn_bot.logging_setup import configure_logging, logger


def main() -> None:
    configure_logging()

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    settings = load_settings(os.getenv("BOT_CONFIG", "config.yml"))
    bot = RebornComfyBot(settings)

    token = os.getenv("DISCORD_TOKEN") or settings.discord.token
    if not token:
        raise RuntimeError("Discord token is missing. Set DISCORD_TOKEN or fill config.yml")

    logger.info("Starting Discord bot")
    bot.run(token)


if __name__ == "__main__":
    main()
