from __future__ import annotations

import asyncio
import io
import json
import sys
import tempfile
import types
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def install_discord_stub() -> None:
    if "discord" in sys.modules:
        return

    discord = types.ModuleType("discord")
    app_commands = types.ModuleType("discord.app_commands")
    ext_mod = types.ModuleType("discord.ext")
    commands_mod = types.ModuleType("discord.ext.commands")
    ui_mod = types.ModuleType("discord.ui")
    utils_mod = types.ModuleType("discord.utils")

    class HTTPException(Exception):
        pass

    class Intents:
        guilds = False
        members = False
        message_content = False

        @classmethod
        def default(cls):
            return cls()

    class DummyAvatar:
        def __init__(self, url: str = "https://example.com/avatar.png"):
            self.url = url

    class DummyUserBase:
        pass

    class Member(DummyUserBase):
        pass

    class Attachment:
        def __init__(self, data: bytes = b"", size: int = 0, content_type: str = "image/png"):
            self._data = data
            self.size = size or len(data)
            self.content_type = content_type

        async def read(self) -> bytes:
            return self._data

    class File:
        def __init__(self, fp, filename: str = "file.bin"):
            self.fp = fp
            self.filename = filename

    class Embed:
        def __init__(self, title=None, description=None, color=None, timestamp=None):
            self.title = title
            self.description = description
            self.color = color
            self.timestamp = timestamp
            self.fields = []
            self.footer = None
            self.author = None
            self.thumbnail = None

        def set_author(self, name=None, icon_url=None):
            self.author = (name, icon_url)

        def add_field(self, name=None, value=None, inline=False):
            self.fields.append((name, value, inline))

        def set_footer(self, text=None):
            self.footer = text

        def set_thumbnail(self, url=None):
            self.thumbnail = url

    class ButtonStyle:
        danger = 1
        secondary = 2

    class Button:
        def __init__(self):
            self.disabled = False
            self.style = None
            self.label = ""
            self.emoji = None

    class View:
        def __init__(self, timeout=None):
            self.timeout = timeout
            self.children = []

    class Response:
        def __init__(self, interaction):
            self.interaction = interaction
            self._done = False

        async def send_message(self, content=None, embed=None, view=None, ephemeral=False):
            self._done = True
            self.interaction._response_payload = (content, embed, view, ephemeral)
            self.interaction._message = FakeMessage(self.interaction, content=content, embed=embed, view=view)

        def is_done(self):
            return self._done

    class Followup:
        def __init__(self, interaction):
            self.interaction = interaction
            self.sent = []
            self.fail = False

        async def send(self, content=None, file=None, ephemeral=False):
            if self.fail:
                raise HTTPException("followup failed")
            self.sent.append((content, file, ephemeral))

    class Interaction:
        def __init__(self, user):
            self.user = user
            self.client = types.SimpleNamespace(get_guild=lambda _id: None, fetch_guild=None)
            self.response = Response(self)
            self.followup = Followup(self)
            self.message = None
            self._message = None
            self._response_payload = None

        async def original_response(self):
            return self._message

    class Bot:
        def __init__(self, command_prefix=None, intents=None):
            self.tree = types.SimpleNamespace(add_command=lambda *a, **k: None, sync=lambda: [])

        async def close(self):
            return None

        def run(self, token):
            return None

    def utcnow():
        return datetime.now(timezone.utc)

    def passthrough_decorator(*args, **kwargs):
        def wrapper(fn):
            return fn

        return wrapper

    class Choice:
        def __init__(self, name, value):
            self.name = name
            self.value = value

        def __class_getitem__(cls, item):
            return cls

    class Group:
        def __init__(self, name=None, description=None):
            self.name = name
            self.description = description

        def command(self, *args, **kwargs):
            return passthrough_decorator(*args, **kwargs)

    def button(*args, **kwargs):
        def wrapper(fn):
            button_obj = Button()
            button_obj.style = kwargs.get("style")
            button_obj.label = kwargs.get("label", "")
            button_obj.emoji = kwargs.get("emoji")
            setattr(fn, "__discord_button__", button_obj)
            return fn

        return wrapper

    utils_mod.utcnow = utcnow
    ui_mod.View = View
    ui_mod.Button = Button
    ui_mod.button = button
    app_commands.Choice = Choice
    app_commands.Group = Group
    app_commands.command = passthrough_decorator
    app_commands.autocomplete = passthrough_decorator
    app_commands.choices = passthrough_decorator

    discord.Intents = Intents
    discord.Attachment = Attachment
    discord.File = File
    discord.Embed = Embed
    discord.HTTPException = HTTPException
    discord.Interaction = Interaction
    discord.Member = Member
    discord.Message = object
    discord.ButtonStyle = ButtonStyle
    discord.ui = ui_mod
    discord.utils = utils_mod
    discord.app_commands = app_commands
    discord.abc = types.SimpleNamespace(User=DummyUserBase)

    commands_mod.Bot = Bot
    ext_mod.commands = commands_mod

    sys.modules["discord"] = discord
    sys.modules["discord.app_commands"] = app_commands
    sys.modules["discord.ext"] = ext_mod
    sys.modules["discord.ext.commands"] = commands_mod
    sys.modules["discord.ui"] = ui_mod
    sys.modules["discord.utils"] = utils_mod


class FakeChannel:
    def __init__(self):
        self.sent = []

    async def send(self, content=None, file=None):
        self.sent.append((content, file))


class FakeMessage:
    def __init__(self, interaction, content=None, embed=None, view=None):
        self.interaction = interaction
        self.content = content
        self.embed = embed
        self.view = view
        self.attachments = []
        self.edits = []
        self.replies = []
        self.channel = FakeChannel()

    async def edit(self, **kwargs):
        self.edits.append(kwargs)

    async def reply(self, content=None, file=None, mention_author=False):
        self.replies.append((content, file, mention_author))


class FakeUser:
    def __init__(self, uid: int, name: str = "tester"):
        self.id = uid
        self.name = name
        self.global_name = name.title()
        self.display_name = name.title()
        self.mention = f"<@{uid}>"
        self.display_avatar = types.SimpleNamespace(url="https://example.com/avatar.png")
        self.roles = []


def build_test_bot(tmpdir: Path):
    import yaml
    from reborn_bot.bot import RebornComfyBot
    from reborn_bot.config import load_settings
    from reborn_bot.models import RoleTier

    (tmpdir / "data").mkdir()
    (tmpdir / "workflows").mkdir()
    (tmpdir / "COMFYUI_INPUT_DIR").mkdir()
    (tmpdir / "data" / "prompt_presets.json").write_text("[]", encoding="utf-8")
    (tmpdir / "data" / "model_presets.json").write_text("[]", encoding="utf-8")
    (tmpdir / "data" / "lora_presets.json").write_text("[]", encoding="utf-8")
    (tmpdir / "workflows" / "wf.json").write_text(json.dumps({"1": {"inputs": {"text": ""}}, "2": {"inputs": {"seed": 1}}}), encoding="utf-8")

    config = {
        "discord": {"token": "x"},
        "comfyui": {"queue_workers": 2, "instances": [{"url": "http://127.0.0.1:8188"}]},
        "security": {
            "role_tiers": [
                {
                    "level": 0,
                    "name": "Public",
                    "role_id": None,
                    "daily_limit": 2,
                    "queue_priority": 0,
                    "max_parallel_generations": 1,
                }
            ]
        },
        "workflows": {
            "BASIC": {
                "name": "BASIC",
                "type": "txt2img",
                "workflow": "workflows/wf.json",
                "default": True,
                "text_prompt_node_id": 1,
                "seed_node_id": 2,
            }
        },
    }
    (tmpdir / "config.yml").write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    settings = load_settings(tmpdir / "config.yml")
    bot = RebornComfyBot(settings)

    class FakeSecurity:
        async def is_blocked(self, uid):
            return False

        async def is_member_of_access_guild(self, interaction):
            return True

        async def has_img2img_access(self, interaction):
            return True

        async def determine_tier(self, interaction):
            return RoleTier(0, "Public", None, 2, 0, 1)

        async def get_img2vid_daily_limit(self, interaction):
            return 1

        def validate_workflow_access(self, member, workflow_cfg, settings_str):
            return (True, "")

    class FakeQueue:
        def __init__(self):
            self.enqueued = []
            self.item = None

        async def enqueue(self, **kwargs):
            self.enqueued.append(kwargs)
            self.item = kwargs["session"]
            await asyncio.sleep(0)

        async def cancel_pending(self, session):
            return self.item is session

        def pending_sessions(self):
            return [self.item] if self.item else []

    class FakeComfy:
        async def generate(self, workflow_json):
            return {"prompt_id": "p1"}

        async def listen_for_updates(self, prompt_id, callback, **kwargs):
            await callback("first", sys.modules["discord"].File(io.BytesIO(b"a"), filename="a.png"))
            await callback("second", sys.modules["discord"].File(io.BytesIO(b"b"), filename="b.png"))

        async def cancel_prompt(self, prompt_id):
            return None

    bot.security = FakeSecurity()
    bot.queue = FakeQueue()
    bot.comfy = FakeComfy()
    return bot


async def test_parallel_limit_gate(bot) -> tuple[bool, str]:
    Interaction = sys.modules["discord"].Interaction
    user = FakeUser(1, "alice")
    interactions = [Interaction(user) for _ in range(5)]

    async def submit(interaction):
        await bot.handle_generation_request(interaction=interaction, workflow_type="txt2img", prompt="hello")
        return interaction._response_payload

    results = await asyncio.gather(*(submit(item) for item in interactions))
    rejected = sum(1 for result in results[1:] if result and "active/queued requests" in str(result[0]))
    ok = len(bot.active_sessions.get("1", [])) == 1 and len(bot.queue.enqueued) == 1 and rejected == 4
    return ok, f"parallel gate -> active={len(bot.active_sessions.get('1', []))}, queued={len(bot.queue.enqueued)}, rejected={rejected}"


async def test_cancel_rollback(bot) -> tuple[bool, str]:
    Interaction = sys.modules["discord"].Interaction
    interaction = Interaction(FakeUser(2, "cancel"))
    await bot.handle_generation_request(interaction=interaction, workflow_type="txt2img", prompt="cancel me")
    session = bot.queue.item
    before = bot.usage.get_generation_count("2")
    await bot._cancel_session(session, source="audit")
    after = bot.usage.get_generation_count("2")
    ok = before == 1 and after == 0
    return ok, f"cancel rollback -> before={before}, after={after}"


async def test_additional_output_fallback(bot) -> tuple[bool, str]:
    Interaction = sys.modules["discord"].Interaction
    interaction = Interaction(FakeUser(3, "fallback"))
    await bot.handle_generation_request(interaction=interaction, workflow_type="txt2img", prompt="multi output")
    session = bot.queue.enqueued[-1]["session"]
    session.interaction.followup.fail = True
    await bot._run_generation_session(session)
    ok = bool(session.message.replies) and not session.interaction.followup.sent
    return ok, f"extra output fallback -> replies={len(session.message.replies)}, followups={len(session.interaction.followup.sent)}"


async def test_queue_refresh_coalescing(bot) -> tuple[bool, str]:
    calls = []

    async def fake_refresh():
        calls.append("tick")

    bot._refresh_queue_views = fake_refresh
    await bot._schedule_queue_refresh()
    await bot._schedule_queue_refresh()
    await bot._schedule_queue_refresh()
    await asyncio.sleep(0.5)
    ok = len(calls) == 1
    return ok, f"queue refresh coalescing -> calls={len(calls)}"


async def main() -> int:
    install_discord_stub()
    with tempfile.TemporaryDirectory() as tmp:
        bot = build_test_bot(Path(tmp))
        tests = [
            test_parallel_limit_gate,
            test_cancel_rollback,
            test_additional_output_fallback,
            test_queue_refresh_coalescing,
        ]
        failures = []
        for test in tests:
            ok, detail = await test(bot)
            print(("[PASS]" if ok else "[FAIL]"), test.__name__, "-", detail)
            if not ok:
                failures.append(test.__name__)
        return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
