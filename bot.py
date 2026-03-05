import os
import re
import sys
import time
import base64
from datetime import datetime

import httpx
from dotenv import load_dotenv
from telegram import Update, Bot
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters

load_dotenv()

TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
ALLOWED_USER_ID = int(os.environ["TELEGRAM_USER_ID"])
GITHUB_TOKEN = os.environ["GITHUB_TOKEN"]
GITHUB_REPO = os.environ.get("GITHUB_REPO", "amijkko/personal-goals")
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]
BACKLOG_FILE = "backlog.md"

GITHUB_API = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{BACKLOG_FILE}"
HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
}


URGENT_KEYWORDS = r"(?:срочно|важно|асап|asap|urgent|горит|критично)"
LOW_KEYWORDS = r"(?:не\s*важно|неважно|когда-нибудь|потом|low|при\s*случае|без\s*спешки)"

SECTION_MAP = {
    "urgent": "## Urgent (this week)",
    "normal": "## Important (next 2-3 weeks)",
    "low": "## Someday (no deadline)",
}

PRIORITY_LABELS = {
    "urgent": "Urgent",
    "normal": "Important",
    "low": "Someday",
}


def detect_priority(text: str) -> tuple[str, str]:
    lower = text.lower()
    if re.search(URGENT_KEYWORDS, lower):
        clean = re.sub(URGENT_KEYWORDS, "", lower, count=1).strip(" ,:-—")
        return "urgent", clean or text
    if re.search(LOW_KEYWORDS, lower):
        clean = re.sub(LOW_KEYWORDS, "", lower, count=1).strip(" ,:-—")
        return "low", clean or text
    return "normal", text


def add_task_to_github(task_text: str) -> str:
    priority, clean_text = detect_priority(task_text)
    section = SECTION_MAP[priority]

    r = httpx.get(GITHUB_API, headers=HEADERS)
    r.raise_for_status()
    data = r.json()
    sha = data["sha"]
    content = base64.b64decode(data["content"]).decode("utf-8")

    task_line = f"- [ ] {clean_text}"

    lines = content.split("\n")
    result = []
    inserted = False
    in_section = False

    for line in lines:
        if line.strip() == section:
            in_section = True
            result.append(line)
            continue

        if in_section:
            if line.startswith("- [ ]"):
                result.append(line)
                continue
            else:
                result.append(task_line)
                inserted = True
                in_section = False
                result.append(line)
                continue

        result.append(line)

    if not inserted:
        result.append(task_line)

    new_content = "\n".join(result)
    encoded = base64.b64encode(new_content.encode("utf-8")).decode("utf-8")

    r = httpx.put(
        GITHUB_API,
        headers=HEADERS,
        json={
            "message": f"backlog: {task_text[:50]}",
            "content": encoded,
            "sha": sha,
        },
    )
    r.raise_for_status()
    return PRIORITY_LABELS[priority]


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ALLOWED_USER_ID:
        return
    await update.message.reply_text(
        "Привет! Напиши задачу — я добавлю её в backlog.\n\n"
        "Приоритет определяю по словам:\n"
        "• «срочно/важно/асап» → Urgent\n"
        "• «не важно/потом/при случае» → Someday\n"
        "• без маркера → Important"
    )


async def transcribe_voice(voice_file) -> str:
    buf = bytearray()
    await voice_file.download_as_bytearray(buf)
    r = httpx.post(
        "https://api.openai.com/v1/audio/transcriptions",
        headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
        data={"model": "whisper-1", "language": "ru"},
        files={"file": ("voice.ogg", bytes(buf), "audio/ogg")},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["text"]


async def process_task(update: Update, task_text: str, source: str = "") -> None:
    try:
        label = add_task_to_github(task_text)
        prefix = f"🎤 " if source == "voice" else ""
        await update.message.reply_text(
            f"{prefix}[{label}] Добавлено в backlog:\n`- [ ] {task_text}`",
            parse_mode="Markdown",
        )
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ALLOWED_USER_ID:
        return

    task_text = update.message.text.strip()
    if not task_text:
        return

    await process_task(update, task_text)


async def handle_voice(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ALLOWED_USER_ID:
        return

    try:
        voice_file = await update.message.voice.get_file()
        task_text = await transcribe_voice(voice_file)
        await process_task(update, task_text, source="voice")
    except Exception as e:
        await update.message.reply_text(f"Ошибка распознавания: {e}")


def main() -> None:
    # Clear any existing webhook to avoid conflicts
    print("Clearing webhook and waiting for old instance...")
    httpx.post(
        f"https://api.telegram.org/bot{TOKEN}/deleteWebhook",
        json={"drop_pending_updates": True},
    )
    time.sleep(5)

    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(MessageHandler(filters.VOICE, handle_voice))
    print(f"Bot started, repo: {GITHUB_REPO}")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
