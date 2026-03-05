import os
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
BACKLOG_FILE = "backlog.md"

GITHUB_API = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{BACKLOG_FILE}"
HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
}


def add_task_to_github(task_text: str) -> None:
    r = httpx.get(GITHUB_API, headers=HEADERS)
    r.raise_for_status()
    data = r.json()
    sha = data["sha"]
    content = base64.b64decode(data["content"]).decode("utf-8")

    section = "## Important (next 2-3 weeks)"
    task_line = f"- [ ] {task_text}"

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


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ALLOWED_USER_ID:
        return
    await update.message.reply_text(
        "Привет! Просто напиши задачу — я добавлю её в backlog."
    )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ALLOWED_USER_ID:
        return

    task_text = update.message.text.strip()
    if not task_text:
        return

    try:
        add_task_to_github(task_text)
        await update.message.reply_text(f"Добавлено в backlog:\n`- [ ] {task_text}`", parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")


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
    print(f"Bot started, repo: {GITHUB_REPO}")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
