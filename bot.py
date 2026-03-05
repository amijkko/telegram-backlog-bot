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

GITHUB_BASE = f"https://api.github.com/repos/{GITHUB_REPO}/contents"
GITHUB_API = f"{GITHUB_BASE}/{BACKLOG_FILE}"
GH_HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github+json",
}

OPENAI_HEADERS = {
    "Authorization": f"Bearer {OPENAI_API_KEY}",
    "Content-Type": "application/json",
}

# --- Priority detection ---

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

# --- Projects for KB ---

PROJECTS = {
    "custody": {"name": "Custody", "path": "kb/custody"},
    "sber": {"name": "Сбер Стейблкоин", "path": "kb/sber"},
    "reksoft": {"name": "Reksoft Consulting", "path": "kb/reksoft"},
    "blind-bets": {"name": "Blind Bets", "path": "kb/blind-bets"},
}

PROJECT_KEYWORDS = {
    "custody": ["custody", "кастоди", "кастодиан", "mpc", "инвестор", "фандрайзинг", "crm"],
    "sber": ["сбер", "стейблкоин", "stablecoin", "гаймаков", "sber"],
    "reksoft": ["reksoft", "рексофт", "консалтинг", "скорочкин", "артём", "артем"],
    "blind-bets": ["blind", "bets", "ставки", "денис", "autoforge"],
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


def detect_project(text: str) -> str | None:
    lower = text.lower()
    for project_id, keywords in PROJECT_KEYWORDS.items():
        for kw in keywords:
            if kw in lower:
                return project_id
    return None


def detect_project_gpt(text: str, filename: str = "") -> str:
    prompt = f"""Определи к какому проекту относится этот документ/текст. Варианты:
- custody (криптокастодиан для банков, инвесторы, фандрайзинг, MPC)
- sber (стейблкоин Сбера, Гаймаков)
- reksoft (консалтинг по крипте через Reksoft, Скорочкин, Артём)
- blind-bets (проект Blind Bets, ставки, Денис, Autoforge)

Файл: {filename}
Текст: {text[:1000]}

Ответь ОДНИМ словом — id проекта (custody/sber/reksoft/blind-bets). Если не можешь определить — ответь unknown."""

    r = httpx.post(
        "https://api.openai.com/v1/chat/completions",
        headers=OPENAI_HEADERS,
        json={
            "model": "gpt-4o-mini",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 10,
            "temperature": 0,
        },
        timeout=15,
    )
    r.raise_for_status()
    answer = r.json()["choices"][0]["message"]["content"].strip().lower()
    return answer if answer in PROJECTS else "unknown"


def summarize_document(text: str, filename: str = "") -> str:
    prompt = f"""Сделай краткое резюме документа на русском (3-5 пунктов). Выдели ключевые факты, цифры, действия.

Файл: {filename}
Текст:
{text[:4000]}"""

    r = httpx.post(
        "https://api.openai.com/v1/chat/completions",
        headers=OPENAI_HEADERS,
        json={
            "model": "gpt-4o-mini",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 500,
            "temperature": 0.3,
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"].strip()


def split_into_tasks(text: str) -> list[str]:
    """Use GPT to split free-form text into actionable tasks."""
    prompt = f"""Пользователь написал сообщение в свободной форме. Разбей его на конкретные actionable задачи.

Правила:
- Каждая задача — одно конкретное действие (позвонить, написать, подготовить, etc.)
- Сохраняй имена, компании, детали из оригинала
- Если в тексте только одна задача — верни только её
- Если слова-маркеры приоритета (срочно, не важно, потом) — сохрани их в задаче
- Формат ответа: каждая задача на отдельной строке, без нумерации и маркеров

Сообщение: {text}"""

    r = httpx.post(
        "https://api.openai.com/v1/chat/completions",
        headers=OPENAI_HEADERS,
        json={
            "model": "gpt-4o-mini",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 300,
            "temperature": 0.2,
        },
        timeout=15,
    )
    r.raise_for_status()
    response = r.json()["choices"][0]["message"]["content"].strip()
    tasks = [line.strip().lstrip("•-–123456789.) ") for line in response.split("\n") if line.strip()]
    return tasks if tasks else [text]


# --- GitHub helpers ---

def github_get_file(path: str) -> tuple[str, str] | None:
    r = httpx.get(f"{GITHUB_BASE}/{path}", headers=GH_HEADERS)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    data = r.json()
    content = base64.b64decode(data["content"]).decode("utf-8")
    return content, data["sha"]


def github_put_file(path: str, content: str, message: str, sha: str = None) -> None:
    encoded = base64.b64encode(content.encode("utf-8")).decode("utf-8")
    payload = {"message": message, "content": encoded}
    if sha:
        payload["sha"] = sha
    r = httpx.put(f"{GITHUB_BASE}/{path}", headers=GH_HEADERS, json=payload)
    r.raise_for_status()


def add_task_to_github(task_text: str) -> str:
    priority, clean_text = detect_priority(task_text)
    section = SECTION_MAP[priority]

    result = github_get_file(BACKLOG_FILE)
    if not result:
        return "error"
    content, sha = result

    task_line = f"- [ ] {clean_text}"
    lines = content.split("\n")
    out = []
    inserted = False
    in_section = False

    for line in lines:
        if line.strip() == section:
            in_section = True
            out.append(line)
            continue
        if in_section:
            if line.startswith("- [ ]"):
                out.append(line)
                continue
            else:
                out.append(task_line)
                inserted = True
                in_section = False
                out.append(line)
                continue
        out.append(line)

    if not inserted:
        out.append(task_line)

    github_put_file(BACKLOG_FILE, "\n".join(out), f"backlog: {task_text[:50]}", sha)
    return PRIORITY_LABELS[priority]


def save_to_kb(project_id: str, filename: str, text: str, summary: str) -> None:
    project = PROJECTS[project_id]
    now = datetime.now().strftime("%Y-%m-%d_%H-%M")
    safe_name = re.sub(r"[^\w\-.]", "_", filename.rsplit(".", 1)[0])
    doc_path = f"{project['path']}/{now}_{safe_name}.md"

    doc_content = f"""# {filename}
*Added: {datetime.now().strftime("%Y-%m-%d %H:%M")}*
*Project: {project['name']}*

## Summary
{summary}

## Full Text
{text[:10000]}
"""
    github_put_file(doc_path, doc_content, f"kb/{project_id}: {filename[:40]}")

    # Update index
    result = github_get_file(f"{project['path']}/index.md")
    if result:
        index_content, index_sha = result
        entry = f"- [{filename}]({now}_{safe_name}.md) — {datetime.now().strftime('%Y-%m-%d')}"
        index_content = index_content.replace(
            "## Documents",
            f"## Documents\n{entry}",
        )
        github_put_file(
            f"{project['path']}/index.md",
            index_content,
            f"kb index: {filename[:40]}",
            index_sha,
        )


# --- Telegram handlers ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ALLOWED_USER_ID:
        return
    await update.message.reply_text(
        "Привет! Вот что я умею:\n\n"
        "📝 **Текст** → задача в backlog (приоритет по словам)\n"
        "🎤 **Голосовое** → распознаю и добавлю в backlog\n"
        "📎 **Файл/документ** → сохраню в базу знаний проекта\n\n"
        "Приоритет: «срочно» → Urgent, «не важно» → Someday\n"
        "Проекты: custody, sber, reksoft, blind-bets\n\n"
        "К файлу можно добавить подпись с названием проекта, "
        "иначе определю автоматически.",
        parse_mode="Markdown",
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
        tasks = split_into_tasks(task_text)
        prefix = "🎤 " if source == "voice" else ""
        results = []
        for task in tasks:
            label = add_task_to_github(task)
            results.append(f"[{label}] `- [ ] {task}`")
        msg = prefix + "Добавлено в backlog:\n" + "\n".join(results)
        await update.message.reply_text(msg, parse_mode="Markdown")
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


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ALLOWED_USER_ID:
        return

    doc = update.message.document
    caption = update.message.caption or ""
    filename = doc.file_name or "document"

    await update.message.reply_text(f"📎 Получил `{filename}`, обрабатываю...", parse_mode="Markdown")

    try:
        # Download file
        tg_file = await doc.get_file()
        buf = bytearray()
        await tg_file.download_as_bytearray(buf)
        raw = bytes(buf)

        # Extract text based on file type
        ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

        if ext == "pdf":
            text = extract_pdf_text(raw)
        elif ext in ("txt", "md", "csv", "json", "py", "js", "ts", "sol"):
            text = raw.decode("utf-8", errors="replace")
        elif ext in ("xlsx", "xls"):
            text = extract_xlsx_text(raw)
        elif ext == "docx":
            text = extract_docx_text(raw)
        elif ext == "doc":
            text = raw.decode("utf-8", errors="replace")[:500] + "\n[Binary .doc — partial extraction]"
        else:
            text = raw.decode("utf-8", errors="replace")[:2000]

        if not text.strip():
            await update.message.reply_text("Не удалось извлечь текст из файла.")
            return

        # Detect project
        project_id = detect_project(caption) if caption else None
        if not project_id:
            project_id = detect_project(filename)
        if not project_id:
            project_id = detect_project_gpt(text, filename)

        if project_id == "unknown":
            await update.message.reply_text(
                "Не могу определить проект. Добавь подпись к файлу:\n"
                "`custody`, `sber`, `reksoft` или `blind-bets`",
                parse_mode="Markdown",
            )
            return

        # Summarize
        summary = summarize_document(text, filename)

        # Save to KB
        save_to_kb(project_id, filename, text, summary)

        project_name = PROJECTS[project_id]["name"]
        await update.message.reply_text(
            f"📚 Сохранено в KB **{project_name}**\n\n"
            f"**{filename}**\n{summary[:500]}",
            parse_mode="Markdown",
        )

    except Exception as e:
        await update.message.reply_text(f"Ошибка обработки файла: {e}")


def extract_pdf_text(raw: bytes) -> str:
    import subprocess
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
        f.write(raw)
        f.flush()
        result = subprocess.run(
            ["pdftotext", f.name, "-"],
            capture_output=True, text=True, timeout=30,
        )
    return result.stdout if result.returncode == 0 else ""


def extract_docx_text(raw: bytes) -> str:
    import zipfile
    import io
    import xml.etree.ElementTree as ET
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as z:
            xml_content = z.read("word/document.xml")
        tree = ET.fromstring(xml_content)
        ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
        paragraphs = []
        for p in tree.iter(f"{{{ns['w']}}}p"):
            texts = [t.text for t in p.iter(f"{{{ns['w']}}}t") if t.text]
            if texts:
                paragraphs.append("".join(texts))
        return "\n\n".join(paragraphs)
    except Exception:
        return ""


def extract_xlsx_text(raw: bytes) -> str:
    try:
        import openpyxl
        import io
        wb = openpyxl.load_workbook(io.BytesIO(raw))
        lines = []
        for sheet_name in wb.sheetnames:
            ws = wb[sheet_name]
            lines.append(f"=== {sheet_name} ===")
            for row in ws.iter_rows(values_only=True):
                vals = [str(c) if c is not None else "" for c in row]
                if any(vals):
                    lines.append("\t".join(vals))
        return "\n".join(lines)
    except Exception:
        return ""


def main() -> None:
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
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    print(f"Bot started, repo: {GITHUB_REPO}")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
