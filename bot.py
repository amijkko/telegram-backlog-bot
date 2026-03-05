import os
import re
import sys
import time
import base64
from datetime import datetime

import httpx
from dotenv import load_dotenv
from telegram import Update, Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters

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


WEEKDAYS_RU = {
    "понедельник": 0, "вторник": 1, "среду": 2, "среда": 2,
    "четверг": 3, "пятницу": 4, "пятница": 4,
    "субботу": 5, "суббота": 5, "воскресенье": 6,
}

MONTHS_RU = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4,
    "мая": 5, "июня": 6, "июля": 7, "августа": 8,
    "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
}

DAY_NAMES_SHORT = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"]


def parse_date(text: str) -> tuple[str | None, str]:
    """Extract date from Russian text. Returns (date_label, cleaned_text)."""
    from datetime import timedelta
    today = datetime.now().date()
    lower = text.lower()

    # "сегодня"
    m = re.search(r"\bсегодня\b", lower)
    if m:
        d = today
        clean = text[:m.start()] + text[m.end():]
        label = f"{DAY_NAMES_SHORT[d.weekday()]} {d.strftime('%d.%m')}"
        return label, clean.strip(" ,:-—")

    # "завтра"
    m = re.search(r"\bзавтра\b", lower)
    if m:
        d = today + timedelta(days=1)
        clean = text[:m.start()] + text[m.end():]
        label = f"{DAY_NAMES_SHORT[d.weekday()]} {d.strftime('%d.%m')}"
        return label, clean.strip(" ,:-—")

    # "послезавтра"
    m = re.search(r"\bпослезавтра\b", lower)
    if m:
        d = today + timedelta(days=2)
        clean = text[:m.start()] + text[m.end():]
        label = f"{DAY_NAMES_SHORT[d.weekday()]} {d.strftime('%d.%m')}"
        return label, clean.strip(" ,:-—")

    # "через N дней/дня"
    m = re.search(r"\bчерез\s+(\d+)\s+(?:день|дня|дней)\b", lower)
    if m:
        d = today + timedelta(days=int(m.group(1)))
        clean = text[:m.start()] + text[m.end():]
        label = f"{DAY_NAMES_SHORT[d.weekday()]} {d.strftime('%d.%m')}"
        return label, clean.strip(" ,:-—")

    # "в понедельник" / "на среду" / "во вторник" (next occurrence)
    for day_name, day_num in WEEKDAYS_RU.items():
        pattern = rf"\b(?:в|на|во)\s+(?:след(?:ующ[а-я]*)?\s+)?{day_name}\b"
        m = re.search(pattern, lower)
        if m:
            is_next = "след" in m.group(0)
            days_ahead = (day_num - today.weekday()) % 7
            if days_ahead == 0:
                days_ahead = 7
            if is_next and days_ahead < 7:
                days_ahead += 7
            d = today + timedelta(days=days_ahead)
            clean = text[:m.start()] + text[m.end():]
            label = f"{DAY_NAMES_SHORT[d.weekday()]} {d.strftime('%d.%m')}"
            return label, clean.strip(" ,:-—")

    # "на следующей неделе" (Monday)
    m = re.search(r"\bна\s+след(?:ующ[а-я]*)?\s+недел[а-яё]+\b", lower)
    if m:
        days_ahead = (0 - today.weekday()) % 7
        if days_ahead == 0:
            days_ahead = 7
        d = today + timedelta(days=days_ahead)
        clean = text[:m.start()] + text[m.end():]
        label = f"{DAY_NAMES_SHORT[d.weekday()]} {d.strftime('%d.%m')}"
        return label, clean.strip(" ,:-—")

    # "N марта" / "N мая" etc
    for month_name, month_num in MONTHS_RU.items():
        pattern = rf"\b(\d{{1,2}})\s+{month_name}\b"
        m = re.search(pattern, lower)
        if m:
            day = int(m.group(1))
            year = today.year
            try:
                from datetime import date
                d = date(year, month_num, day)
                if d < today:
                    d = date(year + 1, month_num, day)
            except ValueError:
                continue
            clean = text[:m.start()] + text[m.end():]
            label = f"{DAY_NAMES_SHORT[d.weekday()]} {d.strftime('%d.%m')}"
            return label, clean.strip(" ,:-—")

    # "в понедельник" without preposition — just weekday name
    for day_name, day_num in WEEKDAYS_RU.items():
        m = re.search(rf"\b{day_name}\b", lower)
        if m:
            days_ahead = (day_num - today.weekday()) % 7
            if days_ahead == 0:
                days_ahead = 7
            d = today + timedelta(days=days_ahead)
            clean = text[:m.start()] + text[m.end():]
            label = f"{DAY_NAMES_SHORT[d.weekday()]} {d.strftime('%d.%m')}"
            return label, clean.strip(" ,:-—")

    return None, text


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


INSIGHT_TAGS = [
    "финансы", "инвест", "продукт", "стратегия", "маркетинг",
    "технологии", "команда", "партнёры", "регуляция", "конкуренты",
    "продажи", "клиенты", "операции", "идея",
]


def detect_insight_tags(text: str) -> list[str]:
    """Use GPT to assign tags to an insight."""
    tags_list = ", ".join(INSIGHT_TAGS)
    prompt = f"""Прочитай инсайт и присвой ему 1-3 тега из списка: {tags_list}.

Инсайт: {text}

Ответь ТОЛЬКО тегами через запятую, без объяснений."""

    r = httpx.post(
        "https://api.openai.com/v1/chat/completions",
        headers=OPENAI_HEADERS,
        json={
            "model": "gpt-4o-mini",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 30,
            "temperature": 0,
        },
        timeout=15,
    )
    r.raise_for_status()
    answer = r.json()["choices"][0]["message"]["content"].strip().lower()
    tags = [t.strip() for t in answer.split(",") if t.strip() in INSIGHT_TAGS]
    return tags if tags else ["идея"]


def save_insight(project_id: str, text: str, tags: list[str]) -> None:
    """Save insight to project's KB index.md under ## Insights section."""
    project = PROJECTS[project_id]
    index_path = f"{project['path']}/index.md"
    result = github_get_file(index_path)
    if not result:
        return

    content, sha = result
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    tags_str = " ".join(f"#{t}" for t in tags)
    entry = f"- {text} — `{tags_str}` _{now}_"

    if "## Insights" in content:
        content = content.replace("## Insights", f"## Insights\n{entry}", 1)
    else:
        content = content.replace("## Documents", f"## Insights\n{entry}\n\n## Documents")

    github_put_file(index_path, content, f"insight/{project_id}: {text[:40]}", sha)


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
    date_label, clean_text = parse_date(task_text)
    priority, clean_text = detect_priority(clean_text)
    section = SECTION_MAP[priority]

    result = github_get_file(BACKLOG_FILE)
    if not result:
        return "error"
    content, sha = result

    date_suffix = f" (→ {date_label})" if date_label else ""
    task_line = f"- [ ] {clean_text}{date_suffix}"
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


def collect_open_tasks() -> list[dict]:
    """Collect all open tasks from daily, weekly, backlog."""
    tasks = []
    for file_id, label in [("daily.md", "today"), ("weekly.md", "week"), ("backlog.md", "backlog")]:
        result = github_get_file(file_id)
        if not result:
            continue
        content, _ = result
        for i, line in enumerate(content.split("\n")):
            s = line.strip()
            if s.startswith("- [ ]") or re.match(r"^\d+\.\s*\[ \]", s):
                task_text = re.sub(r"^(?:-|\d+\.)\s*\[ \]\s*", "", s)
                if task_text and task_text != "[Task]":
                    tasks.append({"file": file_id, "line_idx": i, "text": task_text, "source": label})
    return tasks


def toggle_task_in_file(file_id: str, line_idx: int, close: bool = True) -> bool:
    result = github_get_file(file_id)
    if not result:
        return False
    content, sha = result
    lines = content.split("\n")
    if line_idx >= len(lines):
        return False

    old = "[ ]" if close else "[x]"
    new = "[x]" if close else "[ ]"
    if old in lines[line_idx]:
        lines[line_idx] = lines[line_idx].replace(old, new, 1)
        github_put_file(file_id, "\n".join(lines), f"{'close' if close else 'reopen'}: {lines[line_idx][:50]}", sha)
        return True
    return False


# --- Telegram handlers ---

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ALLOWED_USER_ID:
        return
    await update.message.reply_text(
        "Привет! Вот что я умею:\n\n"
        "📝 **Текст** → задачи в backlog\n"
        "🎤 **Голосовое** → распознаю и добавлю\n"
        "📎 **Файл** → сохраню в KB проекта\n\n"
        "**Команды:**\n"
        "/today — задачи на сегодня\n"
        "/week — план на неделю\n"
        "/backlog — бэклог\n"
        "/crm — CRM инвесторов\n"
        "/tracks — рабочие треки\n"
        "/insights — инсайты по проектам\n"
        "/project — инфо о проекте + документы\n"
        "/done — закрыть задачу\n\n"
        "💡 `инсайт: мысль` → сохранит в KB с тегами\n"
        "Закрыть задачу: `готово написать Грише`\n"
        "Приоритет: «срочно» → Urgent, «не важно» → Someday",
        parse_mode="Markdown",
    )


def escape_md(text: str) -> str:
    """Escape Markdown special characters for Telegram."""
    for ch in ("_", "*", "`", "[", "]", "(", ")", "~", ">", "#", "+", "-", "=", "|", "{", "}", ".", "!"):
        text = text.replace(ch, f"\\{ch}")
    return text


def format_file_pretty(file_id: str, content: str) -> tuple[str, list[dict]]:
    """Format markdown nicely for Telegram, collect open tasks."""
    lines = content.split("\n")
    formatted = []
    tasks = []

    for i, line in enumerate(lines):
        s = line.strip()
        if not s or s.startswith("---") or s.startswith("*Last updated"):
            continue
        if s.startswith("# "):
            formatted.append(f"*{s[2:]}*\n")
        elif s.startswith("## "):
            formatted.append(f"📌 *{s[3:]}*")
        elif s.startswith("### "):
            formatted.append(f"  🔹 *{s[4:]}*")
        elif s.startswith("> "):
            formatted.append(f"_{s[2:]}_")
        elif s.startswith("- [x]") or re.match(r"^\d+\.\s*\[x\]", s):
            text_part = re.sub(r"^(?:-|\d+\.)\s*\[x\]\s*", "", s)
            formatted.append(f"  ✅ ~{text_part}~")
        elif s.startswith("- [ ]") or re.match(r"^\d+\.\s*\[ \]", s):
            task_text = re.sub(r"^(?:-|\d+\.)\s*\[ \]\s*", "", s)
            if task_text and task_text != "[Task]":
                num = len(tasks) + 1
                formatted.append(f"  ⬜ {num}. {task_text}")
                tasks.append({"file": file_id, "line_idx": i, "text": task_text})
        elif s.startswith("- **"):
            formatted.append(f"  {s[2:]}")
        elif s.startswith("|"):
            formatted.append(f"`{s}`")
        else:
            formatted.append(s)

    return "\n".join(formatted), tasks


def build_task_buttons(tasks: list[dict], prefix: str = "close") -> InlineKeyboardMarkup | None:
    if not tasks:
        return None
    keyboard = []
    row = []
    for i, t in enumerate(tasks[:20]):
        row.append(InlineKeyboardButton(f"✅ {i+1}", callback_data=f"{prefix}:{t['file']}:{t['line_idx']}"))
        if len(row) == 5:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    return InlineKeyboardMarkup(keyboard)


async def send_formatted(update, file_id: str, context) -> None:
    result = github_get_file(file_id)
    if not result:
        await update.message.reply_text(f"{file_id} не найден")
        return
    text, tasks = format_file_pretty(file_id, result[0])
    markup = build_task_buttons(tasks)
    await update.message.reply_text(text[:4000], parse_mode="Markdown", reply_markup=markup)


async def cmd_today(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ALLOWED_USER_ID:
        return
    await send_formatted(update, "daily.md", context)


async def cmd_week(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ALLOWED_USER_ID:
        return
    await send_formatted(update, "weekly.md", context)


async def cmd_backlog(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ALLOWED_USER_ID:
        return
    await send_formatted(update, "backlog.md", context)


async def cmd_crm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ALLOWED_USER_ID:
        return
    result = github_get_file("crm.md")
    if not result:
        await update.message.reply_text("crm.md не найден")
        return
    text, _ = format_file_pretty("crm.md", result[0])
    await update.message.reply_text(text[:4000], parse_mode="Markdown")


async def cmd_done(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ALLOWED_USER_ID:
        return
    tasks = collect_open_tasks()
    if not tasks:
        await update.message.reply_text("Нет открытых задач!")
        return

    # Store tasks in context for callback
    context.user_data["tasks"] = tasks

    # Build inline keyboard (max 30 tasks to avoid Telegram limits)
    keyboard = []
    for i, t in enumerate(tasks[:30]):
        icon = {"today": "📅", "week": "📋", "backlog": "📝"}.get(t["source"], "")
        label = f"{icon} {t['text'][:45]}"
        keyboard.append([InlineKeyboardButton(label, callback_data=f"done:{i}")])

    await update.message.reply_text(
        "Выбери задачу для закрытия:",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query.from_user.id != ALLOWED_USER_ID:
        return
    await query.answer()

    data = query.data

    if data.startswith("refresh:"):
        file_id = data.split(":", 1)[1]
        result = github_get_file(file_id)
        if result:
            text, tasks = format_file_pretty(file_id, result[0])
            markup = build_task_buttons(tasks)
            await query.edit_message_text(text[:4000], parse_mode="Markdown", reply_markup=markup)
        else:
            await query.edit_message_text(f"{file_id} не найден")

    elif data.startswith("close:"):
        parts = data.split(":", 2)
        file_id = parts[1]
        line_idx = int(parts[2])
        ok = toggle_task_in_file(file_id, line_idx, close=True)
        if ok:
            # Read closed task text for confirmation
            result = github_get_file(file_id)
            closed_text = ""
            if result:
                lines = result[0].split("\n")
                if line_idx < len(lines):
                    closed_text = re.sub(r"^(?:-|\d+\.)\s*\[x\]\s*", "", lines[line_idx].strip())
            refresh_btn = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Обновить список", callback_data=f"refresh:{file_id}")]
            ])
            await query.edit_message_text(
                f"✅ {closed_text}\n\nНажми «Обновить» чтобы увидеть остальные задачи.",
                reply_markup=refresh_btn,
            )
        else:
            await query.edit_message_text("Не удалось закрыть задачу.")

    elif data.startswith("done:"):
        idx = int(data.split(":")[1])
        tasks = context.user_data.get("tasks", [])
        if idx >= len(tasks):
            await query.edit_message_text("Задача не найдена.")
            return
        task = tasks[idx]
        ok = toggle_task_in_file(task["file"], task["line_idx"], close=True)
        if ok:
            await query.edit_message_text(f"✅ Закрыто:\n~{task['text']}~", parse_mode="Markdown")
        else:
            await query.edit_message_text("Не удалось закрыть задачу.")


async def cmd_insights(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ALLOWED_USER_ID:
        return
    args = context.args
    project_ids = [args[0]] if args and args[0] in PROJECTS else list(PROJECTS.keys())
    parts = []
    for pid in project_ids:
        result = github_get_file(f"{PROJECTS[pid]['path']}/index.md")
        if not result:
            continue
        content = result[0]
        if "## Insights" not in content:
            continue
        # Extract insights section
        section = content.split("## Insights")[1].split("## ")[0].strip()
        if section:
            parts.append(f"💡 *{PROJECTS[pid]['name']}*\n{section}")
    if parts:
        await update.message.reply_text("\n\n".join(parts)[:4000], parse_mode="Markdown")
    else:
        await update.message.reply_text("Пока нет инсайтов. Напиши `инсайт: твоя мысль`", parse_mode="Markdown")


async def cmd_project(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ALLOWED_USER_ID:
        return
    args = context.args
    if not args or args[0] not in PROJECTS:
        keys = ", ".join(f"`{k}`" for k in PROJECTS)
        await update.message.reply_text(f"Укажи проект: /project {keys}", parse_mode="Markdown")
        return
    pid = args[0]
    result = github_get_file(f"{PROJECTS[pid]['path']}/index.md")
    if not result:
        await update.message.reply_text("index.md не найден")
        return
    content = result[0]
    # Extract "О проекте" section
    if "## О проекте" in content:
        about = content.split("## О проекте")[1].split("\n## ")[0].strip()
    else:
        about = None
    # Extract "Ключевые документы" if present
    if "**Ключевые документы**" in content:
        docs = content.split("**Ключевые документы**")[1].split("\n## ")[0].strip()
    else:
        docs = None

    parts = [f"📋 *{PROJECTS[pid]['name']}*"]
    if about:
        # Clean markdown for Telegram (bold markers already in text)
        parts.append(about)
    if docs:
        parts.append(f"\n📎 *Ключевые документы*\n{docs}")
    if not about and not docs:
        parts.append("Описание проекта пока не заполнено.")

    text = "\n\n".join(parts)
    # Strip markdown that Telegram can't handle
    clean = text.replace("**", "*")
    await update.message.reply_text(clean[:4000], parse_mode="Markdown")


async def cmd_tracks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id != ALLOWED_USER_ID:
        return
    await send_formatted(update, "tracks.md", context)


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

    lower = task_text.lower()

    # Check for "инсайт:" prefix
    insight_match = re.match(r"^(?:инсайт|insight|💡)[:\s]+(.+)", lower, re.IGNORECASE)
    if insight_match:
        insight_text = task_text[insight_match.start(1):]  # preserve original case
        try:
            # Detect project
            project_id = detect_project(insight_text)
            if not project_id:
                project_id = detect_project_gpt(insight_text)
            if project_id == "unknown":
                project_id = "custody"  # default

            tags = detect_insight_tags(insight_text)
            save_insight(project_id, insight_text, tags)
            tags_str = " ".join(f"#{t}" for t in tags)
            await update.message.reply_text(
                f"💡 Инсайт сохранён в {PROJECTS[project_id]['name']}\n"
                f"{tags_str}\n\n{insight_text}",
            )
        except Exception as e:
            await update.message.reply_text(f"Ошибка сохранения инсайта: {e}")
        return

    # Check for "done/готово" prefix
    done_match = re.match(r"^(?:готово|done|✅|сделано)[:\s]+(.+)", lower)
    if done_match:
        query = done_match.group(1).strip()
        tasks = collect_open_tasks()
        # Fuzzy match
        matched = [t for t in tasks if query in t["text"].lower()]
        if len(matched) == 1:
            ok = toggle_task_in_file(matched[0]["file"], matched[0]["line_idx"], close=True)
            if ok:
                await update.message.reply_text(f"✅ Закрыто:\n~{matched[0]['text']}~", parse_mode="Markdown")
                return
        elif len(matched) > 1:
            # Show buttons to pick
            context.user_data["tasks"] = matched
            keyboard = []
            for i, t in enumerate(matched[:10]):
                keyboard.append([InlineKeyboardButton(t["text"][:45], callback_data=f"done:{i}")])
            await update.message.reply_text("Несколько совпадений, выбери:", reply_markup=InlineKeyboardMarkup(keyboard))
            return
        else:
            await update.message.reply_text("Не нашёл такую задачу. Попробуй /done для списка.")
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
        clean_summary = summary[:500].replace("*", "").replace("`", "").replace("_", "")
        await update.message.reply_text(
            f"📚 Сохранено в KB {project_name}\n\n"
            f"{filename}\n{clean_summary}",
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
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("today", cmd_today))
    app.add_handler(CommandHandler("week", cmd_week))
    app.add_handler(CommandHandler("backlog", cmd_backlog))
    app.add_handler(CommandHandler("crm", cmd_crm))
    app.add_handler(CommandHandler("tracks", cmd_tracks))
    app.add_handler(CommandHandler("insights", cmd_insights))
    app.add_handler(CommandHandler("project", cmd_project))
    app.add_handler(CommandHandler("done", cmd_done))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_handler(MessageHandler(filters.VOICE, handle_voice))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document))
    print(f"Bot started, repo: {GITHUB_REPO}")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
