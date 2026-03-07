"""CRM Database Layer — PostgreSQL-backed contact management with full history."""

import os
import json
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone, timedelta

MOSCOW_TZ = timezone(timedelta(hours=3))

DATABASE_URL = os.environ.get("CRM_DATABASE_URL") or os.environ.get("DATABASE_URL", "")


def get_conn():
    """Get a database connection."""
    return psycopg2.connect(DATABASE_URL)


def init_db():
    """Create all CRM tables if they don't exist."""
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS contacts (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        company VARCHAR(255),
        role VARCHAR(255),
        project VARCHAR(50),
        telegram VARCHAR(100),
        email VARCHAR(255),
        phone VARCHAR(50),
        warmth VARCHAR(50) DEFAULT 'новый',
        confidence INTEGER DEFAULT 0,
        bio TEXT,
        how_we_met TEXT,
        deal_context TEXT,
        personal_notes TEXT,
        next_action TEXT,
        ping_date DATE,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS contact_aliases (
        id SERIAL PRIMARY KEY,
        contact_id INTEGER REFERENCES contacts(id) ON DELETE CASCADE,
        alias VARCHAR(255) NOT NULL,
        UNIQUE(alias)
    );

    CREATE TABLE IF NOT EXISTS interactions (
        id SERIAL PRIMARY KEY,
        contact_id INTEGER REFERENCES contacts(id) ON DELETE CASCADE,
        type VARCHAR(50) NOT NULL,
        date DATE NOT NULL,
        summary TEXT NOT NULL,
        content TEXT,
        source VARCHAR(50) DEFAULT 'bot',
        created_at TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS contact_facts (
        id SERIAL PRIMARY KEY,
        contact_id INTEGER REFERENCES contacts(id) ON DELETE CASCADE,
        category VARCHAR(50) NOT NULL,
        fact_text TEXT NOT NULL,
        source_interaction_id INTEGER REFERENCES interactions(id),
        created_at TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS contact_tasks (
        id SERIAL PRIMARY KEY,
        contact_id INTEGER REFERENCES contacts(id) ON DELETE CASCADE,
        text TEXT NOT NULL,
        priority VARCHAR(20) DEFAULT 'normal',
        status VARCHAR(20) DEFAULT 'open',
        due_date DATE,
        done_date DATE,
        created_at TIMESTAMP DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_contacts_name ON contacts(LOWER(name));
    CREATE INDEX IF NOT EXISTS idx_contacts_project ON contacts(project);
    CREATE INDEX IF NOT EXISTS idx_aliases_alias ON contact_aliases(LOWER(alias));
    CREATE INDEX IF NOT EXISTS idx_interactions_contact ON interactions(contact_id, date DESC);
    CREATE INDEX IF NOT EXISTS idx_facts_contact ON contact_facts(contact_id);
    CREATE INDEX IF NOT EXISTS idx_tasks_contact ON contact_tasks(contact_id, status);
    """)

    conn.commit()
    cur.close()
    conn.close()


# ============================================================================
# Contact CRUD
# ============================================================================

def find_contact(name_query: str) -> dict | None:
    """Find contact by name or alias (fuzzy match)."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    q = name_query.lower().strip()

    # 1. Exact match on name
    cur.execute("SELECT * FROM contacts WHERE LOWER(name) = %s", (q,))
    row = cur.fetchone()

    # 2. Exact match on alias
    if not row:
        cur.execute("""
            SELECT c.* FROM contacts c
            JOIN contact_aliases a ON c.id = a.contact_id
            WHERE LOWER(a.alias) = %s
        """, (q,))
        row = cur.fetchone()

    # 3. Partial match on name
    if not row:
        cur.execute("SELECT * FROM contacts WHERE LOWER(name) LIKE %s ORDER BY name LIMIT 1",
                     (f"%{q}%",))
        row = cur.fetchone()

    # 4. Partial match on alias
    if not row:
        cur.execute("""
            SELECT c.* FROM contacts c
            JOIN contact_aliases a ON c.id = a.contact_id
            WHERE LOWER(a.alias) LIKE %s
            ORDER BY c.name LIMIT 1
        """, (f"%{q}%",))
        row = cur.fetchone()

    cur.close()
    conn.close()
    return dict(row) if row else None


def add_contact(data: dict) -> int:
    """Create a new contact. Returns contact ID."""
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO contacts (name, company, role, project, telegram, email, phone,
                              warmth, confidence, bio, how_we_met, deal_context,
                              personal_notes, next_action, ping_date)
        VALUES (%(name)s, %(company)s, %(role)s, %(project)s, %(telegram)s, %(email)s,
                %(phone)s, %(warmth)s, %(confidence)s, %(bio)s, %(how_we_met)s,
                %(deal_context)s, %(personal_notes)s, %(next_action)s, %(ping_date)s)
        RETURNING id
    """, {
        "name": data.get("name", ""),
        "company": data.get("company"),
        "role": data.get("role"),
        "project": data.get("project"),
        "telegram": data.get("telegram"),
        "email": data.get("email"),
        "phone": data.get("phone"),
        "warmth": data.get("warmth", "новый"),
        "confidence": int(data.get("confidence", 0)),
        "bio": data.get("bio"),
        "how_we_met": data.get("how_we_met"),
        "deal_context": data.get("deal_context"),
        "personal_notes": data.get("personal_notes"),
        "next_action": data.get("next_action"),
        "ping_date": data.get("ping_date"),
    })

    contact_id = cur.fetchone()[0]

    # Add name as alias + any extra aliases
    aliases = {data["name"].lower()}
    for a in data.get("aliases", []):
        aliases.add(a.lower())
    for alias in aliases:
        try:
            cur.execute("INSERT INTO contact_aliases (contact_id, alias) VALUES (%s, %s)",
                         (contact_id, alias))
        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            # Alias exists for another contact — skip
            continue

    conn.commit()
    cur.close()
    conn.close()
    return contact_id


def update_contact(contact_id: int, data: dict) -> None:
    """Update contact fields. Only updates non-None values."""
    conn = get_conn()
    cur = conn.cursor()

    updatable = ["name", "company", "role", "project", "telegram", "email", "phone",
                 "warmth", "confidence", "bio", "how_we_met", "deal_context",
                 "personal_notes", "next_action", "ping_date"]

    sets = []
    vals = []
    for field in updatable:
        if field in data and data[field] is not None:
            sets.append(f"{field} = %s")
            vals.append(data[field])

    if sets:
        sets.append("updated_at = NOW()")
        vals.append(contact_id)
        cur.execute(f"UPDATE contacts SET {', '.join(sets)} WHERE id = %s", vals)

    conn.commit()
    cur.close()
    conn.close()


def add_alias(contact_id: int, alias: str) -> bool:
    """Add an alias for a contact. Returns False if alias already exists."""
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO contact_aliases (contact_id, alias) VALUES (%s, %s)",
                     (contact_id, alias.lower()))
        conn.commit()
        result = True
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        result = False
    cur.close()
    conn.close()
    return result


# ============================================================================
# Interactions
# ============================================================================

def add_interaction(contact_id: int, type_: str, summary: str,
                    content: str = None, date_: str = None, source: str = "bot") -> int:
    """Log an interaction with a contact. Returns interaction ID."""
    conn = get_conn()
    cur = conn.cursor()

    if date_:
        d = date_
    else:
        d = datetime.now(MOSCOW_TZ).strftime("%Y-%m-%d")

    cur.execute("""
        INSERT INTO interactions (contact_id, type, date, summary, content, source)
        VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
    """, (contact_id, type_, d, summary, content, source))

    interaction_id = cur.fetchone()[0]

    # Update contact's updated_at
    cur.execute("UPDATE contacts SET updated_at = NOW() WHERE id = %s", (contact_id,))

    conn.commit()
    cur.close()
    conn.close()
    return interaction_id


def get_interactions(contact_id: int, limit: int = 10) -> list[dict]:
    """Get recent interactions for a contact."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT * FROM interactions
        WHERE contact_id = %s
        ORDER BY date DESC, created_at DESC
        LIMIT %s
    """, (contact_id, limit))
    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


# ============================================================================
# Facts
# ============================================================================

def add_fact(contact_id: int, category: str, fact_text: str,
             source_interaction_id: int = None) -> int:
    """Add a fact about a contact. Categories: bio, connection, interest, personal, deal, how_met."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO contact_facts (contact_id, category, fact_text, source_interaction_id)
        VALUES (%s, %s, %s, %s) RETURNING id
    """, (contact_id, category, fact_text, source_interaction_id))
    fact_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return fact_id


def get_facts(contact_id: int) -> dict[str, list[str]]:
    """Get all facts about a contact, grouped by category."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT category, fact_text FROM contact_facts
        WHERE contact_id = %s ORDER BY created_at
    """, (contact_id,))
    result = {}
    for row in cur.fetchall():
        cat = row["category"]
        if cat not in result:
            result[cat] = []
        result[cat].append(row["fact_text"])
    cur.close()
    conn.close()
    return result


# ============================================================================
# Tasks
# ============================================================================

def add_contact_task(contact_id: int, text: str, priority: str = "normal",
                     due_date: str = None) -> int:
    """Add a task linked to a contact."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO contact_tasks (contact_id, text, priority, due_date)
        VALUES (%s, %s, %s, %s) RETURNING id
    """, (contact_id, text, priority, due_date))
    task_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return task_id


def complete_contact_task(task_id: int) -> None:
    """Mark a contact task as done."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE contact_tasks SET status = 'done', done_date = %s WHERE id = %s
    """, (datetime.now(MOSCOW_TZ).strftime("%Y-%m-%d"), task_id))
    conn.commit()
    cur.close()
    conn.close()


def get_contact_tasks(contact_id: int, status: str = "open") -> list[dict]:
    """Get tasks for a contact."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT * FROM contact_tasks
        WHERE contact_id = %s AND status = %s
        ORDER BY due_date ASC NULLS LAST
    """, (contact_id, status))
    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


# ============================================================================
# Full Card
# ============================================================================

def get_contact_card(name_query: str) -> str | None:
    """Get full formatted contact card."""
    contact = find_contact(name_query)
    if not contact:
        return None

    cid = contact["id"]
    facts = get_facts(cid)
    interactions = get_interactions(cid, limit=10)
    tasks = get_contact_tasks(cid, "open")

    # Header
    title = contact["name"]
    if contact.get("company"):
        title += f" — {contact['company']}"
    if contact.get("project"):
        title += f" ({contact['project']})"

    lines = [f"*{title}*"]
    lines.append(f"Теплота: {contact.get('warmth', '?')} | Уверенность: {contact.get('confidence', 0)}%")
    lines.append("")

    # Bio & context
    bio_parts = facts.get("bio", [])
    if contact.get("bio"):
        bio_parts = [contact["bio"]] + bio_parts
    if bio_parts:
        lines.append(f"📝 *Кто:* {'; '.join(bio_parts)}")

    if contact.get("how_we_met") or facts.get("how_met"):
        how = contact.get("how_we_met") or "; ".join(facts.get("how_met", []))
        lines.append(f"🤝 *Как познакомились:* {how}")

    connections = facts.get("connection", [])
    if connections:
        lines.append(f"🔗 *Связи:* {', '.join(connections)}")

    interests = facts.get("interest", [])
    if interests:
        lines.append(f"💡 *Интересы:* {', '.join(interests)}")

    personal = facts.get("personal", [])
    if contact.get("personal_notes"):
        personal = [contact["personal_notes"]] + personal
    if personal:
        lines.append(f"🏷 *Личное:* {'; '.join(personal)}")

    deal = facts.get("deal", [])
    if contact.get("deal_context"):
        deal = [contact["deal_context"]] + deal
    if deal:
        lines.append(f"💼 *Сделка:* {'; '.join(deal)}")

    # Contact info
    contact_info = []
    if contact.get("telegram"):
        contact_info.append(f"TG: {contact['telegram']}")
    if contact.get("email"):
        contact_info.append(f"Email: {contact['email']}")
    if contact.get("phone"):
        contact_info.append(f"Tel: {contact['phone']}")
    if contact_info:
        lines.append(f"📱 {', '.join(contact_info)}")

    # Interactions
    if interactions:
        lines.append("")
        lines.append(f"📜 *История ({len(interactions)} касаний):*")
        for ix in interactions:
            d = ix["date"].strftime("%d.%m") if hasattr(ix["date"], "strftime") else str(ix["date"])
            lines.append(f"  • {d} — {ix['summary']}")

    # Tasks
    if tasks:
        lines.append("")
        lines.append("📋 *Открытые задачи:*")
        for t in tasks:
            due = ""
            if t.get("due_date"):
                d = t["due_date"]
                due = f" (→ {d.strftime('%d.%m') if hasattr(d, 'strftime') else d})"
            lines.append(f"  • {t['text']}{due}")

    # Next action & ping
    if contact.get("next_action"):
        lines.append("")
        lines.append(f"➡️ *Следующее:* {contact['next_action']}")
    if contact.get("ping_date"):
        pd = contact["ping_date"]
        lines.append(f"📅 *Пинг:* {pd.strftime('%d.%m') if hasattr(pd, 'strftime') else pd}")

    return "\n".join(lines)


# ============================================================================
# CRM Pings (for morning briefing)
# ============================================================================

def get_pings_today() -> list[dict]:
    """Get contacts to ping today."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    today = datetime.now(MOSCOW_TZ).strftime("%Y-%m-%d")
    cur.execute("""
        SELECT name, company, project, next_action, ping_date
        FROM contacts WHERE ping_date = %s
    """, (today,))
    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


def get_pings_overdue() -> list[dict]:
    """Get overdue pings."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    today = datetime.now(MOSCOW_TZ).strftime("%Y-%m-%d")
    cur.execute("""
        SELECT name, company, project, next_action, ping_date
        FROM contacts WHERE ping_date < %s
        ORDER BY ping_date
    """, (today,))
    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


# ============================================================================
# Search
# ============================================================================

def search_contacts(query: str, limit: int = 10) -> list[dict]:
    """Search contacts by name, company, or facts."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    q = f"%{query.lower()}%"
    cur.execute("""
        SELECT DISTINCT c.id, c.name, c.company, c.project, c.warmth, c.next_action
        FROM contacts c
        LEFT JOIN contact_aliases a ON c.id = a.contact_id
        LEFT JOIN contact_facts f ON c.id = f.contact_id
        WHERE LOWER(c.name) LIKE %s
           OR LOWER(COALESCE(c.company, '')) LIKE %s
           OR LOWER(a.alias) LIKE %s
           OR LOWER(f.fact_text) LIKE %s
        ORDER BY c.updated_at DESC
        LIMIT %s
    """, (q, q, q, q, limit))
    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


# ============================================================================
# Import from Markdown
# ============================================================================

def import_from_markdown(content: str, project: str = None) -> int:
    """Import contacts from existing CRM markdown file. Returns count imported."""
    import re
    contacts_data = []
    current = None

    for line in content.split("\n"):
        if line.startswith("### "):
            if current:
                contacts_data.append(current)
            name_line = line[4:].strip()
            parts = name_line.split(" — ", 1)
            name = parts[0].strip()
            company = parts[1].strip() if len(parts) > 1 else ""
            current = {"name": name, "company": company, "project": project,
                        "comments": [], "raw_lines": []}
        elif current:
            current["raw_lines"].append(line)
            s = line.strip()
            if s.startswith("- **Контакт:**"):
                current["telegram"] = s.replace("- **Контакт:**", "").strip() or None
            elif s.startswith("- **Теплота:**"):
                m = re.search(r"Теплота:\s*([^|]+)", s)
                if m:
                    current["warmth"] = m.group(1).strip()
                m2 = re.search(r"Уверенность:\s*(\d+)", s)
                if m2:
                    current["confidence"] = int(m2.group(1))
            elif s.startswith("- **Следующее действие:**"):
                current["next_action"] = s.replace("- **Следующее действие:**", "").strip() or None
            elif s.startswith("- **Почему интересен:**"):
                why = s.replace("- **Почему интересен:**", "").strip()
                if why:
                    current["bio"] = why
            elif s.startswith("- **Комментарий:**"):
                comment = s.replace("- **Комментарий:**", "").strip()
                if comment:
                    current["comments"].append(comment)
            elif s.startswith("- **Пингануть:**"):
                pd = s.replace("- **Пингануть:**", "").strip()
                if pd:
                    current["ping_date"] = pd

    if current:
        contacts_data.append(current)

    count = 0
    for c in contacts_data:
        # Skip if already exists
        existing = find_contact(c["name"])
        if existing:
            continue

        cid = add_contact(c)

        # Add comments as initial interactions
        for comment in c.get("comments", []):
            add_interaction(cid, "note", comment, source="import")

        # Add bio as fact
        if c.get("bio"):
            add_fact(cid, "bio", c["bio"])

        count += 1

    return count


# ============================================================================
# Export to Markdown (for Claude Code)
# ============================================================================

def export_to_markdown(project: str = None) -> str:
    """Export all contacts to markdown format."""
    conn = get_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    if project:
        cur.execute("SELECT * FROM contacts WHERE project = %s ORDER BY name", (project,))
    else:
        cur.execute("SELECT * FROM contacts ORDER BY project, name")

    contacts = cur.fetchall()
    cur.close()
    conn.close()

    lines = ["# CRM Export", f"*Generated: {datetime.now(MOSCOW_TZ).strftime('%Y-%m-%d %H:%M')}*", ""]

    current_project = None
    for c in contacts:
        if c["project"] != current_project:
            current_project = c["project"]
            lines.append(f"## {current_project or 'Без проекта'}")
            lines.append("")

        title = c["name"]
        if c.get("company"):
            title += f" — {c['company']}"
        lines.append(f"### {title}")
        lines.append(f"- **Теплота:** {c.get('warmth', '?')} | **Уверенность:** {c.get('confidence', 0)}%")
        if c.get("next_action"):
            lines.append(f"- **Следующее действие:** {c['next_action']}")
        if c.get("ping_date"):
            lines.append(f"- **Пингануть:** {c['ping_date']}")

        # Add facts
        facts = get_facts(c["id"])
        if facts.get("bio"):
            lines.append(f"- **Кто:** {'; '.join(facts['bio'])}")
        if facts.get("connection"):
            lines.append(f"- **Связи:** {', '.join(facts['connection'])}")

        # Add recent interactions
        interactions = get_interactions(c["id"], limit=5)
        if interactions:
            lines.append("- **История:**")
            for ix in interactions:
                d = ix["date"].strftime("%d.%m") if hasattr(ix["date"], "strftime") else str(ix["date"])
                lines.append(f"  - {d}: {ix['summary']}")

        lines.append("")

    return "\n".join(lines)
