import json
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Iterator

from app.config import DB_PATH
from app.models import JobCreate


def now() -> str:
    return datetime.now(UTC).isoformat()


@contextmanager
def connection() -> Iterator[sqlite3.Connection]:
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    try:
        yield conn
    finally:
        conn.close()


def initialize() -> None:
    with connection() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, task_type TEXT NOT NULL, payload TEXT NOT NULL, priority INTEGER NOT NULL, status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, max_retries INTEGER NOT NULL, result TEXT, error TEXT, created_at TEXT NOT NULL, scheduled_at TEXT NOT NULL, started_at TEXT, completed_at TEXT, worker_id TEXT);
        CREATE INDEX IF NOT EXISTS idx_jobs_ready ON jobs(status, scheduled_at, priority DESC);
        CREATE TABLE IF NOT EXISTS dead_letters (id TEXT PRIMARY KEY, job_id TEXT NOT NULL, task_type TEXT NOT NULL, payload TEXT NOT NULL, attempts INTEGER NOT NULL, error TEXT, failed_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS workers (worker_id TEXT PRIMARY KEY, pid INTEGER NOT NULL, status TEXT NOT NULL, heartbeat_at TEXT NOT NULL, started_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT, worker_id TEXT, level TEXT NOT NULL, message TEXT NOT NULL, created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS control (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        INSERT OR IGNORE INTO control(key, value) VALUES ('shutdown_requested', '0');
        """)


def log(message: str, level: str = "INFO", job_id: str | None = None, worker_id: str | None = None) -> None:
    with connection() as conn:
        conn.execute("INSERT INTO events(job_id, worker_id, level, message, created_at) VALUES (?, ?, ?, ?, ?)", (job_id, worker_id, level, message, now()))


def _job(row: sqlite3.Row) -> dict:
    item = dict(row)
    item["payload"] = json.loads(item["payload"])
    return item


def create_job(data: JobCreate) -> dict:
    job_id, created = str(uuid.uuid4()), now()
    scheduled = (data.scheduled_at or datetime.now(UTC)).astimezone(UTC).isoformat()
    with connection() as conn:
        conn.execute("INSERT INTO jobs(id, task_type, payload, priority, status, max_retries, created_at, scheduled_at) VALUES (?, ?, ?, ?, 'queued', ?, ?, ?)", (job_id, data.task_type, json.dumps(data.payload), data.priority, data.max_retries, created, scheduled))
    log("Job accepted", job_id=job_id)
    return get_job(job_id)


def get_job(job_id: str) -> dict | None:
    with connection() as conn:
        row = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    return _job(row) if row else None


def list_jobs(limit: int = 100) -> list[dict]:
    with connection() as conn:
        rows = conn.execute("SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
    return [_job(r) for r in rows]


def claim_ready_jobs(limit: int = 50) -> list[dict]:
    with connection() as conn:
        conn.execute("BEGIN IMMEDIATE")
        rows = conn.execute("SELECT * FROM jobs WHERE status='queued' AND scheduled_at <= ? ORDER BY priority DESC, created_at ASC LIMIT ?", (now(), limit)).fetchall()
        ids = [r["id"] for r in rows]
        if ids:
            conn.executemany("UPDATE jobs SET status='leased' WHERE id=? AND status='queued'", [(i,) for i in ids])
        conn.execute("COMMIT")
    return [_job(r) for r in rows]


def start_job(job_id: str, worker_id: str) -> bool:
    with connection() as conn:
        changed = conn.execute("UPDATE jobs SET status='running', worker_id=?, attempts=attempts+1, started_at=? WHERE id=? AND status='leased'", (worker_id, now(), job_id)).rowcount
    return changed == 1


def complete_job(job_id: str, result: str, worker_id: str) -> None:
    with connection() as conn:
        conn.execute("UPDATE jobs SET status='succeeded', result=?, completed_at=? WHERE id=?", (result, now(), job_id))
    log("Job completed", job_id=job_id, worker_id=worker_id)


def fail_job(job: dict, error: str, worker_id: str) -> None:
    attempt = job["attempts"] + 1
    with connection() as conn:
        if attempt <= job["max_retries"]:
            delay = min(2 ** attempt, 60)
            conn.execute("UPDATE jobs SET status='queued', error=?, scheduled_at=? WHERE id=?", (error, (datetime.now(UTC) + timedelta(seconds=delay)).isoformat(), job["id"]))
            message, level = f"Job failed; retry {attempt}/{job['max_retries']} in {delay}s", "WARNING"
        else:
            conn.execute("UPDATE jobs SET status='dead_letter', error=?, completed_at=? WHERE id=?", (error, now(), job["id"]))
            conn.execute("INSERT OR REPLACE INTO dead_letters(id, job_id, task_type, payload, attempts, error, failed_at) VALUES (?, ?, ?, ?, ?, ?, ?)", (str(uuid.uuid4()), job["id"], job["task_type"], json.dumps(job["payload"]), attempt, error, now()))
            message, level = "Job moved to DLQ", "ERROR"
    log(message, level, job["id"], worker_id)


def recover_leases() -> None:
    with connection() as conn:
        conn.execute("UPDATE jobs SET status='queued' WHERE status='leased'")


def heartbeat(worker_id: str, pid: int, status: str = "running") -> None:
    stamp = now()
    with connection() as conn:
        conn.execute("INSERT INTO workers(worker_id, pid, status, heartbeat_at, started_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT(worker_id) DO UPDATE SET status=excluded.status, heartbeat_at=excluded.heartbeat_at", (worker_id, pid, status, stamp, stamp))


def workers() -> list[dict]:
    with connection() as conn:
        return [dict(r) for r in conn.execute("SELECT * FROM workers ORDER BY heartbeat_at DESC").fetchall()]


def dashboard_data() -> dict:
    with connection() as conn:
        counts = {r["status"]: r["n"] for r in conn.execute("SELECT status, COUNT(*) n FROM jobs GROUP BY status")}
        events = [dict(r) for r in conn.execute("SELECT * FROM events ORDER BY id DESC LIMIT 50")]
        dlq = [dict(r) for r in conn.execute("SELECT * FROM dead_letters ORDER BY failed_at DESC LIMIT 50")]
    return {"counts": counts, "workers": workers(), "events": events, "dead_letters": dlq}


def set_shutdown(value: bool) -> None:
    with connection() as conn:
        conn.execute("UPDATE control SET value=? WHERE key='shutdown_requested'", ("1" if value else "0",))


def shutdown_requested() -> bool:
    with connection() as conn:
        return connection_value("SELECT value FROM control WHERE key='shutdown_requested'") == "1"


def connection_value(sql: str) -> str:
    with connection() as conn:
        return conn.execute(sql).fetchone()[0]
