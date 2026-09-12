from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "scheduler.db"
POLL_SECONDS = 0.5
HEARTBEAT_SECONDS = 2
