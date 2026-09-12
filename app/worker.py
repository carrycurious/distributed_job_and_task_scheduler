import argparse
import heapq
import multiprocessing as mp
import os
import signal
import time
import uuid
from app import store
from app.config import HEARTBEAT_SECONDS, POLL_SECONDS

def execute(job: dict) -> str:
    if job["task_type"] == "echo": return str(job["payload"])
    if job["task_type"] == "sleep":
        seconds = float(job["payload"].get("seconds", 1)); time.sleep(min(max(seconds, 0), 30)); return f"Slept for {seconds}s"
    if job["task_type"] == "fail": raise RuntimeError(job["payload"].get("message", "Intentional task failure"))
    raise ValueError(f"Unknown task type: {job['task_type']}")

def worker_main(queue: mp.Queue, stop: mp.Event) -> None:
    worker_id, last_beat = f"{os.getpid()}-{uuid.uuid4().hex[:8]}", 0.0
    while not stop.is_set():
        if time.monotonic() - last_beat >= HEARTBEAT_SECONDS:
            store.heartbeat(worker_id, os.getpid()); last_beat = time.monotonic()
        try: job = queue.get(timeout=POLL_SECONDS)
        except Exception: continue
        if job is None: break
        if not store.start_job(job["id"], worker_id): continue
        try: store.complete_job(job["id"], execute(job), worker_id)
        except Exception as exc: store.fail_job(job, str(exc), worker_id)
    store.heartbeat(worker_id, os.getpid(), "stopped")

def run(worker_count: int) -> None:
    store.initialize(); store.recover_leases(); store.set_shutdown(False)
    queue, stop = mp.Queue(maxsize=200), mp.Event()
    processes = [mp.Process(target=worker_main, args=(queue, stop), daemon=False) for _ in range(worker_count)]
    for process in processes: process.start()
    def request_stop(*_args: object) -> None: stop.set()
    signal.signal(signal.SIGINT, request_stop)
    if hasattr(signal, "SIGTERM"): signal.signal(signal.SIGTERM, request_stop)
    heap: list[tuple[int, str, dict]] = []
    try:
        while not stop.is_set() and not store.shutdown_requested():
            for job in store.claim_ready_jobs(): heapq.heappush(heap, (-job["priority"], job["created_at"], job))
            while heap and not stop.is_set():
                try: queue.put(heapq.heappop(heap)[2], timeout=POLL_SECONDS)
                except Exception: break
            time.sleep(POLL_SECONDS)
    finally:
        stop.set()
        for _ in processes: queue.put(None)
        for process in processes:
            process.join(timeout=10)
            if process.is_alive(): process.terminate()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(); parser.add_argument("--workers", type=int, default=2)
    run(max(1, parser.parse_args().workers))
