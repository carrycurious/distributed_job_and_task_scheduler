# Distributed Job Queue & Task Scheduler

A local, SQLite-backed distributed task scheduler using FastAPI, multiprocessing, a heap priority queue, automatic retries, a Dead Letter Queue (DLQ), worker heartbeats, and a Streamlit observability dashboard.

## Quick start

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m app.worker --workers 3
```

In separate terminals:

```powershell
uvicorn app.api:app --reload
streamlit run dashboard.py
```

Submit a job:

```powershell
Invoke-RestMethod -Method Post http://127.0.0.1:8000/jobs -ContentType 'application/json' -Body '{"task_type":"sleep","payload":{"seconds":2},"priority":10,"max_retries":2}'
```

Available task types are `echo`, `sleep`, and `fail` (use `fail` to exercise retries and the DLQ). Jobs are prioritized by larger `priority` value, then FIFO by creation time.

To request a graceful worker shutdown:

```powershell
Invoke-RestMethod -Method Post http://127.0.0.1:8000/workers/shutdown
```
