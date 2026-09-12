from fastapi import FastAPI, HTTPException
from app import store
from app.models import JobCreate, JobOut

app = FastAPI(title="Distributed Job Queue & Task Scheduler", version="1.0.0")

@app.on_event("startup")
def startup() -> None:
    store.initialize()

@app.get("/health")
def health() -> dict:
    return {"status": "ok"}

@app.post("/jobs", response_model=JobOut, status_code=201)
def submit_job(job: JobCreate) -> dict:
    return store.create_job(job)

@app.get("/jobs", response_model=list[JobOut])
def jobs(limit: int = 100) -> list[dict]:
    return store.list_jobs(min(limit, 500))

@app.get("/jobs/{job_id}", response_model=JobOut)
def job(job_id: str) -> dict:
    item = store.get_job(job_id)
    if not item:
        raise HTTPException(404, "Job not found")
    return item

@app.get("/metrics")
def metrics() -> dict:
    return store.dashboard_data()

@app.post("/workers/shutdown")
def graceful_shutdown() -> dict:
    store.set_shutdown(True)
    return {"message": "Graceful shutdown requested; workers will finish their current job."}

@app.post("/workers/start")
def allow_workers() -> dict:
    store.set_shutdown(False)
    return {"message": "Worker startup enabled."}
