from fastapi import Request
from fastapi.responses import JSONResponse

class JobNotFound(Exception):
    def __init__(self, job_id: str):
        super().__init__(f"Job {job_id} not found.")
        self.job_id = job_id

class WorkerNotRegistered(Exception):
    def __init__(self, worker_id: str):
        super().__init__(f"Worker {worker_id} not registered.")
        self.worker_id = worker_id

class JobQueueEmpty(Exception):
    def __init__(self):
        super().__init__("No jobs available.")

def register_exception_handlers(app):
    @app.exception_handler(JobNotFound)
    async def job_not_found_handler(request: Request, exc: JobNotFound):
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(WorkerNotRegistered)
    async def worker_not_registered_handler(request: Request, exc: WorkerNotRegistered):
        return JSONResponse(status_code=400, content={"detail": str(exc)})

    @app.exception_handler(JobQueueEmpty)
    async def job_queue_empty_handler(request: Request, exc: JobQueueEmpty):
        return JSONResponse(status_code=204, content={"detail": str(exc)})
