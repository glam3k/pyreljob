"""FastAPI example showing worker/manager with clean shutdown.

This example demonstrates the PROPER way to run pyreljob within a FastAPI
server - WITHOUT using signals.py or any signal handlers.

The key insight: FastAPI handles shutdown internally, so we don't need
signal handlers. This avoids the signal registration conflicts when worker
and manager run in the same process.

TO RUN:

    pip install pyreljob[postgres] fastapi uvicorn
    python -m examples.fastapi_example

Or with PostgreSQL:

    TEST_DATABASE_URL=postgresql+psycopg://pyreljob:pyreljob@localhost:5433/pyreljob \
        uvicorn examples.fastapi_example:app --reload

Docker setup for PostgreSQL:

    docker compose up -d postgres
"""

import asyncio
import os
from dataclasses import dataclass
from contextlib import asynccontextmanager
from typing import ClassVar

from fastapi import FastAPI

from pyreljob import Job, Task, TaskContext
from pyreljob import JobManager
from pyreljob.core.worker import Worker


class ProcessData(Task):
    """Example task that processes data."""

    async def run(self, ctx: TaskContext) -> str:
        await asyncio.sleep(2)
        name = ctx.args.get("name", "unknown")
        print(f"Processing data for: {name}")
        await asyncio.sleep(2)
        print(f"Finished processing: {name}")
        return f"processed {name}"


@dataclass
class DataJob(Job):
    """Job that processes data."""
    name: str

    tasks: ClassVar = [ProcessData]


# Initialize components globally
manager = JobManager("sqlite:///fastapi_jobs.db")

# Create database tables if they don't exist
manager.migrate()

# Create example jobs
manager.enqueue(DataJob(name="Alice"))


# Initialize worker
worker = Worker(manager.backend)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan event handler for startup/shutdown."""

    print("FastAPI starting up...")

    # Start manager (beat scheduler)
    asyncio.create_task(manager.run_forever())

    # Start worker  
    asyncio.create_task(worker.run_forever())

    yield  # Application runs here

    print("FastAPI shutting down gracefully...")

    # Graceful shutdown - NO signal handlers needed!
    worker.stop()
    manager.stop()

    print("FastAPI shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="pyreljob with FastAPI",
    description="Example of running worker/manager in FastAPI without signal handlers",
    lifespan=lifespan,
)


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "Hello from pyreljob + FastAPI!",
    }


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {
        "status": "healthy",
    }


@app.post("/shutdown")
async def shutdown():
    """API endpoint to trigger graceful shutdown."""
    worker.stop()
    manager.stop()
    return {"message": "Shutdown initiated"}


@app.get("/jobs")
async def list_jobs():
    """List current jobs."""
    jobs = manager.list_jobs()
    return {"jobs": jobs}


if __name__ == "__main__":
    import uvicorn

    # Determine database URL
    database_url = os.environ.get("TEST_DATABASE_URL")
    if not database_url:
        database_url = "sqlite:///fastapi_jobs.db"

    print(f"Starting FastAPI server with database: {database_url}")
    print("Endpoints available:")
    print("  GET  /         - Root")
    print("  GET  /health   - Health check")
    print("  GET  /jobs     - List jobs")
    print("  POST /shutdown - Graceful shutdown")
    print("\nPress Ctrl+C to stop the server")

    uvicorn.run(app, host="0.0.0.0", port=8000)