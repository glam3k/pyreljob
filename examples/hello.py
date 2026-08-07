"""Hello-world job for manual testing.

Run it as a library::

    from examples.hello import HelloWorld
    from pyreljob import JobManager
    from pyreljob.core.worker import Worker

    manager = JobManager("sqlite:///hello.db")
    manager.migrate()
    manager.enqueue(HelloWorld(name="world"))

    Worker(manager.backend).run_forever()   # polls and runs it

The task sleeps before and after printing so you can watch the run go
``pending -> running -> succeeded`` and see graceful shutdown drain it.

Or run it as a standalone CLI with signal handling::

    python examples/hello.py

This shows the signal handling pattern for graceful shutdown in standalone
applications.
"""

import asyncio
import os
import signal
import threading
from dataclasses import dataclass
from typing import ClassVar

from pyreljob import Job, Task, TaskContext
from pyreljob.core.worker import Worker


class SayHello(Task):
    async def run(self, ctx: TaskContext) -> str:
        await asyncio.sleep(2)
        print(f"hello {ctx.args.get('name', 'world')}!")
        await asyncio.sleep(2)
        return "said hello"


@dataclass
class HelloWorld(Job):
    name: str = "world"

    tasks: ClassVar = [SayHello]


if __name__ == "__main__":
    from pyreljob import JobManager

    manager = JobManager("sqlite:///hello.db")
    manager.migrate()
    manager.enqueue(HelloWorld(name="world"))

    worker = Worker(manager.backend)

    # Register the job with its fully-qualified path name
    # This tells the worker: "when I see 'examples.hello.HelloWorld' in the database,
    # use this HelloWorld class for deserialization"
    worker.register("examples.hello.HelloWorld", HelloWorld)

    def shutdown_handler():
        """Gracefully shutdown worker and manager on SIGINT/SIGTERM."""
        print("\nShutting down gracefully...")
        worker.stop()

    # Register signal handlers for graceful shutdown
    if threading.current_thread() is threading.main_thread():
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda s, f: shutdown_handler())

    print("Starting worker (PID: {})".format(os.getpid()))
    print("Press Ctrl+C to shut down gracefully")
    worker.run_forever()