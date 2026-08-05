"""Hello-world job for manual testing.

Run it as a library::

    from examples.hello import HelloWorld
    from pyreljob import JobManager
    from pyreljob.backends.sqlalchemy_backend import backend_from_url
    from pyreljob.core.worker import Worker

    manager = JobManager("sqlite:///hello.db")
    manager.migrate()
    manager.enqueue(HelloWorld(name="world"))

    Worker(manager.backend).run_forever()   # polls and runs it

The task sleeps before and after printing so you can watch the run go
``pending -> running -> succeeded`` and see graceful shutdown drain it.
"""

import time
from dataclasses import dataclass
from typing import ClassVar

from pyreljob import Job, Task, TaskContext


class SayHello(Task):
    def run(self, ctx: TaskContext) -> str:
        time.sleep(2)
        print(f"hello {ctx.args.get('name', 'world')}!")
        time.sleep(2)
        return "said hello"


@dataclass
class HelloWorld(Job):
    name: str = "world"

    tasks: ClassVar = [SayHello]
