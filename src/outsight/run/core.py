import asyncio
import inspect
from concurrent.futures import Future, wait
from threading import Thread

from ..aiter import Queue
from .exchange import Giver, MulticastQueue, Sender


class AwaitableThread(Thread):
    def __init__(self, target):
        try:
            self.loop = asyncio.get_running_loop()
            self.fut = self.loop.create_future()
        except RuntimeError:  # pragma: no cover
            self.loop = None
            self.fut = None
        super().__init__(target=target)

    def run(self):
        try:
            result = self._target()
            if self.loop:
                self.loop.call_soon_threadsafe(self.fut.set_result, result)
        except Exception as exc:  # pragma: no cover
            if self.loop:
                self.loop.call_soon_threadsafe(self.fut.set_exception, exc)
            else:
                raise

    def __await__(self):
        return self.fut.__await__()


class Outsight:
    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self.thread = None
        self.ready = Future()
        self.queue = Queue()
        self.queues = [self.queue]
        self.send = self.create_sender()
        self.give = self.create_giver()
        self.tasks = []
        self.pretasks = []

    def start(self):
        assert self.thread is None
        self.thread = AwaitableThread(target=self.go)
        self.thread.start()
        wait([self.ready])
        return self.thread

    def create_queue(self):  # pragma: no cover
        q = MulticastQueue(loop=self.loop)
        self.queues.append(q)
        return q

    def create_sender(self):
        s = Sender(loop=self.loop)
        self.queues.append(s)
        return s

    def create_giver(self):
        g = Giver(loop=self.loop)
        self.queues.append(g)
        return g

    def add(self, worker):
        sig = inspect.signature(worker)
        kwargs = {}
        if any(p == "given" for p in sig.parameters):
            kwargs["given"] = self.give.stream()
        if any(p == "sent" for p in sig.parameters):
            kwargs["sent"] = self.send.stream()
        self.queue.put_nowait(worker(**kwargs))

    def go(self):
        self.loop.run_until_complete(self.run())

    async def run(self):
        while not self.queue.empty():
            new_task = self.queue.get_nowait()
            self.tasks.append(self.loop.create_task(new_task))
        await asyncio.sleep(0)
        self.ready.set_result(True)
        async for new_task in self.queue:  # pragma: no cover
            self.tasks.append(self.loop.create_task(new_task))
        for task in self.tasks:
            await task

    def __enter__(self):
        if self.thread is None:  # pragma: no cover
            self.start()
        return self

    def __exit__(self, exct, excv, exctb):
        for q in self.queues:
            q.close()
