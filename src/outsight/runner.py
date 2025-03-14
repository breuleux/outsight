import asyncio
from threading import Thread
from concurrent.futures import Future, wait

from .gvr import Giver
from .ops import Multicast
from .stream import Stream
from .utils import BoundQueue, Queue


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
        self.give = self.create_giver()
        self.given = Stream(self.give.queue)
        self.tasks = []
        self.pretasks = []

    def start(self):
        assert self.thread is None
        self.thread = AwaitableThread(target=self.go)
        self.thread.start()
        wait([self.ready])
        return self.thread

    def create_queue(self):
        q = MulticastQueue(loop=self.loop)
        self.queues.append(q)
        return q

    def create_giver(self):
        return Giver(self.create_queue())

    def add(self, worker):
        self.queue.put_nowait(worker(self.given))

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


class MulticastQueue(Multicast):
    def __init__(self, loop=None, sync=False):
        super().__init__(BoundQueue(loop), sync=sync)

    def put_nowait(self, x):
        return self.source.put_nowait(x)

    def get(self):  # pragma: no cover
        return self.source.get()

    def close(self):
        self.source.close()
