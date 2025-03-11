import asyncio
from threading import Thread

from .gvr import Giver
from .ops import Multicaster
from .stream import Stream
from .utils import BoundQueue, Queue


class Outsight:
    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self.thread = None
        self.queue = Queue()
        self.queues = [self.queue]
        self.give = self.create_giver()
        self.given = Stream(self.give.queue)

    def async_start(self):
        assert self.thread is None
        self.thread = asyncio.to_thread(self.go)
        return self.thread

    def start(self):
        assert self.thread is None
        self.thread = Thread(target=self.go)
        self.thread.start()
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
        tasks = []
        async for new_task in self.queue:
            tasks.append(asyncio.create_task(new_task))
        for task in tasks:
            await task

    def __enter__(self):
        if self.thread is None:
            self.start()
        return self

    def __exit__(self, exct, excv, exctb):
        for q in self.queues:
            q.close()


class MulticastQueue(Multicaster):
    def __init__(self, loop=None):
        super().__init__(BoundQueue(loop), loop=loop)

    def put_nowait(self, x):
        return self.source.put_nowait(x)

    def get(self):
        return self.source.get()

    def close(self):
        self.source.close()
