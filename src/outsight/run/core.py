import asyncio
from concurrent.futures import Future, wait
from threading import Thread

from ..ops import Queue
from .exchange import Giver, MulticastQueue, Sender
from .fixtures import FixtureGroup, StreamFixture, ValueFixture


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
        self.fixtures = FixtureGroup(overseer=ValueFixture(self))
        self.loop = asyncio.new_event_loop()
        self.thread = None
        self.ready = Future()
        self.event_queue = Queue()
        self.queues = [self.event_queue]
        self.log = self.create_sender("logged")
        self.send = self.create_sender("sent")
        self.give = self.create_giver("given")
        self.tasks = []
        self.pretasks = []

    def start(self):
        assert self.thread is None
        self.thread = AwaitableThread(target=self.go)
        self.thread.start()
        wait([self.ready])
        return self.thread

    def create_queue(self, fixture_name):  # pragma: no cover
        q = MulticastQueue(loop=self.loop)
        self.queues.append(q)
        self.fixtures.add_fixture(fixture_name, StreamFixture(q))
        return q

    def create_sender(self, fixture_name):
        s = Sender(loop=self.loop)
        self.queues.append(s)
        self.fixtures.add_fixture(fixture_name, StreamFixture(s))
        return s

    def create_giver(self, fixture_name):
        g = Giver(loop=self.loop)
        self.queues.append(g)
        self.fixtures.add_fixture(fixture_name, StreamFixture(g))
        return g

    def add(self, worker):
        self.event_queue.put_nowait(self.fixtures.execute(worker))

    def go(self):
        self.loop.run_until_complete(self.run())

    async def run(self):
        while not self.event_queue.empty():
            new_task = self.event_queue.get_nowait()
            self.tasks.append(self.loop.create_task(new_task))
        await asyncio.sleep(0)
        self.ready.set_result(True)
        async for new_task in self.event_queue:  # pragma: no cover
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
