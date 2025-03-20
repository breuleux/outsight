import asyncio
import builtins
import inspect

from .queue import CLOSED, BoundQueue, Queue

UNBLOCK = object()


class TaggedMergeStream:
    def __init__(self, streams={}, exit_on_first=False, **streams_kw):
        self.unblock = asyncio.Future()
        self.exit_on_first = exit_on_first
        self.work = {}
        for tag, stream in {**streams, **streams_kw}.items():
            self._add(tag, stream)

    def register(self, **streams):
        for tag, stream in streams.items():
            self._add(tag, stream)

    def _add(self, tag, fut):
        if inspect.isasyncgen(fut) or hasattr(fut, "__aiter__"):
            it = aiter(fut)
            self.work[asyncio.create_task(anext(it))] = (tag, it)
        elif inspect.isawaitable(fut):
            self.work[asyncio.create_task(fut)] = (tag, None)
        else:  # pragma: no cover
            raise TypeError(f"Cannot merge object {fut!r}")

        if self.unblock and not self.unblock.done():
            self.unblock.set_result(UNBLOCK)

    async def __aiter__(self):
        wind_down = False
        while True:
            if not self.work:
                break
            done, _ = await asyncio.wait(
                [self.unblock, *self.work.keys()], return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                if task is self.unblock:
                    self.unblock = asyncio.Future()
                    continue
                tag, s = self.work[task]
                del self.work[task]
                try:
                    yield (tag, task.result())
                except StopAsyncIteration:
                    if self.exit_on_first:
                        wind_down = True
                    continue
                if s and not wind_down:
                    self.work[asyncio.create_task(anext(s))] = (tag, s)

    async def aclose(self):
        pass


tagged_merge = TaggedMergeStream


class MergeStream(TaggedMergeStream):
    def __init__(self, *streams):
        super().__init__(dict(builtins.enumerate(streams)))

    def register(self, *streams):
        for stream in streams:
            self._add(None, stream)

    async def __aiter__(self):
        async for _, result in super().__aiter__():
            yield result


merge = MergeStream


class _MulticastStream:
    def __init__(self, master):
        self.master = master
        self.queue = Queue()
        self.iterator = aiter(self.run())

    async def consume(self):
        result = await self.queue.get()
        if self.master.sync and self.queue.qsize() == self.master.sync - 1:
            self.master._under()
        return result

    async def run(self):
        q = self.queue
        master = self.master

        try:
            while True:
                if q.empty():
                    if master.someone_waits:
                        result = await self.consume()
                    elif master.sync_fut:
                        await master.sync_fut
                        continue
                    else:
                        master.someone_waits = True
                        try:
                            result = await anext(master.iterator)
                            master.notify(result, excepted=q)
                        except StopAsyncIteration:
                            await master.iterator.aclose()
                            master.notify(CLOSED)
                            break
                        finally:
                            master.someone_waits = False
                else:
                    result = await self.consume()
                if result is CLOSED:
                    break
                yield result
        finally:
            await master.destroy(q)

    def __aiter__(self):
        return self

    def __anext__(self):
        return anext(self.iterator)

    async def aclose(self):
        await self.iterator.aclose()
        await self.master.destroy(self.queue)
        self.queue.close()


class Multicast:
    def __init__(self, stream, sync=False):
        self.source = stream
        self.iterator = aiter(stream)
        self.queues = set()
        self.someone_waits = False
        self.sync = int(sync)
        self.sync_fut = None
        self.overs = 0
        self.active = 0

    def notify(self, event, excepted=None):
        for q in self.queues:
            if q is not excepted:
                q.put_nowait(event)
                if self.sync and q.qsize() >= self.sync:
                    self._over()

    def _over(self):
        if self.overs == 0:
            self.sync_fut = asyncio.get_running_loop().create_future()
        self.overs += 1

    def _under(self):
        self.overs -= 1
        if self.sync_fut and self.overs == 0:
            self.sync_fut.set_result(True)
            self.sync_fut = None

    async def destroy(self, q):
        if q in self.queues:
            self.queues.discard(q)
            self.active -= 1
            if not self.active:
                await self.iterator.aclose()

    def stream(self):
        s = _MulticastStream(self)
        self.queues.add(s.queue)
        self.active += 1
        return s

    def __aiter__(self):
        return self.stream()


multicast = Multicast


class MulticastQueue(Multicast):
    def __init__(self, loop=None, sync=False):
        super().__init__(BoundQueue(loop), sync=sync)

    def put_nowait(self, x):
        return self.source.put_nowait(x)

    def get(self):  # pragma: no cover
        return self.source.get()

    def close(self):
        self.source.close()
