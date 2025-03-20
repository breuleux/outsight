import asyncio

ABSENT = object()
CLOSED = object()


class Queue(asyncio.Queue):
    def putleft(self, entry):
        self._queue.appendleft(entry)
        self._unfinished_tasks += 1
        self._finished.clear()
        self._wakeup_next(self._getters)

    def _wakeup_next(self, waiters):
        while waiters:
            waiter = waiters.popleft()
            if not waiter.done():
                self._loop.call_soon_threadsafe(waiter.set_result, None)
                break

    def __aiter__(self):
        return self

    async def __anext__(self):
        value = await self.get()
        if value is CLOSED:
            raise StopAsyncIteration(None)
        return value

    async def aclose(self):
        pass

    def close(self):
        self.put_nowait(CLOSED)


class BoundQueue(Queue):
    def __init__(self, loop):
        super().__init__()
        self._loop = loop
