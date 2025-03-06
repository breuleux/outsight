import asyncio
import functools


DONE = object()
CLOSED = object()


class Queue(asyncio.Queue):
    def putleft(self, entry):
        self._queue.appendleft(entry)
        self._unfinished_tasks += 1
        self._finished.clear()
        self._wakeup_next(self._getters)

    def _wakeup_next(self, waiters):
        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._wakeup_next, self._getters)
        super()._wakeup_next(waiters)

    def __aiter__(self):
        return self

    async def __anext__(self):
        value = await self.get()
        if value is CLOSED:
            raise StopAsyncIteration(None)
        return value

    def close(self):
        self.put_nowait(CLOSED)


class BoundQueue(Queue):
    def __init__(self, loop):
        super().__init__()
        self._loop = loop


def keyword_decorator(deco):
    """Wrap a decorator to optionally takes keyword arguments."""

    @functools.wraps(deco)
    def new_deco(fn=None, **kwargs):
        if fn is None:

            @functools.wraps(deco)
            def newer_deco(fn):
                return deco(fn, **kwargs)

            return newer_deco
        else:
            return deco(fn, **kwargs)

    return new_deco
