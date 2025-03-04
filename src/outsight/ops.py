import asyncio
import builtins
from collections import deque
from contextlib import asynccontextmanager
import functools
import inspect
import math
import time

from .utils import DONE, Queue, keyword_decorator
from itertools import count as _count


NOTSET = object()
SKIP = object()


@keyword_decorator
def reducer(cls, init=NOTSET):
    if isinstance(cls, type):
        obj = cls()
        _reduce = getattr(obj, "reduce", None)
        _postprocess = getattr(obj, "postprocess", None)
        _roll = getattr(obj, "roll", None)
    else:
        _reduce = cls
        _postprocess = None
        _roll = None

    @functools.wraps(cls)
    def wrapped(stream, scan=None, init=init):
        if scan is None:
            oper = reduce(_reduce, stream, init=init)
            if _postprocess:

                async def _oper():
                    return _postprocess(await oper)

                return _oper()
            else:
                return oper

        else:
            if scan is True:
                oper = __scan(_reduce, stream, init=init)

            elif _roll:
                oper = roll(stream, window=scan, reducer=obj.roll, init=init)

            else:
                oper = map(
                    lambda data: functools.reduce(_reduce, data),
                    roll(stream, window=scan, init=init, partial=True),
                )

            if _postprocess:
                return map(_postprocess, oper)
            else:
                return oper

    wrapped._source = cls
    return wrapped


async def acall(fn, *args):
    if inspect.iscoroutinefunction(fn):
        return await fn(*args)
    else:
        return fn(*args)


def aiter(it):
    try:
        return builtins.aiter(it)
    except TypeError:

        async def iterate():
            for x in it:
                yield x

        return iterate()


@reducer(init=(0, 0))
class average:
    def reduce(self, last, add):
        x, sz = last
        return (x + add, sz + 1)

    def postprocess(self, last):
        x, sz = last
        return x / sz

    def roll(self, last, add, drop, last_size, current_size):
        x, _ = last
        if last_size == current_size:
            return (x + add - drop, current_size)
        else:
            return (x + add, current_size)


@reducer(init=(0, 0, 0))
class average_and_variance:
    def reduce(self, last, add):
        prev_sum, prev_v2, prev_size = last
        new_size = prev_size + 1
        new_sum = prev_sum + add
        if prev_size:
            prev_mean = prev_sum / prev_size
            new_mean = new_sum / new_size
            new_v2 = prev_v2 + (add - prev_mean) * (add - new_mean)
        else:
            new_v2 = prev_v2
        return (new_sum, new_v2, new_size)

    def postprocess(self, last):
        sm, v2, sz = last
        avg = sm / sz
        if sz >= 2:
            var = v2 / (sz - 1)
        else:
            var = None
        return (avg, var)

    def roll(self, last, add, drop, last_size, current_size):
        if last_size == current_size:
            prev_sum, prev_v2, prev_size = last
            new_sum = prev_sum - drop + add
            prev_mean = prev_sum / prev_size
            new_mean = new_sum / prev_size
            new_v2 = (
                prev_v2
                + (add - prev_mean) * (add - new_mean)
                - (drop - prev_mean) * (drop - new_mean)
            )
            return (new_sum, new_v2, prev_size)
        else:
            return self.reduce(last, add)


async def cycle(stream):
    saved = []
    async for x in stream:
        saved.append(x)
        yield x
    while True:
        for x in saved:
            yield x


async def chain(streams):
    async for stream in aiter(streams):
        async for x in stream:
            yield x


async def debounce(stream, delay=None, max_wait=None):
    MARK = object()

    async def mark(delay):
        await asyncio.sleep(delay)
        return MARK

    ms = MergeStream()
    max_time = None
    target_time = None
    ms.add(stream)
    current = None
    async for element in ms:
        now = time.time()
        if element is MARK:
            delta = target_time - now
            if delta > 0:
                ms.add(mark(delta))
            else:
                yield current
                max_time = None
                target_time = None
        else:
            new_element = target_time is None
            if max_time is None and max_wait is not None:
                max_time = now + max_wait
            target_time = now + delay
            if max_time:
                target_time = builtins.min(max_time, target_time)
            if new_element:
                ms.add(mark(target_time - now))
            current = element


async def drop(stream, n):
    curr = 0
    async for x in stream:
        if curr >= n:
            yield x
        curr += 1


async def dropwhile(fn, stream):
    go = False
    async for x in stream:
        if go:
            yield x
        elif not await acall(fn, x):
            go = True
            yield x


async def filter(fn, stream):
    async for x in stream:
        if fn(x):
            yield x


async def map(fn, stream):
    async for x in stream:
        yield await acall(fn, x)


@reducer
def min(last, add):
    if add < last:
        return add
    else:
        return last


@reducer
def max(last, add):
    if add > last:
        return add
    else:
        return last


class MergeStream:
    def __init__(self, *streams, stay_alive=False):
        self.queue = Queue()
        self.active = 1 if stay_alive else 0
        for stream in streams:
            self.add(stream)

    async def _add(self, fut, iterator):
        try:
            result = await fut
            self.queue.put_nowait((result, iterator))
        except StopAsyncIteration:
            self.queue.put_nowait((None, False))

    def add(self, fut):
        self.active += 1
        if inspect.isasyncgen(fut) or hasattr(fut, "__aiter__"):
            it = aiter(fut)
            coro = self._add(anext(it), it)
        elif inspect.isawaitable(fut):
            coro = self._add(fut, None)
        else:  # pragma: no cover
            raise TypeError(f"Cannot merge object {fut!r}")
        return asyncio.create_task(coro)

    async def __aiter__(self):
        async for result, it in self.queue:
            if it is False:
                self.active -= 1
            elif it is None:
                yield result
                self.active -= 1
            else:
                asyncio.create_task(self._add(anext(it), it))
                yield result
            if self.active == 0:
                break


merge = MergeStream


class Multicaster:
    def __init__(self, stream=None, loop=None):
        self.source = stream
        self.queues = set()
        self.done = False
        self._is_hungry = loop.create_future() if loop else asyncio.Future()
        if stream is not None:
            self.main_coroutine = (loop or asyncio).create_task(self.run())

    def notify(self, event):
        for q in self.queues:
            q.put_nowait(event)

    def end(self):
        assert not self.main_coroutine
        self.done = True
        self.notify(DONE)

    def _be_hungry(self):
        if not self._is_hungry.done():
            self._is_hungry.set_result(True)

    @asynccontextmanager
    async def _stream_context(self, q):
        try:
            yield q
        finally:
            self.queues.discard(q)

    async def _stream(self, q):
        async with self._stream_context(q):
            if self.done and q.empty():
                return
            self._be_hungry()
            async for event in q:
                if event is DONE:
                    break
                if q.empty():
                    self._be_hungry()
                yield event

    def stream(self):
        q = Queue()
        self.queues.add(q)
        return self._stream(q)

    async def run(self):
        async for event in self.source:
            await self._is_hungry
            self._is_hungry = asyncio.Future()
            self.notify(event)
        self.main_coroutine = None
        self.end()

    def __aiter__(self):
        return self.stream()


multicast = Multicaster


async def pairwise(stream):
    last = NOTSET
    async for x in stream:
        if last is not NOTSET:
            yield (last, x)
        last = x


async def reduce(fn, stream, init=NOTSET):
    current = init
    async for x in stream:
        if current is NOTSET:
            current = x
        else:
            current = await acall(fn, current, x)
    if current is NOTSET:
        raise ValueError("Stream cannot be reduced because it is empty.")
    return current


async def repeat(value_or_func, *, count=None, interval=0):
    i = 0
    if count is None:
        count = math.inf
    while True:
        if callable(value_or_func):
            yield value_or_func()
        else:
            yield value_or_func
        i += 1
        if i < count:
            await asyncio.sleep(interval)
        else:
            break


async def roll(stream, window, reducer=None, partial=None, init=NOTSET):
    q = deque(maxlen=window)

    if reducer is None:
        async for x in stream:
            q.append(x)
            if partial or len(q) == window:
                yield q

    else:
        if partial is not None:  # pragma: no cover
            raise ValueError("Do not use partial=True with a reducer.")

        current = init

        async for x in stream:
            drop = q[0] if len(q) == window else NOTSET
            last_size = len(q)
            q.append(x)
            current = reducer(
                current,
                x,
                drop=drop,
                last_size=last_size,
                current_size=len(q),
            )
            if current is not SKIP:
                yield current


async def scan(fn, stream, init=NOTSET):
    current = init
    async for x in stream:
        if current is NOTSET:
            current = x
        else:
            current = await acall(fn, current, x)
        yield current


@reducer(init=(0, 0, 0))
class std(average_and_variance._source):
    def postprocess(self, last):
        _, v2, sz = last
        if sz >= 2:
            var = (v2 / (sz - 1)) ** 0.5
        else:  # pragma: no cover
            var = None
        return var


class TaggedMergeStream:
    def __init__(self, streams={}, stay_alive=False, **streams_kw):
        self.queue = Queue()
        self.active = 1 if stay_alive else 0
        for tag, stream in {**streams, **streams_kw}.items():
            self.add(tag, stream)

    async def _add(self, tag, fut, iterator):
        try:
            result = await fut
            self.queue.put_nowait(((tag, result), iterator))
        except StopAsyncIteration:
            self.queue.put_nowait((None, False))

    def add(self, tag, fut):
        self.active += 1
        if inspect.isasyncgen(fut) or hasattr(fut, "__aiter__"):
            it = aiter(fut)
            coro = self._add(tag, anext(it), it)
        elif inspect.isawaitable(fut):
            coro = self._add(tag, fut, None)
        else:  # pragma: no cover
            raise TypeError(f"Cannot merge object {fut!r}")
        return asyncio.create_task(coro)

    async def __aiter__(self):
        async for result, it in self.queue:
            if it is False:
                self.active -= 1
            elif it is None:
                yield result
                self.active -= 1
            else:
                tag, _ = result
                asyncio.create_task(self._add(tag, anext(it), it))
                yield result
            if self.active == 0:
                break


tagged_merge = TaggedMergeStream


async def take(stream, n):
    curr = 0
    async for x in stream:
        yield x
        curr += 1
        if curr >= n:
            break


async def takewhile(fn, stream):
    async for x in stream:
        if not await acall(fn, x):
            break
        yield x


def tee(stream, n):
    mt = multicast(stream)
    return [mt.stream() for _ in range(n)]


async def ticktock(interval):
    for i in _count():
        yield i
        await asyncio.sleep(interval)


async def to_list(stream):
    return [x async for x in stream]


@reducer(init=(0, 0, 0))
class variance(average_and_variance._source):
    def postprocess(self, last):
        _, v2, sz = last
        if sz >= 2:
            var = v2 / (sz - 1)
        else:  # pragma: no cover
            var = None
        return var


async def zip(*streams):
    iters = [aiter(s) for s in streams]
    while True:
        try:
            yield [await anext(it) for it in iters]
        except StopAsyncIteration:
            return


__scan = scan
