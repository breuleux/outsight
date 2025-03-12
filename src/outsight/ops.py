import asyncio
from bisect import bisect_left
import builtins
from collections import deque
from contextlib import aclosing, asynccontextmanager
import functools
import inspect
import math
import time

from .utils import ABSENT, CLOSED, BoundQueue, Queue, keyword_decorator
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
            oper = reduce(stream, _reduce, init=init)
            if _postprocess:

                async def _oper():
                    return _postprocess(await oper)

                return _oper()
            else:
                return oper

        else:
            if scan is True:
                oper = __scan(stream, _reduce, init=init)

            elif _roll:
                oper = roll(stream, window=scan, reducer=obj.roll, init=init)

            else:
                oper = map(
                    roll(stream, window=scan, init=init, partial=True),
                    lambda data: functools.reduce(_reduce, data),
                )

            if _postprocess:
                return map(oper, _postprocess)
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


async def all(stream, predicate=bool):
    async with aclosing(stream):
        async for x in stream:
            if not predicate(x):
                return False
        return True


async def any(stream, predicate=bool):
    async with aclosing(stream):
        async for x in stream:
            if predicate(x):
                return True
        return False


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


async def bottom(stream, n=10, key=None, reverse=False):
    assert n > 0

    keyed = []
    elems = []

    async with aclosing(stream):
        async for x in stream:
            newkey = key(x) if key else x
            if len(keyed) < n or (newkey > keyed[0] if reverse else newkey < keyed[-1]):
                ins = bisect_left(keyed, newkey)
                keyed.insert(ins, newkey)
                if reverse:
                    ins = len(elems) - ins
                elems.insert(ins, x)
                if len(keyed) > n:
                    del keyed[0 if reverse else -1]
                    elems.pop()

        return elems


async def chain(streams):
    async for stream in aiter(streams):
        async with aclosing(stream):
            async for x in stream:
                yield x


async def count(stream, filter=None):
    if filter:
        stream = __filter(stream, filter)
    count = 0
    async with aclosing(stream):
        async for _ in stream:
            count += 1
        return count


async def cycle(stream):
    saved = []
    async with aclosing(stream):
        async for x in stream:
            saved.append(x)
            yield x
    while True:
        for x in saved:
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


async def distinct(stream, key=lambda x: x):
    seen = set()
    async with aclosing(stream):
        async for x in stream:
            if (k := key(x)) not in seen:
                yield x
                seen.add(k)


async def drop(stream, n):
    curr = 0
    async with aclosing(stream):
        async for x in stream:
            if curr >= n:
                yield x
            curr += 1


async def drop_while(stream, fn):
    go = False
    async with aclosing(stream):
        async for x in stream:
            if go:
                yield x
            elif not await acall(fn, x):
                go = True
                yield x


async def drop_last(stream, n):
    buffer = deque(maxlen=n)
    async with aclosing(stream):
        async for x in stream:
            if len(buffer) == n:
                yield buffer.popleft()
            buffer.append(x)


async def enumerate(stream):
    i = 0
    async with aclosing(stream):
        async for x in stream:
            yield (i, x)
            i += 1


async def every(stream, n):
    async with aclosing(stream):
        async for i, x in enumerate(stream):
            if i % n == 0:
                yield x


async def filter(stream, fn):
    async with aclosing(stream):
        async for x in stream:
            if fn(x):
                yield x


async def first(stream):
    async with aclosing(stream):
        async for x in stream:
            return x


async def last(stream):
    async with aclosing(stream):
        async for x in stream:
            rval = x
    return rval


async def map(stream, fn):
    async with aclosing(stream):
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

    async def aclose(self):
        pass


merge = MergeStream


class Multicaster:
    def __init__(self, stream=None, loop=None):
        self.source = stream
        self.queues = set()
        self.done = False
        self.loop = loop
        self._is_hungry = loop.create_future() if loop else asyncio.Future()
        if stream is not None:
            self.main_coroutine = (loop or asyncio).create_task(self.run())

    def notify(self, event):
        for q in self.queues:
            q.put_nowait(event)

    def end(self):
        assert not self.main_coroutine
        self.done = True
        self.notify(CLOSED)

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
                if q.empty():
                    self._be_hungry()
                yield event

    def stream(self):
        q = BoundQueue(loop=self.loop)
        self.queues.add(q)
        return self._stream(q)

    async def run(self):
        async with aclosing(self.source):
            async for event in self.source:
                await self._is_hungry
                self._is_hungry = (
                    self.loop.create_future() if self.loop else asyncio.Future()
                )
                self.notify(event)
            self.main_coroutine = None
            self.end()

    def __aiter__(self):
        return self.stream()


multicast = Multicaster


async def nth(stream, n):
    async with aclosing(stream):
        async for i, x in enumerate(stream):
            if i == n:
                return x
    raise IndexError(n)


async def norepeat(stream, key=lambda x: x):
    last = ABSENT
    async with aclosing(stream):
        async for x in stream:
            if (k := key(x)) != last:
                yield x
                last = k


async def pairwise(stream):
    last = NOTSET
    async with aclosing(stream):
        async for x in stream:
            if last is not NOTSET:
                yield (last, x)
            last = x


async def reduce(stream, fn, init=NOTSET):
    current = init
    async with aclosing(stream):
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
        async with aclosing(stream):
            async for x in stream:
                q.append(x)
                if partial or len(q) == window:
                    yield q

    else:
        if partial is not None:  # pragma: no cover
            raise ValueError("Do not use partial=True with a reducer.")

        current = init

        async with aclosing(stream):
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


async def sample(stream, interval, reemit=True):
    if isinstance(interval, (float, int)):
        interval = ticktock(interval)

    current = ABSENT
    ticked = False

    async for tag, value in tagged_merge(
        tick=interval, stream=stream, exit_on_first=True
    ):
        if tag == "stream":
            if current is ABSENT and ticked:
                yield value
                if reemit:
                    current = value
            else:
                current = value
            ticked = False
        else:
            ticked = True
            if current is not ABSENT:
                yield current
                if not reemit:
                    current = ABSENT
                    ticked = False


async def scan(stream, fn, init=NOTSET):
    current = init
    async with aclosing(stream):
        async for x in stream:
            if current is NOTSET:
                current = x
            else:
                current = await acall(fn, current, x)
            yield current


def slice(stream, start=None, stop=None, step=None):
    rval = stream
    if start is None:
        if stop is None:
            return stream
        rval = take(stream, stop) if stop >= 0 else drop_last(stream, -stop)
    else:
        rval = drop(rval, start) if start >= 0 else take_last(rval, -start)
        if stop is not None:
            sz = stop - start
            assert sz >= 0
            rval = take(rval, sz)
    if step:
        rval = every(rval, step)
    return rval


async def sort(stream, key=None, reverse=False):
    li = await to_list(stream)
    li.sort(key=key, reverse=reverse)
    return li


@reducer(init=(0, 0, 0))
class std(average_and_variance._source):
    def postprocess(self, last):
        _, v2, sz = last
        if sz >= 2:
            var = (v2 / (sz - 1)) ** 0.5
        else:  # pragma: no cover
            var = None
        return var


@reducer
def sum(last, add):
    return last + add


class TaggedMergeStream:
    def __init__(self, streams={}, stay_alive=False, exit_on_first=False, **streams_kw):
        self.queue = Queue()
        self.active = 1 if stay_alive else 0
        self.exit_on_first = exit_on_first
        self.tasks = {}
        self.done = False
        for tag, stream in {**streams, **streams_kw}.items():
            self.add(tag, stream)

    async def _add(self, tag, fut, iterator):
        try:
            result = await fut
            self.queue.put_nowait(((tag, result), iterator))
        except StopAsyncIteration:
            self.queue.put_nowait(((tag, None), False))

    def add(self, tag, fut):
        self.active += 1
        if inspect.isasyncgen(fut) or hasattr(fut, "__aiter__"):
            it = aiter(fut)
            coro = self._add(tag, anext(it), it)
        elif inspect.isawaitable(fut):
            coro = self._add(tag, fut, None)
        else:  # pragma: no cover
            raise TypeError(f"Cannot merge object {fut!r}")
        task = asyncio.create_task(coro)
        self.tasks[tag] = task
        return task

    async def __aiter__(self):
        async for result, it in self.queue:
            if it is False:
                self.active -= 1
                if self.exit_on_first:
                    self.done = True
            elif it is None:
                yield result
                self.active -= 1
            else:
                tag, _ = result
                if self.done:
                    self.active -= 1
                else:
                    task = asyncio.create_task(self._add(tag, anext(it), it))
                    self.tasks[tag] = task
                yield result
            if self.active == 0:
                break

    async def aclose(self):
        pass


tagged_merge = TaggedMergeStream


async def take(stream, n):
    curr = 0
    async with aclosing(stream):
        async for x in stream:
            yield x
            curr += 1
            if curr >= n:
                break


async def take_while(stream, fn):
    async with aclosing(stream):
        async for x in stream:
            if not await acall(fn, x):
                break
            yield x


async def take_last(stream, n):
    buffer = deque(maxlen=n)
    async with aclosing(stream):
        async for x in stream:
            buffer.append(x)
    for x in buffer:
        yield x


def tee(stream, n):
    mt = multicast(stream)
    return [mt.stream() for _ in range(n)]


def throttle(stream, delay):
    return sample(stream, delay, reemit=False)


async def ticktock(interval):
    for i in _count():
        yield i
        await asyncio.sleep(interval)


def top(stream, n=10, key=None, reverse=False):
    return bottom(stream, n=n, key=key, reverse=not reverse)


async def to_list(stream):
    async with aclosing(stream):
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
    try:
        while True:
            try:
                yield [await anext(it) for it in iters]
            except StopAsyncIteration:
                return
    finally:
        for it in iters:
            if hasattr(it, "aclose"):
                await it.aclose()


__filter = filter
__scan = scan
