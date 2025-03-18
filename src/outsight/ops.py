import asyncio
from bisect import bisect_left
import builtins
from collections import deque
from contextlib import aclosing
import functools
import inspect
import math
import time

from .utils import ABSENT, CLOSED, Queue, keyword_decorator
from itertools import count as _count


NOTSET = object()
SKIP = object()
UNBLOCK = object()


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
    ms = MergeStream()
    max_time = None
    target_time = None
    ms.register(stream)
    current = None
    async for element in ms:
        now = time.time()
        if element is MARK:
            delta = target_time - now
            if delta > 0:
                ms.register(__delay(MARK, delta))
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
                ms.register(__delay(MARK, target_time - now))
            current = element


async def delay(value, delay):
    await asyncio.sleep(delay)
    return value


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
            master.queues.discard(q)
            master._under()

    def __aiter__(self):
        return self

    def __anext__(self):
        return anext(self.iterator)

    async def aclose(self):
        await self.iterator.aclose()
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

    def stream(self):
        s = _MulticastStream(self)
        self.queues.add(s.queue)
        return s

    def __aiter__(self):
        return self.stream()


multicast = Multicast


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


__delay = delay
__filter = filter
__scan = scan
