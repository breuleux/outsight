import builtins
import inspect
from bisect import bisect_left
from collections import deque
from contextlib import aclosing

from .combiners import multicast
from .queue import ABSENT

NOTSET = object()
SKIP = object()


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


async def flat_map(stream, fn):
    async for x in stream:
        async for y in aiter(fn(x)):
            yield y


async def group(stream, keyfn):
    current_key = ABSENT
    current = []
    async with aclosing(stream):
        async for x in stream:
            kx = keyfn(x)
            if kx != current_key and current_key is not ABSENT:
                yield (current_key, current)
                current = []
            current_key = kx
            current.append(x)
    if current_key is not ABSENT:
        yield (current_key, current)


async def last(stream):
    async with aclosing(stream):
        async for x in stream:
            rval = x
    return rval


async def map(stream, fn):
    async with aclosing(stream):
        async for x in stream:
            yield await acall(fn, x)


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


async def split_boundary(stream, fn):
    first = True
    current = []
    async for a, b in pairwise(stream):
        if first:
            current.append(a)
        if fn(a, b):
            yield current
            current = []
        current.append(b)
        first = False
    if current:
        yield current


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


def top(stream, n=10, key=None, reverse=False):
    return bottom(stream, n=n, key=key, reverse=not reverse)


async def to_list(stream):
    async with aclosing(stream):
        return [x async for x in stream]


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
