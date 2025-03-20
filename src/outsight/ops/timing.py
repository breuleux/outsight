import asyncio
import builtins
import math
import time
from itertools import count as _count

from .combiners import MergeStream, tagged_merge
from .queue import ABSENT

NOTSET = object()
SKIP = object()


async def debounce(stream, delay=None, max_wait=None):
    async for x in buffer_debounce(stream, delay=delay, max_wait=max_wait):
        yield x[-1]


async def delay(value, delay):
    await asyncio.sleep(delay)
    return value


async def buffer(
    stream, control=None, *, align=False, count=None, skip_empty=True, offset=None
):
    if isinstance(control, (int, float)):
        control = ticktock(control, offset=offset)
    elif offset:  # pragma: no cover
        raise NotImplementedError("Cannot use offset if control isn't a number")
    current = []
    it = aiter(stream)
    if align:
        current.append(await anext(it))
    ms = tagged_merge(main=it, exit_on_first=True)
    if control:
        ms.register(control=control)
    async for tag, x in ms:
        if tag == "main":
            current.append(x)
            if not count or len(current) < count:
                continue
        if current or not skip_empty:
            yield current
        current = []
    if current:
        yield current


async def buffer_debounce(stream, delay=None, max_wait=None):
    MARK = object()
    ms = MergeStream()
    max_time = None
    target_time = None
    ms.register(stream)
    current = []
    async for element in ms:
        now = time.time()
        if element is MARK:
            delta = target_time - now
            if delta > 0:
                ms.register(__delay(MARK, delta))
            else:
                yield current
                current = []
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
            current.append(element)


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


async def sample(stream, interval, reemit=True):
    current = ABSENT
    async for group in buffer(stream, interval, align=True, skip_empty=False):
        if group:
            if current is ABSENT and len(group) > 1:  # pragma: no cover
                yield group[0]
            yield (current := group[-1])
        elif reemit and current is not ABSENT:
            yield current


def throttle(stream, delay):
    return sample(stream, delay, reemit=False)


async def ticktock(interval, offset=0):
    if offset:
        await asyncio.sleep(offset)
    for i in _count():
        yield i
        await asyncio.sleep(interval)


__delay = delay
