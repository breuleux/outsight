import functools
from collections import deque
from contextlib import aclosing

from ..utils import keyword_decorator
from . import misc
from .misc import acall

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
                oper = misc.map(
                    roll(stream, window=scan, init=init, partial=True),
                    lambda data: functools.reduce(_reduce, data),
                )

            if _postprocess:
                return misc.map(oper, _postprocess)
            else:
                return oper

    wrapped._source = cls
    return wrapped


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


@reducer(init=(0, 0, 0))
class std(average_and_variance._source):
    def postprocess(self, last):
        _, v2, sz = last
        if sz >= 2:
            var = (v2 / (sz - 1)) ** 0.5
        else:  # pragma: no cover
            var = None
        return var


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


async def scan(stream, fn, init=NOTSET):
    current = init
    async with aclosing(stream):
        async for x in stream:
            if current is NOTSET:
                current = x
            else:
                current = await acall(fn, current, x)
            yield current


@reducer
def sum(last, add):
    return last + add


@reducer(init=(0, 0, 0))
class variance(average_and_variance._source):
    def postprocess(self, last):
        _, v2, sz = last
        if sz >= 2:
            var = v2 / (sz - 1)
        else:  # pragma: no cover
            var = None
        return var


__scan = scan
