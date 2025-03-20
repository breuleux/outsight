from contextlib import aclosing
from types import FunctionType

from ..utils import lax_function
from . import ops


async def affix(stream, **extra):
    async with aclosing(stream):
        async for main, *others in ops.zip(stream, *extra.values()):
            new = {k: v for k, v in zip(extra, others)}
            yield {**main, **new}


async def augment(stream, **fns):
    fns = {k: lax_function(fn) for k, fn in fns.items()}
    async with aclosing(stream):
        async for x in stream:
            yield {**{k: fn(**x) for k, fn in fns.items()}, **x}


async def getitem(stream, key, strict=False):
    async with aclosing(stream):
        if isinstance(key, (list, tuple)):
            async for entry in stream:
                if strict or (isinstance(entry, dict) and all(k in entry for k in key)):
                    yield tuple(entry[k] for k in key)

        else:
            async for entry in stream:
                if strict or (isinstance(entry, dict) and key in entry):
                    yield entry[key]


async def keep(stream, *keys, **remap):
    remap = {**{k: k for k in keys}, **remap}

    async with aclosing(stream):
        async for x in stream:
            if isinstance(x, dict) and any(k in remap for k in x.keys()):
                yield {k2: x[k1] for k1, k2 in remap.items() if k1 in x}


async def kfilter(stream, fn):
    fn = lax_function(fn)
    async with aclosing(stream):
        async for x in stream:
            if fn(**x):
                yield x


async def kmap(stream, _fn=None, **_fns):
    if _fn and _fns or not _fn and not _fns:  # pragma: no cover
        raise TypeError(
            "kmap either takes one fn argument or keyword arguments but not both"
        )

    elif _fn:
        _fn = lax_function(_fn)
        async with aclosing(stream):
            async for x in stream:
                yield _fn(**x)

    else:
        fns = {k: lax_function(fn) for k, fn in _fns.items()}
        async with aclosing(stream):
            async for x in stream:
                yield {k: fn(**x) for k, fn in fns.items()}


async def kscan(stream):
    values = {}
    async with aclosing(stream):
        async for x in stream:
            values.update(x)
            yield values


async def kmerge(stream):
    return await ops.last(kscan(stream))


async def where(stream, *keys, **conditions):
    conditions = {
        k: cond if isinstance(cond, FunctionType) else lambda x, value=cond: value == x
        for k, cond in conditions.items()
    }
    excluded = [k.lstrip("!") for k in keys if k.startswith("!")]
    keys = [k for k in keys if not k.startswith("!")]
    keys = (*keys, *conditions.keys())

    async with aclosing(stream):
        async for x in stream:
            cond = (
                isinstance(x, dict)
                and all(k in x for k in keys)
                and not any(k in x for k in excluded)
                and all(cond(x[k]) for k, cond in conditions.items())
            )
            if cond:
                yield x


async def where_any(stream, *keys):
    async with aclosing(stream):
        async for x in stream:
            if isinstance(x, dict) and any(k in x for k in keys):
                yield x
