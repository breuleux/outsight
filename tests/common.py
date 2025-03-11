import asyncio
from outsight.ops import to_list
from outsight import ops as O


def seq(*elems):
    return O.aiter(elems)


async def timed_sequence(seq, factor=1000):
    for entry in seq.split():
        try:
            await asyncio.sleep(float(entry) / factor)
        except ValueError:
            yield entry


async def delayed(x, delay, factor=1000):
    await asyncio.sleep(delay / factor)
    return x


class Lister:
    async def timed_sequence(self, *args, **kwargs):
        return await to_list(timed_sequence(*args, **kwargs))

    def __getattr__(self, attr):
        async def wrap(*args, **kwargs):
            return await to_list(getattr(O, attr)(*args, **kwargs))

        return wrap


lister = Lister()
