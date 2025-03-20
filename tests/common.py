import asyncio

from outsight import aiter as O
from outsight.aiter import to_list


def seq(*elems):
    return O.aiter(elems)


async def timed_sequence(seq):
    for entry in seq.split():
        try:
            await asyncio.sleep(float(entry))
        except ValueError:
            yield entry


class Lister:
    async def timed_sequence(self, *args, **kwargs):
        return await to_list(timed_sequence(*args, **kwargs))

    def __getattr__(self, attr):
        async def wrap(*args, **kwargs):
            method = getattr(O, attr)
            return await to_list(method(*args, **kwargs))

        return wrap


lister = Lister()
