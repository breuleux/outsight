import asyncio

import pytest

from outsight import ops as O
from outsight.ops import to_list

aio = pytest.mark.asyncio


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


def otest(cls=None, expect_error=None, **fixtures):
    from outsight.run.core import Outsight

    if cls is None:

        def thunk(cls_arg):
            return otest(cls_arg, expect_error=expect_error, **fixtures)

        return thunk

    @aio
    async def test():
        @asyncio.to_thread
        def mthread():
            with outsight:
                cls.main(outsight)

        outsight = Outsight()
        outsight.register_fixtures(**fixtures)

        for name in dir(cls):
            if name.startswith("o_"):
                outsight.add(getattr(cls, name))

        othread = outsight.start()
        if expect_error is not None:
            with pytest.raises(expect_error):
                await asyncio.gather(othread, mthread)
        else:
            await asyncio.gather(othread, mthread)

    return test
