import asyncio
from outsight.runner import Outsight
import pytest

aio = pytest.mark.asyncio


def otest(cls):
    @aio
    async def test():
        @asyncio.to_thread
        def mthread():
            with outsight:
                cls.main(outsight.give)

        outsight = Outsight()
        othread = outsight.async_start()
        for name in dir(cls):
            if name.startswith("o_"):
                outsight.add(getattr(cls, name))

        await asyncio.gather(othread, mthread)

    return test


@otest
class test_two_reductions:
    def main(give):
        give(x=10)
        give(x=-4)
        give(x=71)

    async def o_min(gvn):
        assert await gvn["x"].min() == -4

    async def o_max(gvn):
        assert await gvn["x"].max() == 71


@otest
class test_affix:
    def main(give):
        give(x=10)
        give(x=-4)
        give(x=71)

    async def o_affix(gvn):
        assert await gvn.affix(sum=gvn["x"].sum(scan=True)).to_list() == [
            {"x": 10, "sum": 10},
            {"x": -4, "sum": 6},
            {"x": 71, "sum": 77},
        ]
