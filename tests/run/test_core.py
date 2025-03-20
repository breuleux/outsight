import asyncio

import pytest

from outsight.run.core import Outsight

aio = pytest.mark.asyncio


def otest(cls):
    @aio
    async def test():
        @asyncio.to_thread
        def mthread():
            with outsight:
                cls.main(outsight)

        outsight = Outsight()

        for name in dir(cls):
            if name.startswith("o_"):
                outsight.add(getattr(cls, name))

        othread = outsight.start()
        await asyncio.gather(othread, mthread)

    return test


@otest
class test_two_reductions:
    def main(o):
        o.send(x=10)
        o.send(x=-4)
        o.send(x=71)

    async def o_min(sent):
        assert await sent["x"].min() == -4

    async def o_max(sent):
        assert await sent["x"].max() == 71


@otest
class test_affix:
    def main(o):
        o.send(x=10)
        o.send(x=-4)
        o.send(x=71)

    async def o_affix(sent):
        assert await sent.affix(sum=sent["x"].sum(scan=True)).to_list() == [
            {"x": 10, "sum": 10},
            {"x": -4, "sum": 6},
            {"x": 71, "sum": 77},
        ]


@otest
class test_give:
    def main(o):
        assert o.give("A") == "A"
        assert o.give("A", twice=True) == "AA"
        assert o.give("B", twice=True) == "BB"
        with pytest.raises(Exception, match="on purpose"):
            o.give("C", err=True)

    async def o_exchange(given):
        async for event in given:
            if event.get("err", False):
                event.set_exception(Exception("on purpose"))
            elif event.get("twice", False):
                event.set_result(event.value * 2)


@otest
class test_give_slice:
    def main(o):
        assert o.give("A") == "A"
        assert o.give("B") == "B"
        # Processing starts
        assert o.give("C") == "CC"
        assert o.give("D") == "DD"
        # Processing ends
        assert o.give("E") == "E"

    async def o_exchange(given):
        async for event in given[2:4]:
            event.set_result(event.value * 2)


@otest
class test_give_multiple:
    def main(o):
        assert o.give("A") == "AAA"
        assert o.give("B") == "B"
        # Processing starts
        assert o.give("C") == "CC"
        assert o.give("D") == "DD"
        # Processing ends
        assert o.give("E") == "E"

    async def o_twice(given):
        async for event in given[2:4]:
            event.set_result(event.value * 2)

    async def o_thrice(given):
        async for event in given[:1]:
            event.set_result(event.value * 3)
