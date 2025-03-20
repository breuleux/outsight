import asyncio

import pytest

from outsight import ops as O

from ..common import lister, timed_sequence

aio = pytest.mark.asyncio


@aio
async def test_merge():
    seq1 = timed_sequence("A 1 B 1 C 1 D")
    seq2 = timed_sequence("1.5 x 0.1 y 7 z")

    results = await lister.merge(seq1, seq2)
    assert results == list("ABxyCDz")


@aio
async def test_multicast(ten):
    mt = O.multicast(ten)
    assert (await lister.zip(mt, mt, mt)) == list(
        map(list, zip(range(10), range(10), range(10)))
    )
    # Creating iterators after consumption won't reset the iteration
    assert (await lister.zip(mt, mt, mt)) == []
    # Again
    assert (await lister.zip(mt, mt, mt)) == []


@aio
async def test_multicast_sync(ten):
    mt = O.multicast(ten, sync=True)
    mt1 = mt.stream()
    mt2 = mt.stream()

    async def one():
        async for x in mt1:
            yield "A"

    async def two():
        async for x in mt2:
            await asyncio.sleep(10)
            yield "B"

    results = "".join(await lister.merge(one(), two()))

    # sync=True forces mt1 and mt2 to be consumed at the same rate, forcing
    # an alternance. Two As in a row may happen, but not more than that.
    assert "AAA" not in results


@aio
async def test_tagged_merge():
    seq1 = timed_sequence("A 1 B 1 C 1 D")
    seq2 = timed_sequence("1.5 x 0.2 y 7 z")

    results = await lister.tagged_merge(bo=seq1, jack=seq2, horse=O.delay("!", 1.6))
    assert results == [
        ("bo", "A"),
        ("bo", "B"),
        ("jack", "x"),
        ("horse", "!"),
        ("jack", "y"),
        ("bo", "C"),
        ("bo", "D"),
        ("jack", "z"),
    ]


@aio
async def test_tagged_merge_register():
    seq1 = timed_sequence("A 1 B 1 C 1 D")
    seq2 = timed_sequence("1.5 x 0.2 y 7 z")

    mobj = O.tagged_merge()
    mobj.register(bo=seq1)
    mobj.register(jack=seq2)
    mobj.register(horse=O.delay("!", 1.6))

    assert (await O.to_list(mobj)) == [
        ("bo", "A"),
        ("bo", "B"),
        ("jack", "x"),
        ("horse", "!"),
        ("jack", "y"),
        ("bo", "C"),
        ("bo", "D"),
        ("jack", "z"),
    ]
