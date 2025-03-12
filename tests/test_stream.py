from outsight.stream import Stream
import pytest
from .common import seq, timed_sequence

aio = pytest.mark.asyncio


@aio
async def test_simple_reduction(ten):
    s = Stream(ten)
    assert (await s.average()) == 4.5


@aio
async def test_map_reduce(ten):
    s = Stream(ten)
    assert (await s.map(lambda x: x * x).max()) == 81


@aio
async def test_merge():
    seq1 = timed_sequence("A 1 B 1 C 1 D")
    seq2 = timed_sequence("1.5 x 0.1 y 7 z")

    s = Stream(seq1)
    assert await s.merge(seq2).to_list() == list("ABxyCDz")


@aio
async def test_getitems():
    s = Stream(seq({"x": 1, "y": 2}, {"z": 3}))
    assert (await s["x", "y"].to_list()) == [(1, 2)]


@aio
async def test_slice(ten):
    s = Stream(ten)
    assert (await s[1:3].to_list()) == [1, 2]


@aio
async def test_nth(ten):
    s = Stream(ten)
    assert (await s[2]) == 2


@aio
async def test_await_iterable(ten):
    s = Stream(seq({"x": 1}, {"x": 2}, {"y": 3}, {"x": 4}))
    assert (await s["x"]) == 1
