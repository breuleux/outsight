import pytest

from outsight.stream import Stream, StreamBuildingError, placeholder as _

from .common import seq, timed_sequence

aio = pytest.mark.asyncio


@aio
async def test_simple_reduction(ten):
    s = Stream(ten)
    assert (await s.average()) == 4.5


@aio
async def test_chain():
    seq1 = timed_sequence("A 1 B 1 C 1 D")
    seq2 = timed_sequence("1.5 x 0.1 y 7 z")

    s = Stream(seq1)
    assert await s.chain(seq2).to_list() == list("ABCDxyz")


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
async def test_merge_operator():
    seq1 = timed_sequence("A 1 B 1 C 1 D")
    seq2 = timed_sequence("1.5 x 0.1 y 7 z")

    s = Stream(seq1)
    ss = s + seq2
    assert await ss.to_list() == list("ABxyCDz")


@aio
async def test_tagged_merge():
    seq1 = timed_sequence("A 1 B 1 C 1 D")
    seq2 = timed_sequence("1.5 x 0.1 y 7 z")

    s = Stream(seq1)
    ss = s.tagged_merge("one", two=seq2)
    assert (await ss.to_list())[:3] == [("one", "A"), ("one", "B"), ("two", "x")]


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
async def test_await_iterable():
    s = Stream(seq({"x": 1}, {"x": 2}, {"y": 3}, {"x": 4}))
    assert (await s["x"]) == 1


@aio
async def test_await_iterable_multiple():
    s = Stream(seq({"x": 1}, {"x": 2}, {"y": 3}, {"x": 4}))
    assert (await s["x"]) == 1
    assert (await s["y"]) == 3
    assert (await s["x"]) == 4


async def _double(stream):
    async for x in stream:
        yield x * 2


@aio
async def test_pipe():
    s = Stream(seq(1, 2, 3, 4))
    assert (await s.pipe(_double).to_list()) == [2, 4, 6, 8]


@aio
async def test_pipe_operator():
    s = Stream(seq(1, 2, 3, 4))
    s2 = s | _double
    assert (await s2.to_list()) == [2, 4, 6, 8]


@aio
async def test_pipe_into_stream():
    s = Stream(seq(1, 2, 3, 4))
    s3 = s | _.map(lambda x: x * 3).filter(lambda x: x < 10)
    assert (await s3.to_list()) == [3, 6, 9]


@aio
async def test_build_placeholder_error():
    with pytest.raises(StreamBuildingError):
        await _.min()


@aio
async def test_bad_piping():
    s1 = Stream(seq(1, 2, 3, 4))
    s2 = Stream(seq(1, 2, 3, 4))
    with pytest.raises(StreamBuildingError):
        s1.pipe(s2)


@aio
async def test_with_root():
    ph = _.map(lambda x: x * 3)
    s = ph.with_root(seq(1, 2, 3, 4))
    assert (await s.to_list()) == [3, 6, 9, 12]
