from itertools import count

import pytest

from outsight import ops as O

from ..common import lister, timed_sequence

aio = pytest.mark.asyncio


@aio
async def test_buffer():
    seq = "A 1  B 5  C 1  D 2  E 3  F 1  G 1  H 1"

    results = await lister.buffer(timed_sequence(seq), 2.95, align=True)
    assert results == [["A"], ["B"], ["C", "D"], ["E"], ["F", "G", "H"]]


@aio
async def test_buffer_align():
    seq = "2 A  1 B  3 C"

    results = await lister.buffer(
        timed_sequence(seq), 2.95, align=True, skip_empty=False
    )
    assert results == [["A"], ["B"], ["C"]]

    results = await lister.buffer(
        timed_sequence(seq), 2.95, align=False, skip_empty=False
    )
    assert results == [[], ["A"], ["B"], ["C"]]


@aio
async def test_buffer_count(ten):
    results = await lister.buffer(ten, count=3)
    assert results == [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]


@aio
async def test_buffer_max_count():
    seq = "1 A B C  1 D E F G"

    results = await lister.buffer(timed_sequence(seq), 1.5, count=2)
    assert results == [["A", "B"], ["C"], ["D", "E"], ["F", "G"]]


@aio
async def test_buffer_debounce():
    seq = "A 1  B 5  C 1  D 2  E 3  F 1  G 1  H 1  I 1  J 3  K"

    results = await lister.buffer_debounce(timed_sequence(seq), 1.1)
    assert results == [["A", "B"], ["C", "D"], ["E"], ["F", "G", "H", "I", "J"], ["K"]]


@aio
async def test_debounce():
    seq = "A 1  B 5  C 1  D 2  E 3  F 1  G 1  H 1  I 1  J 3  K"

    results = await lister.timed_sequence(seq)
    assert results == list("ABCDEFGHIJK")

    resultsd = await lister.debounce(timed_sequence(seq), 1.1)
    assert resultsd == list("BDEJK")

    resultsmt = await lister.debounce(timed_sequence(seq), 1.1, max_wait=3.1)
    assert resultsmt == list("BDEIJK")


@aio
async def test_debounce_small_gap():
    seq = "A 1  B 1  C 1"

    results = await lister.debounce(timed_sequence(seq), 0.4)
    assert results == list("ABC")


@aio
async def test_repeat(fakesleep):
    assert await lister.repeat("wow", count=7, interval=1) == ["wow"] * 7
    assert fakesleep[0] == 6


@aio
async def test_repeat_fn(fakesleep):
    cnt = count()
    assert await lister.repeat(lambda: next(cnt), count=7, interval=1) == [*range(7)]
    assert fakesleep[0] == 6


@aio
async def test_repeat_nocount(fakesleep):
    assert await lister.take(O.repeat("wow", interval=1), 100) == ["wow"] * 100
    assert fakesleep[0] == 99


@aio
async def test_sample():
    seq = "A 1  B 1  C 1  D 1"
    results = await lister.sample(timed_sequence(seq), 0.6)
    assert results == list("AABBCDDD")


@aio
async def test_throttle():
    seq = "A 1  B 1  C 1  D 1"

    results = await lister.throttle(timed_sequence(seq), 2.5)
    assert results == list("ACD")


@aio
async def test_ticktock(fakesleep):
    results = await lister.take(O.ticktock(1), 10)
    assert results == list(range(10))
    assert fakesleep[0] == 9


@aio
async def test_ticktock_offset(fakesleep):
    results = await lister.take(O.ticktock(1, offset=2), 10)
    assert results == list(range(10))
    assert fakesleep[0] == 11
