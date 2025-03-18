import asyncio
from itertools import count
import pytest
from outsight import ops as O
from outsight.ops import to_list
from .common import seq, timed_sequence, lister

aio = pytest.mark.asyncio


@aio
async def test_any(ten):
    assert await O.any(ten)


@aio
async def test_any_predicate(ten):
    assert not (await O.any(ten, lambda x: x < 0))


@aio
async def test_all(ten):
    assert not (await O.all(ten))


@aio
async def test_all_predicate(ten):
    assert await O.all(ten, lambda x: x >= 0)


@aio
async def test_average(ten):
    assert await O.average(ten) == sum(range(10)) / 10


@aio
async def test_average_scan(ten):
    assert await lister.average(ten, scan=True) == [
        sum(range(i)) / i for i in range(1, 11)
    ]


@aio
async def test_average_roll(ten):
    assert await lister.average(ten, scan=2) == [0.0] + [
        (i + i + 1) / 2 for i in range(9)
    ]


@aio
async def test_average_and_variance(ten):
    avg = sum(range(10)) / 10
    var = sum([(i - avg) ** 2 for i in range(10)]) / 9
    assert await O.average_and_variance(ten) == (avg, var)


@aio
async def test_average_and_variance_one_element():
    assert await O.average_and_variance(O.aiter([8])) == (8, None)


@aio
async def test_average_and_variance_roll(ten):
    assert (await lister.average_and_variance(ten, scan=3))[4] == (3, 1)


@aio
async def test_bottom():
    assert (await O.bottom(seq(4, -1, 3, 8, 21, 0, -3), 3)) == [-3, -1, 0]


@aio
async def test_bottom_key():
    def key(x):
        return (x - 10) ** 2

    assert (await O.bottom(seq(4, -1, 3, 8, 21, 0, -3), 2, key=key)) == [8, 4]


@aio
async def test_chain():
    assert await lister.chain([O.aiter([1, 2]), O.aiter([7, 8])]) == [1, 2, 7, 8]


@aio
async def test_count(ten):
    ten1, ten2 = O.tee(ten, 2)
    assert await O.count(ten1) == 10
    assert await O.count(ten2, lambda x: x % 2 == 0) == 5


@aio
async def test_enumerate():
    assert await lister.enumerate(seq(5, 9, -3)) == [(0, 5), (1, 9), (2, -3)]


@aio
async def test_generate_chain(ten):
    assert await lister.chain([O.aiter([x, x * x]) async for x in O.aiter([3, 7])]) == [
        3,
        9,
        7,
        49,
    ]


@aio
async def test_cycle(ten):
    assert await lister.take(O.cycle(ten), 19) == [*range(0, 10), *range(0, 9)]


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
async def test_distinct():
    assert await lister.distinct(seq(1, 2, 1, 3, 3, 2, 4, 0, 5)) == [1, 2, 3, 4, 0, 5]


@aio
async def test_distinct_key():
    def key(x):
        return x >= 0

    assert await lister.distinct(seq(1, 2, 3, -7, 4, -3, 0, 2), key=key) == [1, -7]


@aio
async def test_drop(ten):
    assert await lister.drop(ten, 5) == [*range(5, 10)]


@aio
async def test_drop_more(ten):
    assert await lister.drop(ten, 15) == []


@aio
async def test_drop_last(ten):
    assert await lister.drop_last(ten, 3) == [0, 1, 2, 3, 4, 5, 6]


@aio
async def test_drop_while(ten):
    assert await lister.drop_while(ten, lambda x: x < 5) == [*range(5, 10)]


@aio
async def test_filter(ten):
    assert await lister.filter(ten, lambda x: x % 2 == 0) == [0, 2, 4, 6, 8]


@aio
async def test_first(ten):
    assert await O.first(ten) == 0


@aio
async def test_flat_map(ten):
    results = await lister.flat_map(ten, lambda x: [x, x * x])
    assert results[:10] == [0, 0, 1, 1, 2, 4, 3, 9, 4, 16]


@aio
async def test_group(ten):
    results = await lister.group(ten, lambda x: x % 3 == 0)
    assert results[:4] == [(True, [0]), (False, [1, 2]), (True, [3]), (False, [4, 5])]


@aio
async def test_group_empty():
    results = await lister.group(seq(), lambda x: x % 3 == 0)
    assert results == []


@aio
async def test_last(ten):
    assert await O.last(ten) == 9


@aio
async def test_map(ten):
    assert await lister.map(ten, lambda x: x + 83) == list(range(83, 93))


@aio
async def test_map_async(ten):
    async def f(x):
        return x + 84

    assert await lister.map(ten, f) == list(range(84, 94))


@aio
async def test_max():
    assert await O.max(O.aiter([8, 3, 7, 15, 4])) == 15


@aio
async def test_merge():
    seq1 = timed_sequence("A 1 B 1 C 1 D")
    seq2 = timed_sequence("1.5 x 0.1 y 7 z")

    results = await lister.merge(seq1, seq2)
    assert results == list("ABxyCDz")


@aio
async def test_min():
    assert await O.min(O.aiter([8, 3, 7, 15, 4])) == 3


@aio
async def test_min_scan():
    assert await lister.min(O.aiter([8, 3, 7, 15, 4]), scan=3) == [8, 3, 3, 3, 4]


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
async def test_norepeat():
    assert await lister.norepeat(seq(1, 2, 1, 3, 3, 2, 2, 2, 1)) == [1, 2, 1, 3, 2, 1]


@aio
async def test_nth_out_of_bounds(ten):
    with pytest.raises(IndexError):
        await O.nth(ten, 100)


@aio
async def test_pairwise(ten):
    assert await lister.pairwise(ten) == list(zip(range(0, 9), range(1, 10)))


@aio
async def test_reduce(ten):
    assert await O.reduce(ten, lambda x, y: x + y) == sum(range(1, 10))


@aio
async def test_reduce_init(ten):
    assert (
        await O.reduce(ten, lambda x, y: x + y, init=1000) == sum(range(1, 10)) + 1000
    )


@aio
async def test_reduce_empty(ten):
    with pytest.raises(ValueError, match="Stream cannot be reduced"):
        await O.reduce(O.aiter([]), lambda x, y: x + y)


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
async def test_roll(ten):
    assert await lister.map(O.roll(ten, 2), tuple) == list(
        zip(range(0, 9), range(1, 10))
    )


@aio
async def test_roll_partial():
    assert await lister.map(O.roll(O.aiter(range(4)), 3, partial=True), tuple) == [
        (0,),
        (0, 1),
        (0, 1, 2),
        (1, 2, 3),
    ]


@aio
async def test_sample():
    seq = "A 1  B 1  C 1  D 1"
    results = await lister.sample(timed_sequence(seq), 0.6)
    assert results == list("AABBCDDD")


@aio
async def test_scan(ten):
    assert await lister.scan(ten, lambda x, y: x + y) == [
        sum(range(i + 1)) for i in range(10)
    ]


@aio
async def test_slice(ten):
    assert await lister.slice(ten, 3, 8, 2) == [3, 5, 7]


@aio
async def test_slice_onearg(ten):
    assert await lister.slice(ten, 7) == [7, 8, 9]


@aio
async def test_slice_negative(ten):
    assert await lister.slice(ten, -3) == [7, 8, 9]


@aio
async def test_slice_only_stop(ten):
    assert await lister.slice(ten, stop=3) == [0, 1, 2]


@aio
async def test_slice_negative_start_stop(ten):
    assert await lister.slice(ten, -3, -1) == [7, 8]


@aio
async def test_slice_no_args(ten):
    assert await lister.slice(ten) == list(range(10))


@aio
async def test_sort():
    xs = [4, -1, 3, 8, 21, 0, -3]
    assert (await O.sort(seq(*xs))) == list(sorted(xs))


@aio
async def test_split_boundary():
    li = [0, 1, 3, 7, 4, 5, -1, 29, 310]
    results = await lister.split_boundary(O.aiter(li), lambda x, y: x > y)
    assert results == [[0, 1, 3, 7], [4, 5], [-1, 29, 310]]


@aio
async def test_split_boundary_empty():
    li = []
    results = await lister.split_boundary(O.aiter(li), lambda x, y: x > y)
    assert results == []


@aio
async def test_std(ten):
    avg = sum(range(10)) / 10
    var = sum([(i - avg) ** 2 for i in range(10)]) / 9
    assert await O.std(ten) == var**0.5


@aio
async def test_sum(ten):
    assert await O.sum(ten) == sum(range(10))


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


@aio
async def test_take(ten):
    assert await lister.take(ten, 5) == [*range(0, 5)]


@aio
async def test_take_more(ten):
    assert await lister.take(ten, 15) == [*range(0, 10)]


@aio
async def test_take_while(ten):
    assert await lister.take_while(ten, lambda x: x < 5) == [*range(5)]


@aio
async def test_tee(ten):
    t1, t2, t3 = O.tee(ten, 3)
    assert await to_list(t1) == list(range(10))
    assert await to_list(t2) == list(range(10))
    assert await to_list(t3) == list(range(10))


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


@aio
async def test_top():
    assert (await O.top(seq(4, -1, 3, 8, 21, 0, -3), 3)) == [21, 8, 4]


@aio
async def test_variance(ten):
    avg = sum(range(10)) / 10
    var = sum([(i - avg) ** 2 for i in range(10)]) / 9
    assert await O.variance(ten) == var


@aio
async def test_zip():
    assert await lister.zip(O.aiter(range(3)), O.aiter(range(4, 7))) == [
        [0, 4],
        [1, 5],
        [2, 6],
    ]
