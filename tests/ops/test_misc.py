import pytest

from outsight import ops as O
from outsight.ops.misc import to_list

from ..common import lister, seq

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
async def test_top():
    assert (await O.top(seq(4, -1, 3, 8, 21, 0, -3), 3)) == [21, 8, 4]


@aio
async def test_zip():
    assert await lister.zip(O.aiter(range(3)), O.aiter(range(4, 7))) == [
        [0, 4],
        [1, 5],
        [2, 6],
    ]
