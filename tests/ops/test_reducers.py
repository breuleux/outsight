import pytest

from outsight import ops as O

from ..common import lister

aio = pytest.mark.asyncio


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
async def test_max():
    assert await O.max(O.aiter([8, 3, 7, 15, 4])) == 15


@aio
async def test_min():
    assert await O.min(O.aiter([8, 3, 7, 15, 4])) == 3


@aio
async def test_min_scan():
    assert await lister.min(O.aiter([8, 3, 7, 15, 4]), scan=3) == [8, 3, 3, 3, 4]


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
async def test_std(ten):
    avg = sum(range(10)) / 10
    var = sum([(i - avg) ** 2 for i in range(10)]) / 9
    assert await O.std(ten) == var**0.5


@aio
async def test_sum(ten):
    assert await O.sum(ten) == sum(range(10))


@aio
async def test_variance(ten):
    avg = sum(range(10)) / 10
    var = sum([(i - avg) ** 2 for i in range(10)]) / 9
    assert await O.variance(ten) == var
