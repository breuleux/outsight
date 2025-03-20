import pytest

from outsight import ops as O

from ..common import lister, seq

aio = pytest.mark.asyncio


@pytest.fixture
def xten():
    return O.aiter({"x": i} for i in range(10))


@aio
async def test_augment(xten):
    results = await lister.augment(xten, y=lambda x: -x)
    assert results[:2] == [{"x": 0, "y": 0}, {"x": 1, "y": -1}]


@aio
async def test_keep():
    result = await lister.keep(seq({"x": 1}, {"y": 2}, {"x": 3}, {"z": 4}), "x")
    assert result == [
        {"x": 1},
        {"x": 3},
    ]


@aio
async def test_keep_rename():
    result = await lister.keep(seq({"x": 1}, {"y": 2}, {"x": 3}, {"z": 4}), "x", y="q")
    assert result == [
        {"x": 1},
        {"q": 2},
        {"x": 3},
    ]


@aio
async def test_kfilter(xten):
    results = await lister.kfilter(xten, lambda x: x % 2 == 1)
    assert len(results) == 5
    assert results[:2] == [{"x": 1}, {"x": 3}]


@aio
async def test_kmap(xten):
    results = await lister.kmap(xten, lambda x: -x)
    assert results[:4] == [0, -1, -2, -3]


@aio
async def test_kmap_kw(xten):
    results = await lister.kmap(xten, y=lambda x: -x)
    assert results[:2] == [{"y": 0}, {"y": -1}]


@aio
async def test_kmerge():
    result = await O.kmerge(seq({"x": 1}, {"y": 2}, {"x": 3}, {"z": 4}))
    assert result == {"x": 3, "y": 2, "z": 4}


@aio
async def test_kscan():
    result = await lister.map(
        O.kscan(seq({"x": 1}, {"y": 2}, {"x": 3}, {"z": 4})), dict
    )
    assert result == [
        {"x": 1},
        {"x": 1, "y": 2},
        {"x": 3, "y": 2},
        {"x": 3, "y": 2, "z": 4},
    ]


@aio
async def test_where():
    result = await lister.where(
        seq({"x": 1}, {"x": 2, "y": 2}, {"x": 3}, {"z": 4}), "x"
    )
    assert result == [
        {"x": 1},
        {"x": 2, "y": 2},
        {"x": 3},
    ]


@aio
async def test_where_exclude():
    result = await lister.where(
        seq({"x": 1}, {"x": 2, "y": 2}, {"x": 3}, {"z": 4}), "x", "!y"
    )
    assert result == [
        {"x": 1},
        {"x": 3},
    ]


@aio
async def test_where_cond():
    result = await lister.where(
        seq({"x": 1}, {"x": 2, "y": 2}, {"x": 3}, {"z": 4}), "x", x=lambda x: x <= 2
    )
    assert result == [
        {"x": 1},
        {"x": 2, "y": 2},
    ]


@aio
async def test_where_any():
    result = await lister.where_any(
        seq({"x": 1}, {"x": 2, "y": 2}, {"x": 3}, {"z": 4}), "y", "z"
    )
    assert result == [
        {"x": 2, "y": 2},
        {"z": 4},
    ]
