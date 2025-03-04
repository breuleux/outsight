import pytest
import asyncio
import time


@pytest.fixture
def ten():
    from outsight import ops as O

    return O.aiter(range(10))


@pytest.fixture
def fakesleep(monkeypatch):
    t = [0]

    async def mock_sleep(interval):
        t[0] += interval

    def mock_time():
        return t[0]

    monkeypatch.setattr(asyncio, "sleep", mock_sleep)
    monkeypatch.setattr(time, "time", mock_time)
    yield t
