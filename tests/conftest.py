import asyncio
import time

import pytest


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


class TickingEventLoop(asyncio.EventLoop):
    """Event loop that resolves a "tick" every time the loop is run once.

    The point is to then replace asyncio.sleep(n) by waiting for n*100 ticks,
    so that we can simulate the order of sleep wakeup without actually sleeping.
    There's probably a cleverer way to do this.
    """

    def __init__(self):
        self.curr = 0
        self.ticks = {}
        super().__init__()

    def wait_ticks(self, t):
        idx = self.curr + int(t * 100)
        if idx not in self.ticks:
            self.ticks[idx] = self.create_future()
        return self.ticks[idx]

    def tick(self):
        if self.curr in self.ticks:
            self.ticks[self.curr].set_result(True)
            del self.ticks[self.curr]
        self.curr += 1

    def run_forever(self):
        try:
            self._run_forever_setup()
            while True:
                nostuff = not self._ready
                if nostuff:
                    self._stopping = True
                self._run_once()
                self.tick()
                if nostuff:
                    self._stopping = False
                elif self._stopping:
                    break
        finally:
            self._run_forever_cleanup()


class TickingEventLoopPolicy(asyncio.DefaultEventLoopPolicy):
    def new_event_loop(self):
        return TickingEventLoop()


@pytest.fixture
def event_loop_policy(monkeypatch):
    def _mock_sleep(t):
        return asyncio.get_running_loop().wait_ticks(t)

    def _mock_time():
        try:
            return asyncio.get_running_loop().curr / 100
        except RuntimeError:
            return orig_time()

    orig_time = time.time

    monkeypatch.setattr(asyncio, "sleep", _mock_sleep)
    monkeypatch.setattr(time, "time", _mock_time)

    yield TickingEventLoopPolicy()
