from contextlib import asynccontextmanager

from outsight.run.fixtures import Fixture, UnusableFixture
from tests.common import otest


class Counter(Fixture):
    def __init__(self, scope="global"):
        self.count = 0
        self.scope = scope

    @asynccontextmanager
    async def context(self):  # pragma: no cover
        self.count += 1
        yield self.count


@otest(count=Counter("global"))
class test_global_scope:
    def main(o):
        pass

    async def o_first(count):
        assert count == 1

    async def o_second(count):
        assert count == 1


@otest(count=Counter("local"))
class test_local_scope:
    def main(o):
        pass

    async def o_first(count):
        assert count == 1

    async def o_second(count):
        assert count == 2


@otest(count=Counter)
class test_instantiate_fixture_class:
    def main(o):
        pass

    async def o_first(count):
        assert count == 1

    async def o_second(count):
        assert count == 1


@otest(count=Counter("global"), bad=UnusableFixture("unacceptable"))
class test_unused_bad_fixture:
    def main(o):
        pass

    async def o_first(count):
        assert count == 1

    async def o_second(count):
        assert count == 1


@otest(bad=UnusableFixture("unacceptable"), expect_error=RuntimeError)
class test_bad_fixture:
    def main(o):
        pass

    async def o_worker(bad):
        # Should not be called, as fixture raises
        assert False, "Should not reach here"
