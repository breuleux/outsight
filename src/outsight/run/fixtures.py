import inspect
from contextlib import AsyncExitStack, asynccontextmanager


class Fixture:
    async def context(self):  # pragma: no cover
        pass


class ValueFixture:
    def __init__(self, value):
        self.value = value

    @asynccontextmanager
    async def context(self):
        yield self.value


class StreamFixture:
    def __init__(self, stream):
        self.stream = stream

    @asynccontextmanager
    async def context(self):
        yield self.stream.stream()


class FixtureGroup:
    def __init__(self, **fixtures):
        self.fixtures = {}
        for name, fx in fixtures.items():
            self.add_fixture(name, fx)

    def add_fixture(self, name, fixture):
        self.fixtures[name] = fixture

    def get_applicable(self, fn):
        sig = inspect.signature(fn)
        return {
            argname: self.fixtures.get(argname, None)
            for argname, param in sig.parameters.items()
        }

    async def execute(self, fn):
        async with AsyncExitStack() as stack:
            kw = {}
            for name, fixture in self.get_applicable(fn).items():
                value = await stack.enter_async_context(fixture.context())
                kw[name] = value
            await fn(**kw)
