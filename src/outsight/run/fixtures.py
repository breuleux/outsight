import inspect
from contextlib import AsyncExitStack, asynccontextmanager


class Fixture:
    scope = "local"

    @asynccontextmanager
    async def context(self):  # pragma: no cover
        yield self


class ValueFixture(Fixture):
    scope = "global"

    def __init__(self, value):
        self.value = value

    @asynccontextmanager
    async def context(self):
        yield self.value


class StreamFixture(Fixture):
    scope = "local"

    def __init__(self, stream):
        self.stream = stream

    @asynccontextmanager
    async def context(self):
        yield self.stream.stream()


class UnusableFixture(Fixture):
    scope = "local"

    def __init__(self, message):
        self.message = message

    def context(self):
        raise RuntimeError(self.message)


class FixtureGroup:
    def __init__(self, **fixtures):
        self.fixtures = {}
        for name, fx in fixtures.items():
            self.register_fixture(name, fx)

    def register_fixture(self, name, fixture):
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
                match fixture.scope:
                    case "global":
                        if fixture not in self.global_values:
                            value = await self.global_stack.enter_async_context(
                                fixture.context()
                            )
                            self.global_values[fixture] = value
                        else:
                            value = self.global_values[fixture]
                    case "local":
                        value = await stack.enter_async_context(fixture.context())
                    case _:  # pragma: no cover
                        raise RuntimeError(f"Unknown fixture scope: {fixture.scope}")
                kw[name] = value
            await fn(**kw)

    @asynccontextmanager
    async def enter(self):
        async with AsyncExitStack() as stack:
            self.global_values = {}
            self.global_stack = stack
            yield
