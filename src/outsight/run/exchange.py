import asyncio
import sys
import time
from concurrent.futures import Future, wait
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from functools import cached_property

from varname import ImproperUseError, argname, varname

from ..aiter import Multicast, MulticastQueue
from ..stream import Stream


@dataclass
class LinePosition:
    name: str
    filename: str
    lineno: int


def resolve(frame, func, args):
    """Return a {variable_name: value} dictionary depending on usage.

    * ``len(args) == 0`` => Use the variable assigned in the line before the call.
    * ``len(args) == 1`` => Use the variable the call is assigned to.
    * ``len(args) >= 1`` => Use the variables passed as arguments to the call.

    Arguments:
        frame: The number of frames to go up to find the context.
        func: The Giver object that was called.
        args: The arguments given to the Giver.
    """
    nargs = len(args)

    if nargs == 1:
        try:
            assigned_to = varname(frame=frame + 1, strict=True, raise_exc=False)
        except ImproperUseError:
            assigned_to = None
        if assigned_to is not None:
            return {assigned_to: args[0]}

    argnames = argname("args", func=func, frame=frame + 1, vars_only=False)
    if argnames is None:  # pragma: no cover
        # I am not sure how to trigger this
        raise Exception("Could not resolve arg names")

    return {name: value for name, value in zip(argnames, args)}


class Capture(dict):
    def __init__(self, parent=None, **values):
        self.parent = parent
        self.timestamp = time.time()
        self.frame = sys._getframe(3)
        super().__init__(values)

    @property
    def line(self):
        co = self.frame.f_code
        return LinePosition(co.co_name, co.co_filename, self.frame.f_lineno)

    def __missing__(self, key):
        if self.parent is None:  # pragma: no cover
            raise KeyError(key)
        return self.parent[key]


class BaseSender:
    def __init__(self):
        self.inherited = ContextVar("inherited", default={})

    @contextmanager
    def inherit(self, **keys):
        """Create a context manager within which extra values are sent.

        .. code-block:: python

            with send.inherit(a=1):
                send(b=2)   # gives {"a": 1, "b": 2}

        Arguments:
            keys: The key/value pairs to send within the block.
        """
        inh = self.inherited.get()
        token = self.inherited.set({**inh, **keys})
        try:
            yield
        finally:
            self.inherited.reset(token)

    def __call__(self, *args, **values):
        """Give the args and values."""
        if args:
            values = {**resolve(1, self, args), **values}

        result = args[0] if len(args) == 1 else None
        return self.produce(values, result)

    def stream(self):
        return Stream(self)

    def close(self):  # pragma: no cover
        pass

    async def aclose(self):
        self.close()


class Sender(BaseSender):
    """Sender of key/value pairs.

    ``Sender`` is the class of the ``send`` object.

    Arguments:
        loop:
            The event loop for the queue
    """

    def __init__(self, loop=None):
        self.loop = loop
        self.queue = MulticastQueue(loop=self.loop)
        super().__init__()

    def produce(self, values, result):
        """Give the values dictionary."""
        values = Capture(self.inherited.get(), **values)
        self.queue.put_nowait(values)
        return result

    def close(self):
        self.queue.close()

    def __aiter__(self):
        return aiter(self.queue)


class BlockingCapture(Capture):
    def __init__(self, value, parent, **values):
        self.value = value
        self.future = Future()
        super().__init__(parent, **values)

    def done(self):
        return self.future.done()

    def set_result(self, value):
        return self.future.set_result(value)

    def set_exception(self, exc):
        return self.future.set_exception(exc)


class Giver(BaseSender):
    def __init__(self, loop=None):
        self.active = False
        self.loop = loop or asyncio.get_running_loop()
        self.fut = self.loop.create_future()
        super().__init__()

    def produce(self, values, result):
        if not self.active:
            return result
        response = BlockingCapture(result, self.inherited.get(), **values)
        self.loop.call_soon_threadsafe(self.fut.set_result, response)
        ((done,), ()) = wait([response.future])
        return done.result()

    async def __aiter__(self):
        self.active = True
        try:
            while True:
                try:
                    response = await self.fut
                    self.fut = self.loop.create_future()
                    yield response
                except StopAsyncIteration:
                    break
                finally:
                    if not response.done():
                        response.set_result(response.value)
        finally:
            self.active = False

    @cached_property
    def _multicast(self):
        return Multicast(self, sync=True)

    def stream(self):
        return Stream(self._multicast)

    def close(self):
        if self.active:
            self.loop.call_soon_threadsafe(self.fut.set_exception, StopAsyncIteration())
