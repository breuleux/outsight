import inspect
import sys
from textwrap import dedent

import pytest

from outsight import aiter as O
from outsight.aiter import Queue
from outsight.utils import lax_function

aio = pytest.mark.asyncio


def test_lax():
    @lax_function
    def f(x):
        return x * x

    assert f(10) == 100
    assert f(x=10, y=20) == 100


def test_lax_two_arguments():
    @lax_function
    def f(x, y):
        return x * y

    assert f(x=10, y=20) == 200
    assert f(x=10, y=20, z=30) == 200


def test_lax_closure():
    results = []

    @lax_function
    def f(x):
        oi = x * x
        results.append(oi)

    f(x=10, y=20)
    assert results == [100]


def test_lax_defaults():
    singleton = object()

    @lax_function
    def f(x=singleton):
        return x

    assert f() == singleton
    assert f(x=78) == 78
    assert f(y=78) == singleton


@pytest.mark.skipif(sys.version_info < (3, 8), reason="requires python3.8 or higher")
def test_lax_positional():
    code = """
    def f(x, /, y):
        return x + y
    """
    glb = {}
    exec(dedent(code), glb, glb)
    f = lax_function(glb["f"])

    assert f(1, 2) == 3
    with pytest.raises(TypeError):
        assert f(x=1, y=2) == 3
    assert f(1, y=2) == 3
    assert f(1, y=2, z=800) == 3
    assert f(1, y=2, x=800) == 3


@pytest.mark.skipif(sys.version_info < (3, 8), reason="requires python3.8 or higher")
def test_lax_positional_2():
    code = """
    def f(x, /):
        return x + 1
    """
    glb = {}
    exec(dedent(code), glb, glb)
    f = lax_function(glb["f"])

    assert f(2) == 3
    with pytest.raises(TypeError):
        assert f(x=2) == 3
    assert f(2) == 3
    assert f(2, z=800) == 3
    assert f(2, x=800) == 3


def test_lax_vararg():
    @lax_function
    def f(*xs):
        return sum(xs)

    assert f(2, 3, 4, z=8) == 9


def test_lax_kwonly():
    @lax_function
    def f(*xs, y):
        return sum(xs) * y

    assert f(2, 3, 4, y=2, z=8) == 18


def test_lax_kwonly2():
    @lax_function
    def f(*, y):
        return y * y

    with pytest.raises(TypeError):
        assert f(5, z=8) == 25
    assert f(y=5, z=8) == 25


def test_lax_generator():
    @lax_function
    def f(x):
        yield x
        yield x + 1

    assert inspect.isgeneratorfunction(f)
    assert list(f(5, z=9)) == [5, 6]


def test_lax_objcall():
    class Gorble:
        def __call__(self, x):
            return x * x

    f = lax_function(Gorble())

    assert f(x=4, y=6) == 16


def test_lax_already_kw():
    def f(**kws):
        return kws["x"]

    assert lax_function(f) is f


@aio
async def test_queue_putleft():
    q = Queue()
    q.put_nowait(1)
    q.put_nowait(2)
    q.put_nowait(3)
    q.putleft(4)
    q.close()
    assert (await O.to_list(q)) == [4, 1, 2, 3]
