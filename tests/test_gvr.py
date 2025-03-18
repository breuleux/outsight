from textwrap import dedent
from types import FrameType
from outsight.utils import Queue
from outsight.gvr import Giver, LinePosition
import pytest
from outsight.ops import to_list
import varname

aio = pytest.mark.asyncio


def gtest(*expected):
    expected = list(expected)

    def deco(fn):
        @aio
        async def test():
            q = Queue()
            give = Giver(q)
            if isinstance(expected[0], type):
                with pytest.raises(expected[0]):
                    fn(give)
                    return
            else:
                fn(give)
            q.close()
            results = await to_list(q)
            for r, e in zip(results, expected):
                d = dict(r)
                d["$timestamp"] = r.timestamp
                d["$frame"] = r.frame
                d["$line"] = r.line
                for ke, ve in e.items():
                    vr = d[ke]
                    if isinstance(ve, type):
                        assert isinstance(vr, ve)
                    else:
                        if vr != ve:
                            assert results == expected

        return test

    return deco


@gtest({"x": 3})
def test_give_kwargs(give):
    give(x=3)


@gtest({"x": 3, "y": 4})
def test_give_multiple(give):
    give(x=3, y=4)


@gtest({"x": 7})
def test_give_var(give):
    x = 7
    assert give(x) == x


@gtest({"x": 36})
def test_give_assigned_to(give):
    x = give(6 * 6)
    assert x == 36


@gtest({"i": 0}, {"i": 1}, {"i": 2})
def test_give_loop(give):
    for i in range(3):
        give(i)


@gtest({"x": 9, "y": 10})
def test_give_vars(give):
    x, y = 9, 10
    give(x, y)


@gtest({"x": 71})
def test_give_above(give):
    x = 71
    give()
    return x


@gtest({"y": 88})
def test_give_chain_assign(give):
    x = y = 88
    give()
    return x, y


@gtest({"a": 90, "b": 91, "c": 92})
def test_give_multi_assign(give):
    (a, b), c = (90, 91), 92
    give()
    return a, b, c


@gtest({"y": 73})
def test_give_above_annassign(give):
    y: int = 73
    give()
    return y


@gtest({"x": 72})
def test_give_above_augassign(give):
    x = 8
    x += x * x
    give()


grog = 10


@gtest({"grog": 15})
def test_give_global(give):
    global grog
    grog = 15
    give()


@gtest({"x + 10": 25})
def test_give_expr(give):
    x = 15
    give(x + 10)


@gtest({"x": 18, "$line": LinePosition, "$timestamp": float, "$frame": FrameType})
def test_give_specials(give):
    x = 18
    give(x)


@gtest({"x": 55, "inh": 66})
def test_inherit(give):
    with give.inherit(inh=66):
        give(x=55)


@gtest({"x": 23})
def test_give_arg(give):
    def f(x):
        give(x)

    f(23)


@gtest(varname.utils.ImproperUseError)
def test_nothing_above(give):
    give()


@gtest(varname.utils.ImproperUseError)
def test_no_assignment_above(give):
    1 + 1
    give()


@gtest(varname.utils.VarnameRetrievingError)
def test_give_eval_fails(give):
    expr = dedent("""
    def f(give):
        x = 3
        give()
    """)
    glb = {}
    exec(expr, glb, glb)
    glb["f"](give)
