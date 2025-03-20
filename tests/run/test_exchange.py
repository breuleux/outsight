from types import FrameType

import pytest

from outsight.aiter import to_list
from outsight.run.exchange import LinePosition, Sender

aio = pytest.mark.asyncio


def stest(*expected):
    expected = list(expected)

    def deco(fn):
        @aio
        async def test():
            send = Sender()
            if isinstance(expected[0], type):
                with pytest.raises(expected[0]):
                    fn(send)
                    return
            else:
                fn(send)
            send.close()
            results = await to_list(send)
            for r, e in zip(results, expected):
                r["$timestamp"] = r.timestamp
                r["$frame"] = r.frame
                r["$line"] = r.line
                for ke, ve in e.items():
                    vr = r[ke]
                    if isinstance(ve, type):
                        assert isinstance(vr, ve)
                    else:
                        if vr != ve:
                            assert results == expected

        return test

    return deco


@stest({"x": 3})
def test_send_kwargs(send):
    send(x=3)


@stest({"x": 3, "y": 4})
def test_send_multiple(send):
    send(x=3, y=4)


@stest({"x": 7})
def test_send_var(send):
    x = 7
    assert send(x) == x


@stest({"x": 36})
def test_send_assigned_to(send):
    x = send(6 * 6)
    assert x == 36


@stest({"i": 0}, {"i": 1}, {"i": 2})
def test_send_loop(send):
    for i in range(3):
        send(i)


@stest({"x": 9, "y": 10})
def test_send_vars(send):
    x, y = 9, 10
    send(x, y)


@stest({"x + 10": 25})
def test_send_expr(send):
    x = 15
    send(x + 10)


@stest({"x": 18, "$line": LinePosition, "$timestamp": float, "$frame": FrameType})
def test_send_specials(send):
    x = 18
    send(x)


@stest({"x": 55, "inh": 66})
def test_inherit(send):
    with send.inherit(inh=66):
        send(x=55)


@stest({"x": 23})
def test_send_arg(send):
    def f(x):
        send(x)

    f(23)
