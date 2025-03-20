import pytest

from outsight import ops as O
from outsight.ops import Queue

aio = pytest.mark.asyncio


@aio
async def test_queue_putleft():
    q = Queue()
    q.put_nowait(1)
    q.put_nowait(2)
    q.put_nowait(3)
    q.putleft(4)
    q.close()
    assert (await O.to_list(q)) == [4, 1, 2, 3]
