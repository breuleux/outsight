from dataclasses import dataclass, field
from functools import cached_property, wraps

from outsight import ops

MISSING = object()


class StreamBuildingError(Exception):
    pass


@dataclass
class Step:
    operator: object
    args: list
    kwargs: dict

    def build(self, stream):
        return self.operator(stream, *self.args, **self.kwargs)


@dataclass
class BaseStream:
    root: object = None
    steps: list[Step] = field(default_factory=list)
    nclients: int = 0

    @cached_property
    def iterator(self):
        return self.build()

    def build(self):
        if self.root is None:
            raise StreamBuildingError(
                "Root is a placeholder, use x.with_root(source).build()"
            )
        result = aiter(self.root)
        for step in self.steps:
            result = step.build(result)
        return result

    def copy(self, root=MISSING, steps=MISSING):
        return type(self)(
            root=self.root if root is MISSING else root,
            steps=self.steps if steps is MISSING else steps,
        )

    def with_root(self, root):
        if isinstance(root, BaseStream):
            return root.copy(steps=[*root.steps, *self.steps])
        else:
            return self.copy(root=root)

    def step(self, step):
        self.nclients += 1
        return self.copy(steps=[*self.steps, step])

    def pipe(self, operator, *args, **kwargs):
        if isinstance(operator, BaseStream):
            assert not args
            assert not kwargs
            if operator.root:
                raise StreamBuildingError("Cannot pipe into a stream that has a root")
            return operator.with_root(self)
        else:
            return self.step(Step(operator=operator, args=args, kwargs=kwargs))

    def __aiter__(self):
        source = self.iterator
        if not hasattr(source, "__aiter__"):  # pragma: no cover
            raise Exception(f"Stream source {source} is not iterable.")
        return aiter(source)

    def __await__(self):
        source = self.iterator
        if hasattr(source, "__await__"):
            return source.__await__()
        elif hasattr(source, "__aiter__"):
            return anext(source).__await__()
        else:  # pragma: no cover
            raise TypeError(f"Cannot await source: {source}")


class _forward:
    def __init__(self, operator=None, name=None):
        self.operator = operator
        self.name = name

    def __set_name__(self, obj, name):
        if self.name is None:
            self.name = name
        if self.operator is None:
            self.operator = getattr(ops, self.name)

    def __get__(self, obj, objt):
        @wraps(self.operator)
        def wrap(*args, **kwargs):
            return obj.step(Step(operator=self.operator, args=args, kwargs=kwargs))

        return wrap


class Stream(BaseStream):
    #############
    # Operators #
    #############

    any = _forward()
    all = _forward()
    average = _forward()
    bottom = _forward()
    buffer = _forward()
    buffer_debounce = _forward()
    count = _forward()
    cycle = _forward()
    debounce = _forward()
    distinct = _forward()
    drop = _forward()
    drop_while = _forward()
    drop_last = _forward()
    enumerate = _forward()
    every = _forward()
    filter = _forward()
    first = _forward()
    flat_map = _forward()
    group = _forward()
    last = _forward()
    map = _forward()
    max = _forward()
    merge = _forward()
    min = _forward()
    multicast = _forward()
    norepeat = _forward()
    nth = _forward()
    pairwise = _forward()
    reduce = _forward()
    roll = _forward()
    sample = _forward()
    scan = _forward()
    slice = _forward()
    sort = _forward()
    split_boundary = _forward()
    std = _forward()
    sum = _forward()
    tagged_merge = _forward()
    take = _forward()
    take_while = _forward()
    take_last = _forward()
    tee = _forward()
    throttle = _forward()
    top = _forward()
    to_list = _forward()
    variance = _forward()
    zip = _forward()

    # chain
    # repeat
    # ticktock

    ########################
    # Dict-based operators #
    ########################

    def __getitem__(self, item):
        if isinstance(item, int):
            return self.pipe(ops.nth, item)
        elif isinstance(item, slice):
            return self.pipe(ops.slice, item.start, item.stop, item.step)
        else:
            return self.pipe(ops.getitem, item)

    augment = _forward()
    affix = _forward()
    getitem = _forward()
    keep = _forward()
    kfilter = _forward()
    kmap = _forward()
    kmerge = _forward()
    kscan = _forward()
    where = _forward()
    where_any = _forward()

    #############
    # Operators #
    #############

    __add__ = _forward(ops.merge)

    def __or__(self, operator):
        return self.pipe(operator)


placeholder = Stream()
