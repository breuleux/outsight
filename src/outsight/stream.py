from . import keyed, ops


class _forward:
    def __init__(self, operator=None, name=None):
        self.operator = operator
        self.name = name

    def __set_name__(self, obj, name):
        if self.name is None:
            self.name = name
        if self.operator is None:
            self.operator = getattr(ops, self.name, None) or getattr(keyed, self.name)

    def __get__(self, obj, objt):
        obj = aiter(obj)

        def wrap(*args, **kwargs):
            return Stream(self.operator(obj, *args, **kwargs))

        return wrap


class Stream:
    def __init__(self, source):
        self.source = source

    def __aiter__(self):
        if not hasattr(self.source, "__aiter__"):  # pragma: no cover
            raise Exception(f"Stream source {self.source} is not iterable.")
        return aiter(self.source)

    def __await__(self):
        if hasattr(self.source, "__await__"):
            return self.source.__await__()
        elif hasattr(self.source, "__aiter__"):
            return anext(aiter(self.source)).__await__()
        else:  # pragma: no cover
            raise TypeError(f"Cannot await source: {self.source}")

    def close(self):
        self.source.close()

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
        src = aiter(self.source)
        if isinstance(item, int):
            return Stream(ops.nth(src, item))
        elif isinstance(item, slice):
            return Stream(ops.slice(src, item.start, item.stop, item.step))
        else:
            return Stream(keyed.getitem(src, item))

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
