from . import ops, keyed


class _forward:
    def __init__(self, operator=None, name=None, first_arg=False):
        self.operator = operator
        self.name = name
        self.first_arg = first_arg

    def __set_name__(self, obj, name):
        if self.name is None:
            self.name = name
        if self.operator is None:
            self.operator = getattr(ops, self.name, None) or getattr(keyed, self.name)

    def __get__(self, obj, objt):
        obj = aiter(obj)

        if self.first_arg:

            def wrap(*args, **kwargs):
                return Stream(self.operator(obj, *args, **kwargs))

        else:

            def wrap(*args, **kwargs):
                return Stream(self.operator(*args, stream=obj, **kwargs))

        return wrap


class Stream:
    def __init__(self, source):
        self.source = source

    def __aiter__(self):
        if not hasattr(self.source, "__aiter__"):
            raise Exception(f"Stream source {self.source} is not iterable.")
        return aiter(self.source)

    def __await__(self):
        return self.source.__await__()

    #############
    # Operators #
    #############

    average = _forward()
    bottom = _forward(first_arg=True)
    count = _forward()
    cycle = _forward()
    debounce = _forward()
    distinct = _forward(first_arg=True)
    drop = _forward()
    dropwhile = _forward()
    drop_last = _forward(first_arg=True)
    enumerate = _forward(first_arg=True)
    every = _forward(first_arg=True)
    filter = _forward()
    first = _forward()
    last = _forward()
    map = _forward()
    max = _forward()
    merge = _forward(first_arg=True)
    min = _forward()
    multicast = _forward()
    norepeat = _forward(first_arg=True)
    nth = _forward(first_arg=True)
    pairwise = _forward()
    reduce = _forward()
    roll = _forward()
    sample = _forward(first_arg=True)
    scan = _forward()
    slice = _forward(first_arg=True)
    sort = _forward(first_arg=True)
    std = _forward()
    sum = _forward()
    tagged_merge = _forward(first_arg=True)
    take = _forward()
    takewhile = _forward()
    take_last = _forward(first_arg=True)
    tee = _forward()
    throttle = _forward(first_arg=True)
    top = _forward(first_arg=True)
    to_list = _forward()
    variance = _forward()
    zip = _forward(first_arg=True)

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

    augment = _forward(first_arg=True)
    affix = _forward(first_arg=True)
    getitem = _forward(first_arg=True)
    keep = _forward(first_arg=True)
    kfilter = _forward(first_arg=True)
    kmap = _forward(first_arg=True)
    kmerge = _forward(first_arg=True)
    kscan = _forward(first_arg=True)
    where = _forward(first_arg=True)
    where_any = _forward(first_arg=True)
