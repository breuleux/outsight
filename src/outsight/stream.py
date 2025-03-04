from . import ops


class _forward:
    def __init__(self, operator=None, name=None, first_arg=False):
        self.operator = operator
        self.name = name
        self.first_arg = first_arg

    def __set_name__(self, obj, name):
        if self.name is None:
            self.name = name
        if self.operator is None:
            self.operator = getattr(ops, self.name)

    def __get__(self, obj, objt):
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
        return aiter(self.source)

    def __await__(self):
        return self.source.__await__()

    #############
    # Operators #
    #############

    average = _forward()
    cycle = _forward()
    debounce = _forward()
    drop = _forward()
    dropwhile = _forward()
    filter = _forward()
    map = _forward()
    min = _forward()
    max = _forward()
    multicast = _forward()
    pairwise = _forward()
    merge = _forward(first_arg=True)
    reduce = _forward()
    roll = _forward()
    scan = _forward()
    std = _forward()
    tagged_merge = _forward(first_arg=True)
    take = _forward()
    takewhile = _forward()
    tee = _forward()
    to_list = _forward()
    variance = _forward()
    zip = _forward(first_arg=True)

    # chain
    # repeat
    # ticktock
