import ast
from dataclasses import dataclass
import sys
import time
from contextlib import contextmanager
from contextvars import ContextVar
from itertools import count

from varname import ImproperUseError, VarnameRetrievingError, argname, varname
from varname.utils import get_node

global_context = ContextVar("global_context", default=())
global_inherited = ContextVar("global_inherited", default={})

_block_classes = {
    ast.If: ("body", "orelse"),
    ast.For: ("body", "orelse"),
    ast.While: ("body", "orelse"),
    ast.FunctionDef: ("body",),
    ast.AsyncFunctionDef: ("body",),
    ast.With: ("body",),
    ast.AsyncWith: ("body",),
    ast.AsyncFor: ("body", "orelse"),
}


_improper_nullary_give_error = (
    "give() with no arguments must immediately follow an assignment"
)


special_keys = {}


global_count = count(0)


def _special_timestamp():
    return time.time()


def _special_frame():
    return sys._getframe(3)


@dataclass
class LinePosition:
    name: str
    filename: str
    lineno: int


def _special_line():
    fr = sys._getframe(3)
    co = fr.f_code
    return LinePosition(co.co_name, co.co_filename, fr.f_lineno)


def _find_targets(target):
    if isinstance(target, ast.Tuple):
        results = []
        for t in target.elts:
            results += _find_targets(t)
        return results
    else:
        return [target.id]


def _find_above(frame):
    node = get_node(frame + 1)
    if node is None:
        raise VarnameRetrievingError(
            "Cannot retrieve the node where the function is called."
        )

    while node.parent is not None:
        parent = node.parent
        fields = _block_classes.get(type(parent), None)
        if fields is None:
            node = parent
            continue
        else:
            for field in fields:
                f = getattr(parent, field)
                if node in f:
                    idx = f.index(node)
                    if idx == 0:
                        raise ImproperUseError(_improper_nullary_give_error)

                    assignment = f[idx - 1]

                    if isinstance(assignment, ast.Assign):
                        target = assignment.targets[-1]
                        names = _find_targets(target)
                    elif isinstance(assignment, (ast.AugAssign, ast.AnnAssign)):
                        names = [assignment.target.id]
                    else:
                        raise ImproperUseError(_improper_nullary_give_error)

                    fr = sys._getframe(frame)
                    rval = {}

                    for name in names:
                        if name in fr.f_locals:
                            rval[name] = fr.f_locals[name]
                        elif name in fr.f_globals:
                            rval[name] = fr.f_globals[name]
                        else:  # pragma: no cover
                            # I am not sure how to trigger this
                            raise Exception("Could not resolve value")
                    return rval

            else:  # pragma: no cover
                # I am not sure how to trigger this
                raise Exception("Could not find node position")

    # I am not sure how to trigger this
    raise Exception("Could not find node")  # pragma: no cover


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

    if nargs == 0:
        return _find_above(frame=frame + 2)

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


class Giver:
    """Giver of key/value pairs.

    ``Giver`` is the class of the ``give`` object.

    Arguments:
        queue:
            The Queue into which to put given elements.
        special:
            List of special keys to give, mapped to functions.
        inherited:
            A ContextVar to use for inherited key/value pairs to give,
            as set by ``with self.inherit(key=value): ...``.
    """

    def __init__(
        self,
        queue,
        *,
        special={},
        inherited=global_inherited,
    ):
        self.queue = queue
        self.special = special
        self.inherited = inherited

    def copy(
        self,
        special=None,
        inherited=None,
    ):
        """Copy this Giver with modified parameters."""
        return type(self)(
            queue=self.queue,
            special=self.special if special is None else special,
            inherited=self.inherited if inherited is None else inherited,
        )

    @property
    def line(self):
        """Return a giver that gives the line where it is called."""
        return self.copy(special={**self.special, "$line": _special_line})

    @property
    def timestamp(self):
        """Return a giver that gives the time where it is called."""
        return self.copy(special={**self.special, "$timestamp": _special_timestamp})

    @property
    def frame(self):
        """Return a giver that gives the frame where it is called."""
        return self.copy(special={**self.special, "$frame": _special_frame})

    @contextmanager
    def inherit(self, **keys):
        """Create a context manager within which extra values are given.

        .. code-block:: python

            with give.inherit(a=1):
                give(b=2)   # gives {"a": 1, "b": 2}

        Arguments:
            keys: The key/value pairs to give within the block.
        """
        inh = self.inherited.get()
        token = self.inherited.set({**inh, **keys})
        try:
            yield
        finally:
            self.inherited.reset(token)

    def produce(self, values):
        """Give the values dictionary."""
        for special, fn in self.special.items():
            values[special] = fn()

        inh = self.inherited.get()
        if inh is not None:
            values = {**inh, **values}

        self.queue.put_nowait(values)

    def __call__(self, *args, **values):
        """Give the args and values."""
        if args:
            values = {**resolve(1, self, args), **values}
        elif not values:
            values = resolve(1, self, ())

        self.produce(values)

        if len(args) == 1:
            return args[0]
        else:
            return None
