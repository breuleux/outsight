import functools
import inspect
from itertools import count

PON = getattr(inspect.Parameter, "POSITIONAL_ONLY", None)


def keyword_decorator(deco):
    """Wrap a decorator to optionally takes keyword arguments."""

    @functools.wraps(deco)
    def new_deco(fn=None, **kwargs):
        if fn is None:

            @functools.wraps(deco)
            def newer_deco(fn):
                return deco(fn, **kwargs)

            return newer_deco
        else:
            return deco(fn, **kwargs)

    return new_deco


def lax_function(fn):
    """Add a ``**kwargs`` argument to fn if it does not have one.

    ``fn`` is not modified. If it has a varkw argument, ``fn`` is returned directly,
    otherwise a new function is made that takes varkw and does nothing with them (it
    just drops them)

    Example:

        .. code-block:: python

            @lax_function
            def f(x):
                return x * x

            f(x=4, y=123, z="blah")  # works, returns 16. y and z are ignored.
    """
    # NOTE: The purpose of ``lax_function`` is to simplify ``kmap``, ``kfilter``, ``ksubscribe``
    # and other mappers that can be partially applied to the element dictionaries in an
    # observable stream.

    idx = count()

    def gensym():
        return f"___G{next(idx)}"

    sig = inspect.signature(fn)
    last_kind = None
    glb = {}
    argdef = []
    argcall = []
    at_kwonly = False
    for name, parameter in sig.parameters.items():
        kind = parameter.kind
        if kind is inspect.Parameter.VAR_KEYWORD:
            return fn

        if last_kind is PON and kind is not PON:
            argdef.append("/")

        if kind is inspect.Parameter.VAR_POSITIONAL:
            name = f"*{name}"
            at_kwonly = True

        if kind is inspect.Parameter.KEYWORD_ONLY:
            if not at_kwonly:
                argdef.append("*")
            argcall.append(f"{name}={name}")
        else:
            argcall.append(name)

        if parameter.default is not parameter.empty:
            sym = gensym()
            glb[sym] = parameter.default
            name += f"={sym}"

        argdef.append(name)
        last_kind = kind

    if last_kind is PON:
        argdef.append("/")

    argdef.append(f"**{gensym()}")

    fnsym = gensym()
    glb[fnsym] = fn
    wrapsym = gensym()

    argdef = ",".join(argdef)
    argcall = ",".join(argcall)
    yf = "yield from " if inspect.isgeneratorfunction(fn) else ""
    exec(f"def {wrapsym}({argdef}): return ({yf}{fnsym}({argcall}))", glb)

    return functools.wraps(fn)(glb[wrapsym])
