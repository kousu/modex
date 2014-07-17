"""
useful utilities.

TODO: put this module somewhere more generic, like com.sig.modex or SIG.modelling
"""

from warnings import warn

__all__ = ["memoize"]

def memoize(f):
    """
    a decorators that memoizes (ie caches) the results from f.
    f will only be called once for each combination of arguments.
    
    arguments to f must be hashable.
     So, memoize() does not support keyword arguments.
    """
    cache = {}
    def f2(*args, **kwargs):
        if kwargs:
            warn("memoize() does not support keyword arguments")
            return f(*args, **kwargs)
        else:
            # we can cache on args, so long as they are simple args,
            # because the *args syntax gives tuples which are hashable
            # if all their components are hashable
            if args not in cache: cache[args] = f(*args) 
            return cache[args]
    return f2 
