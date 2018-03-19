
import warnings
from functools import wraps

def deprecated(func):
    """Mark a function or method as deprecated."""
    # print( "Deprecating ", func )
    @wraps(func)
    def deprecated_func(*args, **kwargs):
        warnings.warn("Deprecated {}.".format(func.__name__),
            category=DeprecationWarning,
            stacklevel=2)
        return func(*args, **kwargs)
    return deprecated_func
