""" Python with-statement contexts 

For more info: https://docs.python.org/2/library/contextlib.html
"""
import contextlib

@contextlib.contextmanager
def ensure_cleanup(obj):
    """A context manager for ensuring cleanup with-block. 
    
    NOTE: obj must have a cleanup method
    """
    obj = obj
    try:
        yield obj
    finally:
        obj.cleanup()
