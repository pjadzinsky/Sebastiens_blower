# Decorators for profiling
"""
Example usage:

@time_function()
def my_slow_function():
    time.sleep(100)
    return True

Useful reading on decorators: http://thecodeship.com/patterns/guide-to-python-function-decorators/
Reading: https://hg.python.org/cpython/file/3.4/Lib/functools.py

"""

import time

def time_function(f):
    def time_the_function(*args, **kwds):
        t = time.time()
        r = f(*args, **kwds)
        print "[PROFILING] TIME TAKEN [%s] %s" % (time.time()-t,f.__name__)
        return r
    return time_the_function