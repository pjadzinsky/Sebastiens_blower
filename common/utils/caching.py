# Decorators for caching

"""
File-caching
later: Redis or other back-ends for caching

TODO(daniel): use a better format for saving if the data is a protocol buffer or numpy array?

Example usage:

@cache_value()
def my_slow_function():
    # Cache the value for the default length of time
    time.sleep(60)
    return 100

@cache_value(600)
def my_slow_function():
    # Time out the cache after 600 seconds.
    time.sleep(60)
    return 100

@cache_value(0)
def my_slow_function():
    # Cache any values, but never read them back.
    time.sleep(60)
    return 100

@cache_value(-1)
def my_slow_function():
    # Allow caching to be specified later by the caller.
    time.sleep(60)
    return 100

my_slow_function(cache_max_age=86400)  # Override the max_age allowed for cache results

# TODO: if cache_value time is less than zero then don't bother saving either. *******************
# The reason we might want this is that we can override this value from different call locations.

Useful reading on decorators: http://thecodeship.com/patterns/guide-to-python-function-decorators/
Reading: https://hg.python.org/cpython/file/3.4/Lib/functools.py

"""

import collections
import functools
import gflags
import hashlib
import json
import os
import pickle
import random
import shutil
import string
import time
from common.log import LOG, INFO


gflags.DEFINE_integer('cache_timeout_sec',
                      60 * 60,
                      'How frequently to refresh any data cached from a previous api call.')

gflags.DEFINE_string('mousera_cache_directory',
                    '/tmp/mousera_cache/',
                    'Which directory contains the local cache files?')

# Not implemented at the moment
gflags.DEFINE_integer('mousera_cache_byte_limit',
                    10*1024*1024,
                    'How many bytes may the mousera cache directory consume?')


# A unique object, which when returned signifies a cache miss
cache_value_not_found = (None, None, None)

def ensure_dir(fname):
    dirname = os.path.dirname(fname)
    try:
        os.makedirs(dirname)
    except:
        pass

def clear_cache(match=None):
    """
    Clear the cache
    If match is not None, then only clear those files matching the string
    """
    path = gflags.FLAGS.mousera_cache_directory
    ensure_dir(path)
    for fname in os.listdir(path):
        if not match or (match in fname):
            fpath = os.path.join(path, fname)
            try:
                os.remove(fpath)
            except:
                LOG(INFO, "Failed to remove cache file %s" % (fpath))


def object_to_dict_graph(obj, classkey=None):
    """ Recusively collapse python objects to nested dictionary. """
    if isinstance(obj, dict):
        data = {}
        for (k, v) in obj.items():
            data[k] = object_to_dict_graph(v, classkey)
        return data
    elif hasattr(obj, "_ast"):
        return object_to_dict_graph(obj._ast())
    elif hasattr(obj, "__iter__"):
        return [object_to_dict_graph(v, classkey) for v in obj]
    elif hasattr(obj, "__dict__"):
        data = dict([(key, object_to_dict_graph(value, classkey)) for key, value in obj.__dict__.iteritems() if not callable(value) ])
        if classkey is not None and hasattr(obj, "__class__"):
            data[classkey] = obj.__class__.__name__
        return data
    else:
        return obj

def signature_to_string(signature):
    # TODO: consider using pickle.dumps here rather than json.dumps
    # or json.dumps(d, sort_keys=True), or hash(str(my_dict)) ?
    #return hashlib.sha224(json.dumps(signature)).hexdigest()
    
    # This hack allows one caching to work for objects without native json
    # serialization by converting to nested dictionary which can be json
    # seraialized 
    clean_args = []
    for arg in signature['args']:
        try:
            json.dumps(arg)
            clean_args.append(arg)
        except TypeError:
            clean_args.append(object_to_dict_graph(arg))
    signature['args'] = clean_args

    clean_kwargs = {}
    for key, arg in signature['kwds'].iteritems():
        try:
            json.dumps(arg)
            clean_kwargs[key] = arg
        except TypeError:
            clean_kwargs[key] = object_to_dict_graph(arg)
    signature['kwds'] = clean_kwargs
    
    return hashlib.sha224(json.dumps(signature, sort_keys=True)).hexdigest()
    

def signature_to_filename(signature):
    tag = ''
    if type(signature) == type({}):
        if 'module_name' in signature:
            tag += signature['module_name'] + '.'
        if 'function_name' in signature:
            tag += signature['function_name'] + '.'
    return gflags.FLAGS.mousera_cache_directory + tag + signature_to_string(signature)


def try_loading(cache_filename, max_age=None, slack=0.9):
    ensure_dir(cache_filename)
    try:
        fname = cache_filename
        if os.path.exists(fname):
            if (max_age is None or (os.path.getctime(fname) >= time.time() - max_age)):
                data = pickle.load(open(fname, 'rb'))
                return data
        if False:
            if os.path.exists(fname):
                LOG(INFO, "FOUND IN CACHE BUT TOO OLD: %s vs %s" % (time.time() - os.path.getctime(fname), max_age))
            else:
                LOG(INFO, "NOT FOUND IN CACHE %s" % (fname))
        return cache_value_not_found
    except Exception as e:
        print e
        print "ERROR: exception in 'try_loading'"
        return cache_value_not_found
    return cache_value_not_found


def try_saving(cache_filename, data):
    # Try saving this data to this signature
    try:
        tmpfname = gflags.FLAGS.mousera_cache_directory + ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
        if not os.path.exists(gflags.FLAGS.mousera_cache_directory):
            os.makedirs(gflags.FLAGS.mousera_cache_directory)
        ensure_dir(tmpfname)
        pickle.dump(data, open(tmpfname, 'wb'), pickle.HIGHEST_PROTOCOL)
        #LOG(INFO, "TMPname = %s" % (tmpfname))
        #LOG(INFO, "Saving to %s" % (cache_filename))
        shutil.move(tmpfname, cache_filename)
        # print "Cached return values to ", sig
        # Don't bother to clean for now. It takes time and space won't be a problem in practice.
        #clean_cache_directory()
    except Exception as e:
        print e
        print "ERROR: exception in try_saving", cache_filename
        return None
    return None


def clean_cache_directory():
    # TODO: **** clean out old files based on age/size
    pass


def cache_value(max_age=None):
    def cache_value_decorator(f, *args, **kwds):
        @functools.wraps(f)
        def wrapper(*args, **kwds):
            cache_max_age = None
            if 'cache_max_age' in kwds:
                cache_max_age = kwds.pop('cache_max_age')
            else:
                cache_max_age = max_age
            if cache_max_age is None:
                cache_max_age = gflags.FLAGS.cache_timeout_sec
            signature = {'function_name':f.__name__, 'module_name':f.__module__, 'args':args, 'kwds':kwds}
            cache_filename = signature_to_filename(signature)  # Note that we do this here because arguments may be modified.
            if cache_max_age > 0:
                return_value = try_loading(cache_filename, cache_max_age)
                if id(return_value) != id(cache_value_not_found):
                    #LOG(INFO, "RETURN CACHE XXXX %s %s" %(f.__name__, cache_max_age))
                    return return_value
            #LOG(INFO, "COMPUTE XXXX %s %s %s" %(f.__name__, cache_max_age, signature))
            return_value = f(*args, **kwds)
            if cache_max_age is None or cache_max_age >= 0:
                try_saving(cache_filename, return_value)
            return return_value
        return wrapper
    return cache_value_decorator


class memoize(object):
    '''Decorator. Caches a function's return value each time it is called.
    If called later with the same arguments, the cached value is returned
    (not reevaluated).
    '''
    # stolen from https://wiki.python.org/moin/PythonDecoratorLibrary#Memoize

    def __init__(self, func):
        self.func = func
        self._cache_ = {}

    def __call__(self, *args):
        if not isinstance(args, collections.Hashable):
            # uncacheable. a list, for instance.
            # better to not cache than blow up.
            return self.func(*args)
        if args in self._cache_:
            return self._cache_[args]
        else:
            value = self.func(*args)
            self._cache_[args] = value
            return value

    def __repr__(self):
        '''Return the function's docstring.'''
        return self.func.__doc__

    def __get__(self, obj, objtype):
        '''Support instance methods.'''
        return functools.partial(self.__call__, obj)
