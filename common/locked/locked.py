from threading import Lock

class Locked:

    def __init__(self, x):
        self._lock     = Lock()
        self._contents = x

    def __enter__(self):
        self._lock.acquire()
        return self._contents

    def __exit__(self, type, value, trace):
        self._lock.release()

    def __iter__(self):
        with self as x:
            yield x

    def update(self, x):
        with self as _:
            self._contents = x