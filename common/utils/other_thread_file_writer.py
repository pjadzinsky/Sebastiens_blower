"""This is a simple class to wrap file writing (and renaming) so that it
always runs on another thread.
"""

# TODO exceptions, most notably from os.rename().  logging at least?


import sys
import os
from Queue import Queue
from threading import Thread

CLOSE_FILE_JITTER=4

class OtherThreadFileWriter(object):
    def __init__(self, filename, mode):
        self._queue = Queue()
        self._filename = filename
        self._mode = mode
        self._thread = Thread(target=self.worker)
        self._thread.daemon = True
        self._thread.start()

    def worker(self):
        f = open(self._filename, self._mode)
        done = False
        while not done:
            item = self._queue.get()
            if item[0] == 'write':
                f.write(item[1])
            if item[0] == 'close':
                # time.sleep(random.random() * CLOSE_FILE_JITTER)
                f.close()
            if item[0] == 'delete':
                os.remove(self._filename)
            if item[0] == 'rename':
                os.rename(self._filename, item[1])
            if item[0] == 'flush':
                f.flush()
            if item[0] == 'finish':
                done = True
            self._queue.task_done()

    # TODO @methoddecorator that puts member fn on queue, with args,
    # kwargs, rather than using strings and writing dispatch code for
    # each method.
    #
    # I imagine it probably already exists.
    # cf. evidently pebble https://pypi.python.org/pypi/Pebble/1.1.0
    def write(self, data):
        self._queue.put(('write', data))

    def close(self):
        self._queue.put(('close',))

    def delete(self):
        self._queue.put(('delete',))

    def flush(self):
        self._queue.put(('flush',))

    def rename(self, new_filename):
        self._queue.put(('rename', new_filename))

    def finish(self):
        self._queue.put(('finish',))

    def join(self):
        self._thread.join()
