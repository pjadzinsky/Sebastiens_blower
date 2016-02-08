import datetime


class Timer:
    #Usage:
    # with timer.Timer() as t:
    #     ... do stuff
    # timedelta = t.interval
    def __enter__(self):
        self.start = datetime.datetime.utcnow()
        return self

    def __exit__(self, *args):
        self.end = datetime.datetime.utcnow()
        self.interval = self.end - self.start
