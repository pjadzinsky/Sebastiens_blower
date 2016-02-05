
from math import exp
from common.utils import utime
import thread


class Controller():
    """
    Log file format:
        [time] [measured output] [control input]
        ...
    """
    def __init__(self, device, k, dt, init_ctrl=0):
        self.device = device
        self.k = k   # N.B. function of seconds since last setpoint
        self.dt = dt
        self.control = init_ctrl
        self.running = False
        self.set(0)
    def set(self, setpoint):
        self.setpoint = setpoint
        self.t0 = utime.now()
    def start(self, logfile=('LOG_BLOWER_DATA_%d' % utime.now())):
        def loop():
            with open(logfile, 'a+', 0) as logfile:
                while self.running:
                    t1 = utime.now()
                    control, measurement = self.device(self.control)
                    self.control = control - self.k(t1 - self.t0) * (measurement - self.setpoint) * self.dt
                    logfile.write("%f %f %f\n" % (t1, measurement, control))
                    t2 = utime.now()
                    sleep(max(0, self.dt - (t2 - t1)))
        self.running = True
        thread.start_new_thread(loop, ())
    def stop(self):
        self.running = False


