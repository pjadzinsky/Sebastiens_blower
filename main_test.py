
from blower import blower
from common.utils import utime


start_time = utime.now()


with open('LOG_BLOWER_SETTINGS_%d' % start_time, 'a+') as log:
    blower.start(logfile='LOG_BLOWER_DATA_%d' % start_time)
    while True:
        try:
            print "Setting: "
            t, setting = utime.now(), float(raw_input())
            print 'time: %f  \t  setting: %f' % (t, setting)
            log.write('%f %f\n' % (t, setting))
            blower.set(setting)
        except Exception as e:
            print e
            continue


