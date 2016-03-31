from common.utils import utime
from time import sleep

import boto

FLOWS = [320, 290, 260, 230, 200, 170, 140, 120, 100, 80, 65, 50, 35, 20, 0]
S3_BUCKET_NAME = 'mousera-us-west-2-production-calibration'
S3_PREFIX = 'blower-logs/'

print '''
-------------------------
-- Airflow calibration --
-------------------------

For support, email sebastien@mousera.com

Pre-calibration checklist:
  [1] Blower exhaust (side of sensor assembly) is connected to rack supply
  [2] Blower has power and switch is set to ON
  [3] Ethernet is plugged in

Enter the RACK NUMBER and press Enter:'''

try:
    rack_id = int(raw_input())
except ValueError:
    print "\nInvalid rack number. Exiting.\n"
    exit()

print '''
STARTING airflow calibration for RACK %d.
Current time is
\t%s.
Estimated completion time is
\t%s.

10 seconds to cancel (Ctrl+C)...''' % (rack_id, utime.to_string(utime.now(), 'America/Los_Angeles'), utime.to_string(utime.now() + len(FLOWS) * 600., 'America/Los_Angeles'))

sleep(1)

print "Proceeding.\n"


#
# TODO: test internet connection and blower
#


# Airflow measurements occur at _:_5 wall clock time (+/- 15 seconds)
# So change airflows every 10 minutes at _:_7 + 20 seconds
t_offset = 300. + 120. + 20.  # measurement time offset + measurement duration + margin
SIM_T = 62.
def t_wait():
    global SIM_T
    wait = 600. - (SIM_T - t_offset) % 600.
    print "simulated time is", SIM_T
    print "waiting", wait, "secs"
    SIM_T += wait
    return 1

print '''
Synchronizing with rack...
'''
sleep(t_wait())

t_start = utime.now()
bdata_fname = 'LOG_BLOWER_DATA_R%d_%d' % (rack_id, t_start)
bsettings_fname = 'LOG_BLOWER_SETTINGS_R%d_%d' % (rack_id, t_start)

from blower import blower
try:
    blower.start(logfile=bdata_fname)
    with open(bsettings_fname, 'a+') as log:
        for i, flow in enumerate(FLOWS):
            print "\tStep %d of %d: %f L/min" % (i+1, len(FLOWS), flow)
            t = utime.now()
            blower.set(flow)
            log.write('%f %f\n' % (t, flow))
            sleep(t_wait() + (6. if i==0 else 0.))
finally:
    blower.stop()

print '''
Uploading blower data...
'''
try:
    s3_bucket = boto.connect_s3().get_bucket(S3_BUCKET_NAME)
    with open(bdata_fname, 'r') as bdata:
        s3_bucket.new_key(S3_PREFIX + bdata_fname).set_contents_from_string(bdata.read())
    with open(bsettings_fname, 'r') as bsettings:
        s3_bucket.new_key(S3_PREFIX + bsettings_fname).set_contents_from_string(bsettings.read())
except:
    pass

print '''
Airflow calibration of RACK %d complete.
''' % (rack_id,)



