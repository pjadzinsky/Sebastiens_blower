from common.utils import utime
from time import sleep

import boto

FLOWS = [320, 290, 260, 230, 200, 170, 140, 120, 100, 80, 65, 50, 35, 20, 0]
TIMESTEP = 20 * utime.ONEMINUTE
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

t_start = utime.now()
t_end = t_start + len(FLOWS) * TIMESTEP

print '''
STARTING airflow calibration for RACK %d.
Current time is
\t%s.
Estimated completion time is
\t%s.

10 seconds to cancel (Ctrl+C)...''' % (rack_id, utime.to_string(t_start, 'America/Los_Angeles'), utime.to_string(t_end, 'America/Los_Angeles'))

sleep(10)

print "Proceeding.\n"


#
# TODO: test internet connection and blower
#


# Airflow measurements occur at _:_5 wall clock time (+/- 15 seconds)
t_now = utime.now()
t_delay = 600. - (t_now - 300.) % 600.
t_start = t_now + t_delay
t_end = t_start + len(FLOWS) * TIMESTEP
print '''
Synchronizing with rack (%d minutes)...
''' % (t_delay/utime.ONEMINUTE,)
sleep(t_delay)

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
            sleep(TIMESTEP)
finally:
    blower.stop()

print '''
Uploading blower data...
'''
s3_bucket = boto.connect_s3().get_bucket(S3_BUCKET_NAME)
with open(bdata_fname, 'r') as bdata:
    s3_bucket.new_key(S3_PREFIX + bdata_fname).set_contents_from_string(bdata.read())
with open(bsettings_fname, 'r') as bsettings:
    s3_bucket.new_key(S3_PREFIX + bsettings_fname).set_contents_from_string(bsettings.read())

print '''
Airflow calibration of RACK %d complete.
''' % (rack_id,)



