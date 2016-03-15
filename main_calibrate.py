from common.utils import utime
from time import sleep

FLOWS = [320, 290, 260, 230, 200, 170, 140, 120, 100, 80, 65, 50, 35, 20, 0]
TIMESTEP = 20 * utime.ONEMINUTE

print '''
-------------------------
-- Airflow calibration --
-------------------------

For support, email sebastien@mousera.com

Pre-calibration checklist:
  [1] Blower exhaust (side of sensor assembly) is connected to rack supply:
        for RAT racks, BACK side
        for MOUSE racks, FRONT side
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


from blower import blower
try:
    blower.start(logfile='LOG_BLOWER_DATA_R%d_%d' % (rack_id, t_start))
    with open('LOG_BLOWER_SETTINGS_R%d_%d' % (rack_id, t_start), 'a+') as log:
        for i, flow in enumerate(FLOWS):
            print "\tStep %d of %d: %f L/min" % (i+1, len(FLOWS), flow)
            t = utime.now()
            blower.set(flow)
            log.write('%f %f\n' % (t, flow))
            sleep(TIMESTEP)
finally:
    blower.stop()


print '''
Airflow calibration of RACK %d complete.
''' % (rack_id,)



