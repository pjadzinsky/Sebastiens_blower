#!/usr/bin/python

from controller import *
from time import time

def main():
	print "Trying to exectue calibration.main()"
	SETTING = 0
	V_GUESS = 1.5

	d = Control(K, DT)
	d.set(SETTING)
	d.run(device, init=V_GUESS)

	flows = [0, 20, 35, 50, 65, 80, 100, 120, 140, 170, 200, 230, 260, 290, 320]

	cal_name = 'CALIBRATION_LOG_{0}'.format(int(time())) 
	for setting in flows:
		print time(), "calibrating airflow %d" % setting
		d.set(setting)
		with open(cal_name, 'a', 0) as logfile:
			logfile.write("%f %f\n" % (time(), setting))
		sleep(60*20)

	''' Finishing '''
	with open(cal_name, 'a', 0) as logfile:
		logfile.write("{0} \n".format(time()))

if __name__=='__main__':
	main()
