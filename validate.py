#!/usr/bin/python

import time
from controller import *

def main():
	print "Trying to execute validation.main()"
	SETTING = 0
	V_GUESS = 1.5


	d = Control(K, DT)
	d.set(SETTING)
	d.run(device, init=V_GUESS)


	from time import *


	flows = [156, 312, 0, 234, 78, 234, 0, 312, 78, 156]
	
	log_name = 'VALIDATION_LOG_{0}'.format(int(time()))
	for setting in flows:
		print time(), "testing airflow %d" % setting
		d.set(setting)
		with open(log_name, 'a', 0) as logfile:
			logfile.write("%f %f\n" % (time(), setting))
		sleep(30*60)

	with open(log_name, 'a', 0) as logfile:
		logfile.write("{0} \n".format(time()))

if __name__=='__main__':
	main()
