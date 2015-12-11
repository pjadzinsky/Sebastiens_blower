#!/usr/bin/python
import calibrate
import validate
import shutoff

from time import sleep

if __name__=='__main__':
	calibrate.main()
	sleep(60*10)
	validate.main()
	sleep(60*10)
	shutoff.main()
