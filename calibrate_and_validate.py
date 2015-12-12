#!/usr/bin/python
import calibrate
import validate
import validate_for_plot
import shutoff

from time import sleep

if __name__=='__main__':
	validate.main()
	validate_for_plot.main()
