from motor import set_motor_voltage
from sensors import read_airflow
from controller import Controller
from math import *


L = 0.015
K = lambda t: 1e-2 * exp(-L * t) + 1e-4
DT = 0.1


def device(control):
    voltage = set_motor_voltage(control)
    airflow = read_airflow()
    return voltage, airflow


blower = Controller(device, K, DT, init_ctrl=1.5)


