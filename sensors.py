


# I2C multiplexer

from SDL_Pi_TCA9545 import *

tca9545 = SDL_Pi_TCA9545(twi=1)

def switch_i2c_bus(n):
    if n == 0:
        flag = TCA9545_CONFIG_BUS0
    elif n == 1:
        flag = TCA9545_CONFIG_BUS1
    elif n == 2:
        flag = TCA9545_CONFIG_BUS2
    elif n == 3:
        flag = TCA9545_CONFIG_BUS3
    else:
        raise Exception("switch_i2c_bus: invalid bus %d" % n)
    tca9545.write_control_register(flag)



# SFM3000 driver

from fcntl import ioctl
from struct import unpack
from smbus import SMBus

class AirflowSensor():
    SFM3000_ADDRESS = 0x40
    def __init__(self, bus_number):
        # Set I2C multiplexer to our channel
        self.bus_number = bus_number
        switch_i2c_bus(self.bus_number)
        # Command device to start measuring flow
        SMBus(1).write_byte_data(self.SFM3000_ADDRESS, 16, 0)
        # Set up IO handles for reading (SMBus too restricted)
        self.dev_i2c = file("/dev/i2c-1", "rb", buffering=0)
        ioctl(self.dev_i2c, 0x0703, self.SFM3000_ADDRESS)
        # First reading is invalid
        self.dev_i2c.read(3)
    def read_airflow(self):
        switch_i2c_bus(self.bus_number)
        d0, d1, c = unpack('BBB', self.dev_i2c.read(3))
        d = d0 << 8 | d1
        # TODO: CRC checksum with c
        # Polynomial in C source
        a = (float(d) - 32000.) / 140.
        return a



# Setup

from time import sleep

while True:
    try:
        sensor0 = AirflowSensor(0)
        sleep(0.1)
        sensor1 = AirflowSensor(1)
        sleep(0.1)
        sensor2 = AirflowSensor(2)
        sleep(0.1)
        sensors = [sensor0, sensor1, sensor2]
        print "Airflow sensors initialized"
        break
    except:
        print "Error initializing AirflowSensor: retrying in 1"
        sleep(1)
        pass

def read_airflow():
    return sum([sensor.read_airflow() for sensor in sensors])


