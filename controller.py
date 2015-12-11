
MINV = 1.5
MAXV = 3.3


L = 0.015

from math import exp
def K(t):
	return 1e-2 * exp(-L * t) + 1e-4

AF = 120
DT = 0.1



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
        # Todo: CRC checksum with c
        # Polynomial in C source
        a = (float(d) - 32000.) / 140.
        return a





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
        break
    except:
        print "Error initializing AirflowSensor: retrying in 1"
        sleep(1)
        pass





def read_airflow():
    return sum([sensor.read_airflow() for sensor in sensors])







import u3
import sys



daq = u3.U3()
daq.getCalibrationData()



def set_motor_voltage(v):
    v = max(MINV, min(v, MAXV))
    b = daq.voltageToDACBits(v, dacNumber=1, is16Bits=True)
    daq.getFeedback(u3.DAC1_16(b))
    return v







class PID():
    def __init__(self, setpoint, Kp, Ki, Kd):
        self.setpoint = setpoint
        self.Kp = Kp
        self.Ki = Ki
        self.Ke = Kd
        self.previous_error = 0
        self.integral = 0
    def step(self, measured_value, dt):
        error = setpoint - measured_value
        self.integral = self.integral + error*dt
        derivative = (error - previous_error)/dt
        output = Kp*error + Ki*integral + Kd*derivative
        self.previous_error = error
        return output
    def set(self, setpoint):
        self.setpoint = setpoint
        # freeze integral term for a few seconds?



class P():
    def __init__(self, setpoint, Kp):
        self.setpoint = setpoint
        self.Kp = Kp
    def step(self, measured_value, dt):
        error = setpoint - measured_value
        output = Kp*error
        return output
    def set(self, setpoint):
        self.setpoint = setpoint
        # freeze integral term for a few seconds?



from time import time
import thread

class Control():
	def __init__(self, k, dt):
		self.k = k
		self.dt = dt
		self.set(0)
		self.running = False
	def set(self, setpoint):
		self.setpoint = setpoint
		self.t0 = time()
	def run(self, device, init=0):
                self.control = init
		def loop():
			with open('BLOWER_DATA', 'a', 0) as logfile:
				while self.running:
					t1 = time()
					control, measurement = device(self.control)
					self.control = control - self.k(t1 - self.t0) * (measurement - self.setpoint) * self.dt
					logfile.write("%f %f %f\n" % (t1, control, measurement))
					t2 = time()
					sleep(max(0, self.dt - (t2 - t1)))
		self.running = True
		thread.start_new_thread(loop, ())
	def stop(self):
		self.running = False




def device(control):
	voltage = set_motor_voltage(control)
	airflow = read_airflow()
	return voltage, airflow




# control = Control(K, DT)
# control.set(AF)
# control.run(device, init=1.5)


















