
import u3
import sys


MINV = 1.5
MAXV = 3.3


daq = u3.U3()
daq.getCalibrationData()


def set_motor_voltage(v):
    v = max(MINV, min(v, MAXV))
    b = daq.voltageToDACBits(v, dacNumber=1, is16Bits=True)
    daq.getFeedback(u3.DAC1_16(b))
    return v


