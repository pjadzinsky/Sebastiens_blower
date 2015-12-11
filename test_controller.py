from controller import *

d = Control(K, DT)
d.set(300)
d.run(device, init=1.5)

while True:
	a = float(raw_input())
	d.set(a)
