
import os
os.environ['APP_ENV'] = 'production'
os.environ['MOUSERA_API_TOKEN'] = '586e0fc8f9d0df903146d6784718eb18babfac0b'


from common.metrics.metric_client import MetricClient

mc = MetricClient()

from sdk.mus import *

from numpy import *
from scipy.optimize import *

from functools import *

import matplotlib.pyplot as plt



def get_front_slabs(rack):
    slabs = []
    for slot in Rack(rack).slots():
        for device in slot.current_devices():
            if device.is_gen2_slab_front():
                slabs.append(device)
    return map(lambda s: s.get_mac_address(), slabs)



##### hard coded inputs for the moment

rack = 19

direction = 'in'

# 20 minutes long

# Rack 20
# intervals = [
#       (1449137700.000000, 20.000000  )
#     , (1449138900.000000, 50.000000  )
#     , (1449140100.000000, 80.000000  )
#     , (1449141340.218417, 140.000000 )
#     , (1449142857.228787, 200.000000 )
#     , (1449144057.228787, 260.000000 )
#     , (1449145257.228787, 320.000000 )
#     , (1449146457.228787, 290.000000 )
#     , (1449147657.228787, 230.000000 )
#     , (1449149354.959997, 170.000000 )
#     , (1449150555.062982, 110.000000 )
#     , (1449151755.108080, 20.000000  )
#     , (1449152955.115361, 50.000000  )
#     , (1449154155.209350, 80.000000  )
#     , (1449155355.235807, 140.000000 )
#     , (1449157720.833818, 0.000000   )
#     ]

# # Rack 18
# intervals = [
#       (1449164424.972506,  0.000000)
#     , (1449165625.070643,  20.000000)
#     , (1449166825.173007,  50.000000)
#     , (1449168025.269731,  80.000000)
#     , (1449169225.324218,  110.000000)
#     , (1449170425.387524,  140.000000)
#     , (1449171625.488542,  170.000000)
#     , (1449172825.574453,  200.000000)
#     , (1449174025.650299,  230.000000)
#     , (1449175225.744459,  260.000000)
#     , (1449176425.754503,  290.000000)
#     , (1449177625.827196,  320.000000)
#     , (1449178825.904410,  290.000000)
#     , (1449180025.984561,  0.000000)
#     , (1449181226.084565,  20.000000)
#     , (1449182426.130673,  50.000000)
#     , (1449183626.224188,  80.000000)
#     , (1449184826.305614,  110.000000)
#     ]

# RACK 19
intervals = [
      (1449191604.452764, 0.000000)
    , (1449192804.519966, 20.000000)
    , (1449194004.620684, 35.000000)
    , (1449195204.636606, 50.000000)
    , (1449196404.703715, 65.000000)
    , (1449197604.798845, 80.000000)
    , (1449198804.856034, 100.000000)
    , (1449200004.906684, 120.000000)
    , (1449201204.962422, 140.000000)
    , (1449202405.063179, 170.000000)
    , (1449203605.106062, 200.000000)
    , (1449204805.116180, 230.000000)
    , (1449206005.171763, 260.000000)
    , (1449207205.226260, 290.000000)
    , (1449208405.286093, 320.000000)
    , (1449209605.366066, 320.000000)
    , (1449210805.425567, 290.000000)
    , (1449212005.526633, 260.000000)
    , (1449213205.606027, 230.000000)
    , (1449214405.698948, 200.000000)
    , (1449215605.798701, 170.000000)
    , (1449216805.877012, 140.000000)
    , (1449218005.928837, 120.000000)
    , (1449219205.930854, 100.000000)
    , (1449220406.031471, 80.000000)
    , (1449221606.107543, 65.000000)
    , (1449222806.143753, 50.000000)
    , (1449224006.191751, 35.000000)
    , (1449225206.233872, 20.000000)
    , (1449226406.293577, 0.000000)
    , (1449227606.394187, 0.000000)
    , (1449228806.396859, 20.000000)
    , (1449230006.496047, 35.000000)
    , (1449231206.553695, 50.000000)
    , (1449232406.606043, 65.000000)
    , (1449233606.627827, 80.000000)
    , (1449234806.705251, 100.000000)
    , (1449236006.798766, 120.000000)
    , (1449237206.890734, 140.000000)
    , (1449238406.948545, 170.000000)
    , (1449239606.996103, 200.000000)
    , (1449240807.002553, 230.000000)
    , (1449242007.062986, 260.000000)
    , (1449243207.113674, 290.000000)
    , (1449244407.153522, 320.000000)
    ]

BYPASS_CACHE = False

#####



get_slabs = partial(get_front_slabs, rack)



slabs = get_slabs()

slabs = slabs

print 'got %d slabs:' % len(slabs)
print slabs



def get_metrics(slab, *args):
    cachename = 'cache/%s' % slab
    try:
        if BYPASS_CACHE:
            raise Exception ()
        return eval(file(cachename).read())
    except:
        print 'not cached; retrieving'
        result = mc.get_metrics(slab, *args)
        with open(cachename, 'w+') as cache:
            cache.write(result.__repr__())
        return result



data = {}
data_check = {}
METRICS = ['airflow.heated_temperature.%s' % direction, 'airflow.heat.%s' % direction]
for slab in slabs:
    print 'getting data for', slab
    ds = get_metrics(slab, METRICS, intervals[0][0], intervals[-1][0]+60*20)

    [h_temp_data, heat_data] = [[d for d in ds if d['metric_name'] == m] for m in METRICS]

    h_temps = sorted([(t/1000., x) for (t, x) in h_temp_data[0]['values']])
    heats   = sorted([(t/1000., x) for (t, x) in heat_data[0]['values']])
    data[slab] = (heats, h_temps)
    data_check[slab] = (len(heats), len(h_temps))

print data_check




from itertools import *
def get_decay(slab, interval):
    (heat_data, h_temp_data) = data[slab]
    (time, _) = interval
    # use last event more than 1 decay length (+ 1s margin) from end of airflow interval
    event = [t for (t,_) in heat_data if t < time + 60*18 - 1][-1]
    decay = list(takewhile( lambda (t,_): t <= event + 60*2
                                        , dropwhile( lambda (t,_): t < event + 5 + 3 # heat + roundoff
                                        , h_temp_data )))
    return decay




def decay_model(t, a, l, k):
    return a * exp(-l * t) + k

def process_decay(decay):
    # (times, temps) = array(decay).T
    # times = times - times[0]
    # popt, pcov = curve_fit(decay_model, times, temps, [2,0.5,25], maxfev=5000)
    # return popt[1]

    d_model = lambda t, a, l, k: a * exp(-l * t) + k

    decay = array(decay)
    (times, temps) = decay.T
    t0 = decay[0][0]

    popt, pcov = curve_fit(d_model, times - [t0], temps, [2,0.5,25], maxfev=5000)
    return popt[1]
    #^ from airflow worker implementation





timeconsts = {}
for slab in slabs:
    timeconsts[slab] = []
    for interval in intervals:
        (_, airflow) = interval
        decay = get_decay(slab, interval)
        timeconst = process_decay(decay)
        timeconsts[slab].append( (timeconst, airflow) )


print timeconsts








def airflow_model(l, a, b, c):
    return a*log(1 + b*exp(l*1e2)) - c

def process_timeconsts(points):
    (consts, airflows) = array(points).T
    popt, pcov = curve_fit(airflow_model, consts, airflows, maxfev = 100000)
    return list(popt)

calibrations = {}
for slab in slabs:
    calibrations[slab] = process_timeconsts(timeconsts[slab])


print calibrations



