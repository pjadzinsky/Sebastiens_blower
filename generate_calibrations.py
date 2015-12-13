#!/usr/bin/python
''' Code to run Airflow calibration 
It relies on two files VALIDATION_LOG_# and BLOWER_DATA to exist in 
~/data/Airflow/raw/Rack #/ (where # is for example 19)

'''

import sys
import os

from itertools import *
from common.metrics.metric_client import MetricClient
from sdk import mus
from numpy import *
from scipy.optimize import *
import matplotlib.pyplot as plt
import pandas as pd
import glob

def main(rack):
    ''' rack: mus.Rack object '''
    os.environ['APP_ENV'] = 'production'
    os.environ['MOUSERA_API_TOKEN'] = '586e0fc8f9d0df903146d6784718eb18babfac0b'
    mc = MetricClient()

    direction = 'in'
    METRICS = ['airflow.heated_temperature.%s' % direction, 'airflow.heat.%s' % direction]

    intervals = load_rack_conditions(rack)

    print intervals

    BYPASS_CACHE = False

    start_time = intervals[0][0]

    front_macs = get_front_macs(rack, start_time)

    data = {}
    data_check = {}
    for mac in front_macs:
        print 'getting data for', mac
        ds = get_metrics(mac, METRICS, intervals[0][0], intervals[-1][0]+60*20)

        [h_temp_data, heat_data] = [[d for d in ds if d['metric_name'] == m] for m in METRICS]

        h_temps = sorted([(t/1000., x) for (t, x) in h_temp_data[0]['values']])
        heats   = sorted([(t/1000., x) for (t, x) in heat_data[0]['values']])
        data[mac] = (heats, h_temps)
        data_check[mac] = (len(heats), len(h_temps))

    print data_check

    timeconsts = {}
    for mac in front_macs:
        timeconsts[mac] = []
        for interval in intervals:
            (_, airflow) = interval
            decay = get_decay(mac, interval)
            timeconst = process_decay(decay)
            timeconsts[mac].append( (timeconst, airflow) )

    print timeconsts

    calibrations = {}
    for mac in macs:
        calibrations[mac] = process_timeconsts(timeconsts[mac])

    print calibrations


def load_rack_conditions(rack):
    ''' This function is automating what was originaly hardcoded in intervals
    with the addition that instead of using the nominal flow
    it uses the average recorded flow in the interval 
    
    Output:
        intervals   (list):
            list of tuples of the form (time, flow_rate)
            
    1. VALIDATION_LOG has one line per condition, the first 'delay'
    seconds after a condition starts are discarded in computing the 
    average. The condition ends when the next one (next row) starts
    '''
    intervals = []
    path = os.path.expanduser('~/data/Airflow/raw/Rack {0}/'.format(rack.id()))

    blower_data = pd.read_csv(os.path.join(path, 'BLOWER_DATA'), names=[
        'time_sec', 'voltage', 'flow'], sep=' ')

    val_file = glob.glob(os.path.join(path, 'VALIDATION_LOG_*'))[0]

    conditions = pd.read_csv(os.path.join(path, val_file), names=[
        'time_sec', 'nominal_flow'], sep=' ')

    # for each condition, drop about 1 minute of data after condition
    # starts and then average blower_data['flow']

    delay = 60  # in seconds
    for c in range(conditions.shape[0]-1):
        print conditions.loc[c, 'time_sec']
        print type(conditions.loc[c, 'time_sec'])

        start = conditions.loc[c, 'time_sec']+delay
        end = conditions.loc[c+1, 'time_sec']

        mean_blower = blower_data[(blower_data['time_sec']>start) &
                                  (blower_data['time_sec']<end)]['flow'].mean()

        intervals.append( (start, mean_blower) )

    return intervals

def get_front_macs(rack, t0):
    slots = rack.slots()
    slots = [s for s in slots if s.as_str()[0]!='Z'] # some rack slots are not related to cages, they start with 'Z'
    slabs = [s.slab_at_time(t0) for s in slots]
    macs = [s.mac_front() for s in slabs]
    return macs
    """
    for slot in rack.slots():
        for device in slot.current_devices():
            if device.is_gen2_slab_front():
                slabs.append(device)
    return map(lambda s: s.get_mac_address(), slabs)
    """

def get_metrics(mac, *args):
    cachename = 'cache/%s' % mac
    try:
        if BYPASS_CACHE:
            raise Exception ()
        return eval(file(cachename).read())
    except:
        print 'not cached; retrieving'
        result = mc.get_metrics(mac, *args)
        with open(cachename, 'w+') as cache:
            cache.write(result.__repr__())
        return result


def get_decay(mac, interval):
    (heat_data, h_temp_data) = data[mac]
    (time, _) = interval
    # use last event more than 1 decay length (+ 1s margin) from end of airflow interval
    """ why 18 in next line? is it 18 minutes? """
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



def airflow_model(l, a, b, c):
    return a*log(1 + b*exp(l*1e2)) - c

def process_timeconsts(points):
    (consts, airflows) = array(points).T
    popt, pcov = curve_fit(airflow_model, consts, airflows, maxfev = 100000)
    return list(popt)

if __name__=='__main__':
    if len(sys.argv) != 2:
        raise ValueError("""
        parameter rack number is needed
        Try:
            ./generate_calibrations 39
        """)


    rack = sys.argv[1]
    try:
        rack = int(rack)
    except:
        raise ValueError("rack has to be an integer")

    print rack, type(rack)
    rack = mus.Rack(rack)
    print rack, type(rack)
    main(rack)
