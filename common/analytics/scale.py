''' 
Scale utilities
'''


from sdk import mus as _mus
from common.utils import utime
from common import log as _log
import pandas as _pd
import numpy as _np
import analytics.scale.weight_processing as _wp
import pandas as _pd
from sdk import mus

class Scale(object):
    def __init__(self, cage_id):
        self.cage_id = cage_id
        self.cage = mus.Cage(cage_id)
            

        
    def estimated_deviations(self, start_time, end_time, small_delta, big_delta,
                              error_on_prior=0.2, threshold_N=5):
        ''' Return the difference between automated_weights computed over two 
        different time intervals.
        The first set of intervals is np.arange(start_time, end_time, small_delta)
        and the second is np.arange(start_time, end_time, big_delta)
        
        The rationale behind this is that we can pick small_delta = 4 hours and
        big_delta = 1 day and have the fluctuation in weight at a point in time
        from that day's average.
        '''
        small_tis = [utime.Interval(t, t+small_delta) for t in 
                     _np.arange(start_time, end_time, small_delta)]
        big_tis = [utime.Interval(t, t+big_delta) for t in 
                   _np.arange(start_time, end_time, big_delta)]
        
        small_estimates = self.automated_weights(small_tis,
                                                 error_on_prior=error_on_prior,
                                                 threshold_N=threshold_N)
        big_estimates = self.automated_weights(big_tis,
                                               error_on_prior=error_on_prior,
                                               threshold_N=threshold_N)

        ''' now for each small_estimate, subtract the closest big_estimate '''
        small_v = small_estimates['middle_time'].values
        big_v = big_estimates['middle_time'].values
        
        big_index = [_np.argmin(abs(x-big_v)) for x in small_v]
        

        weight_diff = [small_estimates.loc[i,'automatic'] -
                       big_estimates.loc[big_index[i],'automatic'] for i in
                        range(len(small_v))]
        
        time_diff = [small_estimates.loc[i,'middle_time'] - 
                     big_estimates.loc[big_index[i], 'middle_time'] for i in
                     range(len(small_v))]
        
        df = _pd.DataFrame({'time':time_diff, 'weight':weight_diff})
        return df


#from analytics.scale.weight_processing import from_flag
def test_conditions(days=1):
    ''' short way of creating conditions that work '''
    t0 = 1446019200.0 + utime.ONEDAY*3
    t1 = t0 + utime.ONEDAY*days
    cage_id = 605
    delta = 4 * utime.ONEHOUR
    tis = [utime.Interval(t, t+delta) for t in
                      _np.arange(t0, t1, delta)]

    return cage_id, tis
