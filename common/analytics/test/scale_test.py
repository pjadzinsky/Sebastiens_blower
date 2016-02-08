#!/usr/bin/python

from common.utils import utime
from sdk import mus
import numpy as np
from common.analytics import scale
import sys
from common import log
import unittest


class TestScale(unittest.TestCase):
    
    def setUp(self):
        self.t0 = 1446019200.0 + utime.ONEDAY*3
        self.t1 = self.t0 + utime.ONEDAY*20
        self.subject = 551
        self.delta = 4 * utime.ONEHOUR
        self.time_intervals = [utime.Interval(t, t+self.delta) for t in
                          np.arange(self.t0, self.t1, self.delta)]
        self.error_on_prior=0.2
        
        return
    
    def test_raw_weights(self):
        tis = self.time_intervals
        subject = self.subject
        raw = scale.raw_weights(subject, tis[0])
        print raw
        
        self.assertEqual(raw.shape, (4,3))

        return
 
    def test_automated_weights(self):
        tis = self.time_intervals[:3]
        subject = self.subject
        error_on_prior = self.error_on_prior
        weights = scale.automated_weights(subject, tis, error_on_prior)
        
        self.assertEqual(weights.shape, (3,2))
        self.assertAlmostEqual(weights.iloc[0]['automatic'], 28.529844, 4)
        print weights
        
if __name__ == "__main__":
    FLAGS, argv = log.init_flags(sys.argv)
    unittest.main(argv=argv)
    
    
    
    
