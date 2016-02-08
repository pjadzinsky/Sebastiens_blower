import sys
import json
import unittest
import time
import mock
import requests
import common.settings
from common.utils import utime
from common.metrics.parallel_metrics import ParallelMetrics

# Written by ZBS 1 Mar 2016

@unittest.skipIf(hasattr(common.settings, 'CIRCLE_CI'), 
    'SKIPPIG: Circle CI lacks AWS credentials to run this test')
class ParallelMetricTestCase (unittest.TestCase):
    def test_parallel_get(self):

        # REQUEST first: This used to lock it up before I separated the parallel worker from the parent
        response = requests.get( 'http://google.com' )

        pm = ParallelMetrics()
        metric_names = [ 'light.lux', 'light.visible', 'humidity.in', 'humidity.out' ]
        for i in metric_names:
            pm.get_metrics_cached(
                'b827eb574320', [i], utime.from_string('2016-02-22T00:00:00+0'), utime.from_string('2016-02-22T00:20:00+0')
            )
        results = pm.join(override_cache=True,pool_size=16)
        for i in results:
            self.assertEqual( i['source_id'], 'b827eb574320' )
            self.assertTrue( i['metric_names'][0] in metric_names )

if __name__== '__main__':
    unittest.main()