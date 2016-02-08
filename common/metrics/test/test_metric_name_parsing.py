#!/usr/bin/python
import unittest
from common.metrics import metric_name_parsing


class MetricNameParsingTestCase (unittest.TestCase):
    def test_aggregate_parsing_no_aggregate(self):
        #not aggregated
        self.assertEqual(metric_name_parsing.parse_aggregation('jim.bob.hello'), ('jim.bob.hello', None, None, None))
        #invalid aggregation type - should be considered unaggregated
        self.assertEqual(metric_name_parsing.parse_aggregation('jim.bob.hello.600s.shmount'), ('jim.bob.hello.600s.shmount', None, None, None))
        #invalid time period - should be considered unaggregated
        self.assertEqual(metric_name_parsing.parse_aggregation('jim.bob.hello.600st.count'), ('jim.bob.hello.600st.count', None, None, None))


    def test_aggregate_parsing_with_aggregate(self):
        self.assertEqual(metric_name_parsing.parse_aggregation('jim.bob.hello.600s.count'), ('jim.bob.hello', '.600s.count', 'count', 600))
        self.assertEqual(metric_name_parsing.parse_aggregation('jim.bob.hello.600s.min'), ('jim.bob.hello', '.600s.min', 'min', 600))
        self.assertEqual(metric_name_parsing.parse_aggregation('jim.bob.hello.600s.max'), ('jim.bob.hello', '.600s.max', 'max', 600))
        self.assertEqual(metric_name_parsing.parse_aggregation('jim.bob.hello.600s.mean'), ('jim.bob.hello', '.600s.mean', 'mean', 600))
        self.assertEqual(metric_name_parsing.parse_aggregation('jim.bob.hello.600s.sd'), ('jim.bob.hello', '.600s.sd', 'sd', 600))
