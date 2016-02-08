#!/usr/bin/python
import unittest
import datetime
from common.metrics import metric_time
import pytz


class MetricTimeTestCase (unittest.TestCase):
    def test_time_conversion(self):
        original_time = datetime.datetime.now(tz=pytz.utc)
        original_time_millsecond_accuracy = original_time.replace(microsecond=(original_time.microsecond//1000)*1000)

        #Convert there and back, and compare results (to within millisecond accuracy)
        timestamp = metric_time.get_timestamp(original_time)
        converted_time = metric_time.get_datetime(timestamp)
        self.assertEqual(original_time_millsecond_accuracy, converted_time)
