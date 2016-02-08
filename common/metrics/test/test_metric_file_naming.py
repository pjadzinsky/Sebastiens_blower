#!/usr/bin/python
import unittest
import datetime
from common.metrics import metric_file_naming
import dateutil.parser
import os.path
import pytz


class MetricFileNamingTestCase (unittest.TestCase):
    def test_naming_no_aggregate(self):
        category = "category"
        source_id = "MY_SOURCE_ID"
        time = dateutil.parser.parse('2015-01-05 15:53:00Z')
        directory, filename = metric_file_naming.get_metric_filename_ext(source_id, time, category, is_aggregate=False, aggregation_seconds=0)

        (parsed_source_id, parsed_start_time, parsed_category, parsed_is_aggregate, parsed_aggregation_seconds) = metric_file_naming.parse_metric_filename(os.path.join(directory, filename))
        self.assertEquals(parsed_source_id, source_id)
        self.assertEquals(parsed_category, category)
        self.assertEquals(parsed_aggregation_seconds, 600)
        self.assertFalse(parsed_is_aggregate)
        self.assertEquals(parsed_start_time, dateutil.parser.parse('2015-01-05 15:50:00Z'))

    def test_naming_with_aggregate(self):
        metric_name = "silly_metric"
        source_id = "MY_SOURCE_ID"
        time = dateutil.parser.parse('2015-01-05 15:53:00Z')
        aggregation_seconds = 3600
        directory, filename = metric_file_naming.get_metric_filename_ext(source_id, time, metric_name, is_aggregate=True, aggregation_seconds=aggregation_seconds)

        (parsed_source_id, parsed_start_time, parsed_metric_name, parsed_is_aggregate, parsed_aggregation_seconds) = metric_file_naming.parse_metric_filename(os.path.join(directory, filename))
        self.assertEquals(parsed_source_id, source_id)
        self.assertEquals(parsed_metric_name, metric_name)
        self.assertEquals(parsed_aggregation_seconds, aggregation_seconds)
        self.assertTrue(parsed_is_aggregate)
        self.assertEquals(parsed_start_time, dateutil.parser.parse('2015-01-05 15:00:00Z'))

    def test_naming_with_extra_suffixes(self):
        metric_name = "silly_metric"
        source_id = "MY_SOURCE_ID"
        time = dateutil.parser.parse('2015-01-05 15:53:00Z')
        aggregation_seconds = 3600
        directory, filename = metric_file_naming.get_metric_filename_ext(source_id, time, metric_name, is_aggregate=True, aggregation_seconds=aggregation_seconds)
        filename += metric_file_naming.SEPARATOR + "yeehaw-look-at-my-extra-stuff"

        (parsed_source_id, parsed_start_time, parsed_metric_name, parsed_is_aggregate, parsed_aggregation_seconds) = metric_file_naming.parse_metric_filename(os.path.join(directory, filename))
        self.assertEquals(parsed_source_id, source_id)
        self.assertEquals(parsed_metric_name, metric_name)
        self.assertEquals(parsed_aggregation_seconds, aggregation_seconds)
        self.assertTrue(parsed_is_aggregate)
        self.assertEquals(parsed_start_time, dateutil.parser.parse('2015-01-05 15:00:00Z'))


    def test_invalid_source_name(self):
        with self.assertRaises(AssertionError):
            metric_file_naming.get_metric_filename("hello", "invalid-because-separator", 500)

    def test_invalid_metric_name(self):
        with self.assertRaises(AssertionError):
            metric_file_naming.get_metric_filename("hello-there", "oksource", 500)


