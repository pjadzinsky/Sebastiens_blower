#!/usr/bin/python
import unittest
import datetime
import uuid

from common.metrics import metric_time
from common.metrics.metric_writer_file_set import MetricWriterFileSet
from mock import Mock, patch
import pytz
import os.path


def get_mock_metric_filename(metric_name, source_id, timestamp):
    filename = "%d.%s" % (timestamp, metric_name)
    return source_id, filename


class MetricWriterFileSetTest (unittest.TestCase):
    @patch ("common.metrics.metric_writer_file_set.metric_file_naming")
    def test_writer(self, file_naming_mock):
        file_naming_mock.SEPARATOR='-'
        root_path = "metric_file_set" + str(uuid.uuid4())[:4]
        num_files = 0
        file_naming_mock.get_metric_filename = Mock(side_effect=get_mock_metric_filename)
        with MetricWriterFileSet(root_path) as file_set:
            now = datetime.datetime.now(pytz.utc)
            dummy_metrics = [['motion', 'SOURCE_A', metric_time.get_timestamp(now)],
                             ['silliness', 'SOURCE_B', metric_time.get_timestamp(now - datetime.timedelta(days=4))],
                             ['hunger', 'SOURCE_A', metric_time.get_timestamp(now - datetime.timedelta(days=4))]]

            for metric in dummy_metrics:
                num_files = 0
                file_set.get_or_create_writer(*metric)
                directory, filename = get_mock_metric_filename(metric[0], metric[1], metric[2])
                found = False
                for root, dirs, files in os.walk(root_path):
                    for file in files:
                        num_files += 1
                        if file.startswith(filename):
                            found = True
                self.assertTrue(found, "MetricWriterFileSet should have created a file at %s" % os.path.join(root_path, directory, filename))
        self.assertEqual(num_files, len(dummy_metrics), "Expecting exactly %d metrics files" % len(dummy_metrics))
        self.assertFalse(os.path.exists(root_path), "MetricWriterFileSet should have deleted root path %s" % root_path)

    @patch ("common.metrics.metric_writer_file_set.metric_file_naming")
    def test_writer(self, file_naming_mock):
        file_naming_mock.SEPARATOR='-'
        root_path = "metric_file_set" + str(uuid.uuid4())[:4]
        num_files = 0
        file_naming_mock.get_metric_filename = Mock(side_effect=get_mock_metric_filename)
        with MetricWriterFileSet(root_path) as file_set:
            file_set.custom_timestamp = "shooby"
            now = datetime.datetime.now(pytz.utc)
            dummy_metrics = [['motion', 'SOURCE_A', metric_time.get_timestamp(now)],
                             ['silliness', 'SOURCE_B', metric_time.get_timestamp(now - datetime.timedelta(days=4))],
                             ['hunger', 'SOURCE_A', metric_time.get_timestamp(now - datetime.timedelta(days=4))]]

            for metric in dummy_metrics:
                num_files = 0
                file_set.get_or_create_writer(*metric)
                directory, filename = get_mock_metric_filename(metric[0], metric[1], metric[2])
                found = False
                for root, dirs, files in os.walk(root_path):
                    for file in files:
                        num_files += 1
                        if file.startswith(filename) and file.endswith(file_set.custom_timestamp+".metrics"):
                            found = True
                self.assertTrue(found, "MetricWriterFileSet should have created a file at %s" % os.path.join(root_path, directory, filename))
        self.assertEqual(num_files, len(dummy_metrics), "Expecting exactly %d metrics files" % len(dummy_metrics))
        self.assertFalse(os.path.exists(root_path), "MetricWriterFileSet should have deleted root path %s" % root_path)

    @patch ("common.metrics.metric_writer_file_set.metric_file_naming")
    def test_get_suffix(self, file_naming_mock):
        file_naming_mock.SEPARATOR='-'
        timestamp = datetime.datetime.utcnow().strftime("%Y.%m.%d.%H.%M.%S.%f")
        self.assertEqual(MetricWriterFileSet.get_creation_timestamp("sdf-dff-fdff-sdf-%s.metrics" % timestamp), timestamp)
        with self.assertRaises(Exception):
            MetricWriterFileSet.get_creation_timestamp("sljd")  # no metric_file_naming.SEPARATOR
        with self.assertRaises(Exception):
            MetricWriterFileSet.get_creation_timestamp("sljd-dkjf")  # doesn't end with .metrics
