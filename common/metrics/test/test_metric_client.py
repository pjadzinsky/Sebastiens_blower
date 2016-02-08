#!/usr/bin/python
import json
import unittest
import datetime
import uuid
from common.metrics import metric_time
from common.metrics import metric_name_parsing
from common.metrics import metric_file_naming
from common.metrics.metric_client import MetricClient, is_close
from common.metrics.metric_file import MetricFile
from common.metrics import metric_data
import common.bucket.bucket
import dateutil.parser
from mock import Mock, patch
import os.path
import pytz
import common.settings
import time
import dateutil.tz
from requests import Response

metrics_read_count = 0

def count_metric_values(key, filename, metadata=None):
    global metrics_read_count
    with open(filename, "r") as file:
        metric_file = MetricFile(file, file)
        while metric_file.get_value():
            metrics_read_count += 1

metrics_from_uploaded_file = []
def read_uploaded_file(key, filename, metadata=None):
    global metrics_from_uploaded_file
    with open(filename, "r") as file:
        metric_file = MetricFile(file, file)
        metric_tuple = metric_file.get_value()
        while metric_tuple:
            metrics_from_uploaded_file.append(metric_tuple)
            metric_tuple = metric_file.get_value()


class MetricClientUnitTestCase (unittest.TestCase):

        def test_float_comparison(self):
            self.assertTrue(is_close(5.44444,
                                     5.444445645645))  #5 significant digits
            self.assertTrue(is_close(5444440000000,
                                     5444445645645))  #5 significant digits
            self.assertFalse(is_close(5445000000000,
                                      5444445645645))  #5 significant digits
            self.assertTrue(is_close(0.000076656775,
                                     0.000076656566))  #5 significant digits
            self.assertFalse(is_close(0.00007665,
                                      0.000076))  #5 significant digits

        #Make sure the client gathers the correct key paths for the given source ids and metrics
        def test_gather_key_prefixes(self):
            client = MetricClient()
            source_id = "SOURCE"
            metric_names = ["metric_a", "motion", "yummy.60s.mean", "weight.3600s.count", 'airflow.temperature.out.21600s.max']
            start_time = dateutil.parser.parse('2015-01-05 15:50:00Z')
            end_time = start_time + datetime.timedelta(minutes = 30)

            #gather key prefixes for metric names above
            key_prefixes = client.gather_key_prefixes(source_id, metric_names, start_time, end_time)

            #make sure it returned the correct stuff
            for metric_name in metric_names:
                unaggregated_metric_name, full_aggregation_suffix, aggregation_type, aggregation_seconds = metric_name_parsing.parse_aggregation(metric_name)
                file_length = metric_file_naming.get_file_length(aggregation_seconds, aggregation_type!=None)
                current_time = metric_file_naming.round_down_to_file_start_time(start_time, file_length)
                while current_time < end_time:
                    reversed_source = source_id[::-1]
                    directory = os.path.join(reversed_source, str(current_time.year), "%02d" % current_time.month, "%02d" % current_time.day)
                    aggregation_start_time = metric_file_naming.round_down_to_file_start_time(current_time, file_length)
                    filename = "%02d-%02d-%s" % (aggregation_start_time.hour, aggregation_start_time.minute, unaggregated_metric_name)
                    if aggregation_type:
                        directory = os.path.join(directory, "aggregate")
                        filename+="-%ds" % file_length
                    filename += '-'
                    prefix = os.path.join(directory, filename)
                    key_prefixes.remove(prefix)
                    current_time += datetime.timedelta(seconds=file_length)
            self.assertEqual(len(key_prefixes), 0, "Unexpected key prefixes: %s" % (str(key_prefixes)))

        @patch('common.bucket.bucket.Bucket') #don't actually try to instantiate a bucket
        @patch('common.metrics.metric_client.S3FileFetcher') #don't actually try to fetch files
        def test_gather_values_from_keys(self, S3FileFetcherMock, BucketMock):
            client = MetricClient()
            start_time = dateutil.parser.parse('2015-01-05 15:50:00Z')
            end_time = start_time + datetime.timedelta(minutes = 30)

            #Create a dummy metrics file to be fetched (same file will be fetched for key_a and key_b)
            dummy_metric_filename = "dummy.metrics"
            start_timestamp = metric_time.get_timestamp(start_time)
            with open(dummy_metric_filename, "w") as dummy_metric_file_handle:
                dummy_metric_file = MetricFile(dummy_metric_file_handle)
                #These should appear in results a total of 4 times since same-timestamp values should overwrite one another
                dummy_metric_file.put_value('metric_a', start_timestamp, 1, a='should_be_ignored')
                dummy_metric_file.put_value('metric_a', start_timestamp, 5, a='should_be_ignored')
                dummy_metric_file.put_value('metric_a', start_timestamp +1, 7, z='should_be_ignored')
                dummy_metric_file.put_value('metric_b', start_timestamp, 2, y='should_be_ignored')
                dummy_metric_file.put_value('metric_b', start_timestamp, 6)
                dummy_metric_file.put_value('metric_b', start_timestamp +1, 8)
                #These two should not appear in results since their timestamp is exactly the end time
                dummy_metric_file.put_value('metric_a', metric_time.get_timestamp(end_time), 3)
                dummy_metric_file.put_value('metric_b', metric_time.get_timestamp(end_time), 4)
            S3FileFetcherMock.return_value.__enter__ = Mock(return_value=dummy_metric_filename)

            results = client.gather_values_from_keys(['key_a', 'key_b'], ['metric_a', 'metric_b'], start_time, end_time,None)
            self.assertEqual(len(results), 2)   #one result for each metric

            results_metric_names = results.keys()
            self.assertTrue('metric_a' in results_metric_names)
            self.assertTrue('metric_b' in results_metric_names)
            for metric_name in results:
                metric_results_by_tag_combo = results[metric_name]
                self.assertTrue(len(metric_results_by_tag_combo), 1)    #one tag combo
                tag_combo_results_by_timestamp = metric_results_by_tag_combo[frozenset({}.items())]
                self.assertEqual(len(tag_combo_results_by_timestamp), 2)    #one for each unique timestamp
                if metric_name == 'metric_a':
                    self.assertEqual(tag_combo_results_by_timestamp[start_timestamp], 5)
                    self.assertEqual(tag_combo_results_by_timestamp[start_timestamp+1], 7)
                else:   #metric_b
                    self.assertEqual(metric_name, 'metric_b')
                    self.assertEqual(tag_combo_results_by_timestamp[start_timestamp], 6)
                    self.assertEqual(tag_combo_results_by_timestamp[start_timestamp+1], 8)

        @patch('common.bucket.bucket.Bucket') #don't actually try to instantiate a bucket
        @patch('common.metrics.metric_client.S3FileFetcher') #don't actually try to fetch files
        def test_gather_values_from_keys_with_filter(self, S3FileFetcherMock, BucketMock):
            client = MetricClient()
            start_time = dateutil.parser.parse('2015-01-05 15:50:00Z')
            end_time = start_time + datetime.timedelta(minutes = 30)

            #Create a dummy metrics file to be fetched (same file will be fetched for key_a and key_b)
            dummy_metric_filename = "dummy.metrics"
            start_timestamp = metric_time.get_timestamp(start_time)
            with open(dummy_metric_filename, "w") as dummy_metric_file_handle:
                dummy_metric_file = MetricFile(dummy_metric_file_handle)
                #These should appear in results a total of 4 times since same-timestamp values should overwrite one another
                dummy_metric_file.put_value('metric_a', start_timestamp, 1, a='getme')
                dummy_metric_file.put_value('metric_a', start_timestamp, 2)
                dummy_metric_file.put_value('metric_a', start_timestamp +1, 7, a='getme', z='yo')
                dummy_metric_file.put_value('metric_a', start_timestamp +1, 9, a="ditchme")
                dummy_metric_file.put_value('metric_a', start_timestamp+2, 5, a='getme')
                dummy_metric_file.put_value('metric_a', start_timestamp+2, 6, a='getme', b="whodat")
                dummy_metric_file.put_value('metric_a', start_timestamp+3, 2, z='yo', b="me")
            S3FileFetcherMock.return_value.__enter__ = Mock(return_value=dummy_metric_filename)

            results = client.gather_values_from_keys(['key_a'], ['metric_a'], start_time, end_time,None, filter_by_tags={'a':'getme'})
            self.assertEqual(len(results), 1)   #only 'metric_a'
            metric_a_results = results.get('metric_a')
            self.assertEqual(len(metric_a_results), 1)  #one tag combo {}
            self.assertEqual(len(metric_a_results[frozenset([])]), 3)
            self.assertEqual(metric_a_results[frozenset([])][start_timestamp], 1)
            self.assertEqual(metric_a_results[frozenset([])][start_timestamp+1], 7)
            self.assertEqual(metric_a_results[frozenset([])][start_timestamp+2], 6)

            results = client.gather_values_from_keys(['key_a'], ['metric_a'], start_time, end_time,['b'], filter_by_tags={'a':'getme'})
            self.assertEqual(len(results), 1)   #only 'metric_a'
            metric_a_results = results.get('metric_a')
            self.assertEqual(len(metric_a_results), 2)  #two tag combo {} and {'b':'whodat'}
            self.assertEqual(len(metric_a_results[frozenset([])]), 3)
            self.assertEqual(metric_a_results[frozenset([])][start_timestamp], 1)
            self.assertEqual(metric_a_results[frozenset([])][start_timestamp+1], 7)
            self.assertEqual(metric_a_results[frozenset([])][start_timestamp+2], 5)
            self.assertEqual(len(metric_a_results[frozenset({'b':'whodat'}.items())]), 1)
            self.assertEqual(metric_a_results[frozenset({'b':'whodat'}.items())][start_timestamp+2], 6)

            results = client.gather_values_from_keys(['key_a'], ['metric_a'], start_time, end_time,['*'], filter_by_tags={'a':'getme'})
            self.assertEqual(len(results), 1)   #only 'metric_a'
            metric_a_results = results.get('metric_a')
            self.assertEqual(len(metric_a_results), 3)  #three tag combo {'a':'getme'}, {'a':'getme', 'z'='yo'} and {'a':'getme', 'b':'whodat'}
            self.assertEqual(len(metric_a_results[frozenset({'a':'getme'}.items())]), 2)
            self.assertEqual(metric_a_results[frozenset({'a':'getme'}.items())][start_timestamp], 1)
            self.assertEqual(metric_a_results[frozenset({'a':'getme'}.items())][start_timestamp+2], 5)
            self.assertEqual(len(metric_a_results[frozenset({'a':'getme', 'b':'whodat'}.items())]), 1)
            self.assertEqual(metric_a_results[frozenset({'a':'getme', 'b':'whodat'}.items())][start_timestamp+2], 6)
            self.assertEqual(len(metric_a_results[frozenset({'a':'getme', 'z':'yo'}.items())]), 1)
            self.assertEqual(metric_a_results[frozenset({'a':'getme', 'z':'yo'}.items())][start_timestamp+1], 7)


        @patch('common.bucket.bucket.Bucket') #don't actually try to instantiate a bucket
        @patch('common.metrics.metric_client.S3FileFetcher') #don't actually try to fetch files
        def test_gather_values_from_keys_with_group_by(self, S3FileFetcherMock, BucketMock):
            client = MetricClient()
            start_time = dateutil.parser.parse('2015-01-05 15:50:00Z')
            end_time = start_time + datetime.timedelta(minutes = 30)

            #Create a dummy metrics file to be fetched (same file will be fetched for key_a and key_b)
            dummy_metric_filename = "dummy.metrics"
            with open(dummy_metric_filename, "w") as dummy_metric_file_handle:
                dummy_metric_file = MetricFile(dummy_metric_file_handle)
                #These should appear in results a total of 4 times since same-timestamp values should overwrite one another
                dummy_metric_file.put_value('metric_a', metric_time.get_timestamp(start_time), 1, random='hey')
                dummy_metric_file.put_value('metric_a', metric_time.get_timestamp(start_time), 1, version='1', important='yes', random='hey')
                dummy_metric_file.put_value('metric_a', metric_time.get_timestamp(start_time), 1, version='1', important='yes', random='hey')
                dummy_metric_file.put_value('metric_a', metric_time.get_timestamp(start_time), 2, version='1', important='yes', fingle='clingle')
                dummy_metric_file.put_value('metric_a', metric_time.get_timestamp(start_time), 3, version='2', important='yes', random='yo')
                dummy_metric_file.put_value('metric_a', metric_time.get_timestamp(start_time), 4, version='2', important='yes', random='herewego')
            S3FileFetcherMock.return_value.__enter__ = Mock(return_value=dummy_metric_filename)

            #group by 'version' and check we have all expected tag combos
            results = client.gather_values_from_keys(['key_a'], ['metric_a'], start_time, end_time,['version'])
            metric_a_results = results['metric_a']
            self.assertEqual(len(metric_a_results), 3)
            tag_combos = set(metric_a_results.keys())
            self.assertEqual(len(tag_combos), len(metric_a_results))    #no duplicate tag combos in results
            self.assertEqual(tag_combos, {frozenset({'version': '1'}.items()),
                                          frozenset({'version': '2'}.items()),
                                          frozenset({}.items())}, )

            #group by 'version' and 'important' and check we have all expected tag combos
            results = client.gather_values_from_keys(['key_a'], ['metric_a'], start_time, end_time,['version', 'important'])
            metric_a_results = results['metric_a']
            self.assertEqual(len(metric_a_results), 3)
            tag_combos = set(metric_a_results.keys())
            self.assertEqual(len(tag_combos), len(metric_a_results))    #no duplicate tag combos in results
            self.assertEqual(tag_combos, {frozenset({'version': '1', 'important':'yes'}.items()),
                                          frozenset({'version': '2', 'important':'yes'}.items()),
                                          frozenset({}.items())}, )

            #group by 'version' and 'random' and check we have all expected tag combos
            results = client.gather_values_from_keys(['key_a'], ['metric_a'], start_time, end_time,['random','version'])
            metric_a_results = results['metric_a']
            self.assertEqual(len(metric_a_results), 5)
            tag_combos = set(metric_a_results.keys())
            self.assertEqual(tag_combos, {frozenset({'version': '1', 'random':'hey'}.items()),
                                          frozenset({'version': '1'}.items()),
                                          frozenset({'version': '2', 'random':'yo'}.items()),
                                          frozenset({'version': '2', 'random':'herewego'}.items()),
                                          frozenset({'random': 'hey'}.items())})

            #group by everything
            results = client.gather_values_from_keys(['key_a'], ['metric_a'], start_time, end_time,['*'])
            metric_a_results = results['metric_a']
            self.assertEqual(len(metric_a_results), 5)
            tag_combos = set(metric_a_results.keys())
            self.assertEqual(tag_combos, {
                                          frozenset({'random': 'hey'}.items()),
                                          frozenset({'version': '1', 'important':'yes', 'random':'hey'}.items()),
                                          frozenset({'version': '1', 'important':'yes', 'fingle':'clingle'}.items()),
                                          frozenset({'version': '2', 'important':'yes', 'random':'yo'}.items()),
                                          frozenset({'version': '2', 'important':'yes', 'random':'herewego'}.items()),
                                         }
                             )

        @patch('common.bucket.bucket.Bucket') #don't actually try to instantiate a bucket
        @patch('common.metrics.metric_client.S3FileFetcher') #don't actually try to fetch files
        def test_gather_all_values_from_keys(self, S3FileFetcherMock, BucketMock):
            client = MetricClient()
            start_time = dateutil.parser.parse('2015-01-05 15:50:00Z')
            end_time = start_time + datetime.timedelta(minutes = 30)

            #Create a dummy metrics file to be fetched
            dummy_metric_filename = "dummy.metrics"
            start_timestamp = metric_time.get_timestamp(start_time)
            with open(dummy_metric_filename, "w") as dummy_metric_file_handle:
                dummy_metric_file = MetricFile(dummy_metric_file_handle)
                #When queried with metric_names = None, both results should contain both metric names (jim and bob)
                dummy_metric_file.put_value('jim', start_timestamp, 1)
                dummy_metric_file.put_value('bob', start_timestamp, 5)
            S3FileFetcherMock.return_value.__enter__ = Mock(return_value=dummy_metric_filename)

            results = client.gather_values_from_keys(['key_a'], None, start_time, end_time,None)
            self.assertEqual(len(results), 2)   #one result for each metric

            results_metric_names = results.keys()
            self.assertTrue('jim' in results_metric_names)
            self.assertTrue('bob' in results_metric_names)
            for metric_name in results:
                results_by_metric_name = results[metric_name]
                self.assertEqual(len(results_by_metric_name), 1)  #1 tag combo per result
                results_by_tag_combo = results_by_metric_name[frozenset({}.items())]
                if metric_name == 'jim':
                    self.assertEqual(results_by_tag_combo[start_timestamp], 1)
                else:   #bob
                    self.assertEqual(metric_name, 'bob')
                    self.assertEqual(results_by_tag_combo[start_timestamp], 5)


        @patch('common.bucket.bucket.Bucket') #don't actually try to instantiate a bucket
        def test_put_values(self, BucketMock):


            BucketMock.return_value.upload_file = Mock(side_effect=count_metric_values)
            now = datetime.datetime.now(tz=pytz.utc)
            an_hour_ago = now - datetime.timedelta(hours=1)

            client = MetricClient()
            client.metrics_bucket = BucketMock.return_value  #static member, so make sure it points at our mock
            test_source_id = 'TEST_ID'
            test_metric_values = [
                     (metric_time.get_timestamp(an_hour_ago), 1),
                     (metric_time.get_timestamp(now), 2)
                 ]
            test_metrics = [
                {metric_data.METRIC_NAME_KEY:'metric_a',
                 metric_data.METRIC_TAGS_KEY:{},
                 metric_data.METRIC_VALUES_KEY:test_metric_values}
            ]
            global metrics_read_count
            metrics_read_count = 0
            client.put_metrics(test_source_id, test_metrics)
            self.assertEquals(len(test_metric_values), metrics_read_count, "Expected client.put_metrics to upload %d values; observed %d" % (len(test_metric_values), metrics_read_count))

        @patch('common.metrics.metric_client.MetricClient.compact') #don't actually compact
        @patch('common.metrics.metric_client.MetricWriterFileSet') #don't actually try to write files
        @patch('common.bucket.bucket.Bucket') #don't actually try to instantiate a bucket
        @patch('common.metrics.metric_client.S3FileFetcher') #don't actually try to fetch files
        def test_gather_values_from_key_prefixes(self, S3FileFetcherMock, BucketMock, MetricWriterFileSetMock, compact_mock):
            client = MetricClient()
            client.metrics_bucket = BucketMock.return_value  #static member, so make sure it points at our mock
            class MockS3Object(object):
                def __init__(self, key):
                    self.key=key

            dummy_metric_filename = "dummy.metrics"
            start_time = dateutil.parser.parse('2015-01-05 15:50:00Z')
            end_time = start_time + datetime.timedelta(minutes = 30)
            start_timestamp = metric_time.get_timestamp(start_time)
            with open(dummy_metric_filename, "w") as dummy_metric_file_handle:
                dummy_metric_file = MetricFile(dummy_metric_file_handle)
                #These should appear in results a total of 4 times since same-timestamp values should overwrite one another
                dummy_metric_file.put_value('metric_a', start_timestamp, 1)
                dummy_metric_file.put_value('metric_a', start_timestamp, 5)
                dummy_metric_file.put_value('metric_a', start_timestamp +1, 8)
            S3FileFetcherMock.return_value.__enter__ = Mock(return_value=dummy_metric_filename)
            BucketMock.return_value.boto_bucket.objects.filter.return_value = [MockS3Object('a-a.metrics'), MockS3Object('b-b.metrics'), MockS3Object('b-c.metrics')]
            client.gather_values_from_key_prefixes('source_id', ['ignored_by_mock'], ['metric_a'], start_time, end_time, ['*'])
            self.assertTrue(compact_mock.called)

        @patch('common.metrics.metric_client.S3FileFetcher') #don't actually try to fetch files
        @patch('common.bucket.bucket.Bucket') #don't actually try operate on a bucket
        def test_compact(self, BucketMock, S3FileFetcherMock):
            client = MetricClient()
            client.metrics_bucket = BucketMock.return_value  #static member, so make sure it points at our mock
            #3 files with a whole bunch of duplicate keys
            source_keys = ['key-timestamp1.metrics', 'key-timestamp2.metrics', 'key-timestamp3.metrics']

            dummy_metric_filename = "dummy.metrics"
            start_time = dateutil.parser.parse('2015-01-05 15:50:00Z')
            start_timestamp = metric_time.get_timestamp(start_time)
            with open(dummy_metric_filename, "w") as dummy_metric_file_handle:
                dummy_metric_file = MetricFile(dummy_metric_file_handle)
                dummy_metric_file.put_value('metric_a', start_timestamp, 1, )
                dummy_metric_file.put_value('metric_a', start_timestamp+1, 5, a='abc')
                dummy_metric_file.put_value('metric_a', start_timestamp+2, 7, z='xyz')
                dummy_metric_file.put_value('metric_a', start_timestamp+2, 8, z='xyz')  #identical time&tags, so should overwrite prevous value of 7
                dummy_metric_file.put_value('metric_a', start_timestamp+2, 8, z='xyz', d='dljf')  #new tag combo, so new entry in compacted results
                dummy_metric_file.put_value('metric_a', start_timestamp+2, 8, y='wxy')  #new tag combo, so new entry in compacted results
            S3FileFetcherMock.return_value.__enter__ = Mock(return_value=dummy_metric_filename)
            BucketMock.return_value.upload_file = Mock(side_effect = read_uploaded_file)

            client.compact('source_id', source_keys)
            self.assertEqual(BucketMock.return_value.delete_object.call_count, len(source_keys))   #original keys deleted
            deleted_keys = set()
            for delete_calls in BucketMock.return_value.delete_object.call_args_list:
                deleted_key_name = delete_calls[0][0]   #first tuple arg
                deleted_keys.add(deleted_key_name)
            self.assertEqual(deleted_keys, set(source_keys))
            self.assertEqual(BucketMock.return_value.upload_file.call_count, 1)   #compacted file uploaded
            uploaded_key = BucketMock.return_value.upload_file.call_args[0][0]
            self.assertTrue(uploaded_key.endswith("-timestamp3_.metrics"))    #compacted file key timestamp is identical to last key timestamp, plus '_' to make it sort immediately after last compacted key

            expected_compaction_results = {
                ('metric_a', start_timestamp, 1.0, frozenset({'source':'source_id', 'version':'1'}.items())),
                ('metric_a', start_timestamp+1, 5.0, frozenset({'source':'source_id', 'version':'1', 'a':'abc'}.items())),
                ('metric_a', start_timestamp+2, 8.0, frozenset({'source':'source_id', 'version':'1', 'z':'xyz'}.items())),
                ('metric_a', start_timestamp+2, 8.0, frozenset({'source':'source_id', 'version':'1', 'z':'xyz', 'd':'dljf'}.items())),
                ('metric_a', start_timestamp+2, 8.0, frozenset({'source':'source_id', 'version':'1', 'y':'wxy'}.items())),
            }
            for metric_tuple in metrics_from_uploaded_file:
                name, timestamp, value, tags_dict = metric_tuple
                hashable_metric_tuple = (name, timestamp, value, frozenset(tags_dict.items()))
                expected_compaction_results.remove(hashable_metric_tuple)
            self.assertEqual(len(expected_compaction_results), 0)



        def test_convert_to_seconds(self):
            client = MetricClient()
            data = metric_data.create_metric_data('yay', [[1000, 1], [2000,2], [3000,3]])
            client.convert_timestamps_to_seconds(data)
            converted_values = data[0][metric_data.METRIC_VALUES_KEY]
            self.assertAlmostEqual(converted_values[0][0], 1)
            self.assertAlmostEqual(converted_values[1][0], 2)
            self.assertAlmostEqual(converted_values[2][0], 3)


        @patch('requests.post')
        def test_query_kairos(self, post_mock):
            client = MetricClient()
            response = Response()
            kairos_response_dict = {u'queries': [{u'sample_size': 1190, u'results': [{u'group_by': [
                {u'group': {u'source': u'b827eb505442', u'version': u'4'}, u'name': u'tag',
                 u'tags': [u'source', u'version']}, {u'type': u'number', u'name': u'type'}],
                                                                    u'values': [[1445371200000, 9.149407229502824],
                                                                                [1445371260000, 5.4143706598549475],
                                                                                [1445371320000, 5.475621958612053],
                                                                                [1445371380000, 5.34596777427941],
                                                                                [1445371440000, 5.418326327810092],
                                                                                [1445371500000, 5.357992974109929],
                                                                                [1445371560000, 5.844460071297526],
                                                                                [1445371620000, 6.249566743033765],
                                                                                [1445371680000, 5.176467003417463],
                                                                                [1445371740000, 4.408119402360081],
                                                                                [1445371800000, 4.718912587501116],
                                                                                [1445371860000, 5.2786693039817365],
                                                                                [1445442540000, 3.425163080566741]],
                                                                    u'name': u'motion.60s.sum',
                                                                    u'tags': {u'source': [u'b827eb505442'],
                                                                              u'version': [u'4']}}]},
                               {u'sample_size': 1190, u'results': [{u'group_by': [
                                   {u'group': {u'source': u'b827eb505442', u'version': u'4'}, u'name': u'tag',
                                    u'tags': [u'source', u'version']}, {u'type': u'number', u'name': u'type'}],
                                                                    u'values': [[1445371200000, 1343],
                                                                                [1445371260000, 1440],
                                                                                [1445371320000, 1440],
                                                                                [1445371380000, 1440],
                                                                                [1445371440000, 1440],
                                                                                [1445371500000, 1440],
                                                                                [1445371560000, 1440], ],
                                                                    u'name': u'motion.60s.count',
                                                                    u'tags': {u'source': [u'b827eb505442'],
                                                                              u'version': [u'4']}}]}]}
            response._content = json.dumps(kairos_response_dict)
            post_mock.return_value = response
            response.status_code = 200
            dummy_timestamp = datetime.datetime.now(tz = pytz.utc)
            data = client.query_kairos("dummy_source", ["dummy_name"], dummy_timestamp, dummy_timestamp, ['source','version'])
            #sanity check that metric data is same as what was returned by the mocked request.post
            self.assertEqual(len(data), 2)
            self.assertEqual({'motion.60s.count', 'motion.60s.sum'}, set([x[metric_data.METRIC_NAME_KEY] for x in data]))

        def test_convert_metric_data_to_dict(self):
            data = metric_data.create_metric_data('jim', [[0,1], [1,2]], tags={'w': 'z', 'x': 'y'})
            metric_data.append_row(data, 'jim', [[0,0], [1,1]], {'w': 'z'})
            metric_data.append_row(data, 'bob', [[2,3]])
            client = MetricClient()
            result_dict = client.convert_metric_data_to_dict(data)
            self.assertEqual(len(result_dict), 2)   #2 metric names
            jim_results = result_dict.get('jim')
            self.assertEqual(len(jim_results), 2)    #2 tag combos for 'jim'
            jim_w_z_results = jim_results[frozenset({'w': 'z'}.items())]
            self.assertEqual(len(jim_w_z_results), 2)
            self.assertEqual(jim_w_z_results[0], 0)
            self.assertEqual(jim_w_z_results[1], 1)
            jim_w_z_x_y_results = jim_results[frozenset({'w': 'z', 'x': 'y'}.items())]
            self.assertEqual(len(jim_w_z_x_y_results), 2)
            self.assertEqual(jim_w_z_x_y_results[0], 1)
            self.assertEqual(jim_w_z_x_y_results[1], 2)
            bob_results = result_dict.get('bob')
            self.assertEqual(len(bob_results), 1)    #1 tag combo for 'bob'
            bob_notags_results = bob_results[frozenset({}.items())]
            self.assertEqual(len(bob_notags_results), 1)
            self.assertEqual(bob_notags_results[2], 3)

        def test_create_patch(self):
            client = MetricClient()
            current = metric_data.create_metric_data('jim', [[0,0], [1,1]])
            desired = metric_data.create_metric_data('jim', [[0,1], [1,1], [2,2]])
            patch = client.create_patch(desired, current)
            self.assertEqual(len(patch), 1) #one name/tag combo
            self.assertEqual(patch[0][metric_data.METRIC_NAME_KEY], 'jim') #no tags
            self.assertFalse(patch[0][metric_data.METRIC_TAGS_KEY]) #no tags
            patch_values = patch[0][metric_data.METRIC_VALUES_KEY]
            self.assertEqual(len(patch_values), 2)
            self.assertEqual(patch_values[0], [0,1])
            self.assertEqual(patch_values[1], [2,2])


@unittest.skipIf(hasattr(common.settings, 'CIRCLE_CI'), 
                 'SKIPPIG: Circle CI lacks AWS credentials to run this test')
class MetricClientIntegrationTestCase (unittest.TestCase):

    def setUp(self):
        MetricClient.metrics_bucket = None    #for unmocked re-initialization...
        common.bucket.bucket.Bucket.s3_resource_dict = {}    #for unmocked re-initialization...
        common.bucket.bucket.Bucket.boto_bucket_cache = {}   #for unmocked re-initialization...

    def test_put_and_get(self):
        now = datetime.datetime.now(tz=pytz.utc)
        an_hour_ago = now - datetime.timedelta(hours=1)

        client = MetricClient()

        test_source_id = 'UNIT_TEST'[::-1]
        test_metric_name = 'metric_a' + str(uuid.uuid4())[:6]
        test_metric_values = [
                 (metric_time.get_timestamp(an_hour_ago), 1),
                 (metric_time.get_timestamp(now), 2)
             ]
        test_metrics = [
            {metric_data.METRIC_NAME_KEY: test_metric_name,
             metric_data.METRIC_TAGS_KEY:{},
             metric_data.METRIC_VALUES_KEY:test_metric_values}
        ]
        client.put_metrics(test_source_id, test_metrics)

        an_hour_ago_seconds = time.mktime(an_hour_ago.astimezone(dateutil.tz.tzlocal()).timetuple()) #test that it works with seconds since epoch
        results = client.get_metrics(test_source_id, [test_metric_name], an_hour_ago_seconds, now + datetime.timedelta(hours=1))
        self.assertEqual(len(results), 1)
        result = results[0]
        self.assertEquals(result[metric_data.METRIC_NAME_KEY], test_metric_name, "Expecting Metric name '%s', got '%s'" % (test_metric_name, result[metric_data.METRIC_NAME_KEY]))
        self.assertEquals(len(result[metric_data.METRIC_VALUES_KEY]), len(test_metric_values), "Expecting %d values, got %d" % (len(test_metric_values), len(result[metric_data.METRIC_VALUES_KEY])))


    def test_upload_file(self):
        local_filename = "metric_filename.aggregate.txt"
        source_id_1 = 'UNIT_TEST'[::-1]
        metric_name_1 = 'metric_a' + str(uuid.uuid4())[:6]
        now = datetime.datetime.now(tz=pytz.utc)
        with open(local_filename, "w") as file:
            file.write("put %s %d 0.00287401140667 version=4 stat_type=max mac_address=b827eb5ddab0 source=%s" % (metric_name_1, metric_time.get_timestamp(now), source_id_1))


        MetricClient.metrics_bucket = None    #for unmocked re-initialization...
        client = MetricClient()

        client.put_metrics_from_file(local_filename)
        results = client.get_metrics(source_id_1, [metric_name_1], now, now + datetime.timedelta(hours=1))
        self.assertEqual(len(results), 1)
        result = results[0]
        self.assertEquals(result[metric_data.METRIC_NAME_KEY], metric_name_1, "Expecting Metric name '%s', got '%s'" % (metric_name_1, result[metric_data.METRIC_NAME_KEY]))
        self.assertEquals(len(result[metric_data.METRIC_VALUES_KEY]), 1, "Expecting %d values, got %d" % (1, len(result[metric_data.METRIC_VALUES_KEY])))

    def test_get_aggregates_fast(self):
        client = MetricClient()
        start_time = dateutil.parser.parse('2015-10-20 15:50:00Z')
        end_time = dateutil.parser.parse('2015-10-21 15:50:00Z')

        #just get code coverage i guess...
        client.get_aggregate_metrics_fast('b827eb505442', 'motion', start_time, end_time, ['*'], 60, "mean")
        client.get_aggregate_metrics_fast('b827eb505442', 'motion', start_time, end_time, ['*'], 60, "mean", filter_by_tags={})
        client.get_aggregate_metrics_fast('b827eb505442', 'motion', start_time, end_time, ['*'], 70, "mean", filter_by_tags={'jim':'bob'})
        client.get_aggregate_metrics_fast('b827eb505442', 'motion', start_time, end_time, ['*'], 70, "meanofmeans", filter_by_tags={'jim':'bob'})

    def test_get_latest_mod_date(self):
        test_source_name = "tsss"
        test_metric_name = 'heya' + str(uuid.uuid4())[:6]
        test_metric_time = datetime.datetime.now(tz=pytz.utc)
        metrics = metric_data.create_metric_data(test_metric_name, [[metric_time.get_timestamp(test_metric_time),2]])
        client = MetricClient()
        client.put_metrics(test_source_name, metrics)

        last_mod_time = client.get_last_modification_date(test_source_name, test_metric_name, False, 0, test_metric_time)
        time_diff = last_mod_time - test_metric_time
        inv_time_diff = test_metric_time - last_mod_time
        ten_seconds = datetime.timedelta(seconds=10)
        self.assertTrue(last_mod_time is not None and time_diff < ten_seconds and inv_time_diff < ten_seconds)




