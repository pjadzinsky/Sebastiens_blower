import random
import unittest
import datetime
from common.metrics import metric_aggregation
from common.metrics import metric_name_parsing
from common.metrics import metric_data
from common.metrics import metric_time
import dateutil.parser


class AggregatorsUnitTestCase (unittest.TestCase):
    def testSd(self):
        samples = [2,  4,  4,  4,  5,  5,  7,  9]
        mean = metric_aggregation.agg_mean(samples)
        self.assertEqual(metric_aggregation.agg_sd([2,  4,  4,  4,  5,  5,  7,  9], mean), 2)

    def testAvg(self):
        samples = [2,  5, 5.6]
        self.assertEqual(metric_aggregation.agg_mean(samples), 4.2)

    def test_non_numerical(self):
        samples = [2,  5, 'fdf']
        with self.assertRaises(ValueError):
            metric_aggregation.agg_max(samples)

    def test_max(self):
        samples = [2,  5, -97]
        self.assertEqual(metric_aggregation.agg_max(samples), 5)

    def test_min(self):
        samples = [2,  5, -97]
        self.assertEqual(metric_aggregation.agg_min(samples), -97)

    def test_count(self):
        samples = [2,  5, -97]
        self.assertEqual(metric_aggregation.agg_count(samples), 3)


    def test_aggregate_raw_metrics(self):
        start_time = dateutil.parser.parse('2015-10-20 15:50:00Z')
        end_time = start_time + datetime.timedelta(minutes = 3)
        start_timestamp = metric_time.get_timestamp(start_time)
        values = [[start_timestamp, 1], [start_timestamp + 60000, 2], [start_timestamp + 120005, 3]]
        data = metric_data.create_metric_data('metric', values=values)
        aggregated_data = metric_aggregation.aggregate_raw_metrics(data, start_time, end_time, metric_name_parsing.aggregate_types, 60)
        self.assertEqual(len(aggregated_data), len(metric_name_parsing.aggregate_types))
        for metric_row in aggregated_data:
            unaggergated_metric_name, full_aggregation_suffix, aggregation_type, result_aggregation_seconds = metric_name_parsing.parse_aggregation(metric_row['metric_name'])
            values = metric_row[metric_data.METRIC_VALUES_KEY]
            self.assertEqual(len(values), 3) #3 1-minute values per aggregate type
            self.assertEqual(values[0][0], start_timestamp)
            self.assertEqual(values[1][0], start_timestamp+60000)
            if aggregation_type == 'first' or aggregation_type == 'last':
                self.assertEqual(values[2][0], start_timestamp+120005)  #timestamps for first/last are not aligned with aggregation period
            else:
                self.assertEqual(values[2][0], start_timestamp+120000)


    def test_aggregate_raw_numeric_metrics(self):
        metric_name = 'yohoho'
        raw_metrics = [{'metric_name':metric_name, 'tags':{}, 'values':[(0, 1), (1, 2), (2,3.9)]}]
        timestamp=0
        aggregation_seconds = 60
        results = metric_aggregation.aggregate_raw_metrics(raw_metrics, metric_time.get_datetime(0), metric_time.get_datetime(3), metric_name_parsing.aggregate_types, aggregation_seconds)

        #Check that all aggregates were returned and no more
        all_aggregate_names = metric_name_parsing.get_all_aggregate_metric_names(metric_name, aggregation_seconds)
        returned_aggregate_names = set([r['metric_name'] for r in results])
        self.assertEqual(set(returned_aggregate_names), set(all_aggregate_names))

        for result in results:
            unaggergated_metric_name, full_aggregation_suffix, aggregation_type, result_aggregation_seconds = metric_name_parsing.parse_aggregation(result['metric_name'])
            values = result['values']
            self.assertEqual(len(values), 1)
            self.assertEqual(result_aggregation_seconds, aggregation_seconds)
            if aggregation_type == 'first':
                self.assertEqual(values[0][0], 0)
                self.assertEqual(values[0][1], 1)
            elif aggregation_type == 'last':
                self.assertEqual(values[0][0], 2)
                self.assertAlmostEqual(values[0][1], 3.9)
            else:
                self.assertEqual(values[0][0], timestamp)
                if aggregation_type == 'max':
                    self.assertEqual(values[0][1], 3.9)
                elif aggregation_type == 'min':
                    self.assertEqual(values[0][1], 1)
                elif aggregation_type == 'mean':
                    self.assertAlmostEqual(values[0][1], 2.3)
                elif aggregation_type == 'count':
                    self.assertEqual(values[0][1], 3)
                elif aggregation_type == 'sd':
                    self.assertAlmostEqual(values[0][1], metric_aggregation.agg_sd([1,2,3.9], 2.3))
                elif aggregation_type == 'sum':
                    self.assertAlmostEqual(values[0][1], metric_aggregation.agg_sum([1,2,3.9]))

    def test_aggregate_raw_empty_metrics(self):
        metric_name = 'yohoho'
        raw_metrics = [{'metric_name':metric_name, 'tags':{}, 'values':[]}]
        timestamp=0
        aggregation_seconds = 60
        results = metric_aggregation.aggregate_raw_metrics(raw_metrics, metric_time.get_datetime(0), metric_time.get_datetime(3), metric_name_parsing.aggregate_types, aggregation_seconds)

        #Check that all aggregates were returned and no more
        all_aggregate_names = metric_name_parsing.get_all_aggregate_metric_names(metric_name, aggregation_seconds)
        returned_aggregate_names = set([r['metric_name'] for r in results])
        self.assertEqual(set(returned_aggregate_names), set(all_aggregate_names))

        for result in results:
            unaggergated_metric_name, full_aggregation_suffix, aggregation_type, result_aggregation_seconds = metric_name_parsing.parse_aggregation(result['metric_name'])
            values = result['values']
            self.assertEqual(len(values), 0)
            self.assertEqual(result_aggregation_seconds, aggregation_seconds)

    def test_aggregate_raw_numeric_metrics_clip_timestamps(self):

        metric_name = 'yohoho'
        raw_metrics = [{'metric_name':metric_name, 'tags':{}, 'values':[(-1, 5000), (0, 1), (1, 2), (2,3.9), (3, 5000)]}]
        timestamp=0
        aggregation_seconds = 60
        results = metric_aggregation.aggregate_raw_metrics(raw_metrics, metric_time.get_datetime(0), metric_time.get_datetime(3), metric_name_parsing.aggregate_types, aggregation_seconds)

        #Check that all aggregates were returned and no more
        all_aggregate_names = metric_name_parsing.get_all_aggregate_metric_names(metric_name, aggregation_seconds)
        returned_aggregate_names = set([r['metric_name'] for r in results])
        self.assertEqual(set(returned_aggregate_names), set(all_aggregate_names))

        for result in results:
            unaggergated_metric_name, full_aggregation_suffix, aggregation_type, result_aggregation_seconds = metric_name_parsing.parse_aggregation(result['metric_name'])
            values = result['values']
            self.assertEqual(len(values), 1)
            self.assertEqual(result_aggregation_seconds, aggregation_seconds)
            if aggregation_type == 'first':
                self.assertEqual(values[0][0], 0)
                self.assertEqual(values[0][1], 1)
            elif aggregation_type == 'last':
                self.assertEqual(values[0][0], 2)
                self.assertAlmostEqual(values[0][1], 3.9)
            else:
                self.assertEqual(values[0][0], timestamp)
                if aggregation_type == 'max':
                    self.assertEqual(values[0][1], 3.9)
                elif aggregation_type == 'min':
                    self.assertEqual(values[0][1], 1)
                elif aggregation_type == 'mean':
                    self.assertAlmostEqual(values[0][1], 2.3)
                elif aggregation_type == 'count':
                    self.assertEqual(values[0][1], 3)
                elif aggregation_type == 'sd':
                    self.assertAlmostEqual(values[0][1], metric_aggregation.agg_sd([1,2,3.9], 2.3))
                elif aggregation_type == 'sum':
                    self.assertAlmostEqual(values[0][1], metric_aggregation.agg_sum([1,2,3.9]))

    def test_aggregate_versioned_numeric_metrics(self):
        metric_name = 'yohoho'
        raw_metrics = [{'metric_name':metric_name, 'tags':{'version':'1'}, 'values':[(0, 1), (1, 2), (2,3.9)]},
                       {'metric_name':metric_name, 'tags':{'version':'2'}, 'values':[(0, -5), (1, -7), (2,-30)]}]
        timestamp=0
        aggregation_seconds = 60
        results = metric_aggregation.aggregate_raw_metrics(raw_metrics, metric_time.get_datetime(0), metric_time.get_datetime(3), metric_name_parsing.aggregate_types, aggregation_seconds)

        #Check that we got aggregates for each version separately
        returned_version_tags = {r['tags']['version'] for r in results}
        self.assertEqual(returned_version_tags, {'1','2'})
        all_aggregate_names = metric_name_parsing.get_all_aggregate_metric_names(metric_name, aggregation_seconds)
        self.assertEqual(len(results), len(all_aggregate_names)*2)  #all aggregates for each version


    def test_aggregate_raw_string_metrics(self):

        aggregation_seconds = 60
        metric_name = 'yohoho'
        timestamp2 = aggregation_seconds * 1000
        raw_metrics = [{'metric_name':metric_name, 'tags':{}, 'values':[(0, 'joe'), (1, 2), (2,'flea'), (timestamp2, 7)]}]
        timestamp=0
        results = metric_aggregation.aggregate_raw_metrics(raw_metrics, metric_time.get_datetime(0), metric_time.get_datetime(timestamp2+1), metric_name_parsing.aggregate_types, aggregation_seconds)

        returned_aggregate_names = set([r['metric_name'] for r in results])
        all_aggregate_names = set(metric_name_parsing.get_all_aggregate_metric_names(metric_name, aggregation_seconds))
        self.assertEqual(returned_aggregate_names, all_aggregate_names)

        for result in results:
            unaggergated_metric_name, full_aggregation_suffix, aggregation_type, result_aggregation_seconds = metric_name_parsing.parse_aggregation(result['metric_name'])
            self.assertEqual(result_aggregation_seconds, aggregation_seconds)
            values = result['values']
            self.assertEqual(result_aggregation_seconds, aggregation_seconds)
            if aggregation_type == 'mean' or aggregation_type == 'sd':
                #timestamp 0-59999 has non-numeric values, so no mean/sd for that aggregation period
                self.assertEqual(len(values), 1)
                value_timestamp = values[0][0]
                value_value = values[0][1]
                self.assertEqual(value_timestamp, timestamp2)
                if aggregation_type == 'mean':
                    self.assertAlmostEqual(value_value, 7)
                else:
                    self.assertAlmostEqual(value_value, 0)
            else:
                self.assertEqual(len(values), 2)
                slice1_timestamp = values[0][0]
                slice1_value = values[0][1]
                slice2_timestamp = values[1][0]
                slice2_value = values[1][1]
                if aggregation_type == 'count':
                    self.assertEqual(slice1_timestamp, 0)
                    self.assertEqual(slice1_value, 3)
                    self.assertEqual(slice2_timestamp, slice2_timestamp)
                    self.assertEqual(slice2_value, 1)
                elif aggregation_type == 'first':
                    self.assertEqual(slice1_timestamp, 0)
                    self.assertEqual(slice1_value, 'joe')
                    self.assertEqual(slice2_timestamp, slice2_timestamp)
                    self.assertEqual(slice2_value, 7)
                if aggregation_type == 'last':
                    self.assertEqual(slice1_timestamp, 2)
                    self.assertEqual(slice1_value, 'flea')
                    self.assertEqual(slice2_timestamp, slice2_timestamp)
                    self.assertEqual(slice2_value, 7)
                if aggregation_type == 'min' or aggregation_type == 'max':
                    self.assertEqual(slice1_timestamp, 0)
                    self.assertEqual(slice1_value, 2)
                    self.assertEqual(slice2_timestamp, slice2_timestamp)
                    self.assertEqual(slice2_value, 7)
            # else:
            #     self.assertEqual(len(values),0)

    def test_aggregate_preaggregated_metrics(self):

        metric_name = 'yohoho'
        all_values = []
        all_value_pairs = []
        for timestamp in range(0,5):
            values = [random.random() * 50 for _ in range(0, 1+int(random.random()*10))]
            all_values.extend(values)
            all_value_pairs.extend([[timestamp*1000+i,v] for v,i in zip(values, [i for i in range(0, len(values))])])
        data = [{'metric_name':metric_name, 'tags':{}, 'values':all_value_pairs}]
        all_values_by_timestamp = dict(all_value_pairs)
        aggregated_data = metric_aggregation.aggregate_raw_metrics(data, metric_time.get_datetime(0), metric_time.get_datetime(6000), metric_name_parsing.aggregate_types, 2)

        dest_aggregation_seconds = 6
        results = metric_aggregation.aggregate_preaggregated_metrics(aggregated_data, metric_name_parsing.aggregate_types, dest_aggregation_seconds, 0, 6000)

        #Check that all aggregates were returned and no more
        all_aggregate_names = metric_name_parsing.get_all_aggregate_metric_names(metric_name, dest_aggregation_seconds)
        returned_aggregate_names = set([r['metric_name'] for r in results])
        self.assertEqual(set(returned_aggregate_names), set(all_aggregate_names))

        for result in results:
            unaggergated_metric_name, full_aggregation_suffix, aggregation_type, result_aggregation_seconds = metric_name_parsing.parse_aggregation(result['metric_name'])
            values = result['values']
            self.assertEqual(len(values), 1)
            self.assertEqual(result_aggregation_seconds, dest_aggregation_seconds)
            result_timestamp = values[0][0]
            result_value = values[0][1]
            if aggregation_type == 'first':
                min_timestamp = min(x for x in all_values_by_timestamp)
                self.assertEqual(result_timestamp, min_timestamp)
                self.assertAlmostEqual(result_value, all_values_by_timestamp[min_timestamp])
            elif aggregation_type == 'last':
                max_timestamp = max(x for x in all_values_by_timestamp)
                self.assertEqual(result_timestamp, max_timestamp)
                self.assertAlmostEqual(result_value, all_values_by_timestamp[max_timestamp])
            else:
                self.assertEqual(result_timestamp, 0)
                self.assertEqual(result_aggregation_seconds, dest_aggregation_seconds)
                if aggregation_type == 'max':
                    self.assertEqual(result_value, max(all_values))
                elif aggregation_type == 'min':
                    self.assertEqual(result_value, min(all_values))
                elif aggregation_type == 'mean':
                    self.assertAlmostEqual(result_value, sum(all_values)/len(all_values))
                elif aggregation_type == 'count':
                    self.assertEqual(result_value, len(all_values))
                elif aggregation_type == 'sd':
                    sd = metric_aggregation.agg_sd(all_values, sum(all_values) / len(all_values))
                    self.assertAlmostEqual(result_value, sd)
                elif aggregation_type == 'sum':
                    self.assertAlmostEqual(result_value, metric_aggregation.agg_sum(all_values))


    def test_aggregate_empty_preaggregated_metrics(self):

        metric_name = 'yohoho'
        data = [{'metric_name':metric_name, 'tags':{}, 'values':[]}]
        aggregated_data = metric_aggregation.aggregate_raw_metrics(data, metric_time.get_datetime(0), metric_time.get_datetime(6000), metric_name_parsing.aggregate_types, 2)

        dest_aggregation_seconds = 6
        results = metric_aggregation.aggregate_preaggregated_metrics(aggregated_data, metric_name_parsing.aggregate_types, dest_aggregation_seconds, 0, 6000)

        #Check that all aggregates were returned and no more
        all_aggregate_names = metric_name_parsing.get_all_aggregate_metric_names(metric_name, dest_aggregation_seconds)
        returned_aggregate_names = set([r['metric_name'] for r in results])
        self.assertEqual(set(returned_aggregate_names), set(all_aggregate_names))

        for result in results:
            unaggergated_metric_name, full_aggregation_suffix, aggregation_type, result_aggregation_seconds = metric_name_parsing.parse_aggregation(result['metric_name'])
            values = result['values']
            self.assertEqual(len(values), 0)

    def test_aggregate_versioned_preaggregated_metrics(self):

        metric_name = 'yohoho'
        all_results = []
        all_version_values = []
        for timestamp in range(0,5):
            values = [random.random() * 50 for _ in range(0, 1+int(random.random()*10))]
            all_version_values.append(values)
            raw_metrics = [{'metric_name':metric_name, 'tags':{'version':str(timestamp)}, 'values':[(timestamp,v) for v in values]}]
            all_results.extend(metric_aggregation.aggregate_raw_metrics(raw_metrics, metric_time.get_datetime(timestamp), metric_time.get_datetime(timestamp+1), metric_name_parsing.aggregate_types, 1))

        dest_aggregation_seconds = 1    #aggregate everything into one
        results = metric_aggregation.aggregate_preaggregated_metrics(all_results, metric_name_parsing.aggregate_types, dest_aggregation_seconds, 0, 6)

        #Check that all aggregates were returned and no more
        all_aggregate_names = metric_name_parsing.get_all_aggregate_metric_names(metric_name, dest_aggregation_seconds)
        returned_aggregate_names = set([r['metric_name'] for r in results])
        self.assertEqual(set(returned_aggregate_names), set(all_aggregate_names))

        #Check that we got a result for each version
        returned_version_tags = {r['tags']['version'] for r in results}
        self.assertEqual(returned_version_tags, {'0','1','2','3','4'})
        self.assertEqual(len(results), len(all_aggregate_names)*5)  #all aggregates for each version

        for result in results:
            unaggergated_metric_name, full_aggregation_suffix, aggregation_type, result_aggregation_seconds = metric_name_parsing.parse_aggregation(result['metric_name'])
            values = result['values']
            version = int(result['tags']['version'])
            self.assertEqual(len(values), 1)
            result_timestamp = values[0][0]
            result_value = values[0][1]
            self.assertEqual(result_aggregation_seconds, dest_aggregation_seconds)
            if aggregation_type == 'first':
                self.assertEqual(result_timestamp, version)
            elif aggregation_type == 'last':
                self.assertEqual(result_timestamp, version)
            else:
                self.assertEqual(result_timestamp, 0)
                version_values = all_version_values[version]
                if aggregation_type == 'max':
                    self.assertEqual(result_value, max(version_values))
                elif aggregation_type == 'min':
                    self.assertEqual(result_value, min(version_values))
                elif aggregation_type == 'mean':
                    self.assertAlmostEqual(result_value, sum(version_values)/len(version_values))
                elif aggregation_type == 'count':
                    self.assertEqual(result_value, len(version_values))
                elif aggregation_type == 'sd':
                    sd = metric_aggregation.agg_sd(version_values, sum(version_values) / len(version_values))
                    self.assertAlmostEqual(result_value, sd)
                elif aggregation_type == 'sum':
                    self.assertAlmostEqual(result_value, sum(version_values))


    def test_aggregate_preaggregated_string_metrics(self):

        metric_name = 'yohoho'
        all_values = []
        all_results = []
        for timestamp in range(0,6000,1000):
            values = []
            if timestamp < 5000:    #non-numeric values in all but the last two sub-aggregate
                for _ in range(0, 1+int(random.random()*10)):
                    values.append('jdfl')
            values.append(-timestamp)    #throw in a numeric value to test mix
            values.append(timestamp)    #throw in a numeric value to test mix
            all_values.extend(values)
            raw_metrics = [{'metric_name':metric_name, 'tags':{}, 'values':[(timestamp,v) for v in values]}]
            all_results.extend(metric_aggregation.aggregate_raw_metrics(raw_metrics, metric_time.get_datetime(timestamp), metric_time.get_datetime(timestamp+1000), metric_name_parsing.aggregate_types, 1))

        dest_aggregation_seconds = 5
        results = metric_aggregation.aggregate_preaggregated_metrics(all_results, metric_name_parsing.aggregate_types, dest_aggregation_seconds, 0, 10000)

        for result in results:
            unaggergated_metric_name, full_aggregation_suffix, aggregation_type, result_aggregation_seconds = metric_name_parsing.parse_aggregation(result['metric_name'])
            values = result['values']
            result_timestamp_0 = values[0][0]
            result_value_0 = values[0][1]
            self.assertEqual(result_aggregation_seconds, dest_aggregation_seconds)
            if aggregation_type == 'mean':
                self.assertEqual(len(values), 1)    #[0s,5s) contains non-numeric values, so only get a mean for [5s,10s)
                self.assertEqual(result_timestamp_0, 5000)
                self.assertAlmostEqual(result_value_0, 0)
            elif aggregation_type == 'sd':
                self.assertEqual(len(values), 1)    #[0s,5s) contains non-numeric values, so only get a mean for [5s,10s)
                self.assertEqual(result_timestamp_0, 5000)
                self.assertAlmostEqual(result_value_0, 5000)
            else:
                self.assertEqual(len(values), 2)    #a value for [0s,5s) and for [5s,10s)
                result_timestamp_1 = values[1][0]
                result_value_1 = values[1][1]
                self.assertEqual(result_timestamp_1, 5000)
                if aggregation_type == 'first':
                    self.assertEqual(result_timestamp_0, 0)
                elif aggregation_type == 'last':
                    self.assertEqual(result_timestamp_0, 4000)
                else:
                    self.assertEqual(result_timestamp_0, 0)
                    if aggregation_type == 'max':
                        self.assertEqual(result_value_0, 4000)
                        self.assertEqual(result_value_1, 5000)
                    elif aggregation_type == 'min':
                        self.assertEqual(result_value_0, -4000)
                        self.assertEqual(result_value_1, -5000)
                    elif aggregation_type == 'sum':
                        self.assertEqual(result_value_0, 0)
                        self.assertEqual(result_value_1, 0)
                    elif aggregation_type == 'count':
                        self.assertEqual(result_value_0, len(all_values)-2)
                        self.assertEqual(result_value_1, 2)
                    else:
                        self.assertTrue(False)  #unexpected aggregate type
