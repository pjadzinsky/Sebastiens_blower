from collections import defaultdict
import datetime
import json
import uuid
from common.metrics import metric_time
from common.metrics import metric_name_parsing
from common.metrics import metric_file_naming
from common.metrics import metric_aggregation
from common.metrics import metric_data
from common.utils import caching
from common.metrics.metric_file import MetricFile
from common.metrics.metric_writer_file_set import MetricWriterFileSet
import os.path
import common.bucket.bucket
from common.bucket.s3_file_fetcher import S3FileFetcher
import pytz
import requests
import decimal

decimal.getcontext().prec = 5   #5 significant digits for float comparisons
zero = decimal.Decimal(0)   #used in trick to round to significant digits

COMPACTION_METADATA_KEY = "compaction"


def frozenset_to_dict(the_frozenset):
    d = {}
    d.update((k,v) for k,v in the_frozenset)
    return d

def dict_to_sorted_list_of_lists(the_dict):
    return [[k, the_dict[k]] for k in sorted(the_dict)]

def is_close(a, b):
    #Adding Decimal(0) rounds to decimal.getcontext().prec significant digits
    return decimal.Decimal(a) + zero == decimal.Decimal(b) + zero


class MetricClient:
    #High level interface for querying and posting metrics

    metrics_bucket = None

    def __init__(self):
        pass

    def look_up_bucket(self):
        if MetricClient.metrics_bucket is None:
            MetricClient.metrics_bucket = common.bucket.bucket.Bucket(common.settings.METRICS_BUCKET_BASE_NAME, create_if_does_not_exist=False)

    def put_metrics(self, source_id, metrics):
        """
        Posts metric values. Large batches are preferred (rather than individual calls) to reduce S3 object count.
        :param source_id: metric source (e.g. slab ID). Added as a tag to each value (below)
        :param metrics:  data in metric_data.py format
        """
        self.put_metrics_internal(source_id, metrics, None)

    def put_metrics_internal(self, source_id, metrics, custom_timestamp, metadata=None):
        with MetricWriterFileSet("metric_client_writer_%s" % str(uuid.uuid4())) as writer_set:
            writer_set.custom_timestamp = custom_timestamp
            self.write_metrics_to_local_files(metrics, source_id, writer_set)
            writer_set.close_all_writers()
            self.upload_local_files_to_metrics_bucket(writer_set.root_path, metadata)

    def put_metrics_from_file(self, filename):
        with open(filename, "r") as file_handle:
            source_metric_file = MetricFile(file_handle, filename)
            with MetricWriterFileSet("metric_client_writer_%s" % str(uuid.uuid4())) as writer_set:
                metric_tuple = source_metric_file.get_value()
                while metric_tuple:
                    (name, timestamp, value, tags) = metric_tuple
                    timestamp = self.convert_from_second_timestamp(timestamp)
                    source_id = self.get_source_id(tags)
                    self.add_version_tag(tags)

                    writer = writer_set.get_or_create_writer(name, source_id, timestamp)
                    writer.put_value(name, timestamp, value, **tags)
                    metric_tuple = source_metric_file.get_value()

                writer_set.close_all_writers()
                self.upload_local_files_to_metrics_bucket(writer_set.root_path, None)

    @caching.cache_value()
    def get_metrics_cached(self, source_id, metric_names, start_time, end_time, group_by_tags=None, aggregation_seconds=None,
                    aggregator_names=None, filter_by_tags=None, return_timestamps_as_seconds=False):
        """ Same as get_metrics, but with local caching.
        
        :param cache_max_age: number of seconds until the cache is considered stale (via the decorator)
        """
        return self.get_metrics(source_id, metric_names, start_time, end_time, group_by_tags, aggregation_seconds, aggregator_names, filter_by_tags, return_timestamps_as_seconds)
    
    
    def get_metrics(self, source_id, metric_names, start_time, end_time, group_by_tags=None, aggregation_seconds=None,
                    aggregator_names=None, filter_by_tags=None, return_timestamps_as_seconds=False):
        """
        Returns metric values for a given source and time range.
        :param source_id: values' source id (e.g. a slab ID)
        :param metric_names: array of metric names to get (e.g. ['airflow.temperature.out', 'motion.60s.count']
        :param start_time: results start time (inclusive). type is datetime.datetime OR seconds since epoch
        :param end_time: results end time (non-inclusive). type is datetime.datetime OR seconds since epoch
        :param group_by_tags: list of tag values to group by. e.g. ['version'] will return results grouped by their version tag.
                              specifying None or {} will group all datapoints together.
                              specifying ['*'] will create a group for every tag combination
                              NOTE: When two datapoints in the same tag group share a timestamp, the last-written datapoint's value is returned
        :param aggregation_seconds: length over which to aggregate results. if aggregator_name is not None, this value must be >=60s
        :param aggregator_names: array containing one or more of "min", "max", "mean", "sd", "count", "first", "last". None if no aggregation. E.g. ["min", "max", "avg"]
        :param filter_by_tags: dictionary of tag:value pairs - only results that satisfy the filter will be returned. e.g. {"synthehtic":"true"}
        :return: data in metric_data.py format
        """
        # TODO (dr): Need to ensure that there is a 1:1 correlation between wanted metrics and returned metrics,
        #   i.e. get_metrics(metric_names=[NONEXISTENT1, NONEXISTENT2, 'valid_metric']) would return a list of 1
        #        when ideally it would return empty values[] for nonexistent data

        assert source_id, "Invalid source_id: %s" % source_id
        self.look_up_bucket()

        if not isinstance(start_time, datetime.datetime):
            start_time = datetime.datetime.fromtimestamp(start_time, tz=pytz.utc)
        if not isinstance(end_time, datetime.datetime):
            end_time = datetime.datetime.fromtimestamp(end_time, tz=pytz.utc)

        key_prefixes = self.gather_key_prefixes(source_id, metric_names, start_time, end_time)

        results = self.gather_values_from_key_prefixes(source_id, key_prefixes, metric_names, start_time,
                                                                    end_time, group_by_tags, filter_by_tags)

        if aggregator_names:
            results = metric_aggregation.aggregate_raw_metrics(results, start_time, end_time, aggregator_names, aggregation_seconds)

        if return_timestamps_as_seconds:
            self.convert_timestamps_to_seconds(results)
        return results

    def get_aggregate_metrics_fast(self, source_id, unaggregated_metric_name, start_time, end_time, group_by_tags=None, aggregation_seconds=None,
                aggregation_type=None, filter_by_tags=None, internal=True):
        """
        Low-latency metric fetch - only works for getting aggregates > 60s as raw metrics are not stored in Kairos
        :param source_id: values' source id (e.g. a slab ID)
        :param unaggregated_metric_name: unaggregated metric name (e.g. 'airflow.temperature.out')
        :param start_time: results start time (inclusive). type is datetime.datetime OR seconds since epoch
        :param end_time: results end time (non-inclusive). type is datetime.datetime OR seconds since epoch
        :param group_by_tags: list of tag values to group by. e.g. ['version'] will return results grouped by their version tag.
                              specifying None or {} will group all datapoints together.
                              Specifying ['*'] will return a separate time series for each tag combo
        :param aggregation_seconds: length over which to aggregate results. This value must be >= 60 seconds
        :param aggregation_type: one of (min, max, mean, sd, count, sum, first, last). Or special case: "meanofmeans"
        :param filter_by_tags: dictionary of tags to filter by. e.g {'version':'2'} will return only datapoints with version 2
        :return: data in metric_data.py format
        """
        assert not isinstance(group_by_tags, basestring)    #need a list, not a string

        if not isinstance(start_time, datetime.datetime):
            start_time = datetime.datetime.fromtimestamp(start_time, tz=pytz.utc)
        if not isinstance(end_time, datetime.datetime):
            end_time = datetime.datetime.fromtimestamp(end_time, tz=pytz.utc)

        query_aggregation_seconds = self.get_shorter_preaggregation_length(aggregation_seconds)

        #special case for getting mean as an average of next-shortest mean values (sums and counts don't exist for older data)
        if aggregation_type == "meanofmeans":
            kairos_metric_name = metric_name_parsing.get_metric_name(unaggregated_metric_name, "mean", query_aggregation_seconds)
            return self.query_kairos(source_id, [kairos_metric_name], start_time, end_time, group_by_tags,
                                     filter_by_tags=filter_by_tags, internal=internal, kairos_aggregator_name="avg",
                                     kairos_agg_seconds = aggregation_seconds)
        else:
            #if requesting a pre-baked aggregation length, query kairos directly
            if aggregation_seconds in metric_aggregation.aggregation_levels:
                metric_name = metric_name_parsing.get_metric_name(unaggregated_metric_name, aggregation_type, aggregation_seconds)
                return self.query_kairos(source_id, [metric_name], start_time, end_time, group_by_tags, filter_by_tags=filter_by_tags, internal=internal)
            #otherwise calculate the aggregate from shorter pre-baked aggregates
            else:
                #Query kairos for the shorter aggregates needed to caclulate the requested aggregate length
                results = self.get_subaggregates_from_kairos(source_id, unaggregated_metric_name, start_time, end_time, aggregation_type,
                                  query_aggregation_seconds, group_by_tags, filter_by_tags)


                #Aggregate the shorter aggregates from Kairos into the requested aggregation time
                return metric_aggregation.aggregate_preaggregated_metrics(results, [aggregation_type], aggregation_seconds,
                                                                          metric_time.get_timestamp(start_time),
                                                                          metric_time.get_timestamp(end_time))

    def get_shorter_preaggregation_length(self, aggregation_seconds):
        # Find the longest pre-aggregated metric that is still shorter than the requested aggregation
        assert aggregation_seconds >= metric_aggregation.aggregation_levels[
            0], "Cannot retreive an aggregate shorter than %d seconds" % metric_aggregation.aggregation_levels[0]
        query_aggregation_seconds = None
        for aggregation_level in metric_aggregation.aggregation_levels_descending:
            if aggregation_seconds >= aggregation_level:
                query_aggregation_seconds = aggregation_level
                break
        return query_aggregation_seconds



    def get_subaggregates_from_kairos(self, source_id, unaggregated_metric_name, start_time, end_time, aggregation_type,
                     aggregation_seconds, group_by_tags, filter_by_tags):
        """
        Queries kairos for all sub-aggregates needed to calculate the aggregation of length/type passed in
        :param source_id:
        :param unaggregated_metric_name:
        :param start_time:
        :param end_time:
        :param aggregation_type:
        :param aggregation_seconds:
        :param group_by_tags:
        :return:
        """

        # Get all previous aggregates needed to calculate the requested aggregation
        kairos_metric_names = []
        for sub_agg_type in metric_aggregation.prev_aggregates_needed[aggregation_type]:
            kairos_metric_names.append(metric_name_parsing.get_metric_name(unaggregated_metric_name, sub_agg_type,
                                                                       aggregation_seconds))

        subaggregates = self.query_kairos(source_id, kairos_metric_names, start_time, end_time, group_by_tags, filter_by_tags = filter_by_tags, internal=False, print_query=True)
        return subaggregates

    def query_kairos(self, source_id, kairos_metric_names, start_time, end_time, group_by_tags, filter_by_tags = None, internal=True, print_query=False, kairos_aggregator_name=None, kairos_agg_seconds=None):
        """
        Direct kairos query, with results returned in metric_data.py format
        :param source_id: source
        :param kairos_metric_names: array of metric names
        :param start_time: datetime.datetime start time (inclusive)
        :param end_time: datetime.datetime end time (not inclusive)
        :param group_by_tags: array of tags to group results by. If array contains '*', you will
        get a separate time series for each unique tag combo. None means return one timeseries regardless of tags.
        :param filter_by_tags: dictionary of tags to filter by. e.g {'version':'2'} will return only datapoints with version 2
        :param internal: whether to hit internal or external kairos
        :param print_query: whether to print the query to stdout
        :return: results in metric_data.py format
        """
        assert not isinstance(kairos_metric_names, basestring)    #need a list, not a string
        start_timestamp = metric_time.get_timestamp(start_time)
        end_timestamp = metric_time.get_timestamp(end_time)
        query = {
            'metrics': [],
            'cache_time': 0,
            'start_absolute': start_timestamp,
            'end_absolute': end_timestamp - 1
        }

        for kairos_metric_name in kairos_metric_names:
            tag_filter_dict =  {'source': source_id}
            if filter_by_tags:
                tag_filter_dict.update(filter_by_tags)
            metric = {'name': kairos_metric_name, 'tags':tag_filter_dict}
            if group_by_tags:
                if '*' in group_by_tags:
                    query_group_by_tags = self.get_all_kairos_tag_names(source_id, kairos_metric_name, start_timestamp, end_timestamp, internal)
                else:
                    query_group_by_tags = group_by_tags
                if query_group_by_tags:
                    metric['group_by'] = [{'name': 'tag', 'tags': query_group_by_tags}]
            if kairos_aggregator_name:
                metric['aggregators'] = [
                {'name': kairos_aggregator_name,
                 'align_sampling': True,
                 'sampling': {'value': kairos_agg_seconds, 'unit': 'seconds'}
                 }]
            query['metrics'].append(metric)
        host = self.get_kairos_host(internal)
        kairos_url = "http://" + host + "/api/v1/datapoints/query"
        if print_query:
            print "query to %s\n%s" % (kairos_url, json.dumps(query))
        response = requests.post(url=kairos_url, data=json.dumps(query))
        if response.status_code != 200:
            print "Kairos response error (%d): %s" % (response.status_code, response.content)
            raise RuntimeError
        try:
            response_dict = json.loads(response.content)
        except:
            print "Kairos response: %s" % response.content
            raise
        response = self.convert_kairos_response(response_dict)
        return response

    def ship_to_kairos(self, data):
        datapoints = []
        for result in data:
            name = result[metric_data.METRIC_NAME_KEY]
            tags = result[metric_data.METRIC_TAGS_KEY]
            values = result[metric_data.METRIC_VALUES_KEY]
            for value in values:
                try:
                    #if it's a number, serialize everything as floats, becuase kairos will maintain two timeseries for mixed types.
                    #https://github.com/kairosdb/kairosdb/issues/162
                    v = float(value[1])
                except:
                    v = value[1]
                datapoint = {'name':name,
                               'timestamp':value[0],
                               'value': v,
                               'tags':tags}
                datapoints.append(datapoint)

        if len(datapoints):
            r = requests.post("http://%s/api/v1/datapoints" % (common.settings.KAIROS2_INTERNAL_HOST),
                              data=json.dumps(datapoints),
                              timeout=30)

            if r.status_code != 204:
                errstr = "Got error posting to kairos, status code %s, response text %s" % (r.status_code, r.text)
                print errstr
                raise Exception(errstr)


    def get_kairos_host(self, internal):
        host = common.settings.KAIROS2_INTERNAL_HOST if internal else common.settings.KAIROS2_EXTERNAL_HOST
        return host

    def get_all_kairos_tag_names(self, source_id, kairos_metric_name, start_timestamp, end_timestamp, internal = True):
        #returns all tag names that exist in kairos for a given source/metric/time range
        host = self.get_kairos_host(internal)
        response = requests.post(
            url="http://" + host + "/api/v1/datapoints/query/tags",
            data=json.dumps(
                {'metrics': [
                    {
                        "tags": {
                            "source": [
                                source_id
                            ]
                        },
                        'name': kairos_metric_name
                    }
                ],
                    'cache_time': 0,
                    "start_absolute": start_timestamp,
                    "end_absolute": end_timestamp - 1}))
        try:
            response_dict = json.loads(response.content)
        except:
            print "Kairos response: %s" % response.content
            raise
        metric_tags = response_dict['queries'][0]['results'][0]['tags']
        metric_tag_names = [k for k in metric_tags.iterkeys()]
        return metric_tag_names


    def convert_kairos_response(self, response_dict):
        """
        Converts a kairos response to the "standard" metrics format (see metric_data.py)
        :param response_dict: kairos response content
        :return:
        """
        results = []
        kairos_query_responses = response_dict['queries']
        for kairos_query_response in kairos_query_responses:
            kairos_query_results = kairos_query_response['results']


            for kairos_group in kairos_query_results:
                metric_name = kairos_group['name']
                result_values = kairos_group['values']
                result_group_by = kairos_group.get('group_by')
                result_tags = {}
                if result_group_by and result_group_by[0].get('group'):
                    result_tags = result_group_by[0]['group']
                result_tags = dict((k, v) for k, v in result_tags.iteritems() if v) #filter out empty tags
                result = {metric_data.METRIC_NAME_KEY:metric_name, metric_data.METRIC_TAGS_KEY:result_tags, metric_data.METRIC_VALUES_KEY:result_values}
                results.append(result)

        return results

    def convert_from_second_timestamp(self, timestamp):
        if abs(timestamp) < 4102444800:
            timestamp *= 1000
        return timestamp

    def add_version_tag(self, tags):
        # This kind of opinionation is a vestige from before metric_client existed.
        # Safe to remove if desired.
        if not 'version' in tags:
            tags['version'] = 1

    def write_metrics_to_local_files(self, metrics, source_id, writer_set):
        for metric in metrics:
            metric_name = metric[metric_data.METRIC_NAME_KEY]
            tags = metric.get(metric_data.METRIC_TAGS_KEY)
            if tags is None:
                tags = {}
            tags['source'] = source_id
            for value_pair in metric[metric_data.METRIC_VALUES_KEY]:
                timestamp = value_pair[0]
                timestamp = self.convert_from_second_timestamp(timestamp)
                value = value_pair[1]
                writer = writer_set.get_or_create_writer(metric_name, source_id, timestamp)
                self.add_version_tag(tags)
                writer.put_value(metric_name, timestamp, value, **tags)

    def upload_local_files_to_metrics_bucket(self, root_path, metadata):
        self.look_up_bucket()
        for root, dirs, files in os.walk(root_path):
            for file in files:
                filename = os.path.join(root,file)
                key = os.path.relpath(filename, root_path)
                self.metrics_bucket.upload_file(key,filename,metadata=metadata)

    def gather_values_from_key_prefixes(self, source_id, key_prefixes, metric_names, start_time, end_time,
                                        group_by_tags, filter_by_tags=None):

        results_dict = defaultdict(lambda : defaultdict(dict))    #first level is metric name, second level is tag combo
        for key_prefix in key_prefixes:
            keys = self.gather_key_names_from_prefix(key_prefix)
            if len(keys) > 2:  #compact if we should (> 2 keys for this prefix)
                self.compact(source_id, keys)
                keys = self.gather_key_names_from_prefix(key_prefix)
            results_by_metric_name = self.gather_values_from_keys(keys, metric_names, start_time, end_time, group_by_tags, filter_by_tags)
            for metric_name in results_by_metric_name:
                metric_name_results = results_by_metric_name[metric_name]
                for tag_combo in results_by_metric_name[metric_name]:
                    results_by_tag_combo = metric_name_results[tag_combo]
                    results_dict[metric_name][tag_combo].update(results_by_tag_combo)


        return self.convert_dict_to_metric_data(results_dict)

    def convert_dict_to_metric_data(self, results_dict):
        """
        :param results_dict: 2-level dictionary: datapoint_value = results_dict[metric_name][tag_combo][timestamp]
        :return: metric data in metric_data.py format
        """
        results = metric_data.create_metric_data()
        for metric_name in results_dict:
            results_by_tag_combo = results_dict[metric_name]
            for tag_combo in results_by_tag_combo:
                tags_dict = frozenset_to_dict(tag_combo)
                datapoints_by_timestamp = results_by_tag_combo[tag_combo]
                datapoints = dict_to_sorted_list_of_lists(datapoints_by_timestamp) if isinstance(datapoints_by_timestamp, dict) else datapoints_by_timestamp
                metric_data.append_row(results, metric_name, datapoints, tags_dict)
        return results

    def convert_metric_data_to_dict(self, data):
        #converts result from dictionary-format to metric_data.py format
        results_dict = defaultdict(lambda : defaultdict(list))    #first level is metric name, second level is tag combo
        for metric in data:
            metric_name = metric[metric_data.METRIC_NAME_KEY]
            tag_combo = metric[metric_data.METRIC_TAGS_KEY]
            values = metric[metric_data.METRIC_VALUES_KEY]
            hashable_tag_combo = frozenset(tag_combo.items())
            values_dict = {}
            results_dict[metric_name][hashable_tag_combo] = values_dict
            for value in values:
                values_dict[value[0]] = value[1]

        return results_dict

    def gather_key_names_from_prefix(self, key_prefix):
        return [obj.key for obj in self.metrics_bucket.boto_bucket.objects.filter(Prefix=key_prefix)]

    def gather_values_from_keys(self, keys, metric_names, start_time, end_time, group_by_tags, filter_by_tags=None):
        """
        Gathers metric values matching a name & time range from a list of S3 key prefixes
        :param key_prefixes: set of S3 key prefixes
        :param metric_names: array of metric names (None to get all metrics in the key)
        :param start_time: start time for gathering metric values (inclusive)
        :param end_time: end time for gathering metric values (non-inclusive)
        :param group_by_tags: list of tag keys to group by
        :return: data in dict format
        """
        assert not isinstance(group_by_tags, basestring)    #need a list, not a string
        assert not isinstance(metric_names, basestring)    #need a list, not a string
        keys.sort() #sorting by key will go through files in chronoligical order
        start_timestamp = metric_time.get_timestamp(start_time)
        end_timestamp = metric_time.get_timestamp(end_time)

        results_by_metric_name = {}
        if metric_names:
            for metric_name in metric_names:
                results_by_metric_name[metric_name] = {}

        tags_to_return = {}    #get no tags if not grouping by tags
        if group_by_tags:
            assert not isinstance(group_by_tags, basestring)    #need a list, not a string
            if '*' in group_by_tags:
                tags_to_return = None  #confusingly, this will get all tags from each metric
            else:
                tags_to_return = group_by_tags

        for key in keys:
            self.accumulate_values_from_key(key, self.metrics_bucket, start_timestamp, end_timestamp, tags_to_return, metric_names, results_by_metric_name, filter_by_tags)

        return results_by_metric_name

    def accumulate_values_from_key(self, key, bucket, start_timestamp, end_timestamp, tags_to_return, metric_names, results_by_metric_name, filter_by_tags=None):
        """
        Add values read from an S3 object to an accumulator
        :param key: key
        :param bucket: bucket
        :param start_timestamp: start timestamp (inclusive)
        :param end_timestamp: end timestamp (not inclusive)
        :param tags_to_return: the tags whose values should be included in the results (None for all tags)
        :param metric_names: names of metrics to read (None for all metrics)
        :param results_by_metric_name: accumulator - 3 level dict, ie dict[metric_name][tag_combo][timestamp]
        :param filter_by_tags: tags to filter by. only datapoints matching tags in this dict will be returned.
        :return:
        """
        filter_by_tags_set = frozenset(filter_by_tags.items()) if filter_by_tags else None
        empty_tag_set = frozenset()
        with S3FileFetcher(key, bucket=bucket.boto_bucket) as local_metrics_filename:
            with open(local_metrics_filename, "r") as metrics_file_handle:
                metric_file = MetricFile(metrics_file_handle, local_metrics_filename)
                datapoint = metric_file.get_value()
                while datapoint:
                    (name, timestamp, value, tags) = datapoint
                    if timestamp >= start_timestamp and timestamp < end_timestamp:
                        if filter_by_tags_set is None or filter_by_tags_set.issubset(frozenset(tags.items())):
                            results_by_tag_combo = results_by_metric_name.get(name)
                            if results_by_tag_combo is None and metric_names is None:
                                #(metric_names is None) means caller wants all metrics, so create an entry for metric names encountered for the first time
                                results_by_tag_combo = {}
                                results_by_metric_name[name] = results_by_tag_combo
                            if results_by_tag_combo is not None:  # None means caller is not interested in this metric
                                if tags_to_return is None:
                                    hashable_tag_combo = frozenset(tags.items())
                                elif not tags_to_return:
                                    hashable_tag_combo = empty_tag_set
                                else:
                                    hashable_tag_combo = frozenset([(key,tags[key]) for key in tags if key in tags_to_return])
                                results_by_timestamp = results_by_tag_combo.get(hashable_tag_combo)
                                if not results_by_timestamp:
                                    results_by_timestamp = {}
                                    results_by_tag_combo[hashable_tag_combo] = results_by_timestamp
                                results_by_timestamp[timestamp] = value

                    datapoint = metric_file.get_value()

    @staticmethod
    def gather_key_prefixes(source_id, metric_names, start_time, end_time):
        key_prefixes = set()
        assert not isinstance(metric_names, basestring)    #need a list, not a string
        for metric_name in metric_names:
            (unaggergated_metric_name, full_aggregation_suffix, aggregation_type, aggregation_seconds) = metric_name_parsing.parse_aggregation(metric_name)
            is_aggregate = aggregation_type != None
            file_length = metric_file_naming.get_file_length(aggregation_seconds, aggregation_type!=None)
            current_time = metric_file_naming.round_down_to_file_start_time(start_time, file_length)
            while current_time < end_time:
                prefix = MetricClient.get_key_prefix(source_id, unaggergated_metric_name, is_aggregate, current_time,
                                                     file_length)
                key_prefixes.add(prefix)
                current_time += datetime.timedelta(seconds=file_length)
        return key_prefixes

    @staticmethod
    def get_key_prefix(source_id, unaggergated_metric_name, is_aggregate, file_start_time, file_length):
        directory, filename = metric_file_naming.get_metric_filename_ext(source_id, file_start_time,
                                                                         unaggergated_metric_name, is_aggregate,
                                                                         file_length)
        prefix = os.path.join(directory, filename)
        #append separator, to distinguish between keys of metrics that start with the same string,
        #e.g. 'motion' and 'motion.test': when querying for 'motion', we don't want to iterate over the keys of 'motion.test'
        prefix += metric_file_naming.SEPARATOR
        return prefix

    def get_source_id(self, tags):
        if tags.get('source'):
            return tags['source']
        return 'NA'

    def compact(self, source_id, keys):
        results_by_metric_name = self.gather_values_from_keys(keys, None, metric_time.get_datetime(0), datetime.datetime(year=datetime.MAXYEAR, month=1, day=1, tzinfo=pytz.utc), ['*'], None)

        #Come up with a timestamp that immediately follows all original keys when sorted alphabatically
        #(so the compacted values don't overwrite any metrics that have showed up since we queried the list of keys)
        last_key = keys[-1]
        custom_timestamp = MetricWriterFileSet.get_creation_timestamp(last_key) + "_"

        #write the compacted version to S3
        data = self.convert_dict_to_metric_data(results_by_metric_name)
        self.put_metrics_internal(source_id, data, custom_timestamp, {COMPACTION_METADATA_KEY:"true"})

        #remove the original keys
        for key in keys:
            self.metrics_bucket.delete_object(key)
        return

    def convert_timestamps_to_seconds(self, data):
        for row in data:
            converted = [[x[0]/1000.0, x[1]] for x in row[metric_data.METRIC_VALUES_KEY]]
            row[metric_data.METRIC_VALUES_KEY] = converted
        return

    def create_patch(self, desired, current):
        """
        Creates a patch for getting from on set of metrics to another
        :param desired: desired metric data in metric_data.py format
        :param current: current metric data in metric_data.py format
        :return: datapoints for getting from current to desired (contains datapoints missing from a or of different values between a and b)
        """
        desired_dict = self.convert_metric_data_to_dict(desired)
        current_dict = self.convert_metric_data_to_dict(current)
        patch_dict = defaultdict(lambda : defaultdict(list))    #first level is metric name, second level is tag combo

        for metric_name in desired_dict:
            desired_results_by_tag_combo = desired_dict[metric_name]
            current_results_by_tag_combo = current_dict.get(metric_name)
            if current_results_by_tag_combo:
                for tag_combo in desired_results_by_tag_combo:
                    desired_results_by_timestamp = desired_results_by_tag_combo[tag_combo]
                    if desired_results_by_timestamp:
                        patch_results_by_timestamp = {}
                        patch_dict[metric_name][tag_combo] = patch_results_by_timestamp
                        current_results_by_timestamp = current_results_by_tag_combo.get(tag_combo)
                        if current_results_by_timestamp:
                            for timestamp in desired_results_by_timestamp:
                                desired_value = desired_results_by_timestamp[timestamp]
                                current_value = current_results_by_timestamp.get(timestamp)
                                if current_value is None:
                                    patch_results_by_timestamp[timestamp] = desired_value
                                else:
                                    if desired_value != current_value and not is_close(desired_value, current_value):
                                        patch_results_by_timestamp[timestamp] = desired_value

                        else:
                            self.create_patch_by_timestamp(metric_name, tag_combo, desired_results_by_timestamp,
                                                           patch_dict)
            else:
                for tag_combo in desired_results_by_tag_combo:
                    desired_results_by_timestamp = desired_results_by_tag_combo[tag_combo]
                    if desired_results_by_timestamp:
                        self.create_patch_by_timestamp(metric_name, tag_combo, desired_results_by_timestamp,
                                                       patch_dict)

        return self.convert_dict_to_metric_data(patch_dict)

    def create_patch_by_timestamp(self, metric_name, tag_combo, source_datapoints_by_timestamp, patch_dict):
        a_to_b_results_by_timestamp = defaultdict(float)
        patch_dict[metric_name][tag_combo] = a_to_b_results_by_timestamp
        for timestamp in source_datapoints_by_timestamp:
            a_to_b_results_by_timestamp[timestamp] = source_datapoints_by_timestamp[timestamp]

    def get_last_modification_date(self, source_id, unaggregated_metric_name, is_aggregate, aggregation_seconds, time):
        file_length = metric_file_naming.get_file_length(aggregation_seconds, is_aggregate)
        file_start_time = metric_file_naming.round_down_to_file_start_time(time, file_length)
        prefix = MetricClient.get_key_prefix(source_id, unaggregated_metric_name, is_aggregate, file_start_time, file_length)
        dates = [o.last_modified for o in self.metrics_bucket.boto_bucket.objects.filter(Prefix=prefix)]
        if len(dates):
            return max(dates)
        return None




