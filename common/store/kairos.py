""" Client for Kairos time-series database.  """

from common.log import *
from common import settings
from common.utils.decorators import retry
import json
import requests
import requests.exceptions
from common.utils import utime

class KairosError(Exception):

    """Base class for exceptions raised by KairosClient."""

    def __init__(self, msg):
        self.msg = msg
        return

    def __str__(self):
        return '<KairosError %s>' % (self.msg)


class Aggregator(object):
    def __init__(self):
        pass
    
class FirstAggregator(object):
    """ Returns the first sample in each bin of specified size """
    def __init__(self, bin_size_sec):
        self._name = 'first'
        self._sampling = { 'value' : bin_size_sec, 'unit': 'seconds'}
        return
    
    def config(self):
        """ Config to be embedded in a larger kairos query. 
        Returns: a dictionary to be converted to json
        """
        c = {'name' : self._name,
             'sampling' : self._sampling}
        return c



class KairosWriter(object):

    def __init__(self, server, port=80):
        host_and_port = server + ":" + str(port)
        self.write_url = "http://%s/api/v1/datapoints" % (host_and_port)
        return

    def _validate_time(self, time_sec):
        # jan 1, 1970 is the earliest possible timestamp kairos will accept
        min_kairos_time_sec = 28800.0
        if time_sec < min_kairos_time_sec:
            raise KairosError('Provided time less than the earliest allowed by kairos: %s < %s' % (time_sec, min_kairos_time_sec))
        return 

    def write(self, metric_name, time_sec, value, tags):
        CHECK_NOTNONE(value)
        CHECK_GE(type(value), float)
        CHECK_EQ(type(tags), type({}))
        self._validate_time(time_sec)
        time_msec = int(time_sec * 1000.0)
        
        metrics = {"name": metric_name, "datapoints": [
            [time_msec, value]], "tags": tags}
        serialized_metrics = json.dumps(metrics)
        r = requests.post(self.write_url, serialized_metrics)
        if r.status_code != 204:
            raise KairosError("WriteError: \n %s \n %s" % (r.url, r.text))
        return

   
        

class KairosReader(object):

    def __init__(self, server, port=80):
        host_and_port = server + ":" + str(port)
        self.read_url = "http://%s/api/v1/datapoints/query" % (host_and_port)
        return

    def _validate_time(self, time_sec):
        # jan 1, 1970 is the earliest possible timestamp kairos will accept
        min_kairos_time_sec = 28800.0
        if time_sec < min_kairos_time_sec:
            raise KairosError('Provided time less than the earliest allowed by kairos: %s < %s' % (time_sec, min_kairos_time_sec))
        return 
    
    def read(self, metric_name, start_time_sec, end_time_sec, tags, group_by_tags=None, aggregators=[]):
        self._validate_time(start_time_sec)
        self._validate_time(end_time_sec)
        CHECK_GE(end_time_sec, start_time_sec)
        agg = [a.config() for a in aggregators]
        metric = {'tags': tags, 'name': metric_name, 'aggregators' : agg}
        
        if group_by_tags:
            metric['group_by'] = [{'name': 'tag', 'tags': group_by_tags}]
        
        
        if end_time_sec == float('inf'):
            end_time_sec = utime.now()
        
        query = {'metrics': [metric], 'cache_time': 0,
                 'start_absolute': int(start_time_sec * 1000.0),
                 'end_absolute': int(end_time_sec * 1000.0)}
        
        response = requests.post(self.read_url, json.dumps(query))
        if response.status_code != 200:
            raise KairosError("BadRead: %s %s" % (response.status_code, response.text))
        response_json = json.loads(response.content)
        raw_results = response_json['queries'][0]['results']

        # convert time units from milliseconds back to seconds
        for result in raw_results:
            for v in result['values']:
                v[0] = float(v[0]) / 1000.0
        return raw_results


def dict_merge(dct, merge_dct):
    """ Recursive dict merge. Inspired by :meth:``dict.update()``, instead of
    updating only top-level keys, dict_merge recurses down into dicts nested
    to an arbitrary depth, updating keys. The ``merge_dct`` is merged into
    ``dct``.
    :param dct: dict onto which the merge is executed
    :param merge_dct: dct merged into dct
    :return: None
    """
    for k, v in merge_dct.iteritems():
        
        if (k in dct and isinstance(dct[k], dict)
                and isinstance(merge_dct[k], dict)):
            dict_merge(dct[k], merge_dct[k])
        elif k in dct and type(v) == list and type(dct[k]) == list:
            dct[k] = dct[k] + v  
        else:
            dct[k] = merge_dct[k]

class KairosMultiReader(object):
    
    def __init__(self, readers):
        self._readers = readers
        return
    
    def read(self, metric_name, start_time_sec, end_time_sec, tags, group_by_tags=None, aggregators=[]):
        results_list = []
        for reader in self._readers:
            results = reader.read(metric_name, start_time_sec, end_time_sec, tags, group_by_tags=group_by_tags, aggregators=aggregators)[0]
            results_list.append(results)
        return results_list
            
            
    
    
    
        
    
