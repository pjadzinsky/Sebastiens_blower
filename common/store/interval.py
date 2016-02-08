""" Persistent data store indexed by time interval.  

Data store is s3 and time interval index provided by kairos.

Restrictions:
 * Assumes that every interval item from same series has a unique start time 
   Allows simple mapping to unique keys for s3.
"""

from common.log import *
from common.store import kairos
from common.store import s3
from common.utils import net
from common.utils import utime
import pprint
from common import settings

def get_metric_name(data_name):
    metric_name = 'interval_store.%s.event' % (data_name)
    return metric_name


def get_s3_key(id, data_name, version, start_time_sec):
    # convert time in sec to a large integer (milliseconds)
    start_time_msec = int(start_time_sec * 1000.0)
    # reversed milliseconds as a integer (first few characters change a lot so
    # keys are spread evenly over s3 bucket partitions
    s3_key_prefix = str(start_time_msec)[::-1]
    s3_key_name = '%s_%s_%s_%s' % (s3_key_prefix, data_name, version, id)
    return s3_key_name


class DirectSerializer(object):

    def __init__(self):
        return

    def serialize(self, data):
        return data

    def deserialize(self, data):
        return data


class ProtoSerializer(object):

    def __init__(self, proto_cls):
        self._proto_cls = proto_cls
        return

    def serialize(self, proto):
        CHECK_EQ(proto.__class__, self._proto_cls)
        data = proto.SerializeToString()
        return data

    def deserialize(self, data):
        proto = self._proto_cls()
        proto.ParseFromString(data)
        return proto


class IntervalStoreWriter(object):

    def __init__(self, data_name, version, s3_bucket_suffix, serializer=DirectSerializer()):
        self._data_name = data_name
        self._version = version
        self._bucket_name = 'mousera-' + s3_bucket_suffix
        self._serializer = serializer
        self._kairos = kairos.KairosWriter(settings.KAIROS2_EXTERNAL_HOST)
        self._s3 = s3.S3Dictionary(self._bucket_name)
        return

    def write(self, id, time_interval, data_item):
        CHECK_EQ(type(time_interval), utime.Interval)
        CHECK(time_interval.is_finite())
        CHECK_NOTNONE(data_item)
        # construct the tags list
        tags = {'id': id, 'bucket_name': self._bucket_name,
                'version': self._version}
        # write interval end-point entries to kairos
        metric_name = get_metric_name(self._data_name)
        tags['event'] = 'start'
        self._kairos.write(metric_name, time_interval.start_time_sec, time_interval.end_time_sec, tags)
        tags['event'] = 'end'
        self._kairos.write(metric_name, time_interval.end_time_sec, time_interval.start_time_sec, tags)
        # write data blob to s3
        s3_key = get_s3_key(id, self._data_name, self._version, time_interval.start_time_sec)
        data = self._serializer.serialize(data_item)
        
        # TODO(heathkh): create a map-reduce over the interval store and add metadata where they are missing to allow easier switching of range-query backing stores
        
        metadata = {'name' : str(self._data_name), 
                    'version' : str(self._version),
                    'id': str(id),
                    'start_time_sec' : str(time_interval.start_time_sec),
                    'end_time_sec' : str(time_interval.end_time_sec),
                    }
        self._s3.set(s3_key, data, metadata=metadata)
        return


class IntervalDataItem(object):

    """ Data attached to a time interval. 

    A client can look at the time range associated by this data item and
    decide if the potentially large data should be retrieved by calling fetch().
    """

    def __init__(self, interval, s3_bucket, s3_key, serializer):
        CHECK_EQ(type(interval), utime.Interval)
        self.interval = interval
        
        # TODO(heathkh): create a map-reduce over the interval store and update the bucket name in the kairos store, rather than at access time
        bucket_name_remap = {'mousera-queue-processors-slab-video' : 'mousera-queue-processors-slab-video-us-west-1'}
        
        if s3_bucket in bucket_name_remap:
            s3_bucket = bucket_name_remap[s3_bucket]
        
        self._s3_bucket = s3_bucket
        self._s3_key = s3_key
        self._serializer = serializer
        return

    def generate_url(self):
        d = s3.WebInterface(self._s3_bucket)
        url = d.create_public_url(self._s3_key)
        return url

    def fetch(self):
        # actually retrieves the value for the key
        d = s3.S3Dictionary(self._s3_bucket)
        data = d[self._s3_key]
        item = None
        if data:
            item = self._serializer.deserialize(data)
        return item
    
    def metadata(self):
        d = s3.S3Dictionary(self._s3_bucket)
        key = d.get_key(self._s3_key)
        metadata = {}
        metadata['id'] = key.get_metadata('id')
        metadata['name'] = key.get_metadata('name')
        metadata['version'] = key.get_metadata('version')
        metadata['start_time_sec'] = key.get_metadata('start_time_sec')
        metadata['end_time_sec'] = key.get_metadata('end_time_sec')
        return metadata 
        

    def fetch_to_file(self, dest_filename):
        # Atomic fetch
        fs = s3.S3FileStore(self._s3_bucket)
        writing_filename = '%s.writing' % (dest_filename)
        fs.download(self._s3_key, writing_filename)
        os.rename(writing_filename, dest_filename)
        return

    def _cached_filename(self):
        return '%s/intervaldata/%s/%s-%s-%s.cache' % (net.MOUSERA_TMP_DOWNLOAD, self._s3_bucket, self._s3_key, self.interval.start_time_sec, self.interval.end_time_sec)

    def get_cached_file(self):
        filename = self._cached_filename()

        if not os.path.exists(filename):
            net.make_parent_dirs(filename)
            self.fetch_to_file(filename)
        CHECK(os.path.exists(filename))
        return filename

    def __str__(self):
        return '<%s  %s %s>' % (self.interval, self._s3_bucket, self._s3_key)


class IntervalStoreReader(object):

    def __init__(self, data_name, version, serializer=DirectSerializer()):
        self._data_name = data_name
        readers = []
        readers.append(kairos.KairosReader(settings.KAIROS2_EXTERNAL_HOST))
        self._kairos = kairos.KairosMultiReader(readers)
        self._version = version
        self._serializer = serializer
        return

    def query(self, id, query_interval, max_interval_sec=600, debug=False):
        """ Returns items that intersect query time range. 

        args:
          max_interval_sec: guarantee all overlapping intervals smaller than this size will be found...   
        """
        CHECK_EQ(type(query_interval), utime.Interval)
        metric_name = get_metric_name(self._data_name)
        tags = {'id': id, 'version': self._version, 'event': ['start', 'end']}
        group_by = ['id', 'event', 'bucket_name']

        dilated_start_sec = query_interval.start_time_sec - max_interval_sec
        dilated_end_sec = query_interval.end_time_sec + max_interval_sec
        
        result_groups = self._kairos.read(
            metric_name, dilated_start_sec, dilated_end_sec, tags, group_by)

        if debug:
            print metric_name, dilated_start_sec, dilated_end_sec, tags, group_by
            pprint.pprint(result_groups, depth=3)

        # an data-item could be detected by inclusion of it's start or end event or both
        # use the dictionary keyed by data-item start time to handle case where
        # both start and end events are inside query interval
        data_items = {}
        # pretty(result_groups)
        for group in result_groups:
            if 'group_by' not in group:
                continue

            event = group['tags']['event'][0]
            bucket = group['tags']['bucket_name'][0]

            for value in group['values']:
                if event == 'start':
                    s, e = value
                elif event == 'end':
                    e, s = value
                else:
                    raise RuntimeError()
                cur_interval = utime.Interval(s, e)
                
                # because kairos query interval was dilated, need to check
                # intersection with original query interval
                if query_interval.intersection(cur_interval) is None:
                    continue

                s3_key = get_s3_key(id, self._data_name, self._version, s)
                new_item = IntervalDataItem(cur_interval, bucket, s3_key, 
                                            self._serializer)

                if cur_interval.start_time_sec not in data_items:
                    data_items[cur_interval.start_time_sec] = new_item
                else:
                    # Check invariant of the design... detection of both start and
                    # end events should resolve to exact same s3 key
                    CHECK_EQ(
                        data_items[cur_interval.start_time_sec]._s3_key, s3_key)

        data_item_list = list(data_items.itervalues())
        data_item_list.sort(key=lambda x: x.interval.start_time_sec)
        return data_item_list


class ProtoIntervalStoreReader(IntervalStoreReader):

    def __init__(self, proto_cls, data_name, version):
        """ Read an interval store where items are protobuffers. 
        Args
            proto_cls: the protobuffer class name
            data_name: unique name for this data type -> transformed into a karios metric name
        """
        super(ProtoIntervalStoreReader, self).__init__(
            data_name, version, ProtoSerializer(proto_cls))
        return


class ProtoIntervalStoreWriter(IntervalStoreWriter):

    def __init__(self, proto_cls, data_name, version, s3_bucket_suffix):
        super(ProtoIntervalStoreWriter, self).__init__(
            data_name, version, s3_bucket_suffix, ProtoSerializer(proto_cls))
        return
