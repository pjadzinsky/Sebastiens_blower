#Definitions and utilities for the basic metric data format used everywhere by the metrics system
# [ {'metric_name':<name1>, 'tags':{...}, 'values':[[timestamp, value], [timestamp, value]....]},
#   {'metric_name':<name2>, , 'tags':{...}, 'values':[[timestamp, value], [timestamp, value]....]},
#   ....
# ]
#Each element in the list is referred to as a 'row'.



METRIC_NAME_KEY = 'metric_name'
METRIC_VALUES_KEY = 'values'
METRIC_TAGS_KEY = 'tags'


#For convenience, a MetricRow is a dict that acts like a class
#You can access either like:
# row[METRIC_NAME_KEY]
# -or
# row.metric_name
class MetricRow(dict):
    def __init__(self, *args, **kwargs):
        super(MetricRow, self).__init__(*args, **kwargs)
        self.__dict__ = self

    @property
    def version(self):
        return get_version(self)

def append_row(data, metric_name, values, tags=None):
    data.append(MetricRow(metric_name=metric_name, values=values, tags=tags if tags else {}))
    return data


def create_metric_data(name=None, values=None, tags=None):
    data = []
    #For convenience, allow created a single-row structure with one function call
    if name:
        append_row(data, name, values, tags)
    return data


def get_version(row):
    #Gets the version for a row
    try:
        return float(row[METRIC_TAGS_KEY]['version'])
    except (KeyError, ValueError):
        return -1

