import re

aggregate_types = ["mean",
                "max",
                "min",
                "count",
                "sd",
                "sum",
                "first",
                "last"]



def parse_aggregation(metric_name):
    """
    Parses a metric name to determine if it's an aggregate metric and, if so, its aggregation type & length
    :param metric_name: name of metric
    :return: (unaggergated_metric_name, full_aggregation_suffix, aggregation_type, aggregation_seconds). If not an aggregate,
        returns (metric_name, None, None, None)

    """
    suffixes_re = "(\.([0-9]{1,})s\.(" + "|".join(aggregate_types) + "))$"
    m = re.match("(.*)" + suffixes_re + "$", metric_name)
    if m:
        groups = m.groups()
        return (groups[0], groups[1], groups[3], int(groups[2]))
    return (metric_name, None, None, None)


def get_metric_name(unaggregated_metric_name, aggregation_type, aggregation_seconds):
    if aggregation_type:
        assert aggregation_type in aggregate_types
        return "%s.%ds.%s" % (unaggregated_metric_name, aggregation_seconds, aggregation_type)
    return unaggregated_metric_name


def get_all_aggregate_metric_names(unaggregated_metric_name, aggregation_seconds):
    return [get_metric_name(unaggregated_metric_name,t,aggregation_seconds) for t in aggregate_types]