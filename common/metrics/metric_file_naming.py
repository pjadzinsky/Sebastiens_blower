import datetime
from common.metrics import metric_name_parsing
from common.metrics import metric_time
import os
import pytz

SEPARATOR = '-'

file_lengths = [600, 3600, 21600]
for file_length in file_lengths:
    assert (datetime.timedelta(days=1).total_seconds() % file_length) == 0, "File lengths must be divisors of one day"


def get_metric_filename(metric_name, source_id, timestamp):
    unaggergated_metric_name, _, aggregation_type, aggregation_seconds = metric_name_parsing.parse_aggregation(metric_name)
    directory, filename = get_metric_filename_ext(source_id, metric_time.get_datetime(timestamp), unaggergated_metric_name, aggregation_type is not None, aggregation_seconds)
    return directory, filename

def get_metric_filename_ext(source_id, start_time, unaggergated_metric_name, is_aggregate, aggregation_seconds):
    assert not SEPARATOR in source_id and not SEPARATOR in unaggergated_metric_name
    file_length = get_file_length(aggregation_seconds, is_aggregate)
    # returns directory, filename
    #source id will be part of a path, so strip any invalid characters
    valid_path_source_id = ''.join(c for c in source_id if c in '_.() abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789')
    start_time = start_time.astimezone(pytz.utc)    #start_time must be timezone-aware as filenames use UTC time
    start_time = round_down_to_file_start_time(start_time, file_length)
    source_id_path_component = valid_path_source_id if valid_path_source_id else 'NO_SOURCE'
    directory = os.path.join(source_id_path_component[::-1], str(start_time.year), "%02d" % start_time.month,
                             "%02d" % start_time.day)
    filename = SEPARATOR.join(["%02d" % start_time.hour, "%02d" % start_time.minute, unaggergated_metric_name])
    if is_aggregate:
        directory = os.path.join(directory, "aggregate")
        filename = SEPARATOR.join([filename, "%ss" % file_length])
    return directory, filename


def get_file_length(aggregation_seconds, is_aggregate):
    if is_aggregate and aggregation_seconds >= file_lengths[0]:
        assert aggregation_seconds in file_lengths, "Unexpected aggregation level %d. Expecting one of %s" % (
        aggregation_seconds, file_lengths)
        file_length = aggregation_seconds
    else:
        # Unaggregated metrics are grouped into 10-minute files
        file_length = file_lengths[0]
    return file_length


def round_down_to_file_start_time(start_time, file_length):
    day_start = start_time.replace(hour=0, minute=0, second=0, microsecond=0)
    seconds_since_day_start = (start_time - day_start).total_seconds()
    seconds_to_aggregation_start = file_length * (int)((seconds_since_day_start / file_length))
    start_time = day_start + datetime.timedelta(seconds=seconds_to_aggregation_start)
    return start_time

def parse_metric_filename(filename):
    """
    Given a metric filename (or S3 key name), parse it to determine information about the metrics it contains
    :param filename: file or S3 key name
    :return: (source_id, start_time, unaggergated_metric_name, is_aggregate, file_length_seconds)
    """
    components = filename.split(os.sep)

    filename = components[-1]
    if components[-2] == 'aggregate':
        is_aggregate = True
        components = components[-6:-2]
    else:
        is_aggregate = False
        components = components[-5:-1]

    source_id = components[0][::-1]
    start_year = int(components[1])
    start_month = int(components[2])
    start_day = int(components[3])

    filename_components = filename.split(SEPARATOR)
    start_hour = int(filename_components[0])
    start_minute = int(filename_components[1])
    unaggergated_metric_name = filename_components[2]
    file_length_seconds = 600   #the default is 10 minutes
    if is_aggregate:
        assert len(filename_components) >= 4, "Aggregate files should have 4 '%c'-separated components in their filename" % SEPARATOR
        aggregation_time_component = filename_components[3]
        assert aggregation_time_component[-1] == 's', "Aggregate metric filenames should specify aggregation length in their names"
        file_length_seconds = int(aggregation_time_component[:-1])

    start_time = datetime.datetime(year=start_year, month=start_month, day=start_day, hour = start_hour, minute=start_minute, tzinfo=pytz.utc)

    return (source_id, start_time, unaggergated_metric_name, is_aggregate, file_length_seconds)


def get_first_aggregated_file_length():
    return file_lengths[0]

def get_prev_aggregated_file_length(current_file_length_seconds):
    curr_aggregation_level_index = file_lengths.index(current_file_length_seconds)
    if curr_aggregation_level_index > 0:
        return file_lengths[curr_aggregation_level_index  - 1]
    else:
        # We're at the shortest aggregated file
        return None

def get_next_aggregated_file_length(current_file_length_seconds):
    curr_aggregation_level_index = file_lengths.index(current_file_length_seconds)
    if curr_aggregation_level_index < len(file_lengths) - 1:
        return file_lengths[curr_aggregation_level_index + 1]
    else:
        # If we're at the top level, we're done
        return None
