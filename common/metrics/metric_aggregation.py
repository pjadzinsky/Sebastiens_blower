import copy
import math
import datetime
from common.metrics import metric_name_parsing
from common.metrics import metric_time
from common.metrics import metric_data
from collections import defaultdict

aggregation_levels = [60, 600, 3600, 21600]
aggregation_levels_descending = aggregation_levels[::-1]

#aggregates needed to calculate next aggregation level
prev_aggregates_needed = {
    "mean":["sum","count","mean"],  #need mean, as absense of mean in previous level implies presence of non-numerics which throws off count
    "max":["max"],
    "min":["min"],
    "count":["count"],
    "sd":["sd", "mean", "count"],
    "sum":["sum"],
    "first":["first"],
    "last":["last"]
}

def agg_mean(samples):
    return sum(samples)/len(samples)

def agg_max(samples):
    return max(samples, key=float)

def agg_min(samples):
    return min(samples, key=float)

def agg_count(samples):
    return len(samples)

def agg_sd(samples, mean):
    variance = 0
    for sample in samples:
        diff = (sample - mean)
        variance += (diff*diff)
    variance /= len(samples)
    return math.sqrt(variance)

def agg_sum(samples):
    return sum(samples)



def calc_variance_numerator_term(orig_sd, orig_count, orig_mean, new_mean):
    orig_variance = orig_sd * orig_sd
    orig_variance_numerator_term = orig_variance * orig_count

    mean_diff = orig_mean - new_mean
    numerator_diff = orig_count * (mean_diff * mean_diff)

    return orig_variance_numerator_term + numerator_diff

def dict_to_sorted_list_of_lists(the_dict):
    return [[k, the_dict[k]] for k in sorted(the_dict)]

def firstlast_dict_to_sorted_list_of_lists(the_dict):
    return [[the_dict[k][0], the_dict[k][1]] for k in sorted(the_dict)]

def aggregate_raw_metrics(data, start_date, end_date, aggregator_names, dest_aggregation_seconds):
    start_timestamp = metric_time.get_timestamp(start_date)
    end_timestamp = metric_time.get_timestamp(end_date)
    counts = defaultdict(lambda : defaultdict(int))
    sums = defaultdict(lambda : defaultdict(float))
    maxes = defaultdict(lambda : defaultdict(lambda : float('-inf')))
    mins = defaultdict(lambda : defaultdict(lambda : float('inf')))
    firsts = defaultdict(lambda : defaultdict(lambda : (int,None)))   #(timestamp, value)
    lasts = defaultdict(lambda : defaultdict(lambda : (0,None)))   #(timestamp, value)
    all_values_numeric = defaultdict(lambda : defaultdict(lambda: True))  #whether all values encountered were numeric

    for metric_row in data:
        unaggregated_metric_name = metric_row['metric_name']
        assert metric_name_parsing.parse_aggregation(unaggregated_metric_name) == (unaggregated_metric_name, None, None, None)  #This only works for raw unaggregated metrics. Use aggregate_preaggregated_metrics instead.
        key = get_merge_key(metric_row)
        key_counts = counts[key]
        key_sums = sums[key]
        key_maxes = maxes[key]
        key_mins = mins[key]
        key_firsts = firsts[key]
        key_lasts = lasts[key]
        key_all_values_numeric = all_values_numeric[key]

        for value in metric_row['values']:
            timestamp = value[0]
            if start_timestamp <= timestamp < end_timestamp:
                rounded_down_timestamp = round_down(timestamp, start_timestamp, dest_aggregation_seconds)
                key_counts[rounded_down_timestamp] += 1

                value = value[1]
                if not rounded_down_timestamp in key_firsts or key_firsts[rounded_down_timestamp][0] > timestamp:
                    key_firsts[rounded_down_timestamp] = (timestamp, value)
                if timestamp > key_lasts[rounded_down_timestamp][0]:
                    key_lasts[rounded_down_timestamp] = (timestamp, value)
                try:
                    f_value = float(value)
                    key_sums[rounded_down_timestamp] += f_value
                    key_maxes[rounded_down_timestamp] = max(key_maxes[rounded_down_timestamp], f_value)
                    key_mins[rounded_down_timestamp] = min(key_mins[rounded_down_timestamp], f_value)
                except ValueError:
                    #non-numeric value - just skip the numerical aggregations
                    key_all_values_numeric[rounded_down_timestamp] = False
                    pass

    #convert the accumulated values into results
    results = prepare_results(aggregator_names, dest_aggregation_seconds, counts, sums, maxes, mins, firsts, lasts, all_values_numeric)

    if 'sd' in aggregator_names:
        variances = defaultdict(lambda : defaultdict(lambda : 0))
        for metric_row in data:
            key = get_merge_key(metric_row)
            key_totals = sums[key]
            key_counts = counts[key]
            key_variances = variances[key]
            for value in metric_row['values']:
                timestamp = value[0]
                if start_timestamp <= timestamp < end_timestamp:
                    rounded_down_timestamp = round_down(timestamp, start_timestamp, dest_aggregation_seconds)
                    try:
                        f_value = float(value[1])
                        diff_from_mean = f_value - key_totals[rounded_down_timestamp]/key_counts[rounded_down_timestamp]
                        key_variances[rounded_down_timestamp] += diff_from_mean*diff_from_mean
                    except ValueError:
                        #non-numeric values - just skip
                        pass

        for key in variances:
            key_metric_name, key_tags = inflate_merge_key(key)
            sd_metric_name = metric_name_parsing.get_metric_name(key_metric_name, 'sd', dest_aggregation_seconds)
            values = []
            key_counts = counts[key]
            key_variances = variances[key]
            key_all_values_numeric = all_values_numeric[key]
            for timestamp in key_variances:
                if key_all_values_numeric[timestamp]: #counts contain non-numeric values as well, so don't calculate sd
                    values.append([timestamp, math.sqrt(key_variances[timestamp]/key_counts[timestamp])])
            results.append ({'metric_name': sd_metric_name,
                             'tags':key_tags,
                             'values': values})

    return results


def prepare_results(aggregator_names, dest_aggregation_seconds, counts, sums, maxes, mins, firsts, lasts, all_values_numeric):
    assert not isinstance(aggregator_names, basestring) #need a list, not a string
    results = []
    if 'count' in aggregator_names:
        for key in counts:
            key_metric_name, key_tags = inflate_merge_key(key)
            count_metric_name = metric_name_parsing.get_metric_name(key_metric_name, 'count', dest_aggregation_seconds)
            results.append({'metric_name': count_metric_name,
                            'tags': key_tags,
                            'values': dict_to_sorted_list_of_lists(counts[key])})
    if 'sum' in aggregator_names:
        for key in sums:
            key_metric_name, key_tags = inflate_merge_key(key)
            sum_metric_name = metric_name_parsing.get_metric_name(key_metric_name, 'sum', dest_aggregation_seconds)
            results.append({'metric_name': sum_metric_name,
                            'tags': key_tags,
                            'values': dict_to_sorted_list_of_lists(sums[key])})
    if 'max' in aggregator_names:
        for key in maxes:
            key_metric_name, key_tags = inflate_merge_key(key)
            max_metric_name = metric_name_parsing.get_metric_name(key_metric_name, 'max', dest_aggregation_seconds)
            results.append({'metric_name': max_metric_name,
                            'tags': key_tags,
                            'values': dict_to_sorted_list_of_lists(maxes[key])})
    if 'min' in aggregator_names:
        for key in mins:
            key_metric_name, key_tags = inflate_merge_key(key)
            min_metric_name = metric_name_parsing.get_metric_name(key_metric_name, 'min', dest_aggregation_seconds)
            results.append({'metric_name': min_metric_name,
                            'tags': key_tags,
                            'values': dict_to_sorted_list_of_lists(mins[key])})

    if 'mean' in aggregator_names:
        for key in sums:
            key_metric_name, key_tags = inflate_merge_key(key)
            mean_metric_name = metric_name_parsing.get_metric_name(key_metric_name, 'mean', dest_aggregation_seconds)
            values = []
            key_counts = counts[key]
            key_sums = sums[key]
            key_all_numeric = all_values_numeric[key]
            if len(key_sums) != len(key_counts):
                print "Warning: Different number of sums (%d) and counts (%d)" % (len(key_sums), len(key_counts))
            for timestamp in key_sums:
                try:
                    if key_all_numeric[timestamp]: #if there are non-numeric values, we can't rely on 'counts' for avg calculation so skip it
                        values.append([timestamp, key_sums[timestamp] / key_counts[timestamp]])
                except Exception as e:
                    #In theory we should have key_counts[timestamp] whenever we have key_sums[timestamp].
                    #In practice, it appears Kairos queries sometimes drop datapoints so skip timestamps when we don't have
                    #Everything we need.
                    print "Exception '%s' while preparing aggregations: %s[%s]. Skipping timestamp %d, " % (e, key_metric_name,key_tags,timestamp)
            values.sort(key=lambda x: x[0])
            results.append({'metric_name': mean_metric_name,
                            'tags': key_tags,
                            'values': values})

    if 'first' in aggregator_names:
        for key in firsts:
            key_metric_name, key_tags = inflate_merge_key(key)
            first_metric_name = metric_name_parsing.get_metric_name(key_metric_name, 'first', dest_aggregation_seconds)
            results.append({'metric_name': first_metric_name,
                            'tags': key_tags,
                            'values': firstlast_dict_to_sorted_list_of_lists(firsts[key])})

    if 'last' in aggregator_names:
        for key in lasts:
            key_metric_name, key_tags = inflate_merge_key(key)
            last_metric_name = metric_name_parsing.get_metric_name(key_metric_name, 'last', dest_aggregation_seconds)
            results.append({'metric_name': last_metric_name,
                            'tags': key_tags,
                            'values': firstlast_dict_to_sorted_list_of_lists(lasts[key])})


    return results


def get_merge_key(metric_row):
    metric_name = metric_row[metric_data.METRIC_NAME_KEY]
    unaggregated_metric_name, _, _, _ = metric_name_parsing.parse_aggregation(metric_name)
    return (unaggregated_metric_name, frozenset(metric_row[metric_data.METRIC_TAGS_KEY].items()))


def inflate_merge_key(merge_key):
    tags_frozenset = merge_key[1]
    tags = {}
    for tag in tags_frozenset:
        tags[tag[0]] = tag[1]
    return merge_key[0], tags

def round_down(timestamp, agg_start_timestamp, agg_seconds):
    agg_milliseconds = agg_seconds*1000
    return agg_start_timestamp + ((timestamp - agg_start_timestamp) // agg_milliseconds) * agg_milliseconds


def aggregate_preaggregated_metrics(aggregated_metrics, aggregation_types, dest_aggregation_seconds, start_timestamp,
                                    end_timestamp):
    counts = defaultdict(lambda : defaultdict(int))
    sums = defaultdict(lambda : defaultdict(float))
    maxes = defaultdict(lambda : defaultdict(lambda : float('-inf')))
    mins = defaultdict(lambda : defaultdict(lambda : float('inf')))
    firsts = defaultdict(lambda : defaultdict(lambda : (int,None)))   #(timestamp, value)
    lasts = defaultdict(lambda : defaultdict(lambda : (0,None)))   #(timestamp, value)
    old_means = defaultdict(lambda : defaultdict(float))
    old_counts = defaultdict(lambda : defaultdict(float))
    all_values_numeric = defaultdict(lambda : defaultdict(lambda: True))  #whether all values encountered were numeric

    for metric_row in aggregated_metrics:
        metric_name = metric_row['metric_name']
        key = get_merge_key(metric_row)
        (parsed_unaggergated_metric_name, full_aggregation_suffix, aggregation_type, aggregation_seconds) = metric_name_parsing.parse_aggregation(metric_name)
        if aggregation_type == 'count':
            key_counts = counts[key]
            key_old_counts = old_counts[key]
            for value in metric_row['values']:
                timestamp = value[0]
                if start_timestamp <= timestamp < end_timestamp:
                    key_old_counts[timestamp] = value[1]
                    rounded_down_timestamp = round_down(timestamp, start_timestamp, dest_aggregation_seconds)
                    key_counts[rounded_down_timestamp] += value[1]
        elif aggregation_type == 'max':
            key_maxes = maxes[key]
            for value in metric_row['values']:
                timestamp = value[0]
                if start_timestamp <= timestamp < end_timestamp:
                    rounded_down_timestamp = round_down(timestamp, start_timestamp, dest_aggregation_seconds)
                    key_maxes[rounded_down_timestamp] = max(key_maxes[rounded_down_timestamp], value[1])
        elif aggregation_type == 'min':
            key_mins = mins[key]
            for value in metric_row['values']:
                timestamp = value[0]
                if start_timestamp <= timestamp < end_timestamp:
                    rounded_down_timestamp = round_down(timestamp, start_timestamp, dest_aggregation_seconds)
                    key_mins[rounded_down_timestamp] = min(key_mins[rounded_down_timestamp], value[1])
        elif aggregation_type == 'sum':
            key_sums = sums[key]
            for value in metric_row['values']:
                timestamp = value[0]
                if start_timestamp <= timestamp < end_timestamp:
                    rounded_down_timestamp = round_down(timestamp, start_timestamp, dest_aggregation_seconds)
                    key_sums[rounded_down_timestamp] += value[1]
        elif aggregation_type == 'first':
            key_firsts = firsts[key]
            for value in metric_row['values']:
                timestamp = value[0]
                if start_timestamp <= timestamp < end_timestamp:
                    rounded_down_timestamp = round_down(timestamp, start_timestamp, dest_aggregation_seconds)
                    if not rounded_down_timestamp in key_firsts or key_firsts[rounded_down_timestamp][0] > timestamp:
                        key_firsts[rounded_down_timestamp] = (timestamp, value[1])
        elif aggregation_type == 'last':
            key_lasts = lasts[key]
            for value in metric_row['values']:
                timestamp = value[0]
                if start_timestamp <= timestamp < end_timestamp:
                    rounded_down_timestamp = round_down(timestamp, start_timestamp, dest_aggregation_seconds)
                    if timestamp > key_lasts[rounded_down_timestamp][0]:
                        key_lasts[rounded_down_timestamp] = (timestamp, value[1])
        elif aggregation_type == 'mean':
            key_old_means = old_means[key]
            for value in metric_row['values']:
                timestamp = value[0]
                if start_timestamp <= timestamp < end_timestamp:
                    key_old_means[timestamp] = value[1]

    #figure out whether sub-aggregates contained all numeric values
    for key in old_counts:
        key_old_means = old_means[key]
        for timestamp in old_counts[key]:
            #a 'count' without a 'mean' says the subaggregate contains non-numeric values
            if timestamp not in key_old_means:
                rounded_down_timestamp = round_down(timestamp, start_timestamp, dest_aggregation_seconds)
                all_values_numeric[key][rounded_down_timestamp] = False


    #convert the accumulated values into results
    results = prepare_results(aggregation_types, dest_aggregation_seconds, counts, sums, maxes, mins, firsts, lasts, all_values_numeric)

    #calculate sds
    if 'sd' in aggregation_types:
        #calculate the numerators of the variances
        variance_numerators = defaultdict(lambda : defaultdict(float))
        for metric_row in aggregated_metrics:
            metric_name = metric_row['metric_name']
            key = get_merge_key(metric_row)
            (parsed_unaggergated_metric_name, full_aggregation_suffix, aggregation_type, aggregation_seconds) = metric_name_parsing.parse_aggregation(metric_name)
            if aggregation_type == 'sd':
                key_sums = sums[key]
                key_counts = counts[key]
                key_variance_numerators = variance_numerators[key]
                key_old_counts = old_counts[key]
                key_old_means = old_means[key]
                for value in metric_row['values']:
                    timestamp = value[0]
                    if start_timestamp <= timestamp < end_timestamp:
                        rounded_down_timestamp = round_down(timestamp, start_timestamp, dest_aggregation_seconds)
                        mean = key_sums[rounded_down_timestamp] / key_counts[rounded_down_timestamp]
                        f_value = float(value[1])
                        key_variance_numerators[rounded_down_timestamp] += calc_variance_numerator_term(f_value, key_old_counts[timestamp], key_old_means[timestamp], mean )

        for key in variance_numerators:
            key_metric_name, key_tags = inflate_merge_key(key)
            sd_metric_name = metric_name_parsing.get_metric_name(key_metric_name, 'sd', dest_aggregation_seconds)
            values = []
            key_counts = counts[key]
            key_variances = variance_numerators[key]
            for timestamp in key_variances:
                values.append([timestamp, math.sqrt(key_variances[timestamp]/key_counts[timestamp])])
            values.sort(key=lambda x: x[0])
            results.append ({'metric_name': sd_metric_name,
                             'tags':key_tags,
                             'values': values})

    return results
