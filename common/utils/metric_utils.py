import numpy as np

def to_numpy(metric_output):
    ''' Convert 'values' in  MetricRow object as returned by
    MetricClient.get_metric() from list of lists to numpy array
    '''
    for m in metric_output:
        m['values'] = np.array(m['values'])
        m.values = np.array(m.values)
    return metric_output

def merge(metric_output_list):
    ''' metric_output_list is a list of objects as returned by MetricClient
    This is intended to be used with something like mus.Subject.metrics, that 
    returns a list of get_metric objects, one per time_interval
    
    as of 2015/12/21 MetricClient returns a list of common.metrics.metric_data.MetricRaw
    objects, one per metri_name
    
    if any two MetricRaw objects have the same 'metric_name' and 'tags', merge
    their values
    ''' 
    
    output = metric_output_list[0]
    for metrics_raw in metric_output_list[1:]:
        for one_metric in metrics_raw:
            output_keys = [(m.metric_name, m.tags) for m in output]
            
            if (one_metric.metric_name, one_metric.tags) in output_keys:
                ''' merge mraw['values'] to corresponding values in output '''
                index = output_keys.index( (one_metric.metric_name, 
                                            one_metric.tags) )
                
                np.append(output[index]['values'], one_metric['values'])
            else:
                output.append(one_metric)
                
    ''' Sort data according to times '''
    return output


