def get_topic_name(metric_name):
    # Can't have '.' in topic names...
    return metric_name.replace('.', '_')