""" Are queues backed up?  Shell-callable utility. """

# THIS SHOULD BE LIBRARY-CALLABLE and otherwise fixed up

import sys
import boto
import boto.sqs
import json
import time

# from common.store.s3 import S3Dictionary
# state = S3Dictionary('mousera-slab-data-import-state')

conn = boto.sqs.connect_to_region("us-west-1")
aggregation_queue = conn.get_queue('mousera-metric-aggregation')
msd_queue = conn.get_queue('mousera-slab-data')
video_queue = conn.get_queue('mousera-slab-video')

def queue_size(q):
    attrib = q.get_attributes('ApproximateNumberOfMessages')
    return int(attrib['ApproximateNumberOfMessages'])


def queues_ok():
    # print queue_size(msd_queue)

    # if queue_size(msd_queue) > 2000:
    #     return False
    # if queue_size(aggregation_queue) > 5000:
    #     return False
    print queue_size(video_queue)
    if queue_size(video_queue) > 5000:
        return False
    return True


if not queues_ok():
    sys.exit(1)
sys.exit(0)
