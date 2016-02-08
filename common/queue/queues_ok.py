""" Are queues backed up?  Shell-callable utility. """

# THIS SHOULD BE LIBRARY-CALLABLE and otherwise fixed up

# example use

# while `true`; do
#     if `python -m common/queue/queues_full`; then
#         echo queueing
#         python -m common/queue/requeue mousera-slab-data-backup mousera-slab-data 1000
#      fi
#      echo 'sleeping'
#      sleep 60
# done

# rename to queues?
# accept arguments?

import sys
import boto
import boto.sqs

# from common.store.s3 import S3Dictionary
# state = S3Dictionary('mousera-slab-data-import-state')

conn = boto.sqs.connect_to_region("us-west-1")
msd_queue = conn.get_queue('mousera-slab-data')
aggregation_queue = conn.get_queue('mousera-metric-aggregation')
backfill_queue = conn.get_queue('mousera-slab-video-backfill')
video_queue = conn.get_queue('mousera-slab-video-backfill')

def queue_size(q):
    attrib = q.get_attributes('ApproximateNumberOfMessages')
    return int(attrib['ApproximateNumberOfMessages'])

def queues_ok():
    if queue_size(msd_queue) > 2000:
        print 'slab data too full'
        return False
    # if queue_size(aggregation_queue) > 50000:
    #     print 'agg too full'
    #     return False
    # if queue_size(backfill_queue) > state['backfill_queue_limit']:
    #     return False
    # if queue_size(video_queue) > 10000:
    #     return False
    return True

if not queues_ok():
    sys.exit(1)
sys.exit(0)
