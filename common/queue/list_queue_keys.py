# Helper to resent messages from one queue to another.  e.g. resending dead letters:
# .ve/bin/python -m common.queue.requeue mousera-slab-video-dead-letters mousera-slab-video 1000

import sys
from time import sleep
import json

from common.queue import queue

source      = queue.MessageQueue(sys.argv[1])
INNER_COUNT = 10
TOTAL_COUNT = int(sys.argv[2])

# print "Listing:", TOTAL_COUNT, "keys from:", sys.argv[1]
while TOTAL_COUNT > 0:
    # print "Messages remaining:", TOTAL_COUNT
    # print "Gathering 10 messages."
    ms = source.dequeue(INNER_COUNT)
    for m in ms:
        b = m.get_body()
        j = json.loads(b)
        # print json.dumps(j, indent=4); sys.exit(1)
        print j['Records'][0]['s3']['object']['key'], "\t",
        print j['Records'][0]['s3']['object']['size'], "\t", int(j['Records'][0]['s3']['object']['size']) / (1024 * 1024)
    TOTAL_COUNT -= INNER_COUNT
