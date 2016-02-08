# Helper to resent messages from one queue to another.  e.g. resending dead letters:
# .ve/bin/python -m common.queue.requeue mousera-slab-video-dead-letters mousera-slab-video 1000

from sys import argv
from time import sleep

from common.queue import queue

source      = queue.MessageQueue(argv[1], 'us-west-2')
destination = queue.MessageQueue(argv[2], 'us-west-2')
INNER_COUNT = 10
TOTAL_COUNT = int(argv[3])

print "Requeuing:", TOTAL_COUNT, "messages from:", argv[1], "->", argv[2]
while TOTAL_COUNT > 0:
    print "Messages remaining:", TOTAL_COUNT
    print "Gathering 10 messages."
    ms = source.dequeue(INNER_COUNT)
    for m in ms:
        print "  Sending:", m.get_body()
        destination.enqueue(m.get_body())
        print "    Deleting:", m.get_body()
        source.delete_message(m)

    TOTAL_COUNT -= INNER_COUNT
    # Didn't get any messages?  Exit.
    if not len(ms):
        TOTAL_COUNT = 0
        print "!!! Queue empty.  Exiting."

