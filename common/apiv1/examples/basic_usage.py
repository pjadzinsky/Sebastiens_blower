from __future__ import unicode_literals

import sys
import logging
import datetime

from common.utils import utime
from common.apiv1.event_application import EventApplication
logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)


# Values retrieved from /admin/events/application/
# Your keys should be inserted here
CONFIG = {
    'application_id': 1,
    'secret_key': '6ae3c47b-d7be-4264-b43c-f8e373ff5f99'
}

# Example of how to publish events with the Event Application class

if __name__ == '__main__':
    eapp = EventApplication(**CONFIG)

    event = {
        'actor': {'content_type': 'cage', 'id': 1000},
        'target': {'content_type': 'cage', 'id': 1000},
        'event_type': 'annotation.created',
        'event_time': datetime.datetime.now().replace(tzinfo=utime._TZINFOS['-10:00']),
        'priority': 'info',
    }
    eapp.create_event(event)