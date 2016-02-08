# encoding: utf-8

import logging
from datetime import datetime
from urlparse import urljoin
import socket

from common.utils import utime
from common import settings
from common import apiv1

logger = logging.getLogger(__name__)

# Example schema
#
# EVENT_SCHEMA = {
#     'actor': {
#         'type': 'dict',
#         'required': True,
#         'schema': {
#             'id': {'type': 'integer', 'required': True},
#             'content_type': {'type': 'string', 'required': True}
#         }
#     },
#     'target': {
#         'type': 'dict',
#         'required': True,
#         'schema': {
#             'id': {'type': 'integer', 'required': True},
#             'content_type': {'type': 'string', 'required': True}
#         }
#     },
#     'event_type': {'type': 'string'},
#     'event_time': {'type': 'string'},
#     'measurement': {'type': 'float'},
#     'host': {'type': 'string'},
#     'priority': {
#         'type': 'string',
#         'allowed': ['emergency', 'critical', 'error', 'warning', 'notice', 'info']
#     },
#     'data': {
#         'type': 'dict',
#         'required': False
#     }
# }
#


class EventApplication(object):
    def __init__(self, application_id=None, secret_key=None):
        self.application_id = application_id
        self.secret_key = secret_key

    def get_headers(self):
        return {
            'MOUSERA-APP-ID': self.application_id,
            'MOUSERA-ACCESS-KEY': self.secret_key,
            'Accept': 'application/json'
        }

    def get_hostname(self):
        """
        Retrieves the current system hostname

        Returns:
            string of the current system hostname
        """
        if not hasattr(self, '_hostname'):
            try:
                self._hostname = socket.gethostname()
            except:
                self._hostname = 'localhost'

        return self._hostname

    def serialize_payload(self, payload):
        """Helper method to serialize the payload"""

        if isinstance(payload.get('event_time', None), datetime):
            event_time = payload.get('event_time')
            assert utime.is_aware(event_time), 'event_time must be timezone-aware: %s' % event_time

            # Convert a datetime object into string
            payload['event_time'] = event_time.isoformat()

        payload.update({
            'hostname': self.get_hostname(),
        })

        return payload

    def create_event(self, evt):
        """
        Creates an event and posts it to the API server.

        Args:
            evt(dict): event dictionary object.

            Example dictionary object:
                {
                    'actor': {
                        'id': 922,
                        'content_type': 'application'
                    },
                    'target': {
                        'id': 532,
                        'content_type': 'cage'
                    },
                    'event_type': 'annotation.created',  # EventType.name
                    'event_time': '2015-11-29T10:27:30+00:00',
                    'measurement': null,
                    'host': 'my.hostname',
                    'priority': 'info'
                }

        Returns:
            response (dict): completed response data dictionary

        Raises:
            requests.exceptions.HTTPError: http errors for 4xx and 5xx errors.

        """

        payload = self.serialize_payload(evt.copy())
        return apiv1.request_json('/event', 'POST', data_dict=payload, headers=self.get_headers())
