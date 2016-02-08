# encoding: utf-8

from __future__ import unicode_literals

import unittest
import datetime

from common.apiv1.event_application import EventApplication
import requests_mock
import requests
from common.utils import utime

'''
# ZBS Removed until the event system is better integrated

@requests_mock.Mocker()
class ApplicationTestCase(unittest.TestCase):
    def setUp(self):
        self.app = lambda: EventApplication('test', 'test')

    # def test_request_headers(self, rmock):
    #     """EventApplication should have sent the correct app and access valid headers."""
    #
    #     rmock.register_uri(requests_mock.ANY, requests_mock.ANY)
    #
    #     hdrs = 'MOUSERA-APP-ID', 'MOUSERA-ACCESS-KEY'
    #     for hdr in hdrs:
    #         self.assertIn(hdr, self.app().session.headers)

    def test_app_create_event_valid(self, rmock):
        """create_event should publish valid events."""

        rmock.register_uri('POST', '/api/v1/event', status_code=201, json={'actor': {}, 'target': {}})
        app = self.app()
        app.create_event({})

    def test_app_create_event_errors(self, rmock):
        """create_event should raise an event not invalid events, with the validation error, if any."""

        err = {'target': 'This field is required'}
        rmock.register_uri('POST', '/api/v1/event', status_code=400, json=err)

        app = self.app()
        with self.assertRaises(requests.exceptions.HTTPError):
            app.create_event({'zomg': 'thisisbad'})

    def test_app_serialize_datetime(self, rmock):
        """serialize_payload should serialize a datetime into isoformat"""

        app = self.app()

        with self.assertRaises(AssertionError):
            # datetime should be tz-aware
            app.serialize_payload({
                'event_time': datetime.datetime.now()
            })
        event_time = datetime.datetime(2014, 1, 2, 3, 4, 5, tzinfo=utime._TZINFOS['+03:00'])
        got = app.serialize_payload({'event_time': event_time})
        self.assertEqual(got['event_time'], '2014-01-02T03:04:05+03:00')
'''