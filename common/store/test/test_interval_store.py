#!/usr/bin/python

import unittest
import time
from common.utils import utime
from common.log import *
from common.store import interval
import uuid
import common.settings
import os

bucket_name = 'test-scratch'


@unittest.skipIf(os.environ.get('KAIROS_NOT_AVAILABLE') or
                 hasattr(common.settings, 'CIRCLE_CI'), 
                 'SKIPPING: kairos is not available in this environment.')
class TestIntervalStore(unittest.TestCase):
        
    def test_single_entry(self):
        name = 'test'
        version = 1.0

        writer = interval.IntervalStoreWriter(name, version, bucket_name)
        reader = interval.IntervalStoreReader(name, version)

        id = str(uuid.uuid4()) # random id for each run
        data = "i am mock data"
        offset = 1429820106

        test_interval = utime.Interval(offset+10, offset+20)

        writer.write(id, test_interval, data)
        time.sleep(1) # wait for kairos write to complete
        max_interval_sec = 600

        # query interval same as test interval
        items = reader.query(id, test_interval, max_interval_sec)
        self.assertEqual(len(items), 1)
        item = items[0]
        self.assertEqual(item.fetch(), data)

        # query interval entirely inside test interval
        items = reader.query(id, utime.Interval(offset+12, offset+12.5), max_interval_sec)
        self.assertEqual(len(items), 1)
        item = items[0]
        self.assertEqual(item.fetch(), data)

        # test interval entirely inside query interval
        items = reader.query(id, utime.Interval(offset+2, offset+21), max_interval_sec)
        self.assertEqual(len(items), 1)
        item = items[0]
        self.assertEqual(item.fetch(), data)

        # query has tiny overlap on start of test interval
        items = reader.query(id, utime.Interval(offset+2, offset+10+1e-6), max_interval_sec)
        self.assertEqual(len(items), 1)
        item = items[0]
        self.assertEqual(item.fetch(), data)

        # query has tiny overlap on end of test interval
        items = reader.query(id, utime.Interval(offset+20-1e-6, offset+21), max_interval_sec)
        self.assertEqual(len(items), 1)
        item = items[0]
        self.assertEqual(item.fetch(), data)

        # query almost has overlap on start of test interval
        items = reader.query(id, utime.Interval(offset+2, offset+10-1e-6), max_interval_sec)
        self.assertEqual(len(items), 0)

        # query almost has overlap on end of test interval
        items = reader.query(id, utime.Interval(offset+20+1e-6, offset+21), max_interval_sec)
        self.assertEqual(len(items), 0)

        # query after test interval
        items = reader.query(id, utime.Interval(offset+30, offset+40), max_interval_sec)
        self.assertEqual(len(items), 0)

        # query before test interval
        items = reader.query(id, utime.Interval(offset+3, offset+9.5), max_interval_sec)
        self.assertEqual(len(items), 0)

        return

    
    def test_version(self):
        name = 'test'
        version_a = 1.0
        version_b = 1.1

        a_writer = interval.IntervalStoreWriter(name, version_a, bucket_name)
        a_reader = interval.IntervalStoreReader(name, version_a)

        b_writer = interval.IntervalStoreWriter(name, version_b, bucket_name)
        b_reader = interval.IntervalStoreReader(name, version_b)

        id = str(uuid.uuid4()) # random id for each run
        data_a = "i am data a"
        data_b = "i am data b"
        offset = 1429820106
        test_interval = utime.Interval(offset+10, offset+20)

        a_writer.write(id, test_interval, data_a)
        b_writer.write(id, test_interval, data_b)
        time.sleep(1) # wait for kairos write to complete
        max_interval_sec = 600

        # ensure a sees only a's result
        items = a_reader.query(id, test_interval, max_interval_sec)
        self.assertEqual(len(items), 1)
        item = items[0]
        self.assertEqual(item.fetch(), data_a)

        # ensure b sees only b's result
        items = b_reader.query(id, test_interval, max_interval_sec)
        self.assertEqual(len(items), 1)
        item = items[0]
        self.assertEqual(item.fetch(), data_b)
        return


    def test_name(self):

        name_a = 'test_a'
        name_b = 'test_b'
        version = 1.0

        a_writer = interval.IntervalStoreWriter(name_a, version, bucket_name)
        a_reader = interval.IntervalStoreReader(name_a, version)

        b_writer = interval.IntervalStoreWriter(name_b, version, bucket_name)
        b_reader = interval.IntervalStoreReader(name_b, version)

        id = str(uuid.uuid4()) # random id for each run
        data_a = "i am data a"
        data_b = "i am data b"
        offset = 1429820106
        test_interval = utime.Interval(offset+10, offset+20)

        a_writer.write(id, test_interval, data_a)
        b_writer.write(id, test_interval, data_b)
        time.sleep(1) # wait for kairos write to complete
        max_interval_sec = 600

        # ensure a sees only a's result
        items = a_reader.query(id, test_interval, max_interval_sec)
        self.assertEqual(len(items), 1)
        item = items[0]
        self.assertEqual(item.fetch(), data_a)

        # ensure b sees only b's result
        items = b_reader.query(id, test_interval, max_interval_sec)
        self.assertEqual(len(items), 1)
        item = items[0]
        self.assertEqual(item.fetch(), data_b)
        return



if __name__ == "__main__":
    unittest.main()
