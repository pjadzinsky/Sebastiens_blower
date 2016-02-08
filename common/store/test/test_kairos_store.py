#!/usr/bin/python

import unittest
from common import settings
from common.store import kairos
import common.settings
import os

bucket = 'test-scratch'

@unittest.skipIf(os.environ.get('KAIROS_NOT_AVAILABLE') or
                 hasattr(common.settings, 'CIRCLE_CI'), 
                 'SKIPPING: kairos is not available in this environment.')
class TestKairosStore(unittest.TestCase):


    def test_write_read(self):
        server = settings.KAIROS2_EXTERNAL_HOST
        writer = kairos.KairosWriter(server)
        time_sec = 150000.0
        # NOTE(heathkh): kairos read after write is not fast... use fixed time as work around
        tags = {'version': 1, 'id': 0}
        name = 'kairos_store.test'
        writer.write(name, time_sec, 100.001, tags=tags)
        reader = kairos.KairosReader(server)
        result = reader.read(name, time_sec-1.0, time_sec+1.0, tags=tags, group_by_tags=['id'])
        self.assertEqual(result[0]['values'], [[150000.0, 100.001]])
        return



if __name__ == "__main__":
    unittest.main()
