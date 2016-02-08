from unittest import TestCase
from common.metrics import metric_source_naming


class MetricSourceNamingTestCase(TestCase):
    def test_decoration(self):
        self.assertEqual(metric_source_naming.decorate_source_name('ID', 'HELLO'), 'HELLO.ID.H')
        #Test that integer IDs work as well
        self.assertEqual(metric_source_naming.decorate_source_name(7, 'HELLO'), 'HELLO.7.H')

    def test_get_cage_source_id(self):
        self.assertEqual(metric_source_naming.get_cage_source(19), 'CAGE.19.C')

    def test_is_cage_source(self):
        '''is_cage_source should determine if a payload's source is from a cage.'''

        not_cage_source = '098591515'
        cage_payload = metric_source_naming.get_cage_source(10)
        
        self.assertTrue(metric_source_naming.is_cage_source(cage_payload))
        self.assertFalse(metric_source_naming.is_cage_source(not_cage_source))