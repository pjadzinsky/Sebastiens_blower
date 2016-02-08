#!/usr/bin/python
import StringIO
import unittest
from common.metrics.metric_file import MetricFile


class MetricFileTestCase (unittest.TestCase):
        def test_write_read_metrics(self):

            args = ["metric1", 34445, 1.0]
            tags = {'jim':'bob', 'billy':'joe'}
            self.write_read_and_compare(args, tags)

            #no tags
            args = ["metric1", 34445, 1.0]
            tags = None
            self.write_read_and_compare(args, tags)

            #non-float value
            args = ["metric1", 34445, "ggg"]
            tags = None
            self.write_read_and_compare(args, tags, True)

        def write_read_and_compare(self, args, tags_written, value_is_string = False):
            output_file = StringIO.StringIO()
            # Write a metric
            metric_file = MetricFile(output_file)
            if tags_written:
                metric_file.put_value(*args, **tags_written)
            else:
                metric_file.put_value(*args)
            # Read what we just wrote
            output_file.seek(0)
            (name, timestamp, value, tags_read) = metric_file.get_value()
            # Compare
            self.assertEqual(name, args[0])
            self.assertEqual(timestamp, args[1])
            self.assertAlmostEqual(value, args[2])  # floats might be slightly different after read/write
            if value_is_string:
                self.assertEquals(tags_read.pop('type'), 'string')
            self.assertEqual(cmp(tags_written if tags_written else {}, tags_read), 0)
            # No more data!
            self.assertIsNone(metric_file.get_value())

        def test_invalid_format(self):
            crap_file = StringIO.StringIO("what a bunch of junk")
            metric_file = MetricFile(crap_file)
            self.assertRaises(ValueError, metric_file.get_value)

            #invalid timestamp
            crap_file = StringIO.StringIO("put some sugar 3.533 tag=3")
            metric_file = MetricFile(crap_file)
            self.assertRaises(ValueError, metric_file.get_value)

            #non-float value
            crap_file = StringIO.StringIO("put some 3345 3.533s tag=3")
            metric_file = MetricFile(crap_file)
            _, _, value, tags = metric_file.get_value()
            self.assertEquals(value, '3.533s')
            self.assertEquals(tags, {'tag':'3'})

            #invalid tags
            crap_file = StringIO.StringIO("put some 3345 3.533 tag 3")
            metric_file = MetricFile(crap_file)
            self.assertRaises(ValueError, metric_file.get_value)

            #invalid tags too
            crap_file = StringIO.StringIO("put some 3345 3.533 tag=3=7")
            metric_file = MetricFile(crap_file)
            self.assertRaises(ValueError, metric_file.get_value)

        def test_get_some_tags(self):
            output_file = StringIO.StringIO()
            # Write a metric
            metric_file = MetricFile(output_file, tags_to_get='a')
            metric_name = 'the_metric'
            metric_file.put_value(metric_name, 234, 1.0, a='z', b='d')
            metric_file.put_value(metric_name, 235, 1.0, a='y')
            metric_file.put_value(metric_name, 236, 1.0, n='fdf')

            # Read what we just wrote
            output_file.seek(0)
            (name, timestamp, value, tags_read) = metric_file.get_value()
            self.assertEqual(len(tags_read), 1, "Expecting only one tag because tags_to_get is 'a'")
            self.assertEqual(tags_read['a'], 'z', "Expecting a = 'z'")
            (name, timestamp, value, tags_read) = metric_file.get_value()
            self.assertEqual(len(tags_read), 1, "Expecting only one tag because tags_to_get is 'a'")
            self.assertEqual(tags_read['a'], 'y', "Expecting a = 'y'")
            (name, timestamp, value, tags_read) = metric_file.get_value()
            self.assertEqual(len(tags_read), 0, "Expecting no tags because tags_to_get is 'a'")

            # No more data!
            self.assertIsNone(metric_file.get_value())


        def test_get_no_tags(self):
            output_file = StringIO.StringIO()
            # Write a metric
            metric_file = MetricFile(output_file, tags_to_get={})
            metric_name = 'the_metric'
            metric_file.put_value(metric_name, 234, 1.0, a='z', b='d')
            metric_file.put_value(metric_name, 235, 1.0, a='y')
            metric_file.put_value(metric_name, 236, 1.0, n='fdf')

            # Read what we just wrote
            output_file.seek(0)
            (name, timestamp, value, tags_read) = metric_file.get_value()
            self.assertEqual(len(tags_read), 0, "Expecting no tags because tags_to_get is {}")
            (name, timestamp, value, tags_read) = metric_file.get_value()
            self.assertEqual(len(tags_read), 0, "Expecting no tags because tags_to_get is {}")
            (name, timestamp, value, tags_read) = metric_file.get_value()
            self.assertEqual(len(tags_read), 0, "Expecting no tags because tags_to_get is {}")

            # No more data!
            self.assertIsNone(metric_file.get_value())


