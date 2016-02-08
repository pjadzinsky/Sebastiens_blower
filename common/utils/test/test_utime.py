#!/usr/bin/python
from common.utils import utime
import unittest


class TestTime(unittest.TestCase):

    def test_now(self):
        self.assertGreater(utime.now(), 0)
        return

    def test_ensure_valid_time(self):
        # Must be a numeric type, not a string or something else
        self.assertRaises(ValueError, utime.ensure_valid_time, '1452565427')

        # Special values representing unbounded quantaties are OK
        self.assertEquals(utime.ensure_valid_time(float('inf')), float('inf'))
        self.assertEquals(
            utime.ensure_valid_time(float('-inf')), float('-inf'))
        self.assertEquals(utime.ensure_valid_time(None), None)

        # If bounded, can't be negative
        self.assertRaises(ValueError, utime.ensure_valid_time, -10000.000)
        # If bounded, can't be too large to be time in this century
        # This catches errors where the value is provided in milliseconds
        # instead of seconds
        self.assertRaises(ValueError, utime.ensure_valid_time, 1452565427000)
        return


class TestFromTokens(unittest.TestCase):

    def test_from_utc_tokens(self):
        true_time_sec = 1439875510.0
        time_sec = utime.from_utc_tokens(2015, 8, 18, 5, 25, 10)
        self.assertEqual(true_time_sec, time_sec)
        return


class TestFromString(unittest.TestCase):

    def test_from_none_like(self):
        self.assertEqual(utime.from_string('None'), None)
        self.assertEqual(utime.from_string('none'), None)
        self.assertEqual(utime.from_string('null'), None)
        self.assertEqual(utime.from_string(''), None)
        return

    def test_from_invalid_float(self):
        self.assertRaises(ValueError, utime.from_string, '-10000.000')
        self.assertRaises(ValueError, utime.from_string, '-1.0')
        self.assertRaises(ValueError, utime.from_string, '-1')
        return

    def test_from_valid_float(self):
        self.assertEqual(utime.from_string('1448052845.001'), 1448052845.001)
        return

    def test_from_invalid_string(self):
        self.assertRaises(utime.BadFormatError, utime.from_string, 'foobar')
        self.assertRaises(utime.BadFormatError, utime.from_string, 'a b c')
        return

    def test_from_string_missing_timezone_specifier(self):
        self.assertRaises(
            utime.MissingTimeZoneError, utime.from_string, '2015-08-18 05:25:10')
        self.assertRaises(
            utime.MissingTimeZoneError, utime.from_string, '2015-10-2')
        self.assertRaises(
            utime.MissingTimeZoneError, utime.from_string, 'April 10 2015')
        return

    def test_from_string_utc(self):
        true_time_sec = 1439875510.0
        time_sec = utime.from_string('2015-08-18 05:25:10 UTC')
        self.assertEqual(true_time_sec, time_sec)
        time_sec = utime.from_string('Aug 18 2015 05:25:10 UTC')
        self.assertEqual(true_time_sec, time_sec)
        return

    def test_from_string_gmt(self):
        true_time_sec = 1439875510.0
        time_sec = utime.from_string('2015-08-18 05:25:10 GMT')
        self.assertEqual(true_time_sec, time_sec)
        time_sec = utime.from_string('Aug 18 2015 05:25:10 GMT')
        self.assertEqual(true_time_sec, time_sec)
        return


    def test_from_string_minus_8(self):
        true_time_sec = 1447853110.0
        time_sec = utime.from_string('2015-11-18 05:25:10 -08:00')
        self.assertEqual(true_time_sec, time_sec)
        return

    # TODO(heathkh): PST is not supported by locale config on all systems, fix this test
    #def test_tz_offset_pst(self):
    #    # PST is 7 hours offset in summer
    #    time_sec = utime.from_string('2015-8-24 00:00:00 PST')
    #    offset_sec = time_sec - utime.from_utc_tokens(2015, 8, 24, 0, 0, 0)
    #    self.assertEqual(offset_sec, 7 * utime.ONEHOUR)

    #    # PST is 8 hours offset in winter
    #    time_sec = utime.from_string('2015-11-18 05:25:10 PST')
    #    offset_sec = time_sec - utime.from_utc_tokens(2015, 11, 18, 5, 25, 10)
    #    self.assertEqual(offset_sec, 8 * utime.ONEHOUR)
    #    return

    # TODO(heathkh): PDT is not supported locale config on all systems, fix this test
    #def test_from_string_pdt(self):
    #    true_time_sec = 1447853110.0
    #    time_sec = utime.from_string('2015-11-18 05:25:10 PDT')
    #    self.assertEqual(true_time_sec, time_sec)
    #    return

    # TODO(heathkh): PDT is not supported locale config on all systems, fix this test
    #def test_tz_offset_pdt(self):
    #    # PDT is 7 hours offset in summer
    #    time_sec = utime.from_string('2015-08-18 05:25:10 PDT')
    #    offset_sec = time_sec - utime.from_utc_tokens(2015, 8, 18, 5, 25, 10)
    #    self.assertEqual(offset_sec, 7 * utime.ONEHOUR)

    #    # PDT is 8 hours offset in winter
    #    time_sec = utime.from_string('2015-11-18 05:25:10 PDT')
    #    offset_sec = time_sec - utime.from_utc_tokens(2015, 11, 18, 5, 25, 10)
    #    self.assertEqual(offset_sec, 8 * utime.ONEHOUR)
    #    return


class TestToString(unittest.TestCase):

    def test_to_string_no_timezone(self):
        val = utime.to_string(1447853110.0)
        self.assertEqual(val, '2015-11-18T13:25:10+00:00')
        return

    def test_to_string_fixed_offset_timezone(self):

        val = utime.to_string(1447853110.0, '+00:00')
        self.assertEqual(val, '2015-11-18T13:25:10+00:00')

        val = utime.to_string(1447853110.0, '-08:00')
        self.assertEqual(val, '2015-11-18T05:25:10-08:00')

        val = utime.to_string(1447853110.0, '+08:00')
        self.assertEqual(val, '2015-11-18T21:25:10+08:00')
        return

    def test_to_string_olson_timezone(self):
        # America/Los_Angeles has 8 hour offset in winter
        val = utime.to_string(1447853110.0, 'America/Los_Angeles')
        self.assertEqual(val, '2015-11-18T05:25:10-08:00')

        # America/Los_Angeles has 7 hour offset in summer
        val = utime.to_string(1465700380.0, 'America/Los_Angeles')
        self.assertEqual(val, '2016-06-11T19:59:40-07:00')
        return

    def test_to_string_non_olson_timezone(self):
        # Specifying timezone by 3-letter acronym instead of Olson database
        # name is considered an error because it is ambiguous
        self.assertRaises(ValueError, utime.to_string, 1447853110.0, 'PDT')
        self.assertRaises(ValueError, utime.to_string, 1447853110.0, 'PST')
        self.assertRaises(ValueError, utime.to_string, 1447853110.0, 'EST')
        return

    def test_to_string_none_like(self):
        val = utime.to_string(None)
        self.assertEqual(val, None)
        return


class TestInterval(unittest.TestCase):

    def test_init(self):
        self.assertRaises(ValueError, utime.Interval, -100, 0,)

    def test_eq(self):
        t0 = utime.Interval(100, 200)
        t0b = utime.Interval(100, 200)
        self.assertEqual(t0, t0b)
        self.assertEqual(t0, t0b)
        return

    def test_order_increasing(self):
        t0 = utime.Interval(None, 50)
        t1 = utime.Interval(20, 30)
        t2 = utime.Interval(50, 150)
        t3 = utime.Interval(50, 250)
        t4 = utime.Interval(100, 150)
        t5 = utime.Interval(100, 250)
        t6 = utime.Interval(100, None)
        t7 = utime.Interval(150, 170)
        t8 = utime.Interval(150, 250)

        l1 = [t0, t1, t2, t3, t4, t5, t6, t7, t8]
        l2 = [t7, t0, t6, t5, t4, t3, t2, t1, t8]
        interval_list = [t3, t2, t1]
        utime.order_increasing(interval_list)
        self.assertEqual([t1, t2, t3], interval_list)
        utime.order_increasing(l2)
        self.assertEqual(l1, l2)
        return

    def test_point_in_interval(self):
        t0 = utime.from_string("2014-10-5 8:00:00 UTC")
        t1 = utime.from_string("2014-10-21 8:00:00 UTC")
        t2 = utime.from_string("2015-10-5 8:00:00 UTC")
        t3 = utime.from_string("2015-10-21 8:00:00 UTC")
        t0_str = utime.to_string(t0)
        t1_str = utime.to_string(t1)

        # Test initializing Interval in different ways
        interval_utc = utime.Interval(t0, t1)
        interval_str = utime.Interval.from_string(t0_str, t1_str)
        interval_2014 = utime.Interval(t0, t1)
        interval_2015 = utime.Interval(t2, t3)
        interval_long = utime.Interval(t0, t3)
        interval_short = utime.Interval(t1, t2)
        none_1 = utime.Interval(None, t2)
        none_2 = utime.Interval(t2, None)
        none_none = utime.Interval(None, None)

        self.assertEqual(type(none_1), utime.Interval)
        self.assertEqual(type(none_2), utime.Interval)
        self.assertEqual(interval_utc, interval_str)
        self.assertEqual(interval_utc, interval_2014)
        self.assert_(interval_2014.contains_time(t0 + 3600))
        self.assertFalse(interval_2014.contains_time(t0 - 3600))

        self.assertEqual(interval_2014.intersection(interval_2015), None)
        self.assertEqual(
            interval_long.intersection(interval_short), interval_short)

        s = utime.ONEDAY * 365
        interval_2015b = utime.Interval(t0 + s, t1 + s)
        intersection = interval_2015.intersection(interval_2015b)
        self.assertEqual(intersection.duration(), 16 * utime.ONEDAY)

        self.assertEqual(none_1.intersection(none_2), None)
        self.assertEqual(none_1.intersection(interval_2014), interval_2014)
        self.assertEqual(none_2.intersection(interval_2015), interval_2015)
        self.assertEqual(none_1.intersection(interval_2015), None)

        self.assert_(interval_long.contains_time(t1))
        self.assert_(interval_2014.contains_time(t0))

        self.assertFalse(interval_2014.contains_time(t1))
        self.assertFalse(interval_2014.contains_time(t2))
        return

    def test_validate_ti_sequence(self):
        i_0 = utime.Interval(None, 15000)
        i_1 = utime.Interval(10000, 20000)
        i_2 = utime.Interval(20000, 30000)
        i_3 = utime.Interval(40000, 50000)
        i_4 = utime.Interval(50000, 60000)
        i_5 = utime.Interval(10000, 15000)
        i_6 = utime.Interval(70000, None)
        i_7 = utime.Interval(80000, 90000)

        self.assertTrue(utime.intervals_are_ordered_and_disjoint([]))
        self.assertTrue(
            utime.intervals_are_ordered_and_disjoint([i_1, i_2, i_3, i_6]))
        self.assertTrue(
            utime.intervals_are_ordered_and_disjoint([i_0, i_2, i_3, i_4]))
        self.assertFalse(utime.intervals_are_ordered_and_disjoint([i_2, i_1]))
        self.assertFalse(utime.intervals_are_ordered_and_disjoint([i_0, i_1]))
        self.assertFalse(utime.intervals_are_ordered_and_disjoint([i_6, i_7]))
        return

    def test_intersection(self):
        i_0 = utime.Interval(100, 200)
        i_1 = utime.Interval(None, None)
        i_2 = utime.Interval(None, 150)
        i_3 = utime.Interval(100, 150)
        i_4 = utime.Interval(50, 90)
        i_5 = utime.Interval(250, 290)

        self.assertEqual(i_0.intersection(i_1), i_0)
        self.assertEqual(i_0.intersection(i_2), i_3)
        self.assertEqual(i_0.intersection(i_4), i_0.intersection(i_5))
        return

    def test_closed_open_convention(self):
        i_0 = utime.Interval(None, 1)
        i_1 = utime.Interval(1, 2)
        i_2 = utime.Interval(2, 3)
        self.assertEqual(i_0.intersection(i_1), None)
        self.assertEqual(i_1.intersection(i_2), None)
        return

    def test_range(self):
        i_0 = utime.Interval(100, 200)
        i_1 = utime.Interval(None, None)
        i_2 = utime.Interval(None, 150)
        i_3 = utime.Interval(100, 150)
        i_4 = utime.Interval(50, 90)
        i_5 = utime.Interval(250, 290)

        # range of self is self - bounded case
        self.assertEqual(utime.spanning_interval([i_0]), i_0)

        # range of self is self - unbounded case
        self.assertEqual(utime.spanning_interval([i_1]), i_1)

        # range of self and unbounded interval is unbounded interval
        self.assertEqual(utime.spanning_interval([i_0, i_1]), i_1)

        # range of self and partial unbounded interval
        self.assertEqual(
            utime.spanning_interval([i_0, i_2]), utime.Interval(None, 200))

        # range of only bounded intervals
        self.assertEqual(
            utime.spanning_interval([i_3, i_4, i_5]), utime.Interval(50, 290))
        return

if __name__ == "__main__":
    unittest.main()
