#!/usr/bin/python
# Test the caching wrapper.
import time
from common.utils import caching
from common.utils import utime
from common.utils.caching import cache_value
import unittest



class TestCaching(unittest.TestCase):

    def test_cache_value(self):
        caching.clear_cache('test')
        @cache_value()
        def test_cache_function(x,y=3, function_ran={1:0}):
            function_ran[1] = True
            return x,y

        function_ran = {1:False}
        self.assertEqual(test_cache_function(1,y=3, function_ran=function_ran), (1,3))
        self.assertEqual(function_ran, {1:True})
        function_ran = {1:False}
        self.assertEqual(test_cache_function(1,y=3, function_ran=function_ran), (1,3))
        self.assertEqual(function_ran, {1:False})

    def test_clear_cache(self):
        caching.clear_cache('test')
        @cache_value()
        def test_cache_function(x,y=3, function_ran={1:0}):
            function_ran[1] = True
            return x,y

        function_ran = {1:False}
        self.assertEqual(test_cache_function(1,y=3, function_ran=function_ran), (1,3))
        self.assertEqual(function_ran, {1:True})
        caching.clear_cache('test')
        function_ran = {1:False}
        self.assertEqual(test_cache_function(1,y=3, function_ran=function_ran), (1,3))
        self.assertEqual(function_ran, {1:True})

    def test_no_caching(self):
        caching.clear_cache('test')
        @cache_value(-1)
        def test_cache_function(x,y=3, function_ran={1:0}):
            function_ran[1] = True
            return x,y

        function_ran = {1:False}
        self.assertEqual(test_cache_function(1,y=3, function_ran=function_ran), (1,3))
        self.assertEqual(function_ran, {1:True})
        function_ran = {1:False}
        self.assertEqual(test_cache_function(1,y=3, function_ran=function_ran), (1,3))
        self.assertEqual(function_ran, {1:True})
        # TODO: add a test where you turn on the caching in such a case.

    def test_cache_value_timing_out(self):    
        caching.clear_cache('test')
        @cache_value(0.1)
        def test_cache_function(x,y=3, function_ran={1:0}):
            function_ran[1] = True
            return x,y
    
        function_ran = {1:False}
        self.assertEqual(test_cache_function(1,y=3, function_ran=function_ran), (1,3))
        self.assertEqual(function_ran, {1:True})
        time.sleep(0.2)
        function_ran = {1:False}
        self.assertEqual(test_cache_function(1,y=3, function_ran=function_ran), (1,3))
        self.assertEqual(function_ran, {1:True})

    def test_clean_kwargs(self):
        @cache_value()
        def test_cache_function(x,y=3, function_ran={1:0}):
            function_ran[1] = True
            return x,y
    
        interval = utime.Interval(None,None)
        function_ran = {1:False}
        self.assertEqual(test_cache_function(3,y=interval, function_ran=function_ran), (3,interval))
        self.assertEqual(function_ran, {1:True})
        function_ran = {1:False}
        self.assertEqual(test_cache_function(3,y=interval, function_ran=function_ran), (3,interval))
        self.assertEqual(function_ran, {1:False})

    def test_clean_args(self):
        caching.clear_cache('test')
        @cache_value()
        def test_cache_function(x,y=3, function_ran={1:0}):
            function_ran[1] = True
            return x,y
    
        interval = utime.Interval(None,None)
        function_ran = {1:False}
        self.assertEqual(test_cache_function(interval,y=3, function_ran=function_ran), (interval,3))
        self.assertEqual(function_ran, {1:True})
        function_ran = {1:False}
        self.assertEqual(test_cache_function(interval,y=3, function_ran=function_ran), (interval,3))
        self.assertEqual(function_ran, {1:False})


if __name__ == "__main__":
    unittest.main()
