""" DEPRECATED: Port code to use common.util.utime module instead! 

"""
# This module has an awkward name time_utils instead of time to keep it from
# colliding with the built-in module called time
import datetime
from common.log import *
from common.utils import net
from dateutil import parser
from dateutil import tz
import pytz
import copy

###############################################################################
# Constants
###############################################################################
ZERO_DATETIME = datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)
zero_date = datetime.datetime(1970,1,1)  # non-tz version. Do we need both?
onehour = 3600
ONEHOUR = 3600
oneday = 86400
ONEDAY = 86400

###############################################################################
# Basic time operations
###############################################################################


def now():
    """ Get current time in seconds since the epoch. """
    return datetime_to_epochtime(datetime.datetime.now(tz.tzutc()))


def smallest_valid_time():
    """ Get the earliest representable time.  
    Turns out, this is not just zero
    TODO(heathkh): link to documentation on epoch time 
    """
    return datetime_to_epochtime(ZERO_DATETIME)


def time_to_string(time, tz='UTC'):
    if time is None:
        return None
    if time == float('inf'):
        return 'inf'
    if time == float('-inf'):
        return '-inf'
    if type(time) == type(1):
        time = float(time)
        
    try:
        
        tz = pytz.timezone(tz)
    except:
        msg = """
        {0} is not a pytz timezone
        Some popular ones are 'Zulu', 'UTC', 'US/Eastern', 'America/Los_Angeles'
        for a complete list execute pytz.all_timezones
        """.format(tz)
        
        raise ValueError(msg)
        
    """ Render time (seconds since epoch) into a human readable string. """
    dt_utc = _epochtime_to_datetime(time).astimezone(pytz.timezone('UTC'))
    dt_tz = dt_utc.astimezone(tz)
    
    #utc_time_string = dt_utc.strftime("%Y-%m-%dT%H:%M:%S") + ' (UTC)'
    tz_time_string = dt_tz.strftime("%Y-%m-%dT%H:%M:%S %Z") #+ ' %s'%(tz.zone)
    return tz_time_string
    


def time_to_API(time):
    """ Render time (seconds since epoch) into format expected by api. 
    This function is needed to make queries of the form
    https://app.mousera.com/api/v1/cage/589/annotation_report?format=json&from_date=2015-10-20T22:49:25.562Z&to_date=2015-10-21T22:49:25.562Z
    """
    if time is None:
        return None
    if type(time) == type(1):
        time = float(time)    
    dt_utc = _epochtime_to_datetime(time).astimezone(pytz.timezone("UTC"))
    time_string = dt_utc.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    return time_string

TZINFOS = None
def get_timezones():
    """ Returns a dictionary mapping string abbreviation to timezone object. """
    global TZINFOS
    if not TZINFOS:
        TZINFOS = {}
        for zone_name in pytz.all_timezones:
            TZINFOS[zone_name] = pytz.timezone(zone_name)
    return TZINFOS
    

class BadFormatError(ValueError):

    """Exception raised when time string not of any known format.
        msg  -- explanation of the error        
    """

    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg


class MissingTimeZoneError(ValueError):

    """Exception raised when time string missing a timezone specifier.
        msg  -- explanation of the error        
    """

    def __init__(self, bad_input):
        self.msg = 'Failed parsing string as time: %s\n' \
                   'TIP: Expected format like this: 2015-11-23' \
                   ' 11:24:35 UTC' % (bad_input)

    def __str__(self):
        return self.msg
    
    
def string_to_time(input_string):
    """ Attempts to convert human generated text strings to epoch time seconds.
    
    Can be a string of 3 categories:
      None: 'None', 'none', 'null', None
      Float: something that can be converted to a float
      Timestamp String: something that can be parsed as a timestamp with
    """
    # If input looks like a string representation of None 
    if input_string in ['None', 'none', 'null', '', None]:
        return None

    # If input looks like a floating point number, assume it is epoch seconds
    try:
        time_sec = float(input_string)
        if time_sec < smallest_valid_time():
            raise ValueError('This float is not a valid epoch time')
        return time_sec
    except ValueError:
        pass # not a string representation of a float

    # Finally, try to interpret as string rendering with possible timezone code
    dt = None
    try:
        dt = parser.parse(input_string, tzinfos=get_timezones())
    except ValueError as e:
        # Try to give a more helpful error message to help user fix format error
        raise BadFormatError(input_string)

    if _is_naive(dt):
        raise MissingTimeZoneError(input_string)
        
    dt_utc = dt.astimezone(pytz.utc)
    time_sec = datetime_to_epochtime(dt_utc)
    return time_sec



def delta_seconds(hours, minutes, seconds):
    return onehour*hours + 60*minutes + seconds


def utc_tokens_to_time(year, month, day, hour, minute, second=0):
    """ Convert a list of numeric time tokens in UTC frame to epoch time. """
    dt = datetime.datetime(
        year, month, day, hour, minute, second).replace(tzinfo=tz.tzutc())
    return datetime_to_epochtime(dt)

def convert_date_to_time(date):
    return (date - zero_date).total_seconds()

###############################################################################
# Time Zones and Days
###############################################################################


def start_of_day(time_point, day_start_shift=+7*onehour):
    # returns the epoch time corresponding to the start of the day.
    # Default timezone is PDT
    day_start = int((time_point - day_start_shift)/oneday) * oneday + day_start_shift
    return day_start


def middle_of_day(t, day_start_shift=7*onehour):
    return start_of_day(t, day_start_shift) + 12 * onehour


def hour_in_day(t, hour, day_start_shift=7*onehour):
    return start_of_day(t, day_start_shift) + hour * onehour


def time_of_day(t, day_start_shift=7*onehour):
    return t - start_of_day(t, day_start_shift)

###############################################################################
# Time Intervals
###############################################################################


def interval_intersection(interval_a, interval_b):
    """
    Args:
        interval_a (tuple of floats) : first interval
        interval_b (tuple of floats) : second interval

    Returns:
        None if no intersection or a tuple containing the intersection
    Note:
        Use 'None' for start or end of an interval to represent an open interval 
    """
    CHECK_EQ(len(interval_a), 2)
    CHECK_EQ(len(interval_b), 2)
    a_start, a_end = interval_a
    b_start, b_end = interval_b

    i_start = None
    if a_start and b_start:
        i_start = max(a_start, b_start)
    elif a_start and not b_start:
        i_start = a_start
    elif b_start and not a_start:
        i_start = b_start

    i_end = None
    if a_end and b_end:
        i_end = min(a_end, b_end)
    elif a_end and not b_end:
        i_end = a_end
    elif b_end and not a_end:
        i_end = b_end

    intersection_interval = None
    if i_start and i_end:  # i.e. a closed interval
        if i_start < i_end:
            intersection_interval = (i_start, i_end)
        else:
            intersection_interval = None
    elif i_start or i_end:  # i.e. a partially open interval
        intersection_interval = (i_start, i_end)
    else:  # i.e.  open interval on both ends
        intersection_interval = (None, None)

    return intersection_interval


def intervals_intersect(interval_a, interval_b):
    """
    Args:
        interval_a (tuple of floats) : first interval
        interval_b (tuple of floats) : second interval

    Returns:
        True if intervals have non-empty 
    Note:
        Use 'None' for start or end of an interval to represent an open interval
    """
    has_overlap = False
    if interval_intersection(interval_a, interval_b):
        has_overlap = True
    return has_overlap


def time_in_interval(time_sec, interval):
    """
    Args:
        time_sec (floats) : query time
        interval (tuple of floats) : interval

    Returns:
        True if time is inside interval 
    Note:
        Use 'None' for start or end of an interval to represent an open interval
        
        Uses python's convention of closed, open intervals
        ie, t2 is not in (t0,t2)
    """
    has_intersection = False
    # TODO(heathkh): this should be rewritten without using the interval_intersection code + eps hack!   
    '''
    eps = 1e-10
    if interval_intersection((time_sec, time_sec+eps), interval) != None:
        has_intersection = True
    '''
    if interval[0]<=time_sec and time_sec<interval[1]:
        has_intersection = True
        
    return has_intersection



def time_in_time_interval(time_sec, interval):
    """
    Args:
        time_sec (floats) : query time
        interval (Interval object) : interval

    Returns:
        True if time is inside interval 
    Note:
        Use 'None' for start or end of an interval to represent an open interval

        Uses python's convention of closed, open intervals
        ie, t2 is not in (t0,t2)
        
        1 > None is True
        1 < None is False
    """
    has_intersection = True

    #if interval.start_time<=time_sec and time_sec<interval.end_time:
    #    has_intersection = True

    if interval.start_time is not None and time_sec < interval.start_time:
        return False
    
    if interval.end_time is not None and interval.end_time <= time_sec:
        return False
        
    return has_intersection

def time_intervals_max_end_time(intervals):
    max_time = -1
    for ti in intervals:
        if ti.end_time is None:
            return None
        if ti.end_time > max_time:
            max_time = ti.end_time
            
    return max_time

def time_intervals_min_start_time(intervals):
    min_time = intervals[0].start_time
    for ti in intervals:
        if ti.start_time is None:
            return None
        if ti.start_time < min_time:
            min_time = ti.start_time
            
    return min_time



###############################################################################
# Private stuff -- user beware!
###############################################################################

def datetime_to_epochtime(date):
    CHECK_EQ(type(date), datetime.datetime)
    CHECK(not _is_naive(date))
    retval = (date - ZERO_DATETIME).total_seconds()
    CHECK_NOTNONE(retval)
    return retval

def naive_to_utc(dt):
    """ Use when 3rd party gives you a naive datetime and you need to use it
    with mousera code."""
    CHECK(_is_naive(dt))
    return dt.replace(tzinfo=tz.tzutc())


###############################################################################
# Super Private -- don't even think about using this stuff
###############################################################################


def _is_naive(dt):
    return dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None


    
def _epochtime_to_datetime(time_sec):
    CHECK_EQ(type(time_sec), float)
    return datetime.datetime.utcfromtimestamp(time_sec).replace(tzinfo=tz.tzutc())



