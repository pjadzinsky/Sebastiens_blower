""" Represents points and intervals of time unambiguously using unix-time.  

Please use these methods instead of built-in python time objects like datetime.

Rational: Python datetime is easy to use incorrectly and leads to confusion and
subtle bugs.  By convention throughout the codebase, it is always preferable
to represent time as seconds since the unix epoch.

NOTE: Do not use the built-in time module in new code.  It suffers from many of
the same problems as datetime.  
"""
from common import log
from dateutil import parser
import datetime
import numbers
import pytz

###############################################################################
# Constants - leads to easier to read code
###############################################################################
TZ_UTC = pytz.utc
ZERO_DATETIME = datetime.datetime(1970, 1, 1, tzinfo=TZ_UTC)
ONEMINUTE = 60
ONEHOUR = 3600
ONEDAY = 86400

###############################################################################
# Basic time operations
###############################################################################

def now():
    """ Returns current time.
    Returns:
        Current time in seconds since the unix-epoch UTC.
        
    Note: Prefer this to datetime.now(), which is sensitive to locale settings
    and the source of many difficult-to-identify bugs.
    """
    epoch_seconds = from_datetime(datetime.datetime.now(TZ_UTC))
    return epoch_seconds


def from_utc_tokens(year, month, day, hour=0, minute=0, second=0):
    """ Convert a list of numeric time tokens to unix-time.
    
    The tokens must be specified in the UTC reference frame.
    """
    dt = datetime.datetime(year, month, day, hour, minute,
                           second).replace(tzinfo=TZ_UTC)
    return from_datetime(dt)


def from_string(input_string):
    """ Parses string representation of time in various formats.
    
    Raises exception if the conversion from the string to a unix-time could be
    ambiguous.  Ambiguity can arise when parsing strings that do not include a
    specifier of the time-zone.  While this seems pedantic, it forces all users
    of string representations of time to be precise and protects against a large
    number of hard-to-identify bugs.    

    Can be a string of 3 categories:
      None: 'None', 'none', 'null', None
      Float: something that can be converted to a float
      Timestamp string: something that can be parsed as a common timestamp format 
      Natural Language String: Aug 23, 2015 12:00:01 PDT
      
    Args:
        input_string: A human readable string encoding of time in above formats
    
    Returns:
        float representing unix-time or None
    
    Raises:
        BadFormatError: if parsers fail to understand the format
        MissingTimeZoneError: if the string missing a timezone specifier
      
    """
    # If input looks like a string representation of None
    if input_string in ['None', 'none', 'null', '', None]:
        return None
    # If input looks like a floating point number, assume it is epoch seconds
    try:
        time_sec = ensure_valid_time(float(input_string))
        return time_sec
    except ValueError:
        pass  # not a string representation of a float
    # Finally, try to interpret as string rendering with a timezone specifier
    dt = None
    
    tzinfos=_olson_and_fixed_timezones()
    try:
        dt = parser.parse(input_string, tzinfos=tzinfos)
    except ValueError as e:
        raise BadFormatError(input_string)
    except TypeError as e:
        raise BadFormatError(input_string)
    if _is_naive(dt):
        raise MissingTimeZoneError(input_string)
    time_sec = from_datetime(dt.astimezone(TZ_UTC))
    return time_sec


def to_string(time_sec, time_zone='+00:00'):
    """ Render time in a human-readable string in ISO format. 
    Args:
        time_zone (str) : Fixed offset or valid Olson timezone  

    Note: Olson timezones handle time-varying offsets like daylight saving while
    fixed offsets are... fixed.
    
    To get just the date, do to_string(time_sec)[:10]

    Examples: Olson timezone names
    >>> utime.to_string(1447853110.0, 'America/Los_Angeles')
    '2015-11-18T05:25:10-08:00'
    
    Examples: Fixed offset timezone names:
    >>> utime.to_string(1447853110.0, '+00:00')   
    '2015-11-18T13:25:10+00:00'
    >>> utime.to_string(1447853110.0, '-08:00')
    '2015-11-18T05:25:10-08:00'
    >>> utime.to_string(1447853110.0, '+08:00')
    '2015-11-18T21:25:10+08:00'

    Note: 
    This should only be used at the last moment to present data to a human.  
    This string should never be stored or used in any other way because it is 
    not precise-enough for many applications. 
    """
    time_sec = ensure_valid_time(time_sec)
    time_string = None
    if time_sec:
        tz = _timezone_from_string(time_zone)
        time_string = _epochtime_to_datetime(
            time_sec).astimezone(tz).isoformat()
    return time_string


def ensure_valid_time(input):
    """ Checks input is a sane unix-time in units of seconds.  
    
    A valid representation of time in seconds since the unix epoch is:
      * a numeric type or the special values: None, inf, -inf 
      * if numeric and bounded, is in the range (0, 10000000000)
         
    examples:
        ensure_valid_time(2534634) - Returns 2534634.0 
        ensure_valid_time("2534634.0") - Raises ValueError 
            HINT: use utime.from_string() for strings containing a float
        ensure_valid_time(None) - Returns None        
        ensure_valid_time(1452565427000) - Raises ValueError 
            The user provided a time in milliseconds instead of seconds.  
          
    Args:
        input: value we want to ensure is a valid unix-time.
    Returns:
        The input value cast to a float so you can overwrite the input value
        to ensure float arithmetic even if users provide an integer type.
    Raises:
        ValueError: The input is not a valid representation of unix-time.
    """
    output = None
    if input is not None:
        if not isinstance(input, numbers.Number):
            raise ValueError('Expected a numeric type, but got %s '
                             'of type %s' % (input, type(input)))
        output = float(input)
        if output == float('-inf') or output == float('inf'):
            pass
        elif output < 0:
            raise ValueError('Bounded unix-time can not be negative: %f' % output)
        elif output > 10000000000:
            raise ValueError('Too large to be unix-time in seconds during this'
                             ' century: %f \n TIP: Did you provide a value in'
                             'milliseconds instead of seconds?' % output)
        
    return output


class Interval(object):
    """ Represents a contiguous range of unix-time.
    
    Example: Constructing a time interval from unix-times (default constructor)
        >>> utime.Interval(10000, 20000)
        >>> utime.Interval(None, 20000)

    Example: Constructing a time interval from strings (named constructor):
        >>> start = '2015-10-05T08:30:00 UTC', end = '2015-10-23T22:29:37 UTC' 
        >>> utime.Interval.from_string(start, end)
        >>> utime.Interval.from_string(None, end)
    
    Example: Comparing equality
        >>> a = utime.Interval(None, 23634523)
        >>> b = utime.Interval(215234, 23634523)
        >>> c = utime.Interval(215234, 23634523)
        >>> a == b
        False
        >>> b == c
        True
    
    This representation uses the convention where the start and end bounds are 
    closed and open respectively.  This means that intervals where the end of 
    one is the start of the other have no overlap. 
    """

    def __init__(self, start_time_sec, end_time_sec):
        """ Construct an interval from start and end time in seconds.
         Args:
              start_time_sec (float or None): None indicates unbounded interval     
              end_time_sec (float or None): None indicates unbounded interval
        """
        # Ensure times are stored as floating point numbers
        start_time_sec = self._validated_start_time(start_time_sec)
        end_time_sec = self._validated_end_time(end_time_sec)
        log.CHECK_LE(start_time_sec, end_time_sec,
                     'A time interval requires start <= end '
                     'but got {0} and {1}'.format(start_time_sec, end_time_sec))
        self.start_time_sec = start_time_sec
        self.end_time_sec = end_time_sec
        return

    @classmethod
    def from_string(cls, start_str, end_str):
        """ Construct an interval from start and end time provided as strings.
         Args:
              start_str (str or None): None indicates unbounded interval     
              end_str (str or None): None indicates unbounded interval
        """
        start_time_sec = None
        if start_str is not None:
            start_time_sec = from_string(start_str)
        end_time_sec = None
        if end_str is not None:
            end_time_sec = from_string(end_str)
        return Interval(start_time_sec, end_time_sec)

    def to_string(self, tz='UTC'):
        """ Returns a string representation of the time interval.
        Args:
            tz (str) : Optionally use this Olson timezone 
        
        NOTE: Only suitable for immediate display.  Never store this!
        """
        pretty_start = to_string(self.start_time_sec, tz)
        pretty_end = to_string(self.end_time_sec, tz)
        s = "%s - %s" % (pretty_start, pretty_end)
        return s

    def duration(self):
        """ Returns duration of the interval in seconds.
        Returns:
             Difference in seconds between end_time_sec and start_time_sec
        """
        return self.end_time_sec - self.start_time_sec

    def is_finite(self):
        """ Checks if both start and end-points are finite. 
        Returns:
            True if duration is finite, False otherwise.
        """
        return self.duration() != float('inf')

    def midpoint(self):
        """ Returns the midpoint of the interval.
        Returns:
            midpoint in unix-time
        """
        return (self.start_time_sec + self.end_time_sec) / 2.0

    def contains_time(self, time_sec):
        """ Checks if an instant of time is contained in this interval.

        Args:
            time_sec (float) : query unix-time

        Returns:
            True if time is inside interval 
        """
        time_in_interval = True
        if self.start_time_sec is not None and time_sec < self.start_time_sec:
            time_in_interval = False
        if self.end_time_sec is not None and self.end_time_sec <= time_sec:
            time_in_interval = False
        return time_in_interval

    def intersection(self, other):
        """
        Args:
            other (Interval object) : The interval to intersect with this one
        Returns:
            Interval containing the intersection or None if intersection empty 
        """
        log.CHECK_EQ(type(other), Interval)
        intersection_interval = None
        # If other ends before this starts, or this ends before other starts,
        # there can be no overlap
        if (other.end_time_sec > self.start_time_sec and 
            self.end_time_sec > other.start_time_sec):
            i_start = max(self.start_time_sec, other.start_time_sec)
            i_end = min(self.end_time_sec, other.end_time_sec)
            intersection_interval = Interval(i_start, i_end)
        return intersection_interval

    @staticmethod
    def _validated_start_time(input):
        """ Validates time and also resolves None to float('-inf') """
        output = ensure_valid_time(input)
        if output is None:
            output = float('-inf')
        return output

    @staticmethod
    def _validated_end_time(input):
        """ Validates time and also resolves None to float('inf') """
        output = ensure_valid_time(input)
        if output is None:
            output = float('inf')
        return output

    def __eq__(self, other):
        log.CHECK(type(other) == Interval)
        return (self.start_time_sec, self.end_time_sec) == (other.start_time_sec, other.end_time_sec)

    def __repr__(self):
        """ 
        Repr is a precise rendering. for a more human readable format, try to_string()
        """
        s = "<Interval: %0.3f, %0.3f>" % (self.start_time_sec, self.end_time_sec)
        return s


def order_increasing(list_of_intervals):
    """ Modifies the list to be ordered increasing by the start time.
     
    When start times are equal, end time is used as the tie-breaker.
    
    TODO(heathkh): can you define a composite key that encodes unbounded
    intervals properly?  This could make ordering require only one pass.
    """
    def _primary_key(time_interval):
        key = time_interval.start_time_sec
        if key is None:
            key = float('-inf')
        return key

    def _secondary_key(time_interval):
        key = time_interval.end_time_sec
        if key is None:
            key = float('inf')
        return key
    # sort is stable, so sorting first by end time ensures ties on start time
    # are broken consistently
    list_of_intervals.sort(key=_secondary_key)
    list_of_intervals.sort(key=_primary_key)
    return


def intervals_are_ordered_and_disjoint(list_of_time_intervals):
    """ Validates sequence of intervals is ordered and has no overlaps. 
    
    This is a helper function to check a common invariant.
        
    Returns:
        True if every interval in list_of_time_intervals finishes before 
        the next one starts, and False otherwise.
    """
    if len(list_of_time_intervals) == 0:
        return True
    log.CHECK_EQ(type(list_of_time_intervals[0]), Interval)

    prev_start = list_of_time_intervals[0].start_time_sec
    prev_end = list_of_time_intervals[0].end_time_sec
    for ti in list_of_time_intervals[1:]:
        cur_start, cur_end = ti.start_time_sec, ti.end_time_sec
        # Convert None to float inf symbol to make comparison logic simpler
        if cur_start is None:
            cur_start = float('-inf')
        if cur_end is None:
            cur_end = float('inf')
        # ensure interval starts times are increasing
        if prev_start > cur_start:
            return False
        # ensure adjacent intervals do not overlap
        if prev_end > cur_start:
            return False
        prev_start = cur_start
        prev_end = cur_end
    return True


def spanning_interval(list_of_time_intervals):
    """ Computes the smallest interval that contains all those in list. 
    
    Returns:
        utime.Interval: spans all intervals in provided list
    
    Note: This function does not require the list be ordered in any particular
    way, so it has O(n) runtime. 
    """
    log.CHECK_GT(len(list_of_time_intervals), 0)
    range_start = float('inf')
    range_end = float('-inf')
    for i in list_of_time_intervals:
        log.CHECK_EQ(type(i), Interval)
        range_start = min(range_start, i.start_time_sec)
        range_end = max(range_end, i.end_time_sec)
    return Interval(range_start, range_end)
    

###############################################################################
# String parsing exceptions
###############################################################################


class BadFormatError(ValueError):
    """ Exception raised when time string not of any known format.
    Args:
        input_string: The input that caused the parsing error        
    """

    def __init__(self, input_string):
        self.msg = "Failed to parse string as time: %s \n" \
                   "Unknown format or timezone specifier." % (input_string)

    def __str__(self):
        return self.msg


class MissingTimeZoneError(ValueError):
    """ Exception raised when time string missing a timezone specifier.
    Args:
        input_string: The input that caused the parsing error
    """

    def __init__(self, input_string):
        self.msg = 'Failed parsing string as time: %s\n' \
                   'TIP: Expected format like this: 2015-11-23' \
                   ' 11:24:35 UTC' % (input_string)

    def __str__(self):
        return self.msg


###############################################################################
# Conversion to day-quantized frames-of-reference
# TODO(heathkh): These seem specific to analysis, should they be moved?
###############################################################################

def start_of_day(time_point, day_start_shift=+7 * ONEHOUR):
    # returns the epoch time corresponding to the start of the day.
    # Default timezone is PDT
    day_start = int((time_point - day_start_shift) / ONEDAY) * \
        ONEDAY + day_start_shift
    return day_start


def middle_of_day(t, day_start_shift=7 * ONEHOUR):
    return start_of_day(t, day_start_shift) + 12 * ONEHOUR


def hour_in_day(t, hour, day_start_shift=7 * ONEHOUR):
    return start_of_day(t, day_start_shift) + hour * ONEHOUR


def time_of_day(t, day_start_shift=7 * ONEHOUR):
    return t - start_of_day(t, day_start_shift)

def is_night(t, day_start_shift=7 * ONEHOUR):
    time = time_of_day(t, day_start_shift=day_start_shift)
    if time < 6*ONEHOUR or time > 18*ONEHOUR:
        return True
    else:
        return False

###############################################################################
# Dangerous Stuff -- If you use, you are probably doing something wrong.
###############################################################################

def from_datetime(dt):
    """ Convert datetime to standard time representation used in this codebase.
    
    Use when 3rd party codes gives you a time as a datetime. 
     
    Args:
        dt (datetime.Datetime) : a non-naive datetime object
    Returns:
        float : seconds since unix-epoch    
    """
    log.CHECK_EQ(type(dt), datetime.datetime)
    log.CHECK(not _is_naive(dt))
    time_in_seconds = (dt - ZERO_DATETIME).total_seconds()
    log.CHECK_NOTNONE(time_in_seconds)
    return time_in_seconds


def naive_datetime_to_utc(dt):
    """ Coerce a naive datetime into tz aware one assuming UTC.
    
    Use when 3rd party gives you a naive datetime AND you know for sure the 
    naive datetime is in the UTC frame of reference.
    
    Args: 
        dt (DateTime): object that is "naive" (reference-frame not specified)
    returns:
        A non-naive DateTime object by coersion to UTC reference frame
    """
    log.CHECK_EQ(type(dt), datetime.datetime)
    log.CHECK(_is_naive(dt))
    return dt.replace(tzinfo=TZ_UTC)


def is_aware(value):
    """
    Determines if a given datetime.datetime is aware.

    The logic is described in Python's docs:
    http://docs.python.org/library/datetime.html#datetime.tzinfo
    """
    return value.tzinfo is not None and value.tzinfo.utcoffset(value) is not None


###############################################################################
# Super Private -- don't even think about using this stuff
###############################################################################

# Private Constants
_TZINFOS = None

# Private helper function
def _is_naive(dt):
    log.CHECK_EQ(type(dt), datetime.datetime)
    return dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None


def _epochtime_to_datetime(time_sec):
    log.CHECK_EQ(type(time_sec), float)
    return datetime.datetime.utcfromtimestamp(time_sec).replace(tzinfo=TZ_UTC)


def _olson_and_fixed_timezones():
    """ Returns mapping from timezone names to python timezone objects.  
    
    Supports timezone names from Olson database and following fixed offset
    timezones: 
    -11:00,-10:00, -09:00, -08:00, -07:00, -06:00, -05:00, -04:00, -03:00, 
     -02:00, -01:00, +00:00, +01:00, +02:00, +03:00, +04:00, +05:00, +06:00, 
     +07:00, +08:00, +09:00, +10:00, +11:00
    """
    global _TZINFOS
    if not _TZINFOS:
        _TZINFOS = {}
        for zone_name in pytz.common_timezones:
            tz = pytz.timezone(zone_name)
            _TZINFOS[zone_name] = tz
        
        for hour_offset in range(-11,12):
            zone_name = '%+03d:00' % hour_offset
            offset_in_min = hour_offset*60
            tz = pytz.FixedOffset(offset_in_min)
            _TZINFOS[zone_name] = tz
            
    return _TZINFOS
_olson_and_fixed_timezones()

def _timezone_from_string(time_zone_name):
    """ Returns the timezone object by name. 
    
    Note: common abbreviations for timezones like PDT and EST are 
    deprecated and not found in the Olson database of timezones because they 
    are ambigious. 

    Raises:
        ValueError : if not a valid Olson time-zone name 
    """
    tzinfo = _olson_and_fixed_timezones()
    if time_zone_name not in tzinfo:
        msg = "Unknown timezone name: {0} \n".format(time_zone_name)
        msg += "Time-zone abbreviations like PDT and EST are ambiguous " \
            "and have different meanings in different parts of the world. " \
            "Ensure you use an unambiguous time zone specifier from the " \
            "Olson Timezone Database. "
        raise ValueError(msg)
    return tzinfo[time_zone_name]
