import os
import os.path
from datetime import datetime
from dateutil.tz import tzutc
import re

# Returns the root path
def get_root(extra_path="", use_mac_address=None):
    from common.utils import mac_address

    # Get the MAC address stripped of ":" and reversed
    reversed_mac_address = mac_address.clean_and_reverse(
        use_mac_address or mac_address.getSettingsMACAddress())
    return os.path.join(extra_path, reversed_mac_address)

def create_get_file_path(file_name, duration, extra_path="",
                         create_directory=True, use_mac_address=None,
                         path_separator="/"):
    """
      file_name: name of the measurement, signal (e.g. sensor1234.csv)
      duration:  'daily', 'hourly' or how many minutes each file will run.

      Returns: function while will get the current value

    In order to scale request volumes on S3, the object key names should be
    alphebetically unrelated.  So the following key names are bad:
      /2014-03-10-09-19-32/something.txt
      /2014-03-10-09-19-33/else.txt

    In Mousera''s case, the primary ID for data generators is the MAC address,
    which are also globally unique, so they're used as the reversed ID.

    Thereafter, dates are used to create directories and the filetype is used to
    construct the filename.  Files should be broken into chunks as follows:
      Concatenative logs: hour
      Stream            : 10 minutes [e.g. audio, video]
      Snapshots         : no chunking; use full timestamp

    Example:
      MAC address = 0123456789ab

      /{reversed_MAC_address}/{YYYY}/{MM}/{DD}/hour.minute.filename.ext
      /ba9876543210/2014/04/11/08.sensor1234.csv
      /ba9876543210/2014/04/11/08.11.14.camera1.png
    """
    def get_time_string(dt):
        if duration=='daily' : return None

        hours = dt.strftime("%H")
        if duration=='hourly': return hours

        # Handle minutes.
        minutes = ('0' + str(10 * (dt.minute / duration)))[-2:]
        return ".".join([hours, minutes])

    def _get_file_path(use_datetime=None):
        # Times MUST be aware and in UTC.  Make sure our datetime has a timezone...
        _dt = use_datetime or datetime.now(tzutc())
        # Convert numbers to datetimes
        if type(_dt) in [int,float]:
            _dt = datetime.fromtimestamp(_dt, tz=tzutc())
        if not hasattr(_dt, 'tzinfo'):
            raise Exception("common.utils.file_system: _get_file_path datetimes MUST be UTC.")
        # ... now force to UTC.
        dt = _dt.astimezone(tzutc())

        # Build the filename, filtering Nones from get_time_string
        _file_name = '.'.join(filter(None, [get_time_string(dt), file_name]))

        dir = path_separator.join([
            get_root(extra_path=extra_path, use_mac_address=use_mac_address),
            dt.strftime("%Y"),
            dt.strftime("%m"),
            dt.strftime("%d")]
        )

        # Check cached version of dir and create directory if necessary
        if path_separator == "/" and \
           not (getattr(_get_file_path, '_dir', None) == dir) and \
           not os.path.exists(dir):
            os.makedirs(dir)
            _get_file_path._dir = dir

        return path_separator.join([dir, _file_name])

    return _get_file_path

def extract_file_path(file_path):
    # Don't trap exceptions here since we can't log them.
    # Extract components components out of the file_path
    directory_regex = ".*(?P<reversed_mac_address>[0-9a-fA-F]{12})\/(?P<year>[0-9]{4})\/(?P<month>[0-9]{1,2})\/(?P<day>[0-9]{1,2})\/(?P<file>.*)"
    file_regexes = [
        "^(?P<hour>[0-9]{2})\.(?P<minute>[0-9]{2})\.(?P<second>[0-9]{2})\.(?P<name>.*)$",
        "^(?P<hour>[0-9]{2})\.(?P<minute>[0-9]{2})\.(?P<name>.*)$",
        "^(?P<hour>[0-9]{2})\.(?P<name>.*)$"
        "^(?P<name>.*)$"
    ]

    matches = re.match(directory_regex, file_path).groupdict()
    minute = second = 0
    file_matches = None
    for file_regex in file_regexes:
        file_matches = re.match(file_regex, matches['file'])
        if file_matches: break
    if not file_matches:
        raise Exception("common.utils.extract_file_path: file name in path was invalid.")
    file_matches = file_matches.groupdict()
    # Make sure to use UTC
    dt = datetime(int(matches['year']), int(matches['month']), int(matches['day']), int(file_matches.get('hour',0)),
                  int(file_matches.get('minute', 0)), int(file_matches.get('second',0)), 0, tzutc())
    return {
        'mac_address' : matches['reversed_mac_address'][::-1], # re-reverse
        'datetime'   : dt,
        'file_name'   : file_matches['name']}


def make_parent_dirs(filename):  
    parent_dirs = os.path.dirname(filename)  
    if not os.path.exists(parent_dirs):  
        os.makedirs(parent_dirs)
    return 

def make_dirs_if_needed(path):  
    if not os.path.exists(path):  
        os.makedirs(path)
    return