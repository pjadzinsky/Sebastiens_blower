import calendar
import datetime
import pytz


def get_timestamp(time):
    """ Converts datetime to a metrics timestamp (millisecond since epoch)
    :param time: datetime object
    :return: metric timestamp
    """
    return calendar.timegm(time.timetuple()) * 1000 + time.microsecond//1000


def get_datetime(timestamp):
    """
    Converts a metric timestamp (milliseconds since epoch) to a UTC datetime
    :param timestamp: metric timestamp
    :return: UTC datetime
    """
    time = datetime.datetime.utcfromtimestamp(int(timestamp) // 1000)
    utc_time = time.replace(tzinfo=pytz.utc, microsecond=(int(timestamp)%1000)*1000)
    return utc_time
