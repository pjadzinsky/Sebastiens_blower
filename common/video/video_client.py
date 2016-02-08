import datetime
import pytz
import common.bucket.bucket
import common.settings
import common.video.video_info


def reversed_normalized_source_id(mac_address):
    return mac_address.replace(":", "").replace("-", "").lower()[::-1]

class VideoClient(object):

    VIDEO_FILE_LENGTH_MINUTES = 10
    assert (60 % VIDEO_FILE_LENGTH_MINUTES) == 0    #Lots of logic relies on round number of videos per hour
    v2_bucket = None    #class variable to save on S3 bucket lookup calls
    v3_bucket = None    #class variable to save on S3 bucket lookup calls

    def __init__(self):
        if not VideoClient.v2_bucket:
            VideoClient.v2_bucket = common.bucket.bucket.Bucket(common.settings.SLAB_DATA_BUCKET, region=common.settings.AWS_DEFAULT_REGION, decorate_name=False)
        if not VideoClient.v3_bucket:
            VideoClient.v3_bucket = common.bucket.bucket.Bucket(common.settings.SLAB_DATA_BUCKET2_BASE_NAME, region=common.settings.AWS_DEFAULT_REGION2, decorate_name=True)

    def get_v2_base_key(self, mac_address, time):
        time = time.astimezone(pytz.utc)
        self.assert_video_file_alignment(time)
        date_string = time.strftime('%Y/%m/%d')
        filename_prefix = '%02d.%02d.video' % (time.hour, time.minute)

        return '%s/%s/%s' % (reversed_normalized_source_id(mac_address), date_string, filename_prefix)

    def get_v2_video_info(self, mac_address, start_time, camera_position):
        video_info = None
        base_key = self.get_v2_base_key(mac_address, start_time)
        video_file_key = '%s.mp4' % base_key
        if self.v2_bucket.does_object_exist(video_file_key):
            video_info = common.video.video_info.VideoInfo(self.v2_bucket.bucket_name, video_file_key, camera_position=camera_position)
            video_info.set_keys_if_objects_exist(self.v2_bucket,
                                           preview_image_key='%s.poster.jpg' % base_key,
                                           captions_key = '%s.vtt' % base_key,
                                           thumbnails_key = '%s.thumbs.vtt' % base_key)

        return video_info

    def get_v3_base_key(self, slab_id, camera_position, time):
        time = time.astimezone(pytz.utc)
        self.assert_video_file_alignment(time)
        date_string = time.strftime('%Y/%m/%d')
        filename_prefix = '%02d.%02d.video.%s' % (time.hour, time.minute, camera_position)

        return '%s/%s/%s' % (reversed_normalized_source_id(slab_id), date_string, filename_prefix)

    def get_v3_video_info(self, slab_id, start_time, camera_position):
        video_info = None
        base_key = self.get_v3_base_key(slab_id, camera_position, start_time)
        video_file_key = '%s.mp4' % base_key
        if self.v3_bucket.does_object_exist(video_file_key):
            video_info = common.video.video_info.VideoInfo(self.v3_bucket.bucket_name, video_file_key, camera_position=camera_position)
            video_info.set_keys_if_objects_exist(self.v3_bucket,
                                           preview_image_key='%s.poster.jpg' % base_key,
                                           captions_key = '%s.vtt' % base_key,
                                           thumbnails_key = '%s.thumbs.vtt' % base_key)
        return video_info

    def get_videos(self, camera_position, start_time, end_time, mac_address=None, slab_id=None):
        """
        Returns a list of videos satisfying the following
        :param camera_position: camera position ('front' or 'back' or anything we might have in the future)
        :param start_time: datetime.datetime for beginning of time range
        :param end_time: datetime.datetime for end of time range
        :param v2_source: mac_address
        :param v3_source: slab_id
        :return: a list of VideoInfo instances
        """
        curr_start = self.round_down_to_video_file_length(start_time)

        videos = []

        while curr_start <= end_time:
            video_info = None
            #try V3 storage
            if slab_id:
                video_info = self.get_v3_video_info(slab_id, curr_start, camera_position)
            #if no V3 video found, try V2
            if not video_info and mac_address:
                video_info = self.get_v2_video_info(mac_address, curr_start, camera_position)
            if video_info:
                videos.append(video_info)
            curr_start += datetime.timedelta(minutes=self.VIDEO_FILE_LENGTH_MINUTES)

        return videos

    @staticmethod
    def round_down_to_video_file_length(time):
        return time.replace(minute=(time.minute // VideoClient.VIDEO_FILE_LENGTH_MINUTES) * VideoClient.VIDEO_FILE_LENGTH_MINUTES, second=0, microsecond=0)

    @staticmethod
    def assert_video_file_alignment(time):
        assert time.minute%VideoClient.VIDEO_FILE_LENGTH_MINUTES == 0 and time.second == 0 and time.microsecond == 0, "Video start times must be a "



