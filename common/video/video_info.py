class VideoInfo(object):

    def __init__(self, bucket_name = None, video_key = None, preview_image_key = None, captions_key = None, thumnails_key = None, camera_position = None):
        self.bucket_name = bucket_name
        self.video_key = video_key
        self.preview_image_key = preview_image_key
        self.captions_key = captions_key
        self.thumbnails_key = captions_key
        self.camera_position = camera_position

    def as_url(self, key):
        if self.bucket_name == "mousera-slab-data":
            return "%s/%s" % (self.bucket_name, key) if key else None
        return key

    @property
    def video_url(self):
        return self.as_url(self.video_key)

    @property
    def captions_url(self):
        return self.as_url(self.captions_key)

    @property
    def thumbnails_url(self):
        return self.as_url(self.thumbnails_key)

    @property
    def preview_image_url(self):
        return self.as_url(self.preview_image_key)

    def set_keys_if_objects_exist(self, bucket, **kwargs):
        for attribute in kwargs:
            #cause an exception if not a valid attribute of VideoInfo
            self.__getattribute__(attribute)
            s3_key = kwargs[attribute]
            if bucket.does_object_exist(s3_key):
                self.__setattr__(attribute, s3_key)

