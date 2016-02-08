import unittest
import dateutil.parser
from mock import patch, Mock
import common.settings
from common.video.video_client import VideoClient, reversed_normalized_source_id

def mocked(function):
    @patch('common.bucket.bucket.Bucket')
    def decorator(*args, **kwargs):
        BucketMock = args[-1]
        BucketMock.return_value.does_object_exist = Mock(side_effect=mock_does_exist)
        BucketMock.return_value.bucket_name = "MYBUCKET"
        function(*args, **kwargs)

    return decorator

mock_does_exist_lambda = lambda x: True

def mock_does_exist(args):
    return mock_does_exist_lambda(args)



class VideoClientTestCase(unittest.TestCase):

    @mocked
    def test_get_v2_base_key(self, *mocks):
        client = VideoClient()
        mac_address = 'B3-A2-34-54-76-23-AE-69'
        time = dateutil.parser.parse('2014-10-02 15:50:00Z')

        self.assertEqual(client.get_v2_base_key(mac_address, time), '96ea326745432a3b/2014/10/02/15.50.video')

        with self.assertRaises(Exception):
            time = dateutil.parser.parse('2014-10-02 15:52:00') #not 10-minute aligned
            client.get_v2_base_key(mac_address, time)

        with self.assertRaises(Exception):
            time = dateutil.parser.parse('2014-10-02 15:50:00') #timezone-naive date
            client.get_v2_base_key(mac_address, time)

    @mocked
    def test_get_v2_video_info(self, *mocks):
        client = VideoClient()
        mac_address = 'B3-A2-34-54-76-23-AE-69'
        time = dateutil.parser.parse('2014-10-02 15:50:00Z')
        v2_info = client.get_v2_video_info(mac_address, time, 'front')
        BucketMock = mocks[0]
        self.assertEqual(v2_info.bucket_name, BucketMock.return_value.bucket_name)
        self.assertEqual(v2_info.video_key, '96ea326745432a3b/2014/10/02/15.50.video.mp4')
        self.assertEqual(v2_info.preview_image_key, '96ea326745432a3b/2014/10/02/15.50.video.poster.jpg')
        self.assertEqual(v2_info.captions_key, '96ea326745432a3b/2014/10/02/15.50.video.vtt')
        self.assertEqual(v2_info.thumbnails_key, '96ea326745432a3b/2014/10/02/15.50.video.thumbs.vtt')

    @mocked
    def test_get_v3_base_key(self, *mocks):
        client = VideoClient()
        slab_id = 'B3-A2-34-54-76-23-AE-69'
        time = dateutil.parser.parse('2014-10-02 15:50:00Z')

        self.assertEqual(client.get_v3_base_key(slab_id, 'back', time), '96ea326745432a3b/2014/10/02/15.50.video.back')

        with self.assertRaises(Exception):
            time = dateutil.parser.parse('2014-10-02 15:52:00') #not 10-minute aligned
            client.get_v3_base_key(slab_id, 'back', time)

        with self.assertRaises(Exception):
            time = dateutil.parser.parse('2014-10-02 15:50:00') #timezone-naive date
            client.get_v3_base_key(slab_id, 'back', time)

    @mocked
    def test_get_v3_video_info(self, *mocks):
        client = VideoClient()
        slab_id = 'B3-A2-34-54-76-23-AE-69'
        time = dateutil.parser.parse('2014-10-02 15:50:00Z')
        v3_info = client.get_v3_video_info(slab_id, time, 'front')
        BucketMock = mocks[0]
        self.assertEqual(v3_info.bucket_name, BucketMock.return_value.bucket_name)
        self.assertEqual(v3_info.video_key, '96ea326745432a3b/2014/10/02/15.50.video.front.mp4')
        self.assertEqual(v3_info.preview_image_key, '96ea326745432a3b/2014/10/02/15.50.video.front.poster.jpg')
        self.assertEqual(v3_info.captions_key, '96ea326745432a3b/2014/10/02/15.50.video.front.vtt')
        self.assertEqual(v3_info.thumbnails_key, '96ea326745432a3b/2014/10/02/15.50.video.front.thumbs.vtt')

    @mocked
    def test_get_videos(self, *mocks):

        client = VideoClient()
        mac_address = 'B3-A2-34-54-76-23-AE-69'
        reversed_mac = reversed_normalized_source_id(mac_address)
        slab_id = 'ABCDEFG'
        reversed_slab_id = reversed_normalized_source_id(slab_id)
        time = dateutil.parser.parse('2014-10-02 15:50:00Z')
        global mock_does_exist_lambda


        #Test that only V3 is returned when both V2 and V3 exist
        camera_position = 'back'
        videos = client.get_videos(camera_position, time, time, mac_address, slab_id)
        self.assertEqual(len(videos), 1)
        video_info = videos[0]
        self.assert_video_info_equal(video_info, client.get_v3_video_info(slab_id, time, camera_position))

        #Test V3 and no V2
        mock_does_exist_lambda = lambda args: slab_id.lower()[::-1] in args
        camera_position = 'back'
        videos = client.get_videos(camera_position, time, time, mac_address, slab_id)
        self.assertEqual(len(videos), 1)
        video_info = videos[0]
        self.assert_video_info_equal(video_info, client.get_v3_video_info(slab_id, time, camera_position))

        #Test V2 and no V3
        mock_does_exist_lambda = lambda args: reversed_mac in args
        camera_position = 'back'
        videos = client.get_videos(camera_position, time, time, mac_address, slab_id)
        self.assertEqual(len(videos), 1)
        video_info = videos[0]
        self.assert_video_info_equal(video_info, client.get_v2_video_info(mac_address, time, camera_position))

        #Test that None mac address is handled gracefully
        videos = client.get_videos(camera_position, time, time, None, slab_id)
        self.assertEqual(len(videos), 0)

        #restore object_exists mock to always return True
        mock_does_exist_lambda = lambda x: True



    def assert_video_info_equal(self, a, b):
        self.assertEqual(a.bucket_name      ,b.bucket_name)
        self.assertEqual(a.video_key        ,b.video_key)
        self.assertEqual(a.preview_image_key,b.preview_image_key)
        self.assertEqual(a.captions_key     ,b.captions_key)
        self.assertEqual(a.thumbnails_key   ,b.thumbnails_key)
        self.assertEqual(a.camera_position  ,b.camera_position)


