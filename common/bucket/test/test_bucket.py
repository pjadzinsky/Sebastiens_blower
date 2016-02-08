#!/usr/bin/python

import unittest
import uuid
import datetime
import boto3
from botocore.exceptions import ClientError
import common.settings
import common.bucket.bucket
import mock
import os.path


@unittest.skipIf(hasattr(common.settings, 'CIRCLE_CI'), 
                 'SKIPPIG: Circle CI lacks AWS credentials to run this test')
class BucketIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.generated_bucket_name = "unittest-%s" % str(uuid.uuid4())[:6]
        self.region = common.settings.AWS_DEFAULT_REGION2
        self.s3 = boto3.resource('s3', region_name=self.region)
        self.bucket = None  #Created by unit tests

    def tearDown(self):
        if self.bucket:
            for key in self.bucket.boto_bucket.objects.all():
                key.delete()
            self.bucket.boto_bucket.delete()

    def test_create_bucket(self):
        self.bucket = common.bucket.bucket.Bucket(self.generated_bucket_name, create_if_does_not_exist=True)
        date = self.bucket.boto_bucket.creation_date    #will raise exception if bucket does not exists

    def test_upload_file(self):
        self.bucket = common.bucket.bucket.Bucket(self.generated_bucket_name, create_if_does_not_exist=True)
        test_filename = "test.txt"
        uploaded_contents = "TEST CONTENTS"
        with open(test_filename, "w") as test_file:
            test_file.write(uploaded_contents)

        #Upload a dummy file
        key_name = 'YUP'
        self.assertFalse(self.bucket.does_object_exist(key_name))
        source_metadata = {"test": "yay"}
        self.bucket.upload_file(key_name, test_filename, metadata=source_metadata)
        self.assertTrue(self.bucket.does_object_exist(key_name))

        #Download it back to make sure it worked
        s3_client = boto3.client('s3')
        download_filename = "download.txt"
        s3_client.download_file(self.bucket.bucket_name, key_name, download_filename)
        self.assertTrue(os.path.exists(download_filename))

        with open(download_filename, "r") as downloaded_file:
            downloaded_contents = downloaded_file.read()
            self.assertEqual(downloaded_contents, uploaded_contents)

        #also check the metadata
        metadata = self.bucket.boto_bucket.Object(key_name).metadata
        self.assertEqual(metadata, source_metadata)

        os.remove(download_filename)
        os.remove(test_filename)

    def test_notification_queue(self):
        self.bucket = common.bucket.bucket.Bucket(self.generated_bucket_name, create_if_does_not_exist=True)
        notification_queue = self.bucket.set_up_notification_queue()

        try:
            test_filename = "test.txt"
            uploaded_contents = "TEST CONTENTS"
            with open(test_filename, "w") as test_file:
                test_file.write(uploaded_contents)

                #Upload a dummy file
            key_name = 'YUP'
            self.bucket.upload_file(key_name, test_filename)

            wait_start_time = datetime.datetime.now()
            notification_message = None
            while not notification_message and (datetime.datetime.now() - wait_start_time < datetime.timedelta(seconds=30)):
                notification_message = notification_queue.dequeue(1)

            self.assertTrue(notification_message, "Expecting notification message on queue %s from bucket %s" % (notification_queue.queue_name, self.bucket.bucket_name))
        finally:
            notification_queue.delete_queue()





class BucketUnitTest(unittest.TestCase):
    def setUp(self):
        common.bucket.bucket.Bucket.s3_resource_dict = {}   #force re-creation of mock s3 resource

    def test_decorate_name(self):
        base_name = "booya"
        self.assertEqual(
            common.bucket.bucket.Bucket.decorate_bucket_name(common.settings.AWS_DEFAULT_REGION2, app_env, base_name ), 'mousera-%s-%s-%s' % (common.settings.AWS_DEFAULT_REGION2, app_env, base_name))

    @mock.patch('boto3.resource') #don't actually connect to a region
    def test_create_if_not_exists(self, BotoResource):
        s3_mock = mock.Mock()
        BotoResource.return_value = s3_mock
        bucket_mock = mock.MagicMock()
        s3_mock.Bucket.return_value = bucket_mock
        type(bucket_mock).creation_date = mock.PropertyMock(side_effect=ClientError({"Error": {"Code":'2', "Message":"boo"}}, "create bucket")) #simulate non-existent bucket

        bucket = common.bucket.bucket.Bucket("dummy_bucket")
        self.assertTrue(s3_mock.create_bucket.called)

        common.bucket.bucket.Bucket.s3_resource_dict = {}   #reset s3 resource cache so other tests don't see a mock

    @mock.patch('boto3.resource') #don't actually connect to a region
    def test_dont_create_if_not_exists_and_dont_want(self, BotoResource):
        s3_mock = mock.Mock()
        BotoResource.return_value = s3_mock
        bucket_mock = mock.MagicMock()
        s3_mock.create_bucket.return_value = bucket_mock
        s3_mock.Bucket.return_value = bucket_mock
        type(bucket_mock).creation_date = mock.PropertyMock(side_effect=ClientError({"Error": {"Code":'2', "Message":"boo"}}, "create bucket")) #simulate non-existent bucket

        with self.assertRaises(ClientError):
            common.bucket.bucket.Bucket("dummy_bucket", create_if_does_not_exist=False)

        self.assertFalse(s3_mock.create_bucket.called)

        common.bucket.bucket.Bucket.s3_resource_dict = {}   #reset s3 resource cache so other tests don't see a mock

    @mock.patch('boto3.resource') #don't actually connect to a region
    def test_get_bucket_from_cache(self, BotoResource):
        s3_mock = mock.Mock()
        BotoResource.return_value = s3_mock
        bucket_mock = mock.MagicMock()
        s3_mock.create_bucket.return_value = bucket_mock
        s3_mock.Bucket.return_value = bucket_mock

        bucket_name = "dummy_bucketXXX"
        b = common.bucket.bucket.Bucket(bucket_name)
        #Make sure the boto bucket is now cached by name
        self.assertEqual(common.bucket.bucket.Bucket.boto_bucket_cache.get(b.bucket_name), bucket_mock)
        s3_mock.Bucket.reset_mock()

        #Create another Bucket of the same name and make sure it didn't go to boto
        b = common.bucket.bucket.Bucket(bucket_name)
        self.assertFalse(s3_mock.Bucket.called)



