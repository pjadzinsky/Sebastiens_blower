#!/usr/bin/python

import unittest
import uuid
from StringIO import StringIO
from common.bucket.bucket import Bucket

import os
import boto
from boto.s3.key import Key
from common.bucket.s3_file_fetcher import S3FileFetcher
import common.settings

TEST_BUCKET_NAME = "mousera-testbucket"
TEST_KEY_NAME = "lovely-data.boink"
TEST_CONTENTS = "This is our lovely data"

# TODO: These are integration tests w/ S3 - implement a unit test using moto
# 

@unittest.skipIf(hasattr(common.settings, 'CIRCLE_CI'), 
                 'SKIPPIG: Circle CI lacks AWS credentials to run this test')
class S3FileFetcherIntegrationTestCase (unittest.TestCase):
    def setUp(self):
        #Boto2 bucket
        self.c = boto.connect_s3()
        self.bucket = self.c.get_bucket(TEST_BUCKET_NAME)
        self.test_key = Key(self.bucket)
        self.test_key.key = TEST_KEY_NAME
        self.test_key.set_contents_from_string(TEST_CONTENTS)

        #Boto3 bucket
        boto3_bucket_name = "testbucket" + str(uuid.uuid4())[:4]
        self.common_bucket = Bucket(boto3_bucket_name)
        self.common_bucket.boto_bucket.put_object(Key=TEST_KEY_NAME, Body=StringIO(TEST_CONTENTS))


    def tearDown(self):
        self.test_key.delete()

        for key in self.common_bucket.boto_bucket.objects.all():
            key.delete()
        self.common_bucket.delete_bucket()


    def test_invalid_fetch(self):
        with self.assertRaises(ValueError) as cm:
            with S3FileFetcher("Garbagio", self.c, TEST_BUCKET_NAME) as local_file_name:
                pass

    def test_fetch(self):
        with S3FileFetcher(TEST_KEY_NAME, self.c, TEST_BUCKET_NAME) as local_file_name:
            with open(local_file_name, "r") as local_file:
                local_file_contents = local_file.read()
                self.assertEqual(local_file_contents, TEST_CONTENTS, "Fetched file is not the same as S3")
        #Make sure the fetched file was deleted
        with self.assertRaises(IOError) as cm:
            with open(local_file_name, "r") as file:
                pass


    def test_fetch_with_boto3_bucket(self):
        with S3FileFetcher(TEST_KEY_NAME, bucket=self.common_bucket.boto_bucket) as local_file_name:
            with open(local_file_name, "r") as local_file:
                local_file_contents = local_file.read()
                self.assertEqual(local_file_contents, TEST_CONTENTS, "Fetched file is not the same as S3")

        #Make sure the fetched file was deleted
        with self.assertRaises(IOError) as cm:
            with open(local_file_name, "r") as file:
                pass


    def test_fetch_into_directory(self):
        download_dir = "test/1/2/3"
        with S3FileFetcher(TEST_KEY_NAME, self.c, TEST_BUCKET_NAME, download_directory=download_dir) as local_file_name:
            self.assertTrue(os.path.isfile(local_file_name))
            self.assertTrue(local_file_name.startswith(download_dir))


if __name__ == "__main__":
    unittest.main()


