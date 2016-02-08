#!/usr/bin/python
import socket

import uuid
import boto
from boto.s3.bucket import Bucket
from boto3 import s3
import boto3
from botocore.exceptions import IncompleteReadError
from botocore.vendored.requests.packages.urllib3.exceptions import ReadTimeoutError
from common.utils.decorators import retry
import os


class S3FileFetcher(object):
    # Utitlity for fetching local copies of S3 file.
    # Usage:
    # with S3FileFetcher(s3_connection, BUCKET_NAME, KEY_NAME) as local_file_name:
    #   ...do stuff...
    def __init__(self, key_name, s3_connection=None, bucket_name=None, bucket=None, download_directory = None):
        """

        :param key_name: object key
        :param s3_connection: boto2 s3 connection (ignored if bucket is not None)
        :param bucket_name: bucket name (ignored if bucket is not None)
        :param bucket: boto2 or boto3 bucket (if None, s3_connection and bucket_name must be valid)
        :param download_directory: directory to download into
        :return:
        """
        assert bucket or (bucket_name and s3_connection)

        self.bucket = bucket
        self.bucket_name = bucket_name
        self.key_name = key_name
        self.s3_connection = s3_connection
        self.download_directory = download_directory

    def __enter__(self):
        if not self.bucket:
            self.bucket = self.s3_connection.get_bucket(self.bucket_name)
        #mangle filename to avoid collisions
        self.tmpfilename = 'tmp_%s_%s' % (uuid.uuid4().int, os.path.basename(self.key_name))
        if self.download_directory:
            if not os.path.exists(self.download_directory):
                os.makedirs(self.download_directory)
            #if given a download_directory, assume caller knows enough to avoid collisions so don't mangle filename
            self.tmpfilename = os.path.join(self.download_directory, os.path.basename(self.key_name))
        self.get_file()
        return self.tmpfilename

    @retry((socket.timeout, socket.error, ReadTimeoutError, IncompleteReadError), tries=11, delay=0.1, backoff=2)
    def get_file(self):
        if isinstance(self.bucket, Bucket):
            self.get_file_from_boto2_bucket()
        else:  # assume boto3 bucket
            self.get_file_from_boto3_bucket()

    def get_file_from_boto2_bucket(self):
        key = self.bucket.get_key(self.key_name)
        if not key:
            raise ValueError
        with open(self.tmpfilename, "wb") as outfile:
            key.get_contents_to_file(outfile)

    def __exit__(self, exc_type, exc_value, tb):
        try:
            os.remove(self.tmpfilename)
        except:
            print "Could not remove temporary S3 file '%s'. Possibly removed by caller.", self.tmpfilename

    def get_file_from_boto3_bucket(self):
        s3_client = boto3.client('s3')
        s3_client.download_file(self.bucket.name, self.key_name, self.tmpfilename)

if __name__ == "__main__":
    unittest.main()
