import json
import uuid
from botocore.exceptions import ClientError
import common.settings
import boto3
import common.queue
from common.bucket import utils
from common.utils.decorators import retry


class Bucket:

    #per-region cache of s3 resource
    s3_resource_dict = {}
    #cached references to boto buckets
    boto_bucket_cache = {}

    def __init__(self, bucket_base_name, region=common.settings.AWS_DEFAULT_REGION2, create_if_does_not_exist=True, decorate_name=True):
        self.region = region
        self.bucket_base_name = bucket_base_name
        if decorate_name:
            self.bucket_name = Bucket.decorate_bucket_name(self.region, app_env, bucket_base_name)
        else:
            self.bucket_name = bucket_base_name

        self.s3 = Bucket.get_s3_resource(region)

        self.get_boto_bucket(region)

        #check if bucket exists:
        try:
            creation_date = self.boto_bucket.creation_date
        except ClientError as ce:
            if create_if_does_not_exist:
                self.boto_bucket = self.s3.create_bucket(Bucket=self.bucket_name, CreateBucketConfiguration={'LocationConstraint':region})
                self.boto_bucket.wait_until_exists()
            else:
                print ("Bucket %s does not exist or you do not have access." % self.bucket_name)
                raise

    def get_boto_bucket(self, region):
        self.boto_bucket = Bucket.boto_bucket_cache.get(self.bucket_name)
        if not self.boto_bucket:
            self.boto_bucket = self.s3.Bucket(self.bucket_name)
            Bucket.boto_bucket_cache[self.bucket_name] = self.boto_bucket

    @classmethod
    def decorate_bucket_name(cls, region_name, env, bucket_base_name):
        return utils.decorate_bucket_name(bucket_base_name, region_name, env)

    def delete_bucket(self):
        self.boto_bucket.delete()

    def delete_object(self, key):
        object = self.boto_bucket.Object(key)
        object.delete()

    @retry(ClientError, tries=11, delay=0.1, backoff=2)
    def upload_file(self, key, filename, metadata=None):
        self.upload_file_no_retry(filename, key, metadata)

    def upload_file_no_retry(self, filename, key, metadata):
        with open(filename, 'rb') as data:
            self.boto_bucket.put_object(Key=key, Body=data, StorageClass='REDUCED_REDUNDANCY',
                                        Metadata=metadata if metadata is not None else {})

    def set_up_notification_queue(self):
        print "Attaching S3 bucket %s to notification queue..." % (self.bucket_name)

        #Allow the slab-data bucket to send notifications to the queue
        self.notification_queue = common.queue.MessageQueue("s3notify-" + self.bucket_base_name, self.region, prepend_app_env=True, with_dead_letter_queue=True)
        sqs = boto3.resource('sqs', region_name=self.region)
        queue = sqs.get_queue_by_name(QueueName=self.notification_queue.queue_name)    #get the boto3 queue

        queue_arn = queue.attributes['QueueArn']
        policy = {
            "Version": "2008-10-17",
            "Id": str(uuid.uuid4()),
            "Statement": [
                {
                    "Sid": str(uuid.uuid4()),
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": "*"
                    },
                    "Action": [
                        "SQS:SendMessage"
                    ],
                    "Resource": queue_arn,
                    "Condition": {
                        "ArnLike": {
                            "aws:SourceArn": "arn:aws:s3:*:*:%s" % self.bucket_name
                        }
                    }
                }
            ]
        }
        queue.set_attributes(QueueUrl=queue.url, Attributes={'Policy':json.dumps(policy)})

        #Hook up bucket to send notifications to queue (if not already hooked up)
        bucket_notification = self.s3.BucketNotification(self.bucket_name)
        queue_configurations = bucket_notification.queue_configurations
        found_notification = False
        if queue_configurations:
            for queue_configuration in queue_configurations:
                if queue_configuration['QueueArn'] == queue_arn and 's3:ObjectCreated:*' in queue_configuration['Events']:
                    found_notification = True
        if not found_notification:
            bucket_notification.put(
                NotificationConfiguration={'QueueConfigurations':
                   [{u'Events': ['s3:ReducedRedundancyLostObject', 's3:ObjectCreated:*'],
                     u'QueueArn':queue_arn}]
                                           }
                )

            print "Attached S3 bucket %s to notification queue %s" % (self.bucket_name, self.notification_queue.queue_name)
        else:
            print "S3 bucket %s already attached to notification queue %s" % (self.bucket_name, self.notification_queue.queue_name)

        return self.notification_queue

    def does_object_exist(self, key):
        object = self.boto_bucket.Object(key)
        try:
            metadata = object.metadata
            return True
        except ClientError:
            return False

    @classmethod
    def get_s3_resource(cls, region):
        s3_resource = Bucket.s3_resource_dict.get(region)
        if not s3_resource:
            s3_resource = boto3.resource('s3', region_name=region)
            Bucket.s3_resource_dict[region] = s3_resource
        return s3_resource


