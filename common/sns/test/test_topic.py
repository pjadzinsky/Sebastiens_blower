#!/usr/bin/python
import unittest
import uuid
import time
import boto3
import common.sns.topic
import common.settings

sns = boto3.resource('sns', region_name = common.settings.AWS_DEFAULT_REGION2)

class TopicArnTest(unittest.TestCase):
    def test_decorate(self):
        base_name = "my_topic"
        decorated_name = common.sns.topic.Topic.decorate_topic_name(base_name, None)
        self.assertEqual(base_name, decorated_name, "When app_env is None, decoration should not do anything")
        decorated_name = common.sns.topic.Topic.decorate_topic_name(base_name)
        self.assertEqual("%s-%s" % (app_env, base_name), decorated_name, "Decoration should prepend current app_env")
        decorated_name = common.sns.topic.Topic.decorate_topic_name(base_name, "fake_app_env")
        self.assertEqual("fake_app_env-" + base_name, decorated_name, "Decoration should prepend current app_env")

    def test_parse_arn(self):
        with self.assertRaises(ValueError):
            common.sns.topic.Topic.parse_topic_arn("djfld")

        with self.assertRaises(AssertionError):
            common.sns.topic.Topic.parse_topic_arn("djfld:flkjd:lkjdf:lkjk:kjf:4lkj")

        full_name, undecorated_name, region = common.sns.topic.Topic.parse_topic_arn("arn:aws:sns:us-west-2:12345:staging-yay", app_env="staging")
        self.assertEqual(full_name, "staging-yay")
        self.assertEqual(undecorated_name, "yay")
        self.assertEqual(region, "us-west-2")


@unittest.skipIf(hasattr(common.settings, 'CIRCLE_CI'), 
                 'SKIPPIG: Circle CI lacks AWS credentials to run this test')
class TopicIntegrationTest(unittest.TestCase):
    def test_does_exists(self):
        topic_name = "testsns_" + str(uuid.uuid4())[:6]
        new_topic = sns.create_topic(Name=topic_name)
        try:
            self.assertTrue(common.sns.topic.Topic.does_exist(topic_name, region=common.settings.AWS_DEFAULT_REGION2, decorate_name=False))
        finally:
            new_topic.delete()

    def test_create_delete_topic(self):
        topic_name = str(uuid.uuid4())[:6]
        new_topic = common.sns.topic.Topic(topic_name, 'us-west-1')
        try:
            self.assertTrue(new_topic.boto_topic)
            self.assertTrue(common.sns.topic.Topic.does_exist(topic_name, 'us-west-1'))
            new_topic.delete()
            self.assertFalse(common.sns.topic.Topic.does_exist(topic_name, 'us-west-1'))
        finally:
            new_topic.delete()

    def test_attach_queue(self):
        topic_name = "testsns_" + str(uuid.uuid4())[:6]
        new_topic = common.sns.topic.Topic(topic_name)

        try:
            queue = new_topic.attach_queue(topic_name + "_notification")
            queue = new_topic.attach_queue(topic_name + "_notification")    #test idempotence
            try:
                message = "Here's my message!!"
                new_topic.boto_topic.publish(Message=message)
                wait_time = 0
                received_message = False
                while wait_time < 30:
                    messages = queue.dequeue(1)
                    if len(messages):
                        received_message = True
                        break
                    sleep_time = 2
                    time.sleep(sleep_time)
                    wait_time += sleep_time
                self.assertTrue(received_message)
            finally:
                queue.delete_queue()

        finally:
            new_topic.delete()
