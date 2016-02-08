import unittest
import uuid
import time
from boto.sqs.message import RawMessage
import boto3
from common.queue import MessageQueue
import common.settings
import mock


@unittest.skipIf(hasattr(common.settings, 'CIRCLE_CI'), 
                 'SKIPPIG: Circle CI lacks AWS credentials to run this test')
class MessageQueueIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.generated_queue_name = "unittest-%s" % str(uuid.uuid4())[:6]
        self.message_queue = MessageQueue(self.generated_queue_name, common.settings.AWS_DEFAULT_REGION2, prepend_app_env=True, with_dead_letter_queue=True)
        self.client = boto3.client('sqs', region_name = common.settings.AWS_DEFAULT_REGION2)

    def tearDown(self):
        self.client.delete_queue(QueueUrl=self.message_queue.queue.url)
        self.client.delete_queue(QueueUrl=self.message_queue.dead_letter_queue.queue.url)


    def test_dead_letter_queue(self):
        self.message_queue.enqueue("Allo")

        visibility_timeout=1
        time_since_receive = 0
        while time_since_receive < 30:
            if len(self.message_queue.dequeue(1,visibility_timeout)):
                time_since_receive = 0
            time_since_receive += visibility_timeout
            time.sleep(visibility_timeout)

        wait_time = 0
        while not len(self.message_queue.dead_letter_queue.dequeue(1)):
            time.sleep(5)
            wait_time += 5
            self.assertTrue(wait_time < 120, "Message did not show up in dead letter queue")



class MessageQueueUnitTest(unittest.TestCase):
    special_string = 'special-string'
    @mock.patch('boto.sqs.connect_to_region') #don't actually connect to a region
    def test_it(self, ConnectToRegion):
        ConnectToRegion.return_value = mock.Mock()
        ConnectToRegion.return_value.create_queue.return_value = mock.Mock()

        q = MessageQueue(self.special_string)
        q.conn.create_queue.assert_called_once_with(self.special_string)
        q.queue.set_message_class.assert_called_once_with(RawMessage)

        with self.assertRaises(Exception):
            q.enqueue(1)

        q.queue.write = mock.Mock()
        q.enqueue("text")
        m = q.queue.write.call_args[0][0]
        self.assertEqual(RawMessage, type(m))
        self.assertEqual(m.get_body(), "text")
        q.enqueue(u"text")
        m = q.queue.write.call_args[0][0]
        self.assertEqual(RawMessage, type(m))
        self.assertEqual(m.get_body(), u"text")
        self.assertEqual(type(m.get_body()), unicode)

        q.queue.get_messages.return_value = []
        q.dequeue(10, visibility_timeout=14)
        q.queue.get_messages.assert_called_once_with(10, attributes=None, visibility_timeout=14)
        q.queue.get_messages = mock.Mock()
        q.dequeue(10, message_attributes_to_get='YAY')
        q.queue.get_messages.assert_called_once_with(10, attributes="YAY", visibility_timeout=180)

        with self.assertRaises(Exception):
            q.delete_message('a')
        q.queue.delete_message = mock.Mock()
        q.queue.delete_message(m)
        q.queue.delete_message.assert_called_once_with(m)
