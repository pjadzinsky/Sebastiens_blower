import json
import boto.sqs
import boto.s3
import boto.s3.key
from boto.sqs.message import RawMessage
from common.log import *
import common.settings
import base64


class MessageQueue(object):

    def __init__(self, queue_name, region="us-west-1", prepend_app_env=False, with_dead_letter_queue=False):
        if prepend_app_env:
            self.queue_name = "%s-%s" % (app_env, queue_name)
        else:
            self.queue_name = queue_name
        self.conn = boto.sqs.connect_to_region(region)
        self.queue = self.conn.create_queue(self.queue_name)
        self.queue.set_message_class(RawMessage)
        self.region = region

        self.max_receive_count = -1
        self.dead_letter_queue = None
        if with_dead_letter_queue:
            self.set_up_dead_letter_queue()
        return

    def enqueue(self, message, delay_seconds=None):
        if not type(message) in (str, unicode):
            raise Exception("MessageQueue.enqueue: message must be of type str or unicode.  Was: %s" % type(message))
        m = RawMessage()
        m.set_body(message)
        self.queue.write(m, delay_seconds=delay_seconds)
        return

    def dequeue(self, count, visibility_timeout=180, message_attributes_to_get=None):
        # NOTE: The visibility_timeout should be at least as long as the 
        # time-to-process-one-message... See AWS SQS docs for details. 
        # TODO(heathkh): Perhaps remove the default so user knows they have to
        # set this carefully for their application if they expect something like 
        # exactly-once semantics 
        return self.queue.get_messages(count, visibility_timeout=visibility_timeout, attributes=message_attributes_to_get)

    def delete_message(self, message):
        if not issubclass(message.__class__, RawMessage):
            raise Exception("MessageQueue.delete_message: message must be of type RawMessage.  Was: %s" % message)
        self.queue.delete_message(message)
        return

    def delete_queue(self):
        print "Deleting queue %s" % self.queue_name
        self.queue.delete()
        if self.dead_letter_queue:
            self.dead_letter_queue.delete_queue()

    def set_up_dead_letter_queue(self, dead_letter_queue_name=None, max_receive_count=10):
        self.max_receive_count = max_receive_count
        if not dead_letter_queue_name:
            dead_letter_queue_name = "%s-dead-letters" % self.queue_name
        print "Attaching dead letter queue %s to queue %s" % (dead_letter_queue_name, self.queue.url)
        self.dead_letter_queue = MessageQueue(dead_letter_queue_name, self.region, False, False)

        policy = {
          "maxReceiveCount" : max_receive_count,
          "deadLetterTargetArn": self.dead_letter_queue.queue.arn
        }

        self.queue.set_attribute('RedrivePolicy', json.dumps(policy))


class ProtobufferMessageQueue(MessageQueue):

    def __init__(self, proto_buffer_class, queue_name, region="us-west-1", prepend_app_env=False, with_dead_letter_queue=False):
        super(ProtobufferMessageQueue, self).__init__(queue_name, region, prepend_app_env, with_dead_letter_queue)
        self._proto_buffer_class = proto_buffer_class
        return

    def enqueue(self, proto):
        if not hasattr(proto, 'SerializeToString'):
            raise Exception("Expected a protobuf.")
        data = base64.b64encode(proto.SerializeToString())
        super(ProtobufferMessageQueue, self).enqueue(data)
        return

    def dequeue(self, visibility_timeout=180):  
        message_list = super(ProtobufferMessageQueue, self).dequeue(1, visibility_timeout)
        result = None, None
        if message_list:
            message = message_list[0]
            proto = self._proto_buffer_class()
            proto.ParseFromString(base64.b64decode(message.get_body()))
            result = (message, proto)
        return result
        

    



class PrioritizedQueueConsumer(object):
    """ Dequeue tasks from a set of queues, in order of priority. """
    def __init__(self, queues):
        self._queues = queues  # earlier in list = higher priority
        return

    def dequeue(self, visibility_timeout):  
        # NOTE: The visibility_timeout should be at least as long as the 
        # time-to-process-one-message... See AWS SQS docs for details. 
        message = None
        for q in self._queues:
            messages = q.dequeue(1, visibility_timeout)
            if messages:
                LOG(INFO, 'Dequeued message from: %s' % (q.queue.name))
                message = messages[0]
                break
        return message
    

