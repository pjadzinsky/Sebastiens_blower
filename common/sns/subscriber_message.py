import common.settings
import common.sns.topic


class SubscriberMessage(object):
    """
    A utility class for parsing SQS messages from a queue that is subscribed to an SQS topic
    """

    def __init__(self, sqs_message_dict):
        super(SubscriberMessage, self).__init__()
        self.sqs_message_dict = sqs_message_dict

    def get_message(self):
        return self.sqs_message_dict['Message']

    def get_undecordated_topic_name(self, app_env=app_env):
        topic_arn = self.sqs_message_dict['TopicArn']
        _, undecorated_topic_name, _ = common.sns.topic.Topic.parse_topic_arn(topic_arn, app_env=app_env)
        return undecorated_topic_name
