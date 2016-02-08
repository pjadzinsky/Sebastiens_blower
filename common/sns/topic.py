import json
import uuid
import boto3
import common.settings
import common.queue.queue
import common.queue.sqs

class Topic(object):

    #per-region cache of sns resource
    sns_resource_dict = {}

    def __init__(self, name, region=common.settings.AWS_DEFAULT_REGION2, decorate_name=True, app_env=app_env):
        self.region = region
        self.undecordated_name = name
        self.name = name
        if decorate_name:
            self.name = Topic.decorate_topic_name(name, app_env)
        sns_resource = Topic.get_sns_resource(region)
        self.boto_topic = sns_resource.create_topic(Name=self.name)

    def attach_queue(self, queue_name):
        notification_queue = common.queue.queue.MessageQueue(queue_name, self.region, True, True)

        # Ensure that we are subscribed to the SNS topic
        subscribed = False
        for subscription in self.boto_topic.subscriptions.all():
            if subscription.attributes['Endpoint'] == notification_queue.queue.arn:
                subscribed = True
                break

        if not subscribed:
            self.boto_topic.subscribe(Protocol='sqs', Endpoint=notification_queue.queue.arn)

        # Set up a policy to allow SNS access to the queue
        sqs = common.queue.sqs.SQS.get_sqs_resource(region=self.region)
        boto3_queue = sqs.get_queue_by_name(QueueName=notification_queue.queue_name)
        if 'Policy' in boto3_queue.attributes:
            policy = json.loads(boto3_queue.attributes['Policy'])
        else:
            policy = {'Version': '2008-10-17'}

        statement = {"Sid": str(uuid.uuid4()),
                     "Effect": "Allow",
                     "Principal": {
                        "AWS": "*"
                     },
                     "Action": "SQS:SendMessage",
                     "Resource": notification_queue.queue.arn,
                     "Condition": {
                        "StringLike": {
                            "aws:SourceArn": self.boto_topic.arn
                        }
                    },
                    'Resource': notification_queue.queue.arn
                   }
        policy['Statement'] = [statement]

        boto3_queue.set_attributes(Attributes={
            'Policy': json.dumps(policy)
        })

        return notification_queue

    @classmethod
    def does_exist(cls, name, region=common.settings.AWS_DEFAULT_REGION2, decorate_name=True, app_env=app_env):
        sns_resource = Topic.get_sns_resource(region)
        if decorate_name:
            name = Topic.decorate_topic_name(name, app_env)
        for topic in sns_resource.topics.all():
            curr_name, curr_undecorated_name, curr_region = cls.parse_topic_arn(topic.arn, app_env if decorate_name else None)
            assert region == curr_region
            if name == curr_name:
                return True
        return False

    @classmethod
    def parse_topic_arn(cls, topic_arn, app_env=app_env):
        _arn_, _aws_, _sns_, region, _, name = topic_arn.split(':')
        assert _arn_ == "arn" and _aws_=="aws" and _sns_ == "sns"
        return name, cls.undecorate_topic_name(name, app_env), region

    @classmethod
    def decorate_topic_name(cls, name, app_env=app_env):
        if app_env is not None:
            return "%s-%s" % (app_env, name)
        return name

    @classmethod
    def undecorate_topic_name(cls, name, app_env=app_env):
        if app_env is not None and name.startswith("%s-" % app_env):
            return name[len(app_env)+1:]
        return name

    @classmethod
    def get_sns_resource(cls, region):
        sns_resource = Topic.sns_resource_dict.get(region)
        if not sns_resource:
            sns_resource = boto3.resource('sns', region_name=region)
            Topic.sns_resource_dict[region] = sns_resource
        return sns_resource

    def delete(self):
        self.boto_topic.delete()

