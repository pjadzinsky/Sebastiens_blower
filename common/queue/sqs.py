import boto3


class SQS(object):
    #per-region cache of boto3 sqs resource
    sqs_resource_dict = {}


    @classmethod
    def get_sqs_resource(cls, region):
        sqs_resource = SQS.sqs_resource_dict.get(region)
        if not sqs_resource:
            sqs_resource = boto3.resource('sqs', region_name=region)
            SQS.sqs_resource_dict[region] = sqs_resource
        return sqs_resource


