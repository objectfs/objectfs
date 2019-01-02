import boto3
from objectfs.settings import Settings
settings = Settings()

class SnsConnection(object):

    def __init__(self):
        self.conn = boto3.resource('sns', region_name=settings.S3_AWS_REGION, endpoint_url=settings.S3_ENDPOINT, aws_access_key=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)


class SnsTopic(object):

    def __init__(self, topic_name):
        sns = boto3.resource('sns', region_name=settings.S3_AWS_REGION, endpoint_url=settings.S3_ENDPOINT, aws_access_key=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
        self.topic = sns.Topic(topic_name)
        self.endpoint = 'http://{}:{}'.format(settings.SNS_HOST, settings.SNS_PORT)

    def subscribe(self):
        """Subscribe to SNS topic"""

        response = self.topic.subscribe(Protocol='http', Endpoint=self.endpoint)
        response = self.topic.confirm_subscription(Token=response)

    def delete(self):
        """Delete the topic"""

        response = self.topic.delete()
