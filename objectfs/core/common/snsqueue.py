# Copyright 2017 IBM Corporation
# Copyright 2017 The Johns Hopkins University
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function, absolute_import
import boto3
from objectfs.settings import Settings
settings = Settings()

class SnsConnection(object):

    def __init__(self):
        self.conn = boto3.client('sns', region_name=settings.S3_AWS_REGION, endpoint_url=settings.S3_ENDPOINT, aws_access_key=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)


class SnsTopic(object):

    def __init__(self):
        self.conn = boto3.client('sns', region_name=settings.S3_AWS_REGION, endpoint_url=settings.S3_ENDPOINT, aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
        sns = boto3.resource('sns', region_name=settings.S3_AWS_REGION, endpoint_url=settings.S3_ENDPOINT, aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
        self.topic_name = settings.SNS_TOPIC_NAME
        self.topic = sns.Topic(self.topic_name)
        self.endpoint = 'http://{}:{}'.format(settings.SNS_HOST, settings.SNS_PORT)
    
    def create(self):
        """Create topic"""
        response = self.conn.create_topic(
                    Name = self.topic_name,
                    Attributes={
                      'DisplayName': self.topic_name
                    }
                  )
        return response

    def subscribe(self):
        """Subscribe to SNS topic"""
        response = self.topic.subscribe(Protocol='http', Endpoint=self.endpoint)
    
    def confirm_subscription(self, token):
        """Confirm topic subsscription"""
        response = self.topic.confirm_subscription(Token=token)
        return response

    def delete(self):
        """Delete the topic"""
        response = self.topic.delete()
