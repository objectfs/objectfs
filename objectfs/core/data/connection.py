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
from swiftclient.client import Connection
import google.cloud.storage as gcstorage
import google.auth.compute_engine as gcengine
import boto3
from objectfs.settings import Settings
settings = Settings()
import logging
logger = logging.getLogger(__name__)

class StoreConnection(object):

    def __init__(self):
        return
    
    @staticmethod
    def load():
        if settings.OBJECT_STORE == 'Swift':
            return SwiftConnection()
        elif settings.OBJECT_STORE == 'S3':
            return S3Connection()
        elif settings.OBJECT_STORE == 'Google':
            return GoogleConnection()
        else:
            logger.error('Object store {} not yet supported'.format(settings.OBJECT_STORE), exec_info=True)
            raise

class SwiftConnection(StoreConnection):

    def __init__(self):
        self.conn = Connection(authurl=settings.SWIFT_AUTH_URL, user=settings.SWIFT_AUTH_USER, key=settings.SWIFT_AUTH_KEY)

class S3Connection(StoreConnection):
    
    def __init__(self):
        self.conn = boto3.resource('s3', region_name=settings.S3_AWS_REGION, endpoint_url=settings.S3_ENDPOINT, aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)

class GoogleConnection(StoreConnection):

    def __init__(self):
        self.conn = gcstorage.client.Client.from_service_account_json(settings.GOOGLE_SERVICE_ACCOUNT_JSON_PATH)
