from __future__ import print_function, absolute_import
from swiftclient.client import Connection
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
        else:
            logger.error('Object store {} not yet supported'.format(settings.OBJECT_STORE), exec_info=True)
            raise

class SwiftConnection(StoreConnection):

    def __init__(self):
        self.conn = Connection(authurl=settings.SWIFT_AUTH_URL, user=settings.SWIFT_AUTH_USER, key=settings.SWIFT_AUTH_KEY)

class S3Connection(StoreConnection):
    
    def __init__(self):
        self.conn = boto3.resource('s3', region_name=settings.S3_AWS_REGION, endpoint_url=settings.S3_ENDPOINT, aws_access_key_id=settings.AWS_ACCESS_KEY_ID, aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
