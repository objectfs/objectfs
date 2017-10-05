from __future__ import absolute_import, print_function
from abc import ABCMeta, abstractmethod
import six
import boto3
from swiftclient.exceptions import ClientException
from objectfs.core.data.connection import SwiftConnection, S3Connection
from objectfs.core.data.object import SwiftObject, S3Object
import logging
logger = logging.getLogger(__name__)

@six.add_metaclass(ABCMeta)
class Container(object):
    
    def __init__(self, container_name, store_connection):
        self._name = container_name
        self._connection = store_connection
    
    @property
    def name(self):
        return self._name

    @abstractmethod
    def create(self):
        """Create a container"""
        return NotImplemented
    
    @abstractmethod
    def post(self):
        """Update a container"""
        return NotImplemented
    
    @abstractmethod
    def delete(self):
        """Delete a container"""
        return NotImplemented
    
    @staticmethod
    @abstractmethod
    def list():
        """List Containers"""
        return NotImplemented

    @abstractmethod
    def object(object_name):
        """Load a object"""
        return NotImplemented

class SwiftContainer(Container):

    def __init__(self, container_name, store_connection):
        super(SwiftContainer, self).__init__(container_name, store_connection)
    
    def object(self, object_name):
        return SwiftObject(self, object_name, SwiftConnection())
    
    @staticmethod
    def list():
        """List containers"""
        connection = SwiftConnection()
        try:
            logger.debug("LIST all Swift containers")
            response_headers, container_list = connection.conn.get_account()
        except ClientException as e:
            logger.error("Failed to LIST all Swift containers", exc_info=True)
            raise e
        # iterate over the container list and return a container object
        for container_item in container_list:
            yield SwiftContainer(container_item['name'], SwiftConnection())

    def create(self):
        """Create a container"""
        try:
            logger.debug("CREATE swift container {}".format(self.name))
            response = self._connection.conn.put_container(self.name)
            return response
        except ClientException as e:
            logger.error("Failed to CREATE swift container {}".format(self.name))
            raise e

    def post(self):
        """Update a container"""
        try:
            logger.debug("UPDATE swift container {}".format(self.name))
            response = self._connection.conn.post_container(self.name)
            return response
        except ClientException as e:
            logger.error("Failed to UPDATE swift container {}".format(self.name), exc_info=True)
            raise e

    def delete(self):
        """Delete a container"""
        try:
            self.delete_all_objects()
            logger.debug("DELETE swift container {}".format(self.name))
            response = self._connection.conn.delete_container(self.name)
            return response
        except ClientException as e:
            logger.error("Failed to DELETE swift container {}".format(self.name), exc_info=True)
            raise e
    
    def list_objects(self, prefix=None, full_listing=False):
        """Return list of objects"""
        try:
            logger.debug("LIST objects swift container {}".format(self.name))
            response = self._connection.conn.get_container(self.name, prefix=prefix, full_listing=full_listing)
            for object_item in response[1]:
                yield(self.object(object_item['name']))
        except Exception as e:
            logger.error("Failed to LIST objects in swift container {}".format(self.name), exc_info=True)
            raise e
    
    def delete_all_objects(self):
        """Delete all objects"""
        logger.debug("DELETE all objects in container {}".format(self.name))
        for object_item in self.list_objects():
            object_item.delete()

class S3Container(Container):

    def __init__(self, container_name, store_connection):
        super(S3Container, self).__init__(container_name, store_connection)
        self._bucket = self._connection.conn.Bucket(self.name)

    def create(self):
        """Create container"""
        try:
            response = self._bucket.create(
                        ACL = 'private',
                     )
        except Exception as e:
            print(e)
            raise e

    def delete(self):
        """Delete container"""
        try:
            response = self._bucket.delete()
            return response
        except Exception as e:
            print(e)
            raise e

    def post(self):
        """Update a container"""
        return NotImplemented
    
    def object(self, object_name):
        return S3Object(self, object_name, S3Connection())
    
    def list_objects(self, prefix=None, full_listing=False):
        """Return list of objects"""
        try:
            logger.debug("LIST objects s3 container {}".format(self.name))
            response = self._bucket.objects.all()
            for page in response.pages():
                for object_item in page:
                    yield(self.object(object_item.key))
        except Exception as e:
            logger.error("Failed to LIST objects in s3 container {}".format(self.name), exc_info=True)
            raise e
    
    def delete_all_objects(self):
        """Delete all objects"""
        logger.debug("DELETE all objects in container {}".format(self.name))
        for object_item in self.list_objects():
            object_item.delete()

    @staticmethod
    def list():
        """List containers"""
        connection = S3Connection()
        for bucket in list(connection.conn.buckets.all()):
            yield S3Container(bucket.name, S3Connection())
