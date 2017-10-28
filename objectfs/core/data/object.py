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

from __future__ import absolute_import, print_function
from abc import ABCMeta, abstractmethod
import six
import botocore
import cStringIO
from swiftclient.exceptions import ClientException
from objectfs.settings import Settings
settings = Settings()
import logging
logger = logging.getLogger(__name__)

OBJECT_DELIMITER = '#'

@six.add_metaclass(ABCMeta)
class DataObject(object):
    
    def __init__(self, container_obj, object_name, store_connection):
        self._container = container_obj
        self._connection = store_connection
        self._name = str(object_name)
    
    @property
    def container(self):
        return self._container

    @property
    def name(self):
        return self._name

    @property
    def inode(self):
        return self._inode

    @abstractmethod
    def get(self):
        """Get an object"""
        return NotImplemented
    
    @abstractmethod
    def put(self, contents):
        """Put an object"""
        return NotImplemented
    
    @abstractmethod
    def post(self):
        """Update an object"""
        return NotImplemented

    @abstractmethod
    def delete(self):
        """Delete an object"""
        return NotImplemented
    
    @abstractmethod
    def head(self):
        """Head an object. This method does not get an object but just check if it exists or not"""
        return NotImplemented
    
    @abstractmethod
    def move(self):
        """Renames an object. This method copies the object into the new object name and 
        then deletes the old copy"""
        return NotImplemented
    
    @abstractmethod
    def initiate_multipart_upload(self):
        """Initiate a multipart upload"""
        return NotImplemented
    
    @abstractmethod
    def fetch_multipart(self, multipart_id):
        """Fetch a current multipart upload"""
        return NotImplemented
    
    @abstractmethod
    def complete_multipart_upload(self, multipart_id, etag_part_list):
        """Complete a multipart upload"""
        return NotImplemented

class SwiftObject(DataObject):

    def __init__(self, container_obj, object_name, store_connection):
        super(SwiftObject, self).__init__(container_obj, object_name, store_connection)
    
    def initiate_multipart_upload(self):
        """Initiate a multipart upload"""
        return NotImplemented
    
    def fetch_multipart(self, multipart_id):
        """Fetch a current multipart upload"""
        return NotImplemented
    
    def complete_multipart_upload(self, multipart_id, etag_part_list):
        """Complete a multipart upload"""
        return NotImplemented
    
    def put(self, contents, content_type='text/plain'):
        """Put an object"""
        try:
            logger.debug("PUT an object {} to swift container {}".format(self.name, self.container.name))
            self._connection.conn.put_object(self.container.name, self.name, contents=contents, content_type=content_type)
        except ClientException as e:
            logger.error("Failed to PUT an object {} into swift container {}".format(self.name, self.container.name), exc_info=True)
            raise e
    
    def get(self):
        """Get an object"""
        try:
            logger.debug("GET an object {} from swift container {}".format(self.name, self.container.name))
            response_headers, object_contents = self._connection.conn.get_object(self.container.name, self.name)
            return object_contents
        except ClientException as e:
            logger.error("Failed to GET an object {} from swift container {}".format(self.name, self.container.name), exc_info=True)
            raise e

    def delete(self):
        """Delete an object"""
        try:
            logger.debug("DELETE an object {} from swift container {}".format(self.name, self.container.name))
            if self.head():
                self._connection.conn.delete_object(self.container.name, self.name)
        except ClientException as e:
            logger.error("Failed to DELETE object {} from container {}.".format(self.name, self.container.name), exc_info=True)
            raise e

    def head(self):
        """Head an object. This method does not get an object but just check if it exists or not.
        For now return bool"""
        try:
            logger.debug("HEAD an object {} from swift container {}".format(self.name, self.container.name))
            response_headers = self._connection.conn.head_object(self.container.name, self.name)
            return True
        except ClientException as e:
            if e.http_status == 404:
                return False
            else:
                logger.error("Failed to HEAD object {} from container {}.".format(self.name, self.container.name), exc_info=True)
                raise(e)
    
    def move(self, new_object_name):
        """Renames an object. This method copies the object into the new object name and 
        then delets the old copy"""
        try:
            logger.debug("COPY object {} to {} in container {}".format(self.name, new_object_name, self.container.name))
            self._connection.conn.copy_object(self.container.name, self.name, destination='/{}/{}'.format(self.container.name, new_object_name))
            self.delete()
        except ClientException as e:
            logger.error("Failed to COPY object {} to object in container {}.".format(self.name, new_object_name, self.container.name), exc_info=True)
            raise e

    def post(self):
        """Update object metadata"""
        return NotImplemented
    
class S3Object(DataObject):

    def __init__(self, container_obj, object_name, store_connection):
        super(S3Object, self).__init__(container_obj, object_name, store_connection)
        self._object = self._connection.conn.Object(self.container.name, str(self.name))

    def put(self, contents):
        """Put an object"""
        try:
            file_obj = cStringIO.StringIO()
            file_obj.write(contents)
            file_obj.seek(0)
            response = self._object.upload_fileobj(file_obj)
            # response = self._object.put(
                    # ACL = 'private',
                    # Body = contents,
                    # StorageClass = 'Standard'
            # )
            return response
        except Exception as e:
            logger.error("Failed to GET an object {} from S3 bucket {}".format(self.name, self.container.name), exc_info=True)
            raise e
    
    def fetch_multipart(self, multipart_id):
        """Fetch a current multipart upload"""
        logger.debug("Fetching multipart for multipart {}".format(multipart_id))
        try:
            return self._connection.conn.MultipartUpload(self.container.name, self.name, multipart_id)
        except Exception as e:
            logger.error("Error in fetching multipart for multipart {}".format(multipart_id, exc_info=True))

    def initiate_multipart_upload(self):
        """Initiate a multipart upload"""
        try:
            logger.debug("Initating a multipart upload for object {} for S3 bucket {}".format(self.name, self.container.name))
            return self._object.initiate_multipart_upload()
        except Exception as e:
            logger.debug("Error in initating a multipart upload for object {} for S3 bucket {}".format(self.name, self.container.name, exc_info=True))
            raise e
    
    def upload_part(self, object_block_id, multipart_id, data):
        """Upload a multipart part"""
        try:
            logger.debug("Uploading multipart {} part {} for object {} for S3 bucket {}".format(multipart_id, object_block_id+1, self.name, self.container.name))
            # parts in S3 range from 1 to n, so add 1 to the object block id
            response = self.fetch_multipart(multipart_id).Part(object_block_id+1).upload(
                    Body = data
            )
            return (response['ETag'].strip('"'), object_block_id+1)
        except Exception as e:
            logger.debug("Error in uploading MULTIPART {} part {} for object {} for S3 bucket {}".format(multipart_id, object_block_id+1, self.name, self.container.name, exc_info=True))

    def complete_multipart_upload(self, multipart_id, etag_part_list):
        """Complete a multipart upload"""
        try:
            logger.debug("Completing upload for multipart {}".format(multipart_id))
            response = self.fetch_multipart(multipart_id).complete(
                    MultipartUpload = {
                        'Parts': etag_part_list
                    }
            )
            return response
        except Exception as e:
            logger.error("Error in completing upload for multipart {}".format(multipart_id, exc_info=True))

    def get(self, object_block_id=None):
        """Get an object"""
        try:
            if object_block_id is not None:
                # print(object_block_id, object_block_id*settings.DATA_BLOCK_SIZE, (object_block_id+1)*settings.DATA_BLOCK_SIZE)
                response = self._object.get(Range='bytes={}-{}'.format(object_block_id*settings.DATA_BLOCK_SIZE, (object_block_id+1)*settings.DATA_BLOCK_SIZE))
            else:
                response = self._object.get()
            return response['Body'].read()
        except Exception as e:
            print(e)
            raise e
        
    def post(self):
        """Update an object"""
        return NotImplemented

    def delete(self):
        """Delete an object"""
        try:
            response = self._object.delete()
            return response
        except Exception as e:
            print (e)
            raise e

    def head(self):
        """Check if an object exists or not"""
        try:
            response = self._object.load()
            return True
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                print(e)
                raise e

    def move(self, new_object_name):
        """Renames an object. This method copies the object into the new object name and 
        then delets the old copy"""
        try:
            new_object = self._connection.conn.Object(self.container.name, '{}{}{}'.format(self._inode.parent_inode_id, OBJECT_DELIMITER, new_object_name))
            new_object.copy_from(CopySource='{}/{}'.format(self.container.name, self.name))
            self.delete()
        except Exception as e:
            print(e)
            raise e
