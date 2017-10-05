from __future__ import absolute_import, print_function
from abc import abstractmethod
from objectfs.core.data.connection import SwiftConnection, S3Connection
from objectfs.core.data.container import SwiftContainer, S3Container
from objectfs.settings import Settings
settings = Settings()
import logging
logger = logging.getLogger(__name__)

class ObjectStore(object):
    
    def __init__(self, container_name):
        self._container_name = container_name
    
    @staticmethod
    def load(fs_name, object_store=settings.OBJECT_STORE):
        return ObjectStore.get_store(object_store)(fs_name)
    
    @staticmethod
    def get_store(object_store=settings.OBJECT_STORE):
        if object_store == 'Swift':
            return SwiftStore
        elif object_store == 'S3':
            return S3Store
        else:
            logging.error('Object store {} not yet supported'.format(settings.OBJECT_STORE), exec_func=True)
            raise
    
    def get_dnode(self, inode_id, object_block_id=None):
        """Return the corresponding data node for this inode"""
        logger.debug('GET Dnode for inode {}'.format(inode_id))
        return self.container.object(inode_id).get(object_block_id)

    def put_dnode(self, inode_id, data):
        """Insert a data node for this inode"""
        logger.debug('PUT Dnode for inode {}'.format(inode_id))
        return self.container.object(inode_id).put(data)

    def delete_dnode(self, inode_id):
        """Delete the data node for this inode"""
        logger.debug('DELETE Dnode for inode {}'.format(inode_id))
        return self.container.object(inode_id).delete()
    
    def multipart_upload_dnode(self, inode_id, object_block_id, multipart_id, data):
        """Return the multipart for this inode"""
        logger.debug('MULTIPART Dnode for inode {}'.format(inode_id))
        return self.container.object(inode_id).upload_part(object_block_id, multipart_id, data)
    
    def multipart_upload_initiate(self, inode_id):
        """Initiate a multipart upload"""
        logger.debug('Start MULTIPART upload for inode {} multipart {}'.format(inode_id, multipart_id))
        return self.container.object(inode_id).initiate_multipart_upload()

    def multipart_upload_complete(self, inode_id, multipart_id, etag_part_list):
        """Complete a multipart upload"""
        logger.debug('Complete MULTIPART upload for inode {} multipart {}'.format(inode_id, multipart_id))
        return self.container.object(inode_id).complete_multipart_upload(multipart_id, etag_part_list)

    @property
    def container(self):
        return self.__class__.load_container()(self._container_name, self.__class__.connection()())

class SwiftStore(ObjectStore):

    @staticmethod
    def load_container():
        """Returns a class for static methods"""
        return SwiftContainer

    @staticmethod
    def connection():
        return SwiftConnection
    
class S3Store(ObjectStore):

    @staticmethod
    def load_container():
        return S3Container

    @staticmethod
    def connection():
        return S3Connection
