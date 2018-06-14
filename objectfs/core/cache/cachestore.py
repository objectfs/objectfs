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
from abc import abstractmethod
import redis
import llfuse
import os
import stat
from objectfs.core.common.redispool import RedisPool
from objectfs.settings import Settings
settings = Settings()
import logging
logger = logging.getLogger(__name__)

FS_DELIMITER = '%'
NAME_DELIMITER = '#'
LIST_DELMITER = '&'
DATA_DELIMITER = '&'

class CacheStore(object):
    
    # @staticmethod
    # def load(fs_name, cache_store=settings.CACHE_STORE):
        # return CacheStore.get_store(cache_store)(fs_name)
    
    def __init__(self, fs_name):
        self._fs_name = fs_name
        
    @abstractmethod
    def write_inode(self, inode_id, offset, buf, object_block_id=0):
        """Write an inode to cache"""
        return NotImplemented

    @abstractmethod
    def read_inode(self, inode_id, offset, size, object_block_id=0):
        """Read an inode from cache"""
        return NotImplemented
    
    @abstractmethod
    def put_inode(self, inode_id, data, object_block_id=0):
        """Put an inode inside cache"""
        return NotImplemented
    
    @abstractmethod
    def get_inode(self, inode_id, object_block_id=0):
        """Get an inode from the cache"""
        return NotImplemented
    
    @abstractmethod
    def remove_inode(self, inode_id, object_block_id=0):
        """Delete the inode from cache"""
        return NotImplemented
    
    @abstractmethod
    def exists_inode(self, inode_id, object_block_id=0):
        """Check if inode exists"""
        return NotImplemented
    
class RedisCacheStore(CacheStore):

    def __init__(self, fs_name):
        super(self.__class__, self).__init__(fs_name)
        try:
            # self._client = redis.StrictRedis(connection_pool=RedisPool.blocking_pool)
            self._client = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=0)
            self._pipe = self._client.pipeline(transaction=False)
        except redis.ConnectionError as e:
            logger.error("Cannot connect to Redis server")
            raise e
    
    def _cache_key(self, inode_id, object_block_id):
        return '{}{}{}{}{}{}{}'.format(self._fs_name, FS_DELIMITER, 'data', FS_DELIMITER, inode_id, FS_DELIMITER, object_block_id)

    def write_inode(self, inode_id, offset, buf, object_block_id=0):
        """Write an inode to cache"""
        try:
            logger.debug("Write inode:{} to cache at offset:{},length:{}".format(inode_id, offset, len(buf)))
            response = self._client.setrange(self._cache_key(inode_id, object_block_id), offset, buf)
            return response
        except Exception as e:
            logger.error("Failed to write inode:{} to cache at offset:{},length:{}".format(inode_id, offset, len(buf)), exc_info=True)
            raise e
    
    def read_inode(self, inode_id, offset, size, object_block_id=0):
        """Read an inode from cache"""
        try:
            logger.debug("Read inode:{} from cache with offset:{},size:{}".format(inode_id, offset, size))
            response = self._client.getrange(self._cache_key(inode_id, object_block_id), offset, size)
            return response
        except Exception as e:
            logger.error("Failed to read inode:{} from cache with offset:{},size:{}".format(inode_id, offset, size), exc_info=True)
            raise e
    
    def put_inode(self, inode_id, data, object_block_id=0):
        """Put an inode inside cache"""
        try:
            logger.debug("Put inode:{} into cache".format(inode_id))
            response = self._client.set(self._cache_key(inode_id, object_block_id), data)
            return response
        except Exception as e:
            logger.error("Failed to put inode:{} into cache".format(inode_id), exc_info=True)
            raise e
    
    def get_inode(self, inode_id, object_block_id=0):
        """Get an inode from the cache"""
        try:
            logger.debug("Get inode:{} from cache".format(inode_id))
            response = self._client.get(self._cache_key(inode_id, object_block_id))
            return response
        except Exception as e:
            logger.error("Failed to get inode:{} from cache".format(inode_id), exc_info=True)
            raise e

    def remove_inode(self, inode_id, object_block_id=0):
        """Delete the inode from cache"""
        try:
            logger.debug("Remove inode:{} from cache".format(inode_id))
            response = self._client.delete(self._cache_key(inode_id, object_block_id))
            return response
        except Exception as e:
            logger.error("Failed to remove inode:{} from cache".format(inode_id), exc_info=True)
            raise e
     
    def exists_inode(self, inode_id, object_block_id=0):
        """Check if inode exists"""
        try:
            logger.debug("Check if inode:{} exists".format(inode_id))
            response = self._client.exists(self._cache_key(inode_id, object_block_id))
            return response
        except Exception as e:
            logger.error("Failed to check if inode:{} exists".format(inode_id), exc_info=True)
            raise e

class FileCacheStore(CacheStore):

    def __init__(self, fs_name):
        super(self.__class__, self).__init__(fs_name)
        
    def _cache_key(self, inode_id, object_block_id):
        return '{}{}{}{}{}{}{}{}'.format(settings.FILE_CACHE_MOUNT_POINT, self._fs_name, FS_DELIMITER, 'data', FS_DELIMITER, inode_id, FS_DELIMITER, object_block_id)
    
    def _open_cache_block(self, inode_id, object_block_id, file_flag):
        return os.open(self._cache_key(inode_id, object_block_id), file_flag) 

    def write_inode(self, inode_id, object_block_id, offset, buf):
        """Write an inode to cache"""
        try:
            logger.debug("Write inode:{} to cache at offset:{},length:{}".format(inode_id, offset, len(buf)))
            # opening the file with write only and direct mode
            file_descp = self._open_cache_block(inode_id, object_block_id, os.O_WRONLY | os.O_CREAT)
            # set to SEEK_SET which is relative to start of file
            os.lseek(file_descp, offset, os.SEEK_SET)
            os.write(file_descp, buf)
        except Exception as e:
            print(e)
            raise e

    def read_inode(self, inode_id, object_block_id, offset, size):
        """Read an inode from cache"""
        try:
            logger.debug("Read inode:{} from cache with offset:{},size:{}".format(inode_id, offset, size))
            # opening the file with read only and direct mode
            file_descp = self._open_cache_block(inode_id, object_block_id, os.O_RDONLY | os.O_CREAT)
            # set to SEEK_SET which is relative to start of file
            os.lseek(file_descp, offset, os.SEEK_SET)
            return os.read(file_descp, size)
        except Exception as e:
            print(e)
            raise e
    
    def get_inode(self, inode_id, object_block_id):
        """Get an inode from the cache"""
        try:
            return ''
        except Exception as e:
            print(e)
            raise e

    def put_inode(self, inode_id, data, object_block_id=0):
        """Put an inode inside cache"""
        try:
            logger.debug("Put inode:{} into cache".format(inode_id))
            # os.mknod(self._cache_key(inode_id, object_block_id), mode=0600|stat.S_IFREG)
            file_descp = os.open(self._cache_key(inode_id, object_block_id), os.O_CREAT)
            os.close(file_descp)
        except Exception as e:
            print(e)
            raise e
    
    def remove_inode(self, inode_id, object_block_id=0):
        """Delete the inode from cache"""
        try:
            logger.debug("Remove inode:{} from cache".format(inode_id))
            os.unlink(self._cache_key(inode_id, object_block_id))
        except Exception as e:
            print(e)
            raise e

    def exists_inode(self, inode_id, object_block_id=0):
        """Check if inode exists"""
        try:
            logger.debug("Check if inode:{} exists".format(inode_id))
            return os.path.exists(self._cache_key(inode_id, object_block_id))
        except Exception as e:
            print(e)
            raise e

class CacheStoreFactory(object):
    
    __store_classes = {
        'Redis': RedisCacheStore,
        'File': FileCacheStore
    }
    
    @staticmethod
    def create_store(fs_name, cache_store=settings.CACHE_STORE):
        store_class = CacheStoreFactory.__store_classes.get(cache_store)
        if store_class:
            return store_class(fs_name)
        else:
            logger.error("Cache store in {} is not supported".format(cache_store))
            raise NotImplementedError("Cache store in {} is not supported".format(cache_store))
