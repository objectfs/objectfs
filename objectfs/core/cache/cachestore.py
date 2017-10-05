from __future__ import print_function, absolute_import
from abc import abstractmethod
import redis
import llfuse
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
    
    @staticmethod
    def load(fs_name, cache_store='Redis'):
        return CacheStore.get_store(cache_store)(fs_name)
    
    @staticmethod
    def get_store(cache_store='Redis'):
        if cache_store == 'Redis':
            return RedisCacheStore
        else:
            logger.error("Cache store in {} is not supported".format(cache_store))
            raise e

    @abstractmethod
    def write_inode(self, inode_id, offset, buf):
        """Write an inode to cache"""
        return NotImplemented

    @abstractmethod
    def read_inode(self, inode_id, offset, size):
        """Read an inode from cache"""
        return NotImplemented
    
    @abstractmethod
    def put_inode(self, inode_id, data):
        """Put an inode inside cache"""
        return NotImplemented
    
    @abstractmethod
    def get_inode(self, inode_id):
        """Get an inode from the cache"""
        return NotImplemented
    
    @abstractmethod
    def remove_inode(self, inode_id):
        """Delete the inode from cache"""
        return NotImplemented
    
    @abstractmethod
    def exists_inode(self, inode_id):
        """Check if inode exists"""
        return NotImplemented
    
class RedisCacheStore(CacheStore):

    def __init__(self, fs_name):
        try:
            self._fs_name = fs_name
            # self._client = redis.StrictRedis(connection_pool=RedisPool.blocking_pool)
            self._client = redis.StrictRedis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=0)
            self._pipe = self._client.pipeline(transaction=False)
        except redis.ConnectionError as e:
            logger.error("Cannot connect to Redis server")
            raise e
    
    def _cache_key(self, inode_id, object_block_id):
        return '{}{}{}{}{}{}{}'.format(self._fs_name, FS_DELIMITER, 'data', FS_DELIMITER, inode_id, FS_DELIMITER, object_block_id)

    def write_inode(self, inode_id, object_block_id, offset, buf):
        """Write an inode to cache"""
        try:
            logger.debug("Write inode:{} to cache at offset:{},length:{}".format(inode_id, offset, len(buf)))
            response = self._client.setrange(self._cache_key(inode_id, object_block_id), offset, buf)
            return response
        except Exception as e:
            logger.error("Failed to write inode:{} to cache at offset:{},length:{}".format(inode_id, offset, len(buf)), exc_info=True)
            raise e
    
    def read_inode(self, inode_id, object_block_id, offset, size):
        """Read an inode from cache"""
        try:
            logger.debug("Read inode:{} from cache with offset:{},size:{}".format(inode_id, offset, size))
            response = self._client.getrange(self._cache_key(inode_id, object_block_id), offset, size)
            return response
        except Exception as e:
            logger.error("Failed to read inode:{} from cache with offset:{},size:{}".format(inode_id, offset, size), exc_info=True)
            raise e
    
    def put_inode(self, inode_id, object_block_id, data):
        """Put an inode inside cache"""
        try:
            logger.debug("Put inode:{} into cache".format(inode_id))
            response = self._client.set(self._cache_key(inode_id, object_block_id), data)
            return response
        except Exception as e:
            logger.error("Failed to put inode:{} into cache".format(inode_id), exc_info=True)
            raise e
    
    def get_inode(self, inode_id, object_block_id):
        """Get an inode from the cache"""
        try:
            logger.debug("Get inode:{} from cache".format(inode_id))
            response = self._client.get(self._cache_key(inode_id, object_block_id))
            return response
        except Exception as e:
            logger.error("Failed to get inode:{} from cache".format(inode_id), exc_info=True)
            raise e

    def remove_inode(self, inode_id, object_block_id):
        """Delete the inode from cache"""
        try:
            logger.debug("Remove inode:{} from cache".format(inode_id))
            response = self._client.delete(self._cache_key(inode_id, object_block_id))
            return response
        except Exception as e:
            logger.error("Failed to remove inode:{} from cache".format(inode_id), exc_info=True)
            raise e
     
    def exists_inode(self, inode_id, object_block_id):
        """Check if inode exists"""
        try:
            logger.debug("Check if inode:{} exists".format(inode_id))
            response = self._client.exists(self._cache_key(inode_id, object_block_id))
            return response
        except Exception as e:
            logger.error("Failed to check if inode:{} exists".format(inode_id), exc_info=True)
            raise e
