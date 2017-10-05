from __future__ import print_function, absolute_import
from abc import abstractmethod
import redis
import llfuse
from objectfs.core.common.redispool import RedisPool
from objectfs.core.metadata.inode import Inode
from objectfs.settings import Settings
settings = Settings()
import logging
logger = logging.getLogger(__name__)

FS_DELIMITER = '%'
NAME_DELIMITER = '#'
LIST_DELMITER = '&'

class MetaStore(object):
    
    @staticmethod
    def load(fs_name, memory_store='Redis'):
        return MetaStore.get_store(memory_store)(fs_name)
    
    @staticmethod
    def get_store(memory_store='Redis'):
        if memory_store == 'Redis':
            return RedisMetaStore
        else:
            logger.error("Meta store in {} is not supported".format(memory_store))
            raise e

    @abstractmethod
    def get_inode(self, inode_id):
        """Get an inode using an inode id
           Return inode"""
        return NotImplemented

    @abstractmethod
    def put_inode(self, inode):
        """Put an inode"""
        return NotImplemented
    
    @abstractmethod
    def build_index(self, parent_inode_id, inode_id, file_name):
        """Build an index from file_name to inode_id"""
        return NotImplemented
    
    @abstractmethod
    def get_inode_id(self, parent_inode_id, file_name):
        """Get an inode id based on parent inode id and file_name"""
        return NotImplemented
    
    @abstractmethod
    def delete_inode(self, inode_id):
        """Delete an inode based on inode id"""
        return NotImplemented

class RedisMetaStore(MetaStore):

    def __init__(self, fs_name):
        try:
            # logger.debug("Init file-system {}".format(fs_name))
            # super(RedisMetaStore, self).__init__()
            self._fs_name = fs_name
            # self._client = redis.StrictRedis(connection_pool=RedisPool.blocking_pool)
            self._client = redis.StrictRedis(host=settings.REDIS_HOST, port=6379, db=0)
            self._pipe = self._client.pipeline(transaction=False)
        except redis.ConnectionError as e:
            logger.error("Cannot connect to Redis server", exc_info=True)
            raise e
    
    def _clean_store(self):
        """Clean store"""
        try:
            file_system_keys = self._client.keys('{}{}*'.format(self._fs_name, FS_DELIMITER))
            logger.debug("Delete all keys for file-system {}".format(self._fs_name))
            response = self._client.delete(*file_system_keys)
            # response = self._client.flushdb()
        except Exception as e:
            logger.error("Failed to delete all keys for file-system {}".format(self._fs_name), exc_info=True)
            raise e
    
    def set_superblock_key(self, key, value):
        """Set the superblock key"""
        try:
            logger.debug("Set superblock key:{} to value:{}".format(key, value))
            response = self._client.set(key, value)
            return response
        except Exception as e:
            logger.error("Failed to set superblock key:{} to value:{}".format(key, value), exc_info=True)
            raise e
    
    def get_superblock_key(self, key):
        """Get the superblock key"""
        try:
            logger.debug("Get superblock key:{}".format(key))
            response = self._client.get(key)
            if response:
                return int(response)
            else:
                return None
        except Exception as e:
            logger.error("Failed to get superblock key:{}".format(key), exc_info=True)
            raise e
    
    def incr_superblock_key(self, key, value):
        """Increment the superblock key by value"""
        try:
            logger.debug("Increment superblock key:{} by value:{}".format(key, value))
            response = self._client.incrby(key, value)
            if response:
                return int(response)
            else:
                return None
        except Exception as e:
            logger.error("Failed to increment superblock key:{} by value:{}".format(key, value), exc_info=True)
            raise e
    
    def decr_superblock_key(self, key, value):
        """Decrement the superblock key by value"""
        try:
            logger.debug("Decrement superblock key:{} by value:{}".format(key, value))
            response = self._client.decr(key, value)
            if response:
                return int(response)
            else:
                return None
        except Exception as e:
            logger.debug("Failed to decrement superblock key:{} by value:{}".format(key, value), exc_info=True)
            raise e
    
    def delete_superblock_key(self, keys):
        """Delete the superblock keys"""
        try:
            logger.debug("Delete superblock key:{}".format(keys))
            response = self._client.delete(*keys)
            return response
        except Exception as e:
            logger.error("Failed to delete superblock key:{}".format(keys), exc_info=True)
            raise e
    
    def exists(self, key):
        """Check if keys exist or not"""
        try:
            logger.debug("Check if key:{} exists".format(key))
            response = self._client.exists(key)
            return response
        except Exception as e:
            logger.error("Failed to check if key:{} exists".format(key), exc_info=True)
            raise e
    
    def _wrap_fs_delimiter(self, key):
        """Prepend the key with file-system name"""
        return '{}{}{}'.format(self._fs_name, FS_DELIMITER, key)

    def get_inode(self, inode_id):
        """Get an inode using an inode id
           Return inode"""
        try:
            logger.debug("Get inode {} for file-system {}".format(inode_id, self._fs_name))
            data = self._client.get(self._wrap_fs_delimiter(inode_id))
            if data is None:
                return data
            else:
                return Inode.from_string(data)
        except Exception as e:
            logger.error("Falied to get inode {} for file-system {}".format(inode_id, self._fs_name), exc_info=True)
            raise e

    def put_inode(self, inode):
        """Put an inode"""
        try:
            logger.debug("Put inode {} for file-system {}".format(inode.id, self._fs_name))
            response = self._client.set(self._wrap_fs_delimiter(inode.id), Inode.to_string(inode))
            self.build_index(inode.parent_inode_id, inode.id, inode.name)
            return response
        except Exception as e:
            logger.error("Failed to put inode {} for file-system {}".format(inode.id, self._fs_name), exc_info=True)
            raise e
    
    def update_inode(self, inode):
        """Update an inode"""
        try:
            logger.debug("Update inode {} for file-system {}".format(inode.id, self._fs_name))
            response = self._client.set(self._wrap_fs_delimiter(inode.id), Inode.to_string(inode))
            return response
        except Exception as e:
            logger.error("Failed to update inode {} for file-system {}".format(inode.id, self._fs_name), exc_info=True)
            raise e

    def get_inode_id(self, parent_inode_id, file_name):
        """Get an inode id based on parent inode id and file_name"""
        try:
            logger.debug("Get inode for parent:{}, name:{}".format(parent_inode_id, file_name))
            response = self._client.get(self._wrap_fs_delimiter(self._inode_reverse_key(parent_inode_id, file_name)))
            if response:
                return int(response)
            else:
                return None
        except Exception as e:
            logger.error("Failed to get inode for parent:{}, name:{}".format(parent_inode_id, file_name), exc_info=True)
            raise e

    def delete_inode(self, inode_id):
        """Delete an inode based on inode id"""
        try:
            logger.debug("Delete inode:{}".format(inode_id))
            inode = self.get_inode(inode_id)
            response = self._client.delete(self._wrap_fs_delimiter(inode_id))
            self.clean_index(inode.parent_inode_id, inode.name)
            self.delete_inode_id_list(inode_id)
            return response
        except Exception as e:
            logger.error("Failed to delete inode:{}".format(inode_id), exc_info=True)
            raise e

    def build_index(self, parent_inode_id, inode_id, file_name):
        """Build an index from file_name to inode_id"""
        try:
            logger.debug("Build index for parent inode:{}, inode:{}, file_name:{}".format(parent_inode_id, inode_id, file_name))
            response = self._client.set(self._wrap_fs_delimiter(self._inode_reverse_key(parent_inode_id, file_name)), inode_id)
            return response
        except Exception as e:
            logger.error("Failed to build index for parent inode:{}, inode:{}, file_name:{}".format(parent_inode_id, inode_id, file_name), exc_info=True)
            raise e
    
    def clean_index(self, parent_inode_id, file_name):
        """Clean an index based on parent node id and file name"""
        try:
            logger.debug("Clean index for parent inode:{}, file-name:{}".format(parent_inode_id, file_name))
            response = self._client.delete(self._wrap_fs_delimiter(self._inode_reverse_key(parent_inode_id, file_name)))
            return response
        except Exception as e:
            logger.error("Failed to clean index for parent inode:{}, file-name:{}".format(parent_inode_id, file_name), exc_info=True)
            raise e
    
    def _inode_list_key(self, inode_id):
        return '{}{}{}{}{}'.format(self._fs_name, FS_DELIMITER, inode_id, LIST_DELMITER, 'list')
    
    def _inode_reverse_key(self, parent_inode_id, file_name):
        return '{}{}{}'.format(parent_inode_id, NAME_DELIMITER, file_name)

    def get_inode_id_list(self, inode_id, offset=0):
        """Get inode id list for inode"""
        try:
            logger.debug("Get inode:{} from list at offset:{}".format(inode_id, offset))
            response = self._client.lrange(self._inode_list_key(inode_id), offset, -1)
            for inode_string in response:
                (inode_id, file_name) = inode_string.split(NAME_DELIMITER)
                yield(int(inode_id), file_name)
        except Exception as e:
            logger.error("Error in fetching inode id list for inode {}".format(inode_id), exc_info=True)
            raise e

    def add_inode_id_to_list(self, inode_id, new_id, new_name):
        """Add new id to inode id list"""
        try:
            logger.debug("Add new:{}, name:{} to list for inode:{} list".format(new_id, new_name, inode_id))
            # this has to be appended to the list not prepended
            response = self._client.rpush(self._inode_list_key(inode_id), self._inode_reverse_key(new_id, new_name))
            return response
        except Exception as e:
            logger.error("Failed to add new:{}, name:{} to list for inode:{} list".format(new_id, new_name, inode_id), exc_info=True)
            return e

    def remove_inode_id_from_list(self, inode_id, existing_id, existing_name):
        """Remove existing id from inode list"""
        try:
            logger.debug("Remove id:{},name:{} from inode:{} list".format(existing_id, existing_name, inode_id))
            response = self._client.lrem(self._inode_list_key(inode_id), 1, self._inode_reverse_key(existing_id, existing_name))
            return response
        except Exception as e:
            logger.error("Failed to remove inode:{},name:{} from inode:{} list".format(existing_id, existing_name, inode_id), exc_info=True)
            raise e

    def delete_inode_id_list(self, inode_id):
        """Delete inode id list"""
        try:
            logger.debug("Delete inode:{} list".format(inode_id))
            self._client.delete(self._inode_list_key(inode_id))
        except Exception as e:
            logger.error("Failed to Delete inode list for inode:{}".format(inode_id), exc_info=True)
            raise e

    def length_inode_id_list(self, inode_id):
        """Get the length of the inode id list"""
        try:
            logger.debug("Get length of inode:{} list".format(inode_id))
            response = self._client.llen(self._inode_list_key(inode_id))
            if response:
                return int(response)
            else:
                return 0
        except Exception as e:
            logger.error("Get length of inode:{} list".format(inode_id), exc_info=True)
            raise e
